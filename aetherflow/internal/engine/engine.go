package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aetherflow/aetherflow/internal/store"
	"github.com/aetherflow/aetherflow/internal/telemetry"
	"github.com/aetherflow/aetherflow/internal/worker"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

type TimerScheduler interface {
	Schedule(ctx context.Context, timer store.Timer) error
	Clear(ctx context.Context, instanceID string) error
}

type Engine struct {
	repo        store.Repository
	redis       *redis.Client
	stream      string
	group       string
	consumer    string
	executor    *worker.Executor
	timers      TimerScheduler
	metrics     *telemetry.Metrics
	logger      *slog.Logger
	concurrency int
	lease       time.Duration
	pendingIdle time.Duration
	claimSeq    atomic.Uint64
}

type Config struct {
	Stream      string
	Group       string
	Consumer    string
	Concurrency int
	Lease       time.Duration
	PendingIdle time.Duration
}

func New(repo store.Repository, redisClient *redis.Client, executor *worker.Executor, timers TimerScheduler, metrics *telemetry.Metrics, logger *slog.Logger, config Config) *Engine {
	if config.Stream == "" {
		config.Stream = "aetherflow:tasks"
	}
	if config.Group == "" {
		config.Group = "aetherflow-workers"
	}
	if config.Consumer == "" {
		host, _ := os.Hostname()
		config.Consumer = host + "-" + strconv.Itoa(os.Getpid())
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 4
	}
	if config.Lease <= 0 {
		config.Lease = 2 * time.Minute
	}
	if config.PendingIdle <= 0 {
		config.PendingIdle = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		repo:        repo,
		redis:       redisClient,
		stream:      config.Stream,
		group:       config.Group,
		consumer:    config.Consumer,
		executor:    executor,
		timers:      timers,
		metrics:     metrics,
		logger:      logger,
		concurrency: config.Concurrency,
		lease:       config.Lease,
		pendingIdle: config.PendingIdle,
	}
}

func (e *Engine) Run(ctx context.Context) error {
	if err := e.ensureGroup(ctx); err != nil {
		return fmt.Errorf("ensure redis stream group: %w", err)
	}
	if err := e.Recover(ctx); err != nil {
		e.logger.Error("initial recovery failed", "error", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < e.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			e.consume(ctx, workerID)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.recoveryLoop(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (e *Engine) Enqueue(ctx context.Context, instanceID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := e.redis.XAdd(queryCtx, &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"instance_id": instanceID},
	}).Err(); err != nil {
		return fmt.Errorf("enqueue instance: %w", err)
	}
	return nil
}

func (e *Engine) Recover(ctx context.Context) error {
	instances, err := e.repo.ListRecoverableInstances(ctx, 500)
	if err != nil {
		return fmt.Errorf("list recoverable instances: %w", err)
	}
	for _, instance := range instances {
		if err := e.Enqueue(ctx, instance.ID); err != nil {
			return fmt.Errorf("enqueue recoverable instance %s: %w", instance.ID, err)
		}
	}
	return nil
}

func (e *Engine) Advance(ctx context.Context, instanceID string) error {
	owner := e.nextClaimOwner()
	instance, err := e.repo.ClaimInstance(ctx, instanceID, owner, time.Now().UTC().Add(e.lease))
	if err != nil {
		return fmt.Errorf("claim instance for advance: %w", err)
	}
	defer func() {
		if err := e.repo.ReleaseInstance(context.Background(), instanceID, owner); err != nil {
			e.logger.Error("release workflow instance lease", "instance_id", instanceID, "owner", owner, "error", err)
		}
	}()

	if handled, err := e.ensureWaitingTimer(ctx, instance); handled || err != nil {
		return err
	}

	definition, err := e.repo.GetDefinition(ctx, instance.DefinitionID)
	if err != nil {
		return fmt.Errorf("get definition for advance: %w", err)
	}
	machine := NewMachine(definition.DSL)

	if instance.Status == workflow.InstanceCompensating || len(instance.State.CompensationQueue) > 0 {
		return e.advanceCompensation(ctx, instance, definition, machine)
	}

	step, ok := machine.StepForCurrent(instance.State, instance.CurrentStepID)
	if !ok {
		return e.finishInstance(ctx, instance, workflow.InstanceCompleted, nil)
	}
	return e.executeNormalStep(ctx, instance, definition, machine, step)
}

func (e *Engine) ensureWaitingTimer(ctx context.Context, instance *store.Instance) (bool, error) {
	if instance.Status != workflow.InstanceWaitingTimer || instance.CurrentStepID == nil {
		return false, nil
	}
	state := instance.State.Steps[*instance.CurrentStepID]
	if state.WaitingTime == nil || !state.WaitingTime.After(time.Now().UTC()) {
		return false, nil
	}
	if e.timers == nil {
		return true, fmt.Errorf("instance %s is waiting on timer but no timer scheduler is configured", instance.ID)
	}
	if err := e.timers.Schedule(ctx, store.Timer{InstanceID: instance.ID, StepID: *instance.CurrentStepID, FireAt: *state.WaitingTime}); err != nil {
		return true, fmt.Errorf("reschedule waiting timer: %w", err)
	}
	e.logger.Info("workflow timer rescheduled", "instance_id", instance.ID, "step_id", *instance.CurrentStepID, "fire_at", *state.WaitingTime)
	return true, nil
}

func (e *Engine) executeNormalStep(ctx context.Context, instance *store.Instance, definition *store.Definition, machine Machine, step workflow.Step) error {
	attempt := instance.State.Steps[step.ID].Attempt + 1
	previousStepState := instance.State.Steps[step.ID]
	if err := e.markStepRunning(ctx, instance, step.ID, attempt, workflow.InstanceRunning); err != nil {
		return fmt.Errorf("mark step running: %w", err)
	}

	history, err := e.repo.AppendHistory(ctx, &store.History{
		InstanceID: instance.ID,
		StepID:     step.ID,
		Status:     workflow.StepRunning,
		Input:      worker.BuildEnv(instance.Input, instance.State),
		Attempt:    attempt,
		StartedAt:  time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("append step history: %w", err)
	}

	start := time.Now()
	result, execErr := e.executeWithSpan(ctx, instance, definition, step, previousStepState)
	duration := time.Since(start)
	if execErr == nil {
		e.metrics.ObserveStep(step.Type, "success", duration)
		return e.handleStepSuccess(ctx, instance, machine, step, history.ID, attempt, result)
	}

	if waiting, ok := worker.IsWaitingTimer(execErr); ok {
		e.metrics.ObserveStep(step.Type, workflow.InstanceWaitingTimer, duration)
		return e.handleStepWaiting(ctx, instance, step, history.ID, attempt, result, waiting.FireAt)
	}

	e.metrics.ObserveStep(step.Type, "error", duration)
	return e.handleStepFailure(ctx, instance, machine, step, history.ID, attempt, result, execErr)
}

func (e *Engine) advanceCompensation(ctx context.Context, instance *store.Instance, definition *store.Definition, machine Machine) error {
	instance.State.Normalize()
	if len(instance.State.CompensationQueue) == 0 {
		return e.finishInstance(ctx, instance, workflow.InstanceFailed, nil)
	}
	stepID := instance.State.CompensationQueue[0]
	step, ok := machine.StepByID(stepID)
	if !ok {
		return e.finishInstance(ctx, instance, workflow.InstanceFailed, fmt.Errorf("compensation step %q not found", stepID))
	}

	attempt := instance.State.Steps[step.ID].Attempt + 1
	if err := e.markStepRunning(ctx, instance, step.ID, attempt, workflow.InstanceCompensating); err != nil {
		return fmt.Errorf("mark compensation step running: %w", err)
	}
	history, err := e.repo.AppendHistory(ctx, &store.History{
		InstanceID: instance.ID,
		StepID:     step.ID,
		Status:     workflow.StepRunning,
		Input:      worker.BuildEnv(instance.Input, instance.State),
		Attempt:    attempt,
		StartedAt:  time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("append compensation history: %w", err)
	}

	start := time.Now()
	result, execErr := e.executeWithSpan(ctx, instance, definition, step, workflow.StepState{})
	duration := time.Since(start)
	if execErr == nil {
		e.metrics.ObserveStep(step.Type, "success", duration)
		output := map[string]any{}
		var body any
		if result != nil {
			output = result.Output
			body = result.Body
		}
		if err := e.repo.CompleteHistory(ctx, history.ID, "compensated", output, nil); err != nil {
			return fmt.Errorf("complete compensation history: %w", err)
		}
		instance.State.MarkCompleted(step.ID, workflow.StepState{Status: workflow.StepCompleted, Output: output, Body: body, Attempt: attempt})
		instance.State.CompensationQueue = instance.State.CompensationQueue[1:]
		if len(instance.State.CompensationQueue) == 0 {
			return e.finishInstance(ctx, instance, workflow.InstanceFailed, nil)
		}
		next := instance.State.CompensationQueue[0]
		instance.CurrentStepID = &next
		instance.Status = workflow.InstanceCompensating
		expected := instance.Version
		if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
			return fmt.Errorf("update compensation progress: %w", err)
		}
		return e.Enqueue(ctx, instance.ID)
	}

	if waiting, ok := worker.IsWaitingTimer(execErr); ok {
		e.metrics.ObserveStep(step.Type, workflow.InstanceWaitingTimer, duration)
		return e.handleStepWaiting(ctx, instance, step, history.ID, attempt, result, waiting.FireAt)
	}
	e.metrics.ObserveStep(step.Type, "error", duration)
	errText := execErr.Error()
	output := map[string]any{}
	if result != nil {
		output = result.Output
	}
	if err := e.repo.CompleteHistory(ctx, history.ID, workflow.StepFailed, output, &errText); err != nil {
		return fmt.Errorf("complete failed compensation history: %w", err)
	}
	return e.finishInstance(ctx, instance, workflow.InstanceFailed, execErr)
}

func (e *Engine) markStepRunning(ctx context.Context, instance *store.Instance, stepID string, attempt int, status string) error {
	instance.State.Normalize()
	instance.Status = status
	instance.CurrentStepID = &stepID
	instance.State.Steps[stepID] = workflow.StepState{Status: workflow.StepRunning, Attempt: attempt}
	expected := instance.Version
	if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
		return fmt.Errorf("update running state: %w", err)
	}
	return nil
}

func (e *Engine) executeWithSpan(ctx context.Context, instance *store.Instance, definition *store.Definition, step workflow.Step, previousStepState workflow.StepState) (*worker.Result, error) {
	tracer := otel.Tracer("aetherflow/internal/engine")
	spanCtx, span := tracer.Start(ctx, "workflow.step")
	defer span.End()
	span.SetAttributes(
		attribute.String("workflow.instance_id", instance.ID),
		attribute.String("workflow.definition_id", definition.ID),
		attribute.Int("workflow.definition_version", definition.Version),
		attribute.String("workflow.step_id", step.ID),
		attribute.String("workflow.step_type", step.Type),
	)
	if step.Type == workflow.StepDelay && previousStepState.Status == workflow.StepWaitingTimer {
		waitingTime := previousStepState.WaitingTime
		if waitingTime == nil || !waitingTime.After(time.Now().UTC()) {
			return &worker.Result{
				Output: map[string]any{"fired_at": time.Now().UTC().Format(time.RFC3339Nano)},
				Body:   map[string]any{"fired": true},
			}, nil
		}
		return &worker.Result{
			Output:     map[string]any{"fire_at": waitingTime.Format(time.RFC3339Nano)},
			Body:       map[string]any{"waiting": true},
			DelayUntil: waitingTime,
		}, worker.WaitingTimerError{FireAt: *waitingTime}
	}
	return e.executor.Execute(spanCtx, instance, step)
}

func (e *Engine) handleStepSuccess(ctx context.Context, instance *store.Instance, machine Machine, step workflow.Step, historyID string, attempt int, result *worker.Result) error {
	output := map[string]any{}
	var body any
	if result != nil {
		output = result.Output
		body = result.Body
	}
	if err := e.repo.CompleteHistory(ctx, historyID, "success", output, nil); err != nil {
		return fmt.Errorf("complete successful history: %w", err)
	}
	instance.State.MarkCompleted(step.ID, workflow.StepState{Status: workflow.StepCompleted, Output: output, Body: body, Attempt: attempt})
	if e.timers != nil {
		if err := e.timers.Clear(ctx, instance.ID); err != nil {
			return fmt.Errorf("clear durable timer: %w", err)
		}
	}

	next, ok := machine.NextAfter(step, instance.State, result)
	expected := instance.Version
	if !ok {
		instance.Status = workflow.InstanceCompleted
		instance.CurrentStepID = nil
		if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
			return fmt.Errorf("update completed instance: %w", err)
		}
		e.metrics.IncInstance(workflow.InstanceCompleted)
		if !instance.State.StartedAt.IsZero() {
			e.metrics.ObserveInstance(time.Since(instance.State.StartedAt))
		}
		e.logger.Info("workflow instance completed", "instance_id", instance.ID, "step_id", step.ID)
		return nil
	}

	instance.Status = workflow.InstanceRunning
	instance.CurrentStepID = &next.ID
	if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
		return fmt.Errorf("update next step: %w", err)
	}
	e.logger.Info("workflow step completed", "instance_id", instance.ID, "step_id", step.ID, "next_step_id", next.ID)
	return e.Enqueue(ctx, instance.ID)
}

func (e *Engine) handleStepWaiting(ctx context.Context, instance *store.Instance, step workflow.Step, historyID string, attempt int, result *worker.Result, fireAt time.Time) error {
	output := map[string]any{"fire_at": fireAt.Format(time.RFC3339Nano)}
	var body any
	if result != nil && result.Output != nil {
		output = result.Output
		body = result.Body
	}
	if err := e.repo.CompleteHistory(ctx, historyID, workflow.StepWaitingTimer, output, nil); err != nil {
		return fmt.Errorf("complete waiting history: %w", err)
	}
	instance.Status = workflow.InstanceWaitingTimer
	instance.State.Steps[step.ID] = workflow.StepState{
		Status:      workflow.StepWaitingTimer,
		Output:      output,
		Body:        body,
		Attempt:     attempt,
		WaitingTime: &fireAt,
	}
	instance.CurrentStepID = &step.ID
	expected := instance.Version
	if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
		return fmt.Errorf("update waiting instance: %w", err)
	}
	if e.timers != nil {
		if err := e.timers.Schedule(ctx, store.Timer{InstanceID: instance.ID, StepID: step.ID, FireAt: fireAt}); err != nil {
			return fmt.Errorf("schedule timer: %w", err)
		}
	}
	e.logger.Info("workflow step waiting on timer", "instance_id", instance.ID, "step_id", step.ID, "fire_at", fireAt)
	return nil
}

func (e *Engine) handleStepFailure(ctx context.Context, instance *store.Instance, machine Machine, step workflow.Step, historyID string, attempt int, result *worker.Result, execErr error) error {
	output := map[string]any{}
	var body any
	if result != nil {
		output = result.Output
		body = result.Body
	}
	errText := execErr.Error()
	if err := e.repo.CompleteHistory(ctx, historyID, workflow.StepFailed, output, &errText); err != nil {
		return fmt.Errorf("complete failed history: %w", err)
	}
	instance.State.Steps[step.ID] = workflow.StepState{
		Status:  workflow.StepFailed,
		Output:  output,
		Body:    body,
		Error:   errText,
		Attempt: attempt,
	}

	if step.Retry != nil && attempt <= step.Retry.MaxRetries {
		fireAt := time.Now().UTC().Add(step.Retry.Backoff(attempt))
		instance.Status = workflow.InstanceWaitingTimer
		instance.CurrentStepID = &step.ID
		instance.State.Steps[step.ID] = workflow.StepState{
			Status:      workflow.StepWaitingTimer,
			Output:      map[string]any{"fire_at": fireAt.Format(time.RFC3339Nano)},
			Body:        body,
			Error:       errText,
			Attempt:     attempt,
			WaitingTime: &fireAt,
		}
		expected := instance.Version
		if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
			return fmt.Errorf("update retry wait state: %w", err)
		}
		if e.timers != nil {
			if err := e.timers.Schedule(ctx, store.Timer{InstanceID: instance.ID, StepID: step.ID, FireAt: fireAt}); err != nil {
				return fmt.Errorf("schedule retry timer: %w", err)
			}
		}
		e.logger.Warn("workflow step scheduled for retry", "instance_id", instance.ID, "step_id", step.ID, "attempt", attempt, "error", execErr)
		return nil
	}

	queue := machine.CompensationQueue(step.ID, instance.State)
	if len(queue) > 0 {
		instance.State.CompensationQueue = queue
		instance.Status = workflow.InstanceCompensating
		next := queue[0]
		instance.CurrentStepID = &next
		expected := instance.Version
		if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
			return fmt.Errorf("update compensation state: %w", err)
		}
		e.logger.Warn("workflow instance entering compensation", "instance_id", instance.ID, "step_id", step.ID, "error", execErr)
		return e.Enqueue(ctx, instance.ID)
	}
	return e.finishInstance(ctx, instance, workflow.InstanceFailed, execErr)
}

func (e *Engine) finishInstance(ctx context.Context, instance *store.Instance, status string, cause error) error {
	instance.Status = status
	instance.CurrentStepID = nil
	expected := instance.Version
	if err := e.repo.UpdateInstance(ctx, instance, expected); err != nil {
		return fmt.Errorf("finish instance: %w", err)
	}
	e.metrics.IncInstance(status)
	if !instance.State.StartedAt.IsZero() {
		e.metrics.ObserveInstance(time.Since(instance.State.StartedAt))
	}
	if cause != nil {
		e.logger.Error("workflow instance finished with error", "instance_id", instance.ID, "status", status, "error", cause)
		return nil
	}
	e.logger.Info("workflow instance finished", "instance_id", instance.ID, "status", status)
	return nil
}

func (e *Engine) consume(ctx context.Context, workerID int) {
	consumerName := e.consumer + "-" + strconv.Itoa(workerID)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		e.reclaimPending(ctx, consumerName)
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		streams, err := e.redis.XReadGroup(readCtx, &redis.XReadGroupArgs{
			Group:    e.group,
			Consumer: consumerName,
			Streams:  []string{e.stream, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()
		cancel()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			e.logger.Error("read redis stream", "error", err)
			continue
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				e.processMessage(ctx, message)
			}
		}
	}
}

func (e *Engine) reclaimPending(ctx context.Context, consumerName string) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	messages, _, err := e.redis.XAutoClaim(queryCtx, &redis.XAutoClaimArgs{
		Stream:   e.stream,
		Group:    e.group,
		Consumer: consumerName,
		MinIdle:  e.pendingIdle,
		Start:    "0-0",
		Count:    10,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		e.logger.Error("reclaim redis pending messages", "consumer", consumerName, "error", err)
		return
	}
	for _, message := range messages {
		e.processMessage(ctx, message)
	}
}

func (e *Engine) processMessage(ctx context.Context, message redis.XMessage) {
	instanceID := fmt.Sprint(message.Values["instance_id"])
	if instanceID == "" || instanceID == "<nil>" {
		e.logger.Error("redis task missing instance_id", "message_id", message.ID)
		_ = e.ack(ctx, message.ID)
		return
	}
	if err := e.Advance(ctx, instanceID); err != nil {
		if errors.Is(err, store.ErrInstanceBusy) {
			if ackErr := e.ack(ctx, message.ID); ackErr != nil {
				e.logger.Error("ack duplicate redis task", "message_id", message.ID, "error", ackErr)
			}
			return
		}
		e.logger.Error("advance workflow instance", "instance_id", instanceID, "message_id", message.ID, "error", err)
		return
	}
	if err := e.ack(ctx, message.ID); err != nil {
		e.logger.Error("ack redis task", "message_id", message.ID, "error", err)
	}
}

func (e *Engine) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.Recover(ctx); err != nil {
				e.logger.Error("recover workflow instances", "error", err)
			}
		}
	}
}

func (e *Engine) ensureGroup(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := e.redis.XGroupCreateMkStream(queryCtx, e.stream, e.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create redis stream group: %w", err)
	}
	return nil
}

func (e *Engine) ack(ctx context.Context, messageID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := e.redis.XAck(queryCtx, e.stream, e.group, messageID).Err(); err != nil {
		return fmt.Errorf("ack redis stream message: %w", err)
	}
	return nil
}

func (e *Engine) nextClaimOwner() string {
	return e.consumer + "-claim-" + strconv.FormatUint(e.claimSeq.Add(1), 10)
}

func isTerminal(status string) bool {
	return status == workflow.InstanceCompleted || status == workflow.InstanceFailed
}
