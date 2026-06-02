package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/expr-lang/expr"

	"github.com/aetherflow/aetherflow/internal/security"
	"github.com/aetherflow/aetherflow/internal/store"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

const (
	defaultHTTPTimeout      = 15 * time.Second
	defaultMaxRequestBytes  = 1 << 20
	defaultMaxResponseBytes = 2 << 20
)

type Executor struct {
	httpClient           *http.Client
	allowPrivateNetworks bool
	maxRequestBytes      int64
	maxResponseBytes     int64
}

type ExecutorOptions struct {
	HTTPClient           *http.Client
	AllowPrivateNetworks bool
	MaxRequestBytes      int64
	MaxResponseBytes     int64
}

type Result struct {
	Output     map[string]any
	Body       any
	NextStepID *string
	DelayUntil *time.Time
}

type WaitingTimerError struct {
	FireAt time.Time
}

func (e WaitingTimerError) Error() string {
	return "waiting for durable timer"
}

func NewExecutor(httpClient *http.Client) *Executor {
	return NewExecutorWithOptions(ExecutorOptions{HTTPClient: httpClient})
}

func NewExecutorWithOptions(options ExecutorOptions) *Executor {
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	configuredClient := *httpClient
	httpClient = &configuredClient
	if httpClient.Timeout <= 0 {
		httpClient.Timeout = defaultHTTPTimeout
	}
	previousRedirectPolicy := httpClient.CheckRedirect
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if previousRedirectPolicy != nil {
			if err := previousRedirectPolicy(req, via); err != nil {
				return err
			}
		}
		if len(via) >= 5 {
			return fmt.Errorf("too many http_request redirects")
		}
		if err := security.EnsureHTTPDestinationAllowed(req.Context(), req.URL.String(), options.AllowPrivateNetworks); err != nil {
			return fmt.Errorf("validate http_request redirect destination: %w", err)
		}
		return nil
	}
	if options.MaxRequestBytes <= 0 {
		options.MaxRequestBytes = defaultMaxRequestBytes
	}
	if options.MaxResponseBytes <= 0 {
		options.MaxResponseBytes = defaultMaxResponseBytes
	}
	return &Executor{
		httpClient:           httpClient,
		allowPrivateNetworks: options.AllowPrivateNetworks,
		maxRequestBytes:      options.MaxRequestBytes,
		maxResponseBytes:     options.MaxResponseBytes,
	}
}

func (e *Executor) Execute(ctx context.Context, instance *store.Instance, step workflow.Step) (*Result, error) {
	env := BuildEnv(instance.Input, instance.State)
	switch step.Type {
	case workflow.StepHTTPRequest:
		return e.executeHTTPRequest(ctx, step, env)
	case workflow.StepTransform:
		return e.executeTransform(ctx, step, env)
	case workflow.StepDelay:
		return e.executeDelay(ctx, instance, step)
	case workflow.StepCondition:
		return e.executeCondition(ctx, step, env)
	case workflow.StepFork:
		return e.executeFork(ctx, step)
	case workflow.StepJoin:
		return e.executeJoin(ctx, step)
	default:
		return nil, fmt.Errorf("execute step %q: unsupported step type %q", step.ID, step.Type)
	}
}

func (e *Executor) executeHTTPRequest(ctx context.Context, step workflow.Step, env map[string]any) (*Result, error) {
	rendered, err := renderValue(step.Config, env)
	if err != nil {
		return nil, fmt.Errorf("render http_request config: %w", err)
	}
	config, ok := rendered.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("render http_request config: expected object")
	}

	rawURL, _ := config["url"].(string)
	method, _ := config["method"].(string)
	if rawURL == "" || method == "" {
		return nil, fmt.Errorf("execute http_request: url and method are required")
	}
	if err := security.EnsureHTTPDestinationAllowed(ctx, rawURL, e.allowPrivateNetworks); err != nil {
		return nil, fmt.Errorf("validate http_request destination: %w", err)
	}
	method = strings.ToUpper(method)

	var bodyReader io.Reader
	if body, ok := config["body"]; ok && body != nil {
		bodyRaw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal http_request body: %w", err)
		}
		if int64(len(bodyRaw)) > e.maxRequestBytes {
			return nil, fmt.Errorf("http_request body exceeds %d bytes", e.maxRequestBytes)
		}
		bodyReader = bytes.NewReader(bodyRaw)
	}

	timeout := e.httpClient.Timeout
	if configuredTimeout, ok := firstString(config, "timeout"); ok && configuredTimeout != "" {
		parsed, err := time.ParseDuration(configuredTimeout)
		if err != nil {
			return nil, fmt.Errorf("parse http_request timeout: %w", err)
		}
		if parsed <= 0 || parsed > e.httpClient.Timeout {
			return nil, fmt.Errorf("http_request timeout must be between 1ns and %s", e.httpClient.Timeout)
		}
		timeout = parsed
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, method, rawURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create http_request: %w", err)
	}
	if bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if headers, ok := config["headers"].(map[string]any); ok {
		for key, value := range headers {
			req.Header.Set(key, fmt.Sprint(value))
		}
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform http_request: %w", err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(io.LimitReader(resp.Body, e.maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read http_response body: %w", err)
	}
	if int64(len(rawBody)) > e.maxResponseBytes {
		return nil, fmt.Errorf("http_response body exceeds %d bytes", e.maxResponseBytes)
	}
	var parsedBody any
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &parsedBody); err != nil {
			parsedBody = string(rawBody)
		}
	}

	output := map[string]any{
		"status_code": resp.StatusCode,
		"headers":     flattenHeaders(resp.Header),
		"body":        parsedBody,
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &Result{Output: output, Body: parsedBody}, fmt.Errorf("http_request returned status %d", resp.StatusCode)
	}
	return &Result{Output: output, Body: parsedBody}, nil
}

func (e *Executor) executeTransform(ctx context.Context, step workflow.Step, env map[string]any) (*Result, error) {
	expression, _ := firstString(step.Config, "expr", "expression")
	if expression == "" {
		return nil, fmt.Errorf("execute transform: config.expr is required")
	}
	program, err := expr.Compile(expression, expr.Env(env), expr.AllowUndefinedVariables())
	if err != nil {
		return nil, fmt.Errorf("compile transform expression: %w", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	select {
	case <-runCtx.Done():
		return nil, fmt.Errorf("execute transform expression: %w", runCtx.Err())
	default:
	}
	value, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("run transform expression: %w", err)
	}
	resultKey, _ := firstString(step.Config, "result_key")
	if resultKey == "" {
		resultKey = "result"
	}
	output := map[string]any{resultKey: value}
	return &Result{Output: output, Body: value}, nil
}

func (e *Executor) executeDelay(ctx context.Context, instance *store.Instance, step workflow.Step) (*Result, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("execute delay context: %w", ctx.Err())
	default:
	}
	if current, ok := instance.State.Steps[step.ID]; ok && current.Status == workflow.StepWaitingTimer {
		if current.WaitingTime != nil && current.WaitingTime.After(time.Now().UTC()) {
			return &Result{
				Output:     map[string]any{"fire_at": current.WaitingTime.Format(time.RFC3339Nano)},
				Body:       map[string]any{"waiting": true},
				DelayUntil: current.WaitingTime,
			}, WaitingTimerError{FireAt: *current.WaitingTime}
		}
		return &Result{
			Output: map[string]any{"fired_at": time.Now().UTC().Format(time.RFC3339Nano)},
			Body:   map[string]any{"fired": true},
		}, nil
	}
	durationText, _ := firstString(step.Config, "duration", "for")
	duration, err := time.ParseDuration(durationText)
	if err != nil {
		return nil, fmt.Errorf("parse delay duration: %w", err)
	}
	fireAt := time.Now().UTC().Add(duration)
	return &Result{DelayUntil: &fireAt}, WaitingTimerError{FireAt: fireAt}
}

func (e *Executor) executeCondition(ctx context.Context, step workflow.Step, env map[string]any) (*Result, error) {
	condition, _ := firstString(step.Config, "if")
	if condition == "" {
		return nil, fmt.Errorf("execute condition: config.if is required")
	}
	program, err := expr.Compile(condition, expr.Env(env), expr.AllowUndefinedVariables())
	if err != nil {
		return nil, fmt.Errorf("compile condition expression: %w", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	select {
	case <-runCtx.Done():
		return nil, fmt.Errorf("execute condition expression: %w", runCtx.Err())
	default:
	}
	value, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("run condition expression: %w", err)
	}
	matched, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("condition expression must return bool")
	}
	var next *string
	if matched {
		if thenStep, ok := stringValue(step.Config, "then"); ok && thenStep != "" {
			next = &thenStep
		}
	} else {
		if elseStep, ok := stringValue(step.Config, "else"); ok && elseStep != "" {
			next = &elseStep
		}
	}
	output := map[string]any{"matched": matched}
	if next != nil {
		output["next_step"] = *next
	}
	return &Result{Output: output, Body: output, NextStepID: next}, nil
}

func (e *Executor) executeFork(ctx context.Context, step workflow.Step) (*Result, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("execute fork context: %w", ctx.Err())
	default:
	}
	branches, ok := stringSliceValue(step.Config, "branches")
	if !ok || len(branches) == 0 {
		return nil, fmt.Errorf("execute fork: config.branches is required")
	}
	next := branches[0]
	output := map[string]any{"branches": branches, "next_step": next}
	if join, ok := stringValue(step.Config, "join"); ok && join != "" {
		output["join_step"] = join
	}
	return &Result{Output: output, Body: output, NextStepID: &next}, nil
}

func (e *Executor) executeJoin(ctx context.Context, step workflow.Step) (*Result, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("execute join context: %w", ctx.Err())
	default:
	}
	output := map[string]any{"joined": true}
	if branches, ok := stringSliceValue(step.Config, "branches"); ok {
		output["branches"] = branches
	}
	if next, ok := stringValue(step.Config, "next"); ok && next != "" {
		output["next_step"] = next
		return &Result{Output: output, Body: output, NextStepID: &next}, nil
	}
	return &Result{Output: output, Body: output}, nil
}

func BuildEnv(input map[string]any, state workflow.RuntimeState) map[string]any {
	state.Normalize()
	steps := make(map[string]any, len(state.Steps))
	for stepID, stepState := range state.Steps {
		steps[stepID] = map[string]any{
			"status":          stepState.Status,
			"body":            stepState.Body,
			"output":          stepState.Output,
			"error":           stepState.Error,
			"attempt":         stepState.Attempt,
			"idempotency_key": stepState.IdempotencyKey,
		}
	}
	return map[string]any{
		"input": input,
		"steps": steps,
		"state": map[string]any{
			"completed":          state.Completed,
			"compensation_queue": state.CompensationQueue,
			"forks":              state.Forks,
		},
	}
}

func IsWaitingTimer(err error) (WaitingTimerError, bool) {
	var waiting WaitingTimerError
	if errors.As(err, &waiting) {
		return waiting, true
	}
	return WaitingTimerError{}, false
}

var directTemplatePattern = regexp.MustCompile(`^\s*\{\{\s*\.([a-zA-Z0-9_.$\[\]-]+)\s*\}\}\s*$`)

func renderValue(value any, env map[string]any) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			rendered, err := renderValue(child, env)
			if err != nil {
				return nil, fmt.Errorf("render object key %q: %w", key, err)
			}
			out[key] = rendered
		}
		return out, nil
	case []any:
		out := make([]any, 0, len(typed))
		for i, child := range typed {
			rendered, err := renderValue(child, env)
			if err != nil {
				return nil, fmt.Errorf("render array index %d: %w", i, err)
			}
			out = append(out, rendered)
		}
		return out, nil
	case string:
		if match := directTemplatePattern.FindStringSubmatch(typed); len(match) == 2 {
			if resolved, ok := resolvePath(env, match[1]); ok {
				return resolved, nil
			}
		}
		tpl, err := template.New("config").Option("missingkey=error").Parse(typed)
		if err != nil {
			return nil, fmt.Errorf("parse template: %w", err)
		}
		var buf bytes.Buffer
		if err := tpl.Execute(&buf, env); err != nil {
			return nil, fmt.Errorf("execute template: %w", err)
		}
		return buf.String(), nil
	default:
		return value, nil
	}
}

func resolvePath(env map[string]any, path string) (any, bool) {
	path = strings.TrimPrefix(path, ".")
	parts := strings.Split(path, ".")
	var current any = env
	for _, part := range parts {
		if part == "" {
			return nil, false
		}
		asMap, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := asMap[part]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func flattenHeaders(headers http.Header) map[string]any {
	out := make(map[string]any, len(headers))
	for key, values := range headers {
		if len(values) == 1 {
			out[key] = values[0]
			continue
		}
		copied := make([]string, len(values))
		copy(copied, values)
		out[key] = copied
	}
	return out
}

func firstString(config map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		if value, ok := stringValue(config, key); ok {
			return value, true
		}
	}
	return "", false
}

func stringValue(config map[string]any, key string) (string, bool) {
	if config == nil {
		return "", false
	}
	value, ok := config[key]
	if !ok || value == nil {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}

func stringSliceValue(config map[string]any, key string) ([]string, bool) {
	raw, ok := config[key]
	if !ok || raw == nil {
		return nil, false
	}
	switch values := raw.(type) {
	case []string:
		return values, true
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			text, ok := value.(string)
			if !ok || text == "" {
				return nil, false
			}
			out = append(out, text)
		}
		return out, true
	default:
		return nil, false
	}
}
