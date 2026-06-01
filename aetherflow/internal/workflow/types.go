package workflow

import "time"

const (
	StepHTTPRequest = "http_request"
	StepTransform   = "transform"
	StepDelay       = "delay"
	StepCondition   = "condition"
	StepFork        = "fork"
	StepJoin        = "join"
)

const (
	InstancePending      = "pending"
	InstanceRunning      = "running"
	InstanceWaitingTimer = "waiting_timer"
	InstanceCompensating = "compensating"
	InstanceCompleted    = "completed"
	InstanceFailed       = "failed"
)

const (
	StepRunning      = "running"
	StepWaitingTimer = "waiting_timer"
	StepCompleted    = "completed"
	StepFailed       = "failed"
	StepSkipped      = "skipped"
)

type DefinitionDSL struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Steps   []Step `json:"steps"`
}

type Step struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Config    map[string]any `json:"config"`
	Retry     *RetryPolicy   `json:"retry,omitempty"`
	OnFailure string         `json:"on_failure,omitempty"`
}

type RetryPolicy struct {
	MaxRetries      int     `json:"max_retries"`
	InitialInterval string  `json:"initial_interval"`
	MaxInterval     string  `json:"max_interval,omitempty"`
	Multiplier      float64 `json:"multiplier,omitempty"`
}

func (r RetryPolicy) InitialBackoff() time.Duration {
	if r.InitialInterval == "" {
		return time.Second
	}
	d, err := time.ParseDuration(r.InitialInterval)
	if err != nil || d <= 0 {
		return time.Second
	}
	return d
}

func (r RetryPolicy) MaxBackoff() time.Duration {
	if r.MaxInterval == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(r.MaxInterval)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

func (r RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	multiplier := r.Multiplier
	if multiplier <= 1 {
		multiplier = 2
	}
	backoff := r.InitialBackoff()
	for i := 1; i < attempt; i++ {
		next := time.Duration(float64(backoff) * multiplier)
		if next <= backoff {
			next = backoff * 2
		}
		backoff = next
		if backoff >= r.MaxBackoff() {
			return r.MaxBackoff()
		}
	}
	if backoff > r.MaxBackoff() {
		return r.MaxBackoff()
	}
	return backoff
}

type RuntimeState struct {
	Steps             map[string]StepState `json:"steps"`
	Completed         []string             `json:"completed"`
	CompensationQueue []string             `json:"compensation_queue,omitempty"`
	StartedAt         time.Time            `json:"started_at,omitempty"`
}

type StepState struct {
	Status      string         `json:"status"`
	Body        any            `json:"body,omitempty"`
	Output      map[string]any `json:"output,omitempty"`
	Error       string         `json:"error,omitempty"`
	Attempt     int            `json:"attempt"`
	WaitingTime *time.Time     `json:"waiting_time,omitempty"`
}

func NewRuntimeState() RuntimeState {
	return RuntimeState{
		Steps:     make(map[string]StepState),
		Completed: make([]string, 0),
	}
}

func (s *RuntimeState) Normalize() {
	if s.Steps == nil {
		s.Steps = make(map[string]StepState)
	}
	if s.Completed == nil {
		s.Completed = make([]string, 0)
	}
}

func (s RuntimeState) HasCompleted(stepID string) bool {
	for _, id := range s.Completed {
		if id == stepID {
			return true
		}
	}
	return false
}

func (s *RuntimeState) MarkCompleted(stepID string, result StepState) {
	s.Normalize()
	result.Status = StepCompleted
	s.Steps[stepID] = result
	if !s.HasCompleted(stepID) {
		s.Completed = append(s.Completed, stepID)
	}
}

func (d DefinitionDSL) StepByID(id string) (Step, bool) {
	for _, step := range d.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return Step{}, false
}

func (d DefinitionDSL) CompensationRefs() map[string]struct{} {
	refs := make(map[string]struct{})
	for _, step := range d.Steps {
		if step.OnFailure != "" {
			refs[step.OnFailure] = struct{}{}
		}
	}
	return refs
}
