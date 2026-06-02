package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aetherflow/aetherflow/internal/store"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

func TestHTTPRequestRendersTemplatesAndPreservesJSONValues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer demo-token" {
			t.Fatalf("expected rendered auth header, got %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		items, ok := body["items"].([]any)
		if !ok || len(items) != 1 {
			t.Fatalf("expected items array to be preserved, got %#v", body["items"])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"reservation_id":"r-1","total":42}`))
	}))
	defer server.Close()

	instance := &store.Instance{
		Input: map[string]any{
			"token": "demo-token",
			"order": map[string]any{
				"items": []any{map[string]any{"sku": "abc", "qty": float64(1)}},
			},
		},
		State: workflow.NewRuntimeState(),
	}
	step := workflow.Step{
		ID:   "reserve",
		Type: workflow.StepHTTPRequest,
		Config: map[string]any{
			"url":     server.URL,
			"method":  "POST",
			"headers": map[string]any{"Authorization": "Bearer {{.input.token}}"},
			"body":    map[string]any{"items": "{{.input.order.items}}"},
		},
	}

	result, err := NewExecutorWithOptions(ExecutorOptions{
		HTTPClient:           server.Client(),
		AllowPrivateNetworks: true,
	}).Execute(context.Background(), instance, step)
	if err != nil {
		t.Fatalf("execute http request: %v", err)
	}
	if result.Output["status_code"] != http.StatusCreated {
		t.Fatalf("expected status %d, got %#v", http.StatusCreated, result.Output["status_code"])
	}
	body, ok := result.Body.(map[string]any)
	if !ok || body["reservation_id"] != "r-1" {
		t.Fatalf("unexpected response body %#v", result.Body)
	}
}

func TestTransformExecutesExpression(t *testing.T) {
	instance := &store.Instance{
		Input: map[string]any{"value": 21},
		State: workflow.NewRuntimeState(),
	}
	step := workflow.Step{
		ID:     "double",
		Type:   workflow.StepTransform,
		Config: map[string]any{"expr": "input.value * 2", "result_key": "answer"},
	}

	result, err := NewExecutor(nil).Execute(context.Background(), instance, step)
	if err != nil {
		t.Fatalf("execute transform: %v", err)
	}
	if result.Output["answer"] != 42 {
		t.Fatalf("expected 42, got %#v", result.Output["answer"])
	}
}

func TestDelayReturnsWaitingTimerThenCompletesAfterRehydrate(t *testing.T) {
	instance := &store.Instance{State: workflow.NewRuntimeState()}
	step := workflow.Step{ID: "delay", Type: workflow.StepDelay, Config: map[string]any{"duration": "10ms"}}

	result, err := NewExecutor(nil).Execute(context.Background(), instance, step)
	if err == nil {
		t.Fatal("expected waiting timer error")
	}
	if _, ok := IsWaitingTimer(err); !ok {
		t.Fatalf("expected waiting timer error, got %v", err)
	}
	if result == nil || result.DelayUntil == nil {
		t.Fatalf("expected delay fire time, got %#v", result)
	}

	instance.State.Steps["delay"] = workflow.StepState{Status: workflow.StepWaitingTimer}
	result, err = NewExecutor(nil).Execute(context.Background(), instance, step)
	if err != nil {
		t.Fatalf("expected delay to complete after rehydrate, got %v", err)
	}
	if result.Output["fired_at"] == "" {
		t.Fatalf("expected fired_at output, got %#v", result.Output)
	}
}

func TestDelayDoesNotCompleteBeforePersistedFireTime(t *testing.T) {
	fireAt := time.Now().UTC().Add(time.Hour)
	instance := &store.Instance{State: workflow.NewRuntimeState()}
	instance.State.Steps["delay"] = workflow.StepState{Status: workflow.StepWaitingTimer, WaitingTime: &fireAt}
	step := workflow.Step{ID: "delay", Type: workflow.StepDelay, Config: map[string]any{"duration": "10ms"}}

	result, err := NewExecutor(nil).Execute(context.Background(), instance, step)
	if err == nil {
		t.Fatal("expected waiting timer error")
	}
	waiting, ok := IsWaitingTimer(err)
	if !ok {
		t.Fatalf("expected waiting timer error, got %v", err)
	}
	if !waiting.FireAt.Equal(fireAt) {
		t.Fatalf("expected persisted fire time %s, got %s", fireAt, waiting.FireAt)
	}
	if result == nil || result.DelayUntil == nil || !result.DelayUntil.Equal(fireAt) {
		t.Fatalf("expected persisted delay result, got %#v", result)
	}
}

func TestHTTPRequestBlocksPrivateNetworkByDefault(t *testing.T) {
	instance := &store.Instance{Input: map[string]any{}, State: workflow.NewRuntimeState()}
	step := workflow.Step{
		ID:   "private",
		Type: workflow.StepHTTPRequest,
		Config: map[string]any{
			"url":    "http://127.0.0.1:8080/private",
			"method": "GET",
		},
	}

	_, err := NewExecutor(nil).Execute(context.Background(), instance, step)
	if err == nil {
		t.Fatal("expected private network validation error")
	}
}

func TestHTTPRequestBlocksPrivateRedirectByDefault(t *testing.T) {
	client := &http.Client{
		Timeout: time.Second,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "93.184.216.34" {
				return &http.Response{
					StatusCode: http.StatusFound,
					Header:     http.Header{"Location": []string{"http://127.0.0.1/admin"}},
					Body:       io.NopCloser(http.NoBody),
					Request:    req,
				}, nil
			}
			t.Fatalf("unexpected redirected request to %s", req.URL.String())
			return nil, nil
		}),
	}
	instance := &store.Instance{Input: map[string]any{}, State: workflow.NewRuntimeState()}
	step := workflow.Step{
		ID:   "redirect",
		Type: workflow.StepHTTPRequest,
		Config: map[string]any{
			"url":    "http://93.184.216.34/start",
			"method": "GET",
		},
	}

	_, err := NewExecutorWithOptions(ExecutorOptions{HTTPClient: client}).Execute(context.Background(), instance, step)
	if err == nil {
		t.Fatal("expected private redirect validation error")
	}
}

func TestBuildEnvExposesIdempotencyKey(t *testing.T) {
	state := workflow.NewRuntimeState()
	state.Steps["charge"] = workflow.StepState{
		Status:         workflow.StepRunning,
		Attempt:        1,
		IdempotencyKey: "idem-123",
	}

	env := BuildEnv(map[string]any{}, state)
	steps := env["steps"].(map[string]any)
	charge := steps["charge"].(map[string]any)
	if charge["idempotency_key"] != "idem-123" {
		t.Fatalf("expected idempotency key in env, got %#v", charge["idempotency_key"])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
