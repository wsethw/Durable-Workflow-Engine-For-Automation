package dsl

import (
	"context"
	"testing"

	"github.com/aetherflow/aetherflow/internal/workflow"
)

func TestValidatorAcceptsOrderSagaShape(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "Order Processing Saga",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:   "reserve_inventory",
				Type: workflow.StepHTTPRequest,
				Config: map[string]any{
					"url":    "https://inventory.example/reserve",
					"method": "POST",
					"body":   map[string]any{"items": "{{.input.order.items}}"},
				},
				Retry:     &workflow.RetryPolicy{MaxRetries: 3, InitialInterval: "1s", MaxInterval: "10s", Multiplier: 2},
				OnFailure: "release_inventory",
			},
			{
				ID:   "process_payment",
				Type: workflow.StepHTTPRequest,
				Config: map[string]any{
					"url":    "https://payment.example/charge",
					"method": "POST",
				},
				OnFailure: "reverse_payment",
			},
			{ID: "release_inventory", Type: workflow.StepHTTPRequest, Config: map[string]any{"url": "https://inventory.example/release", "method": "POST"}},
			{ID: "reverse_payment", Type: workflow.StepHTTPRequest, Config: map[string]any{"url": "https://payment.example/refund", "method": "POST"}},
			{
				ID:   "notify_customer",
				Type: workflow.StepCondition,
				Config: map[string]any{
					"if":   "steps.process_payment.status == 'completed'",
					"then": "send_email",
					"else": nil,
				},
			},
			{ID: "send_email", Type: workflow.StepHTTPRequest, Config: map[string]any{"url": "https://notification.example/send", "method": "POST"}},
		},
	}

	if err := NewValidator().Validate(context.Background(), definition); err != nil {
		t.Fatalf("expected valid definition, got %v", err)
	}
}

func TestValidatorRejectsCycle(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "cycle",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "a", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
			{ID: "b", Type: workflow.StepCondition, Config: map[string]any{"if": "true", "then": "a"}},
		},
	}

	if err := NewValidator().Validate(context.Background(), definition); err == nil {
		t.Fatal("expected cycle validation error")
	}
}

func TestValidatorRejectsUnknownFailureReference(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "bad-ref",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "a", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}, OnFailure: "missing"},
		},
	}

	if err := NewValidator().Validate(context.Background(), definition); err == nil {
		t.Fatal("expected unknown reference validation error")
	}
}
