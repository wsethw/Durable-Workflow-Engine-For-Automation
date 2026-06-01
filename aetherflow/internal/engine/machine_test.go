package engine

import (
	"testing"

	"github.com/aetherflow/aetherflow/internal/worker"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

func TestMachineSkipsCompensationStepsOnHappyPath(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "saga",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "reserve", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}, OnFailure: "release"},
			{ID: "release", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
			{ID: "notify", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
		},
	}
	machine := NewMachine(definition)
	state := workflow.NewRuntimeState()
	state.MarkCompleted("reserve", workflow.StepState{Output: map[string]any{"result": 1}})

	next, ok := machine.NextAfter(definition.Steps[0], state, nil)
	if !ok {
		t.Fatal("expected next step")
	}
	if next.ID != "notify" {
		t.Fatalf("expected notify, got %s", next.ID)
	}
}

func TestMachineBuildsReverseCompensationQueue(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "saga",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "reserve", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}, OnFailure: "release"},
			{ID: "charge", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}, OnFailure: "refund"},
			{ID: "release", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
			{ID: "refund", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
		},
	}
	state := workflow.NewRuntimeState()
	state.MarkCompleted("reserve", workflow.StepState{Output: map[string]any{"result": 1}})
	state.MarkCompleted("charge", workflow.StepState{Output: map[string]any{"result": 1}})

	queue := NewMachine(definition).CompensationQueue("charge", state)
	want := []string{"refund", "release"}
	if len(queue) != len(want) {
		t.Fatalf("expected queue length %d, got %d: %#v", len(want), len(queue), queue)
	}
	for i := range want {
		if queue[i] != want[i] {
			t.Fatalf("expected queue %#v, got %#v", want, queue)
		}
	}
}

func TestMachineConditionWithNilElseTerminates(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "condition",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "decide", Type: workflow.StepCondition, Config: map[string]any{"if": "false", "then": "send", "else": nil}},
			{ID: "send", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
		},
	}
	machine := NewMachine(definition)

	_, ok := machine.NextAfter(definition.Steps[0], workflow.NewRuntimeState(), &worker.Result{Output: map[string]any{"matched": false}})
	if ok {
		t.Fatal("expected condition with nil else to terminate")
	}
}
