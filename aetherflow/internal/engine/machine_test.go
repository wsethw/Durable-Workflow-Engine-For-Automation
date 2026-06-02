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

	next, ok := machine.NextAfter(definition.Steps[0], &state, nil)
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

	state := workflow.NewRuntimeState()
	_, ok := machine.NextAfter(definition.Steps[0], &state, &worker.Result{Output: map[string]any{"matched": false}})
	if ok {
		t.Fatal("expected condition with nil else to terminate")
	}
}

func TestMachineForkJoinRunsAllBranchesBeforeJoin(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "fork-join",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "fanout", Type: workflow.StepFork, Config: map[string]any{"branches": []any{"reserve", "score"}, "join": "join"}},
			{ID: "reserve", Type: workflow.StepTransform, Config: map[string]any{"expr": "1"}},
			{ID: "score", Type: workflow.StepTransform, Config: map[string]any{"expr": "2"}},
			{ID: "join", Type: workflow.StepJoin, Config: map[string]any{"next": "notify"}},
			{ID: "notify", Type: workflow.StepTransform, Config: map[string]any{"expr": "3"}},
		},
	}
	machine := NewMachine(definition)
	state := workflow.NewRuntimeState()

	next, ok := machine.NextAfter(definition.Steps[0], &state, &worker.Result{Output: map[string]any{"branches": []any{"reserve", "score"}}})
	if !ok || next.ID != "reserve" {
		t.Fatalf("expected first branch reserve, got %q ok=%v", next.ID, ok)
	}
	next, ok = machine.NextAfter(definition.Steps[1], &state, &worker.Result{Output: map[string]any{"result": 1}})
	if !ok || next.ID != "score" {
		t.Fatalf("expected second branch score, got %q ok=%v", next.ID, ok)
	}
	next, ok = machine.NextAfter(definition.Steps[2], &state, &worker.Result{Output: map[string]any{"result": 2}})
	if !ok || next.ID != "join" {
		t.Fatalf("expected join after all branches, got %q ok=%v", next.ID, ok)
	}
	next, ok = machine.NextAfter(definition.Steps[3], &state, &worker.Result{Output: map[string]any{"joined": true}})
	if !ok || next.ID != "notify" {
		t.Fatalf("expected configured next step notify, got %q ok=%v", next.ID, ok)
	}
	if state.Forks["fanout"].Status != "joined" {
		t.Fatalf("expected fork to be joined, got %#v", state.Forks["fanout"])
	}
}

func TestMachineForkBranchSupportsExplicitNext(t *testing.T) {
	definition := workflow.DefinitionDSL{
		Name:    "fork-chain",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "fanout", Type: workflow.StepFork, Config: map[string]any{"branches": []any{"a1", "b1"}, "join": "join"}},
			{ID: "a1", Type: workflow.StepTransform, Config: map[string]any{"expr": "1", "next": "a2"}},
			{ID: "a2", Type: workflow.StepTransform, Config: map[string]any{"expr": "2"}},
			{ID: "b1", Type: workflow.StepTransform, Config: map[string]any{"expr": "3"}},
			{ID: "join", Type: workflow.StepJoin},
		},
	}
	machine := NewMachine(definition)
	state := workflow.NewRuntimeState()

	next, _ := machine.NextAfter(definition.Steps[0], &state, nil)
	if next.ID != "a1" {
		t.Fatalf("expected a1, got %s", next.ID)
	}
	next, _ = machine.NextAfter(definition.Steps[1], &state, nil)
	if next.ID != "a2" {
		t.Fatalf("expected a2, got %s", next.ID)
	}
	next, _ = machine.NextAfter(definition.Steps[2], &state, nil)
	if next.ID != "b1" {
		t.Fatalf("expected b1, got %s", next.ID)
	}
}
