package engine

import (
	"github.com/aetherflow/aetherflow/internal/worker"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

type Machine struct {
	definition workflow.DefinitionDSL
	normal     []workflow.Step
}

func NewMachine(definition workflow.DefinitionDSL) Machine {
	compensation := definition.CompensationRefs()
	normal := make([]workflow.Step, 0, len(definition.Steps))
	for _, step := range definition.Steps {
		if _, ok := compensation[step.ID]; ok {
			continue
		}
		normal = append(normal, step)
	}
	return Machine{definition: definition, normal: normal}
}

func (m Machine) StepForCurrent(state workflow.RuntimeState, currentStepID *string) (workflow.Step, bool) {
	state.Normalize()
	if currentStepID != nil {
		current, ok := m.definition.StepByID(*currentStepID)
		if !ok {
			return workflow.Step{}, false
		}
		stepState, hasState := state.Steps[*currentStepID]
		if !hasState || stepState.Status != workflow.StepCompleted {
			return current, true
		}
		var result *worker.Result
		if next, ok := stepState.Output["next_step"].(string); ok && next != "" {
			result = &worker.Result{NextStepID: &next}
		}
		return m.NextAfter(current, state, result)
	}
	if len(m.normal) == 0 {
		return workflow.Step{}, false
	}
	return m.normal[0], true
}

func (m Machine) NextAfter(step workflow.Step, state workflow.RuntimeState, result *worker.Result) (workflow.Step, bool) {
	if result != nil && result.NextStepID != nil {
		return m.definition.StepByID(*result.NextStepID)
	}
	if step.Type == workflow.StepCondition {
		return workflow.Step{}, false
	}
	for i, candidate := range m.normal {
		if candidate.ID == step.ID && i+1 < len(m.normal) {
			return m.normal[i+1], true
		}
	}
	return workflow.Step{}, false
}

func (m Machine) CompensationQueue(failedStepID string, state workflow.RuntimeState) []string {
	state.Normalize()
	queue := make([]string, 0, len(state.Completed)+1)
	if failed, ok := m.definition.StepByID(failedStepID); ok && failed.OnFailure != "" {
		queue = append(queue, failed.OnFailure)
	}
	seen := make(map[string]struct{}, len(queue))
	for _, id := range queue {
		seen[id] = struct{}{}
	}
	for i := len(state.Completed) - 1; i >= 0; i-- {
		step, ok := m.definition.StepByID(state.Completed[i])
		if !ok || step.OnFailure == "" {
			continue
		}
		if _, duplicated := seen[step.OnFailure]; duplicated {
			continue
		}
		queue = append(queue, step.OnFailure)
		seen[step.OnFailure] = struct{}{}
	}
	return queue
}

func (m Machine) StepByID(stepID string) (workflow.Step, bool) {
	return m.definition.StepByID(stepID)
}
