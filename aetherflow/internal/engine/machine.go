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
	branchSteps := collectForkBranchSteps(definition)
	normal := make([]workflow.Step, 0, len(definition.Steps))
	for _, step := range definition.Steps {
		if _, ok := compensation[step.ID]; ok {
			continue
		}
		if _, ok := branchSteps[step.ID]; ok {
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
		return m.NextAfter(current, &state, result)
	}
	if len(m.normal) == 0 {
		return workflow.Step{}, false
	}
	return m.normal[0], true
}

func (m Machine) NextAfter(step workflow.Step, state *workflow.RuntimeState, result *worker.Result) (workflow.Step, bool) {
	state.Normalize()
	if state.Forks == nil {
		state.Forks = make(map[string]workflow.ForkState)
	}
	if step.Type == workflow.StepFork {
		if err := m.startFork(step, state); err == nil {
			return m.nextForkStep(step.ID, state)
		}
		return workflow.Step{}, false
	}
	if forkID, branchIndex, ok := m.branchForCurrentStep(*state, step.ID); ok {
		return m.nextAfterBranchStep(forkID, branchIndex, step, state, result)
	}
	if step.Type == workflow.StepJoin {
		m.markJoined(step.ID, state)
	}
	if nextID := resultNextStepID(result); nextID != "" {
		return m.definition.StepByID(nextID)
	}
	if nextID := configString(step.Config, "next"); nextID != "" {
		return m.definition.StepByID(nextID)
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

func (m Machine) startFork(step workflow.Step, state *workflow.RuntimeState) error {
	branches, ok := configStringSlice(step.Config, "branches")
	if !ok || len(branches) == 0 {
		return nil
	}
	if current, ok := state.Forks[step.ID]; ok && current.Status != "" {
		return nil
	}
	joinStepID := configString(step.Config, "join")
	if joinStepID == "" {
		joinStepID = m.firstJoinAfter(step.ID)
	}
	if joinStepID == "" {
		return nil
	}
	branchStates := make([]workflow.BranchState, 0, len(branches))
	for _, branch := range branches {
		branchStates = append(branchStates, workflow.BranchState{
			StartStepID:   branch,
			CurrentStepID: branch,
			Status:        "pending",
		})
	}
	state.Forks[step.ID] = workflow.ForkState{
		Status:     "running",
		JoinStepID: joinStepID,
		Branches:   branchStates,
	}
	return nil
}

func (m Machine) nextForkStep(forkID string, state *workflow.RuntimeState) (workflow.Step, bool) {
	fork := state.Forks[forkID]
	for i := range fork.Branches {
		if fork.Branches[i].Status == "pending" || fork.Branches[i].Status == "running" {
			fork.Branches[i].Status = "running"
			if fork.Branches[i].CurrentStepID == "" {
				fork.Branches[i].CurrentStepID = fork.Branches[i].StartStepID
			}
			state.Forks[forkID] = fork
			return m.definition.StepByID(fork.Branches[i].CurrentStepID)
		}
	}
	fork.Status = "completed"
	state.Forks[forkID] = fork
	return m.definition.StepByID(fork.JoinStepID)
}

func (m Machine) nextAfterBranchStep(forkID string, branchIndex int, step workflow.Step, state *workflow.RuntimeState, result *worker.Result) (workflow.Step, bool) {
	fork := state.Forks[forkID]
	branch := fork.Branches[branchIndex]
	if !contains(branch.Completed, step.ID) {
		branch.Completed = append(branch.Completed, step.ID)
	}
	nextID := resultNextStepID(result)
	if nextID == "" {
		nextID = configString(step.Config, "next")
	}
	if nextID != "" && nextID != fork.JoinStepID {
		branch.CurrentStepID = nextID
		branch.Status = "running"
		fork.Branches[branchIndex] = branch
		state.Forks[forkID] = fork
		return m.definition.StepByID(nextID)
	}
	branch.CurrentStepID = ""
	branch.Status = "completed"
	fork.Branches[branchIndex] = branch
	state.Forks[forkID] = fork
	return m.nextForkStep(forkID, state)
}

func (m Machine) MarkBranchFailed(state *workflow.RuntimeState, stepID string, err error) {
	state.Normalize()
	forkID, branchIndex, ok := m.branchForCurrentStep(*state, stepID)
	if !ok {
		return
	}
	fork := state.Forks[forkID]
	fork.Status = "failed"
	fork.Branches[branchIndex].Status = "failed"
	if err != nil {
		fork.Branches[branchIndex].Error = err.Error()
	}
	state.Forks[forkID] = fork
}

func (m Machine) branchForCurrentStep(state workflow.RuntimeState, stepID string) (string, int, bool) {
	for forkID, fork := range state.Forks {
		if fork.Status != "running" {
			continue
		}
		for i, branch := range fork.Branches {
			if branch.Status == "running" && branch.CurrentStepID == stepID {
				return forkID, i, true
			}
		}
	}
	return "", 0, false
}

func (m Machine) markJoined(joinStepID string, state *workflow.RuntimeState) {
	for forkID, fork := range state.Forks {
		if fork.JoinStepID != joinStepID || fork.Status != "completed" {
			continue
		}
		fork.Status = "joined"
		state.Forks[forkID] = fork
	}
}

func (m Machine) firstJoinAfter(stepID string) string {
	found := false
	for _, step := range m.definition.Steps {
		if step.ID == stepID {
			found = true
			continue
		}
		if found && step.Type == workflow.StepJoin {
			return step.ID
		}
	}
	return ""
}

func collectForkBranchSteps(definition workflow.DefinitionDSL) map[string]struct{} {
	out := make(map[string]struct{})
	for _, step := range definition.Steps {
		if step.Type != workflow.StepFork {
			continue
		}
		joinStepID := configString(step.Config, "join")
		if joinStepID == "" {
			joinStepID = firstJoinAfter(definition, step.ID)
		}
		branches, ok := configStringSlice(step.Config, "branches")
		if !ok {
			continue
		}
		for _, branch := range branches {
			collectBranchStep(definition, branch, joinStepID, out, make(map[string]struct{}))
		}
	}
	return out
}

func collectBranchStep(definition workflow.DefinitionDSL, stepID string, joinStepID string, out map[string]struct{}, seen map[string]struct{}) {
	if stepID == "" || stepID == joinStepID {
		return
	}
	if _, ok := seen[stepID]; ok {
		return
	}
	seen[stepID] = struct{}{}
	step, ok := definition.StepByID(stepID)
	if !ok {
		return
	}
	out[stepID] = struct{}{}
	for _, next := range branchRefs(step) {
		collectBranchStep(definition, next, joinStepID, out, seen)
	}
}

func branchRefs(step workflow.Step) []string {
	refs := make([]string, 0, 3)
	if next := configString(step.Config, "next"); next != "" {
		refs = append(refs, next)
	}
	if step.Type == workflow.StepCondition {
		if thenRef := configString(step.Config, "then"); thenRef != "" {
			refs = append(refs, thenRef)
		}
		if elseRef := configString(step.Config, "else"); elseRef != "" {
			refs = append(refs, elseRef)
		}
	}
	return refs
}

func firstJoinAfter(definition workflow.DefinitionDSL, stepID string) string {
	found := false
	for _, step := range definition.Steps {
		if step.ID == stepID {
			found = true
			continue
		}
		if found && step.Type == workflow.StepJoin {
			return step.ID
		}
	}
	return ""
}

func resultNextStepID(result *worker.Result) string {
	if result == nil || result.NextStepID == nil {
		return ""
	}
	return *result.NextStepID
}

func configString(config map[string]any, key string) string {
	if config == nil {
		return ""
	}
	value, ok := config[key]
	if !ok || value == nil {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

func configStringSlice(config map[string]any, key string) ([]string, bool) {
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

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
