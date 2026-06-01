package dsl

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/expr-lang/expr"

	"github.com/aetherflow/aetherflow/internal/workflow"
)

type Validator struct{}

func NewValidator() Validator {
	return Validator{}
}

func (Validator) Validate(ctx context.Context, definition workflow.DefinitionDSL) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("validate workflow context: %w", err)
	}
	if strings.TrimSpace(definition.Name) == "" {
		return fmt.Errorf("validate workflow: name is required")
	}
	if definition.Version < 1 {
		return fmt.Errorf("validate workflow: version must be >= 1")
	}
	if len(definition.Steps) == 0 {
		return fmt.Errorf("validate workflow: at least one step is required")
	}

	ids := make(map[string]int, len(definition.Steps))
	for i, step := range definition.Steps {
		if strings.TrimSpace(step.ID) == "" {
			return fmt.Errorf("validate workflow: step at index %d has empty id", i)
		}
		if _, ok := ids[step.ID]; ok {
			return fmt.Errorf("validate workflow: duplicate step id %q", step.ID)
		}
		ids[step.ID] = i
		if err := validateStep(step); err != nil {
			return fmt.Errorf("validate step %q: %w", step.ID, err)
		}
	}

	for _, step := range definition.Steps {
		if step.OnFailure != "" {
			if _, ok := ids[step.OnFailure]; !ok {
				return fmt.Errorf("validate workflow: step %q on_failure references unknown step %q", step.ID, step.OnFailure)
			}
		}
		for _, ref := range explicitRefs(step) {
			if _, ok := ids[ref]; !ok {
				return fmt.Errorf("validate workflow: step %q references unknown step %q", step.ID, ref)
			}
		}
	}

	if err := validateAcyclic(definition, ids); err != nil {
		return fmt.Errorf("validate workflow graph: %w", err)
	}
	return nil
}

func validateStep(step workflow.Step) error {
	switch step.Type {
	case workflow.StepHTTPRequest:
		rawURL, ok := stringValue(step.Config, "url")
		if !ok || strings.TrimSpace(rawURL) == "" {
			return fmt.Errorf("http_request requires config.url")
		}
		parsed, err := url.Parse(rawURL)
		if err != nil {
			return fmt.Errorf("parse http_request url: %w", err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return fmt.Errorf("http_request url must use http or https")
		}
		method, ok := stringValue(step.Config, "method")
		if !ok || strings.TrimSpace(method) == "" {
			return fmt.Errorf("http_request requires config.method")
		}
		if !isHTTPMethod(method) {
			return fmt.Errorf("unsupported http method %q", method)
		}
	case workflow.StepTransform:
		expression, ok := firstString(step.Config, "expr", "expression")
		if !ok || strings.TrimSpace(expression) == "" {
			return fmt.Errorf("transform requires config.expr or config.expression")
		}
		if _, err := expr.Compile(expression, expr.Env(defaultExprEnv()), expr.AllowUndefinedVariables()); err != nil {
			return fmt.Errorf("compile transform expression: %w", err)
		}
	case workflow.StepDelay:
		duration, ok := firstString(step.Config, "duration", "for")
		if !ok || strings.TrimSpace(duration) == "" {
			return fmt.Errorf("delay requires config.duration")
		}
		if _, err := time.ParseDuration(duration); err != nil {
			return fmt.Errorf("parse delay duration: %w", err)
		}
	case workflow.StepCondition:
		condition, ok := stringValue(step.Config, "if")
		if !ok || strings.TrimSpace(condition) == "" {
			return fmt.Errorf("condition requires config.if")
		}
		if _, err := expr.Compile(condition, expr.Env(defaultExprEnv()), expr.AllowUndefinedVariables()); err != nil {
			return fmt.Errorf("compile condition expression: %w", err)
		}
	case workflow.StepFork:
		branches, ok := stringSliceValue(step.Config, "branches")
		if !ok || len(branches) == 0 {
			return fmt.Errorf("fork requires config.branches")
		}
	case workflow.StepJoin:
		return nil
	default:
		return fmt.Errorf("unsupported step type %q", step.Type)
	}

	if step.Retry != nil {
		if step.Retry.MaxRetries < 0 {
			return fmt.Errorf("retry.max_retries must be >= 0")
		}
		if step.Retry.InitialInterval != "" {
			if _, err := time.ParseDuration(step.Retry.InitialInterval); err != nil {
				return fmt.Errorf("parse retry.initial_interval: %w", err)
			}
		}
		if step.Retry.MaxInterval != "" {
			if _, err := time.ParseDuration(step.Retry.MaxInterval); err != nil {
				return fmt.Errorf("parse retry.max_interval: %w", err)
			}
		}
	}
	return nil
}

func validateAcyclic(definition workflow.DefinitionDSL, ids map[string]int) error {
	edges := make(map[string][]string, len(definition.Steps))
	compensation := definition.CompensationRefs()
	normalSteps := make([]workflow.Step, 0, len(definition.Steps))
	for _, step := range definition.Steps {
		if _, isCompensation := compensation[step.ID]; !isCompensation {
			normalSteps = append(normalSteps, step)
		}
	}
	for i, step := range normalSteps {
		if i+1 < len(normalSteps) {
			edges[step.ID] = append(edges[step.ID], normalSteps[i+1].ID)
		}
		for _, ref := range explicitRefs(step) {
			edges[step.ID] = append(edges[step.ID], ref)
		}
		if step.OnFailure != "" {
			edges[step.ID] = append(edges[step.ID], step.OnFailure)
		}
	}
	for _, step := range definition.Steps {
		if step.OnFailure != "" {
			edges[step.OnFailure] = edges[step.OnFailure]
		}
	}

	visiting := make(map[string]bool, len(ids))
	visited := make(map[string]bool, len(ids))
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return fmt.Errorf("cycle detected at %q", id)
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		for _, next := range edges[id] {
			if err := visit(next); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	for id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func explicitRefs(step workflow.Step) []string {
	refs := make([]string, 0, 4)
	switch step.Type {
	case workflow.StepCondition:
		if thenRef, ok := stringValue(step.Config, "then"); ok && thenRef != "" {
			refs = append(refs, thenRef)
		}
		if elseRef, ok := stringValue(step.Config, "else"); ok && elseRef != "" {
			refs = append(refs, elseRef)
		}
	case workflow.StepFork:
		if branches, ok := stringSliceValue(step.Config, "branches"); ok {
			refs = append(refs, branches...)
		}
	case workflow.StepJoin:
		if next, ok := stringValue(step.Config, "next"); ok && next != "" {
			refs = append(refs, next)
		}
	}
	return refs
}

func defaultExprEnv() map[string]any {
	return map[string]any{
		"input": map[string]any{},
		"steps": map[string]any{},
		"state": map[string]any{},
	}
}

func isHTTPMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead:
		return true
	default:
		return false
	}
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

func firstString(config map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		if value, ok := stringValue(config, key); ok {
			return value, true
		}
	}
	return "", false
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
