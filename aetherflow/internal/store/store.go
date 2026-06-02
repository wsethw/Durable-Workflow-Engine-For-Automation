package store

import (
	"context"
	"errors"
	"time"

	"github.com/aetherflow/aetherflow/internal/workflow"
)

type Definition struct {
	ID        string
	TenantID  string
	Name      string
	Version   int
	DSL       workflow.DefinitionDSL
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Instance struct {
	ID                string
	TenantID          string
	DefinitionID      string
	DefinitionVersion int
	Status            string
	Input             map[string]any
	CurrentStepID     *string
	State             workflow.RuntimeState
	Version           int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type History struct {
	ID          string
	InstanceID  string
	StepID      string
	Status      string
	Input       map[string]any
	Output      map[string]any
	Error       *string
	Attempt     int
	StartedAt   time.Time
	CompletedAt *time.Time
}

type Timer struct {
	InstanceID string
	StepID     string
	FireAt     time.Time
}

type Repository interface {
	Ping(ctx context.Context) error
	CreateDefinition(ctx context.Context, tenantID string, dsl workflow.DefinitionDSL) (*Definition, error)
	GetDefinition(ctx context.Context, id string) (*Definition, error)
	GetDefinitionForTenant(ctx context.Context, tenantID string, id string) (*Definition, error)
	CreateInstance(ctx context.Context, tenantID string, definition *Definition, input map[string]any) (*Instance, error)
	GetInstance(ctx context.Context, id string) (*Instance, error)
	GetInstanceForTenant(ctx context.Context, tenantID string, id string) (*Instance, error)
	ClaimInstance(ctx context.Context, id string, owner string, leaseUntil time.Time) (*Instance, error)
	ReleaseInstance(ctx context.Context, id string, owner string) error
	ListRecoverableInstances(ctx context.Context, limit int) ([]Instance, error)
	UpdateInstance(ctx context.Context, instance *Instance, expectedVersion int) error
	AppendHistory(ctx context.Context, history *History) (*History, error)
	CompleteHistory(ctx context.Context, historyID string, status string, output map[string]any, stepErr *string) error
	ListHistory(ctx context.Context, instanceID string) ([]History, error)
	UpsertTimer(ctx context.Context, timer Timer) error
	DeleteTimer(ctx context.Context, instanceID string) error
	FireTimer(ctx context.Context, instanceID string) (*Timer, bool, error)
	ListDueTimers(ctx context.Context, now time.Time, limit int) ([]Timer, error)
	ClaimDueTimers(ctx context.Context, now time.Time, limit int) ([]Timer, error)
}

var ErrInstanceBusy = errors.New("instance is owned by another worker")
