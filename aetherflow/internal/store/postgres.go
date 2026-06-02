package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aetherflow/aetherflow/internal/workflow"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

func (p *Postgres) CreateDefinition(ctx context.Context, tenantID string, dsl workflow.DefinitionDSL) (*Definition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if tenantID == "" {
		tenantID = "default"
	}

	raw, err := json.Marshal(dsl)
	if err != nil {
		return nil, fmt.Errorf("marshal definition dsl: %w", err)
	}

	definition := &Definition{TenantID: tenantID, DSL: dsl}
	err = p.pool.QueryRow(queryCtx, `
		INSERT INTO definitions (tenant_id, name, version, dsl)
		VALUES ($1, $2, $3, $4)
		RETURNING id, tenant_id, name, version, created_at, updated_at
	`, tenantID, dsl.Name, dsl.Version, raw).Scan(
		&definition.ID,
		&definition.TenantID,
		&definition.Name,
		&definition.Version,
		&definition.CreatedAt,
		&definition.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert definition: %w", err)
	}
	return definition, nil
}

func (p *Postgres) GetDefinition(ctx context.Context, id string) (*Definition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var raw []byte
	definition := &Definition{}
	err := p.pool.QueryRow(queryCtx, `
		SELECT id, tenant_id, name, version, dsl, created_at, updated_at
		FROM definitions
		WHERE id = $1
	`, id).Scan(
		&definition.ID,
		&definition.TenantID,
		&definition.Name,
		&definition.Version,
		&raw,
		&definition.CreatedAt,
		&definition.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("definition not found: %w", err)
		}
		return nil, fmt.Errorf("select definition: %w", err)
	}
	if err := json.Unmarshal(raw, &definition.DSL); err != nil {
		return nil, fmt.Errorf("unmarshal definition dsl: %w", err)
	}
	return definition, nil
}

func (p *Postgres) GetDefinitionForTenant(ctx context.Context, tenantID string, id string) (*Definition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if tenantID == "" {
		tenantID = "default"
	}

	var raw []byte
	definition := &Definition{}
	err := p.pool.QueryRow(queryCtx, `
		SELECT id, tenant_id, name, version, dsl, created_at, updated_at
		FROM definitions
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, id).Scan(
		&definition.ID,
		&definition.TenantID,
		&definition.Name,
		&definition.Version,
		&raw,
		&definition.CreatedAt,
		&definition.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("definition not found: %w", err)
		}
		return nil, fmt.Errorf("select tenant definition: %w", err)
	}
	if err := json.Unmarshal(raw, &definition.DSL); err != nil {
		return nil, fmt.Errorf("unmarshal definition dsl: %w", err)
	}
	return definition, nil
}

func (p *Postgres) CreateInstance(ctx context.Context, tenantID string, definition *Definition, input map[string]any) (*Instance, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if tenantID == "" {
		tenantID = "default"
	}

	inputRaw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal instance input: %w", err)
	}
	state := workflow.NewRuntimeState()
	state.StartedAt = time.Now().UTC()
	stateRaw, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("marshal instance state: %w", err)
	}

	instance := &Instance{
		TenantID:          tenantID,
		DefinitionID:      definition.ID,
		DefinitionVersion: definition.Version,
		Status:            workflow.InstancePending,
		Input:             input,
		State:             state,
	}
	err = p.pool.QueryRow(queryCtx, `
		INSERT INTO instances (tenant_id, definition_id, definition_version, status, input, state)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, tenant_id, version, created_at, updated_at
	`, tenantID, definition.ID, definition.Version, workflow.InstancePending, inputRaw, stateRaw).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.Version,
		&instance.CreatedAt,
		&instance.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert instance: %w", err)
	}
	return instance, nil
}

func (p *Postgres) GetInstance(ctx context.Context, id string) (*Instance, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	instance := &Instance{}
	var inputRaw []byte
	var stateRaw []byte
	err := p.pool.QueryRow(queryCtx, `
		SELECT id, tenant_id, definition_id, definition_version, status, input, current_step_id, state, version, created_at, updated_at
		FROM instances
		WHERE id = $1
	`, id).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.DefinitionID,
		&instance.DefinitionVersion,
		&instance.Status,
		&inputRaw,
		&instance.CurrentStepID,
		&stateRaw,
		&instance.Version,
		&instance.CreatedAt,
		&instance.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("instance not found: %w", err)
		}
		return nil, fmt.Errorf("select instance: %w", err)
	}
	if err := json.Unmarshal(inputRaw, &instance.Input); err != nil {
		return nil, fmt.Errorf("unmarshal instance input: %w", err)
	}
	if err := json.Unmarshal(stateRaw, &instance.State); err != nil {
		return nil, fmt.Errorf("unmarshal instance state: %w", err)
	}
	instance.State.Normalize()
	return instance, nil
}

func (p *Postgres) GetInstanceForTenant(ctx context.Context, tenantID string, id string) (*Instance, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if tenantID == "" {
		tenantID = "default"
	}

	instance := &Instance{}
	var inputRaw []byte
	var stateRaw []byte
	err := p.pool.QueryRow(queryCtx, `
		SELECT id, tenant_id, definition_id, definition_version, status, input, current_step_id, state, version, created_at, updated_at
		FROM instances
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, id).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.DefinitionID,
		&instance.DefinitionVersion,
		&instance.Status,
		&inputRaw,
		&instance.CurrentStepID,
		&stateRaw,
		&instance.Version,
		&instance.CreatedAt,
		&instance.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("instance not found: %w", err)
		}
		return nil, fmt.Errorf("select tenant instance: %w", err)
	}
	if err := json.Unmarshal(inputRaw, &instance.Input); err != nil {
		return nil, fmt.Errorf("unmarshal instance input: %w", err)
	}
	if err := json.Unmarshal(stateRaw, &instance.State); err != nil {
		return nil, fmt.Errorf("unmarshal instance state: %w", err)
	}
	instance.State.Normalize()
	return instance, nil
}

func (p *Postgres) ClaimInstance(ctx context.Context, id string, owner string, leaseUntil time.Time) (*Instance, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	instance := &Instance{}
	var inputRaw []byte
	var stateRaw []byte
	err := p.pool.QueryRow(queryCtx, `
		UPDATE instances
		SET locked_by = $2,
		    locked_until = $3
		WHERE id = $1
		  AND status NOT IN ('completed', 'failed')
		  AND (locked_until IS NULL OR locked_until <= now() OR locked_by = $2)
		RETURNING id, tenant_id, definition_id, definition_version, status, input, current_step_id, state, version, created_at, updated_at
	`, id, owner, leaseUntil).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.DefinitionID,
		&instance.DefinitionVersion,
		&instance.Status,
		&inputRaw,
		&instance.CurrentStepID,
		&stateRaw,
		&instance.Version,
		&instance.CreatedAt,
		&instance.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("claim instance: %w", ErrInstanceBusy)
		}
		return nil, fmt.Errorf("claim instance: %w", err)
	}
	if err := json.Unmarshal(inputRaw, &instance.Input); err != nil {
		return nil, fmt.Errorf("unmarshal instance input: %w", err)
	}
	if err := json.Unmarshal(stateRaw, &instance.State); err != nil {
		return nil, fmt.Errorf("unmarshal instance state: %w", err)
	}
	instance.State.Normalize()
	return instance, nil
}

func (p *Postgres) ReleaseInstance(ctx context.Context, id string, owner string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := p.pool.Exec(queryCtx, `
		UPDATE instances
		SET locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1 AND locked_by = $2
	`, id, owner); err != nil {
		return fmt.Errorf("release instance: %w", err)
	}
	return nil
}

func (p *Postgres) ListRecoverableInstances(ctx context.Context, limit int) ([]Instance, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = 100
	}

	rows, err := p.pool.Query(queryCtx, `
		SELECT id, tenant_id, definition_id, definition_version, status, input, current_step_id, state, version, created_at, updated_at
		FROM instances
		WHERE status IN ('pending', 'running', 'waiting_timer', 'compensating')
		  AND (locked_until IS NULL OR locked_until <= now())
		ORDER BY updated_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("select recoverable instances: %w", err)
	}
	defer rows.Close()

	instances := make([]Instance, 0)
	for rows.Next() {
		instance, err := scanInstance(rows)
		if err != nil {
			return nil, fmt.Errorf("scan recoverable instance: %w", err)
		}
		instances = append(instances, *instance)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recoverable instances: %w", err)
	}
	return instances, nil
}

func (p *Postgres) UpdateInstance(ctx context.Context, instance *Instance, expectedVersion int) error {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	stateRaw, err := json.Marshal(instance.State)
	if err != nil {
		return fmt.Errorf("marshal instance state: %w", err)
	}
	command, err := p.pool.Exec(queryCtx, `
		UPDATE instances
		SET status = $1,
		    current_step_id = $2,
		    state = $3,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $4 AND version = $5
	`, instance.Status, instance.CurrentStepID, stateRaw, instance.ID, expectedVersion)
	if err != nil {
		return fmt.Errorf("update instance: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("update instance optimistic lock: %w", ErrOptimisticLock)
	}
	instance.Version = expectedVersion + 1
	return nil
}

func (p *Postgres) AppendHistory(ctx context.Context, history *History) (*History, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	inputRaw, err := json.Marshal(history.Input)
	if err != nil {
		return nil, fmt.Errorf("marshal history input: %w", err)
	}
	outputRaw, err := json.Marshal(history.Output)
	if err != nil {
		return nil, fmt.Errorf("marshal history output: %w", err)
	}
	if history.StartedAt.IsZero() {
		history.StartedAt = time.Now().UTC()
	}

	out := *history
	err = p.pool.QueryRow(queryCtx, `
		INSERT INTO execution_history (instance_id, step_id, status, input, output, error, attempt, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, history.InstanceID, history.StepID, history.Status, inputRaw, outputRaw, history.Error, history.Attempt, history.StartedAt, history.CompletedAt).Scan(&out.ID)
	if err != nil {
		return nil, fmt.Errorf("insert execution history: %w", err)
	}
	return &out, nil
}

func (p *Postgres) CompleteHistory(ctx context.Context, historyID string, status string, output map[string]any, stepErr *string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	outputRaw, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("marshal history output: %w", err)
	}
	command, err := p.pool.Exec(queryCtx, `
		UPDATE execution_history
		SET status = $1,
		    output = $2,
		    error = $3,
		    completed_at = now()
		WHERE id = $4
	`, status, outputRaw, stepErr, historyID)
	if err != nil {
		return fmt.Errorf("complete execution history: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("complete execution history: %w", pgx.ErrNoRows)
	}
	return nil
}

func (p *Postgres) ListHistory(ctx context.Context, instanceID string) ([]History, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := p.pool.Query(queryCtx, `
		SELECT id, instance_id, step_id, status, input, output, error, attempt, started_at, completed_at
		FROM execution_history
		WHERE instance_id = $1
		ORDER BY started_at ASC
	`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("select execution history: %w", err)
	}
	defer rows.Close()

	history := make([]History, 0)
	for rows.Next() {
		item, err := scanHistory(rows)
		if err != nil {
			return nil, fmt.Errorf("scan execution history: %w", err)
		}
		history = append(history, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution history: %w", err)
	}
	return history, nil
}

func (p *Postgres) ListHistoryForTenant(ctx context.Context, tenantID string, instanceID string) ([]History, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if tenantID == "" {
		tenantID = "default"
	}

	rows, err := p.pool.Query(queryCtx, `
		SELECT h.id, h.instance_id, h.step_id, h.status, h.input, h.output, h.error, h.attempt, h.started_at, h.completed_at
		FROM execution_history h
		JOIN instances i ON i.id = h.instance_id
		WHERE i.tenant_id = $1 AND h.instance_id = $2
		ORDER BY h.started_at ASC
	`, tenantID, instanceID)
	if err != nil {
		return nil, fmt.Errorf("select tenant execution history: %w", err)
	}
	defer rows.Close()

	history := make([]History, 0)
	for rows.Next() {
		item, err := scanHistory(rows)
		if err != nil {
			return nil, fmt.Errorf("scan tenant execution history: %w", err)
		}
		history = append(history, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant execution history: %w", err)
	}
	return history, nil
}

func (p *Postgres) UpsertTimer(ctx context.Context, timer Timer) error {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := p.pool.Exec(queryCtx, `
		INSERT INTO timers (instance_id, step_id, fire_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (instance_id)
		DO UPDATE SET step_id = EXCLUDED.step_id, fire_at = EXCLUDED.fire_at, created_at = now()
	`, timer.InstanceID, timer.StepID, timer.FireAt)
	if err != nil {
		return fmt.Errorf("upsert timer: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteTimer(ctx context.Context, instanceID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if _, err := p.pool.Exec(queryCtx, `DELETE FROM timers WHERE instance_id = $1`, instanceID); err != nil {
		return fmt.Errorf("delete timer: %w", err)
	}
	return nil
}

func (p *Postgres) FireTimer(ctx context.Context, instanceID string) (*Timer, bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var timer Timer
	err := p.pool.QueryRow(queryCtx, `
		DELETE FROM timers
		WHERE instance_id = $1
		RETURNING instance_id, step_id, fire_at
	`, instanceID).Scan(&timer.InstanceID, &timer.StepID, &timer.FireAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("fire timer: %w", err)
	}
	return &timer, true, nil
}

func (p *Postgres) ListDueTimers(ctx context.Context, now time.Time, limit int) ([]Timer, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = 100
	}

	rows, err := p.pool.Query(queryCtx, `
		SELECT instance_id, step_id, fire_at
		FROM timers
		WHERE fire_at <= $1
		ORDER BY fire_at ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select due timers: %w", err)
	}
	defer rows.Close()

	timers := make([]Timer, 0)
	for rows.Next() {
		var timer Timer
		if err := rows.Scan(&timer.InstanceID, &timer.StepID, &timer.FireAt); err != nil {
			return nil, fmt.Errorf("scan due timer: %w", err)
		}
		timers = append(timers, timer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due timers: %w", err)
	}
	return timers, nil
}

func (p *Postgres) ClaimDueTimers(ctx context.Context, now time.Time, limit int) ([]Timer, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = 100
	}

	rows, err := p.pool.Query(queryCtx, `
		WITH due AS (
			SELECT instance_id, step_id, fire_at
			FROM timers
			WHERE fire_at <= $1
			ORDER BY fire_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM timers
		USING due
		WHERE timers.instance_id = due.instance_id
		RETURNING due.instance_id, due.step_id, due.fire_at
	`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("claim due timers: %w", err)
	}
	defer rows.Close()

	timers := make([]Timer, 0)
	for rows.Next() {
		var timer Timer
		if err := rows.Scan(&timer.InstanceID, &timer.StepID, &timer.FireAt); err != nil {
			return nil, fmt.Errorf("scan claimed timer: %w", err)
		}
		timers = append(timers, timer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed timers: %w", err)
	}
	return timers, nil
}

func scanInstance(rows pgx.Rows) (*Instance, error) {
	instance := &Instance{}
	var inputRaw []byte
	var stateRaw []byte
	if err := rows.Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.DefinitionID,
		&instance.DefinitionVersion,
		&instance.Status,
		&inputRaw,
		&instance.CurrentStepID,
		&stateRaw,
		&instance.Version,
		&instance.CreatedAt,
		&instance.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan instance row: %w", err)
	}
	if err := json.Unmarshal(inputRaw, &instance.Input); err != nil {
		return nil, fmt.Errorf("unmarshal instance input: %w", err)
	}
	if err := json.Unmarshal(stateRaw, &instance.State); err != nil {
		return nil, fmt.Errorf("unmarshal instance state: %w", err)
	}
	instance.State.Normalize()
	return instance, nil
}

func scanHistory(rows pgx.Rows) (History, error) {
	var history History
	var inputRaw []byte
	var outputRaw []byte
	if err := rows.Scan(
		&history.ID,
		&history.InstanceID,
		&history.StepID,
		&history.Status,
		&inputRaw,
		&outputRaw,
		&history.Error,
		&history.Attempt,
		&history.StartedAt,
		&history.CompletedAt,
	); err != nil {
		return History{}, fmt.Errorf("scan history row: %w", err)
	}
	if len(inputRaw) > 0 {
		if err := json.Unmarshal(inputRaw, &history.Input); err != nil {
			return History{}, fmt.Errorf("unmarshal history input: %w", err)
		}
	}
	if len(outputRaw) > 0 {
		if err := json.Unmarshal(outputRaw, &history.Output); err != nil {
			return History{}, fmt.Errorf("unmarshal history output: %w", err)
		}
	}
	return history, nil
}

var ErrOptimisticLock = errors.New("optimistic lock conflict")
