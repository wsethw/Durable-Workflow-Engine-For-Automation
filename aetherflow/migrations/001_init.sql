CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    dsl JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX ux_definitions_tenant_name_version ON definitions(tenant_id, name, version);
CREATE UNIQUE INDEX ux_definitions_tenant_id ON definitions(tenant_id, id);
CREATE INDEX idx_definitions_tenant ON definitions(tenant_id, id);

CREATE TABLE instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL DEFAULT 'default',
    definition_id UUID NOT NULL REFERENCES definitions(id),
    definition_version INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    input JSONB NOT NULL,
    current_step_id TEXT,
    state JSONB NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 0,
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_instances_tenant ON instances(tenant_id, id);
CREATE INDEX idx_instances_recoverable ON instances(status, updated_at) WHERE status IN ('pending', 'running', 'waiting_timer', 'compensating');
CREATE INDEX idx_instances_lock_expiry ON instances(locked_until) WHERE locked_until IS NOT NULL;
ALTER TABLE instances
    ADD CONSTRAINT fk_instances_tenant_definition
    FOREIGN KEY (tenant_id, definition_id)
    REFERENCES definitions(tenant_id, id);

CREATE TABLE execution_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES instances(id),
    step_id TEXT NOT NULL,
    status TEXT NOT NULL,
    input JSONB,
    output JSONB,
    error TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    CONSTRAINT fk_instance FOREIGN KEY(instance_id) REFERENCES instances(id)
);
CREATE INDEX idx_history_instance ON execution_history(instance_id, started_at);

CREATE TABLE timers (
    instance_id UUID PRIMARY KEY REFERENCES instances(id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    fire_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_timers_due ON timers(fire_at) WHERE fire_at IS NOT NULL;
