CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    dsl JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(name, version)
);

CREATE TABLE instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    definition_id UUID NOT NULL REFERENCES definitions(id),
    definition_version INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    input JSONB NOT NULL,
    current_step_id TEXT,
    state JSONB NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_instances_status ON instances(status) WHERE status IN ('running', 'waiting_timer');

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
