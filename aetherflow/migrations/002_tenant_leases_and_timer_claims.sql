ALTER TABLE definitions
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

ALTER TABLE instances
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS locked_by TEXT,
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

ALTER TABLE definitions
    DROP CONSTRAINT IF EXISTS definitions_name_version_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_definitions_tenant_name_version
    ON definitions(tenant_id, name, version);

CREATE UNIQUE INDEX IF NOT EXISTS ux_definitions_tenant_id
    ON definitions(tenant_id, id);

CREATE INDEX IF NOT EXISTS idx_definitions_tenant
    ON definitions(tenant_id, id);

CREATE INDEX IF NOT EXISTS idx_instances_tenant
    ON instances(tenant_id, id);

CREATE INDEX IF NOT EXISTS idx_instances_recoverable
    ON instances(status, updated_at)
    WHERE status IN ('pending', 'running', 'waiting_timer', 'compensating');

CREATE INDEX IF NOT EXISTS idx_instances_lock_expiry
    ON instances(locked_until)
    WHERE locked_until IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_instances_tenant_definition'
    ) THEN
        ALTER TABLE instances
            ADD CONSTRAINT fk_instances_tenant_definition
            FOREIGN KEY (tenant_id, definition_id)
            REFERENCES definitions(tenant_id, id);
    END IF;
END $$;
