-- Forward replay for the legacy duplicate 0029_worker_nodes migration.
-- Keep the original 0029 file immutable; the runtime migrator skips that
-- duplicate version and applies this idempotent forward migration instead.

CREATE TABLE IF NOT EXISTS worker_nodes (
    instance_id        TEXT PRIMARY KEY,
    hostname           TEXT,
    process_id         INTEGER,
    role               TEXT NOT NULL DEFAULT 'auto',
    effective_role     TEXT NOT NULL DEFAULT 'worker',
    status             TEXT NOT NULL DEFAULT 'online',
    labels             JSONB NOT NULL DEFAULT '[]'::jsonb,
    capabilities       JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_heartbeat_at  TIMESTAMPTZ,
    started_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_nodes_status_heartbeat
    ON worker_nodes (status, last_heartbeat_at DESC);

CREATE INDEX IF NOT EXISTS idx_worker_nodes_effective_role
    ON worker_nodes (effective_role);
