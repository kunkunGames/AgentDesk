-- Routines: durable, scheduler-driven automation independent of policy hooks.
--
-- `routines` is the definition + state row (one per attached routine).
-- `routine_runs` is the run history (one per execution attempt).
--
-- Design notes:
--   * in_flight_run_id guards against duplicate concurrent runs (SET on claim,
--     cleared on finish/fail/interrupt). Running rows carry a lease; executors
--     must heartbeat that lease while executing. Boot recovery only interrupts
--     expired leases and only clears the parent lock when it still points at
--     the interrupted run.
--   * checkpoint/last_result are the only durable state payload columns;
--     no separate checkpoint or observation table is created in this migration.
--   * execution_strategy 'fresh'/'persistent' — 'fresh' does NOT guarantee
--     provider context reset until RoutineAgentExecutor verifies it;
--     fresh_context_guaranteed is surfaced in the API response, not here.

CREATE TABLE IF NOT EXISTS routines (
    id                  TEXT PRIMARY KEY,
    agent_id            TEXT,
    script_ref          TEXT NOT NULL,
    name                TEXT NOT NULL,
    -- 'enabled' | 'paused' | 'detached'
    status              TEXT NOT NULL DEFAULT 'enabled',
    -- 'fresh' | 'persistent'
    execution_strategy  TEXT NOT NULL DEFAULT 'fresh',
    -- @every/every duration expression or NULL for manual-only
    schedule            TEXT,
    next_due_at         TIMESTAMPTZ,
    last_run_at         TIMESTAMPTZ,
    last_result         TEXT,
    checkpoint          JSONB,
    in_flight_run_id    TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS routine_runs (
    id                  TEXT PRIMARY KEY,
    routine_id          TEXT NOT NULL REFERENCES routines(id),
    -- 'running' | 'succeeded' | 'failed' | 'skipped' | 'paused' | 'interrupted'
    status              TEXT NOT NULL DEFAULT 'running',
    -- RoutineAction kind: 'complete' | 'agent' | 'skip' | 'pause'
    action              TEXT,
    turn_id             TEXT,
    lease_expires_at    TIMESTAMPTZ,
    result_json         JSONB,
    error               TEXT,
    -- 'ok' | 'failed' | 'skipped' | NULL (not attempted yet)
    discord_log_status  TEXT,
    discord_log_error   TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE IF EXISTS routine_runs
    ADD COLUMN IF NOT EXISTS discord_log_status TEXT;

ALTER TABLE IF EXISTS routine_runs
    ADD COLUMN IF NOT EXISTS discord_log_error TEXT;

CREATE INDEX IF NOT EXISTS idx_routines_agent_status
    ON routines(agent_id, status);

-- Partial index for the due scan: only enabled routines with a schedule.
CREATE INDEX IF NOT EXISTS idx_routines_due_scan
    ON routines(next_due_at)
    WHERE status = 'enabled' AND next_due_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_routine_runs_routine_id
    ON routine_runs(routine_id);

-- Partial index for boot recovery and in-flight deduplication.
CREATE INDEX IF NOT EXISTS idx_routine_runs_running
    ON routine_runs(routine_id)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_routine_runs_running_lease
    ON routine_runs(lease_expires_at)
    WHERE status = 'running' AND lease_expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_routine_runs_running_agent
    ON routine_runs(started_at)
    WHERE status = 'running' AND action = 'agent' AND turn_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_routine_runs_turn_id
    ON routine_runs(turn_id)
    WHERE turn_id IS NOT NULL;
