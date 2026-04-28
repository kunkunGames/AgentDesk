-- Keep the PostgreSQL auto-queue schema aligned with the SQLite migration.
ALTER TABLE auto_queue_runs
    ADD COLUMN IF NOT EXISTS phase_gate_grace_until TIMESTAMPTZ;

-- save_phase_gate_state_on_pg upserts by dispatch_id.
CREATE UNIQUE INDEX IF NOT EXISTS uq_aq_phase_gates_dispatch_id
    ON auto_queue_phase_gates(dispatch_id);
