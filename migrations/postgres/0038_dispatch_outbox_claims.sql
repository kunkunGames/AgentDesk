-- Forward replay for the legacy duplicate 0030_dispatch_outbox_claims migration.
-- Keep the original 0030 file immutable; the runtime migrator skips that
-- duplicate version and applies this idempotent forward migration instead.

ALTER TABLE dispatch_outbox
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_owner TEXT;

CREATE INDEX IF NOT EXISTS idx_dispatch_outbox_status_claimed_at
    ON dispatch_outbox (status, claimed_at, next_attempt_at, id);
