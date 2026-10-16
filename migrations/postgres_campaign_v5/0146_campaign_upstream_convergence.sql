-- Forward convergence of immutable PostgreSQL histories.
-- Generated and verified by scripts/check_migration_convergence.py.
-- Historical receipts are preserved; only this version is recorded.

-- BEGIN migrations/postgres/0131_sessions_hosted_execution.sql sha256=48b3814552340658f95f27ec23ee689a8af8a0a8c803bb419b44594e1b30d76d
-- Session-lifetime record of a hosted (Herdr) execution. NULL marks a legacy row
-- and stays the value for every existing and newly inserted row.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS hosted_execution JSONB;

-- Ordinary cleanup may delete only a legacy row or a retired v1 record; any other
-- payload, including one this schema cannot read, keeps the row.
CREATE OR REPLACE FUNCTION agentdesk_hosted_execution_deletable(payload JSONB)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT payload IS NULL
        OR COALESCE(
            jsonb_typeof(payload) = 'object'
                AND payload -> 'schema' = '1'::jsonb
                AND payload ->> 'state' = 'retired',
            FALSE
        )
$$;

-- END migrations/postgres/0131_sessions_hosted_execution.sql

-- BEGIN migrations/postgres/0133_o_channel_homes.sql sha256=8abee1a7c56480b0f90954c1c64bb0a725104313e143d8d66bcd67941b6b79c2
-- One row per delegated channel. While the row exists it is the only source of the channel's
-- O home; no row means the channel stays on the gateway rules.
CREATE TABLE IF NOT EXISTS o_channel_homes (
    channel_id TEXT PRIMARY KEY CHECK (BTRIM(channel_id) <> ''),
    provider TEXT NOT NULL CHECK (BTRIM(provider) <> ''),
    state TEXT NOT NULL CHECK (
        state IN ('releasing', 'released', 'worker', 'reclaiming', 'reclaimed', 'orphaned')
    ),
    holder TEXT CHECK (BTRIM(holder) <> ''),
    target TEXT CHECK (BTRIM(target) <> ''),
    epoch BIGINT NOT NULL CHECK (epoch >= 1),
    renewed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    detail TEXT,
    -- Exactly the holding states name a holder, and a holder always carries its lease time.
    CONSTRAINT o_channel_homes_holder_by_state CHECK (
        (state IN ('releasing', 'worker', 'reclaiming')) = (holder IS NOT NULL)
        AND (holder IS NULL OR renewed_at IS NOT NULL)
    ),
    -- A hand-off names its receiver; a worker-owned row has nobody to hand to.
    CONSTRAINT o_channel_homes_target_by_state CHECK (
        CASE state
            WHEN 'worker' THEN target IS NULL
            WHEN 'orphaned' THEN TRUE
            ELSE target IS NOT NULL
        END
        AND (holder IS NULL OR target IS NULL OR holder <> target)
    )
);

-- END migrations/postgres/0133_o_channel_homes.sql

-- BEGIN migrations/postgres/0134_channel_home_epochs.sql sha256=bce70458c71e133d49de7d1f77215d4d56053c8f452c2f9a530e432b09c88e77
-- Intake rows routed to a delegated channel carry the home epoch they were routed under; a claim
-- runs only while the channel's row still names that epoch. NULL is a gateway-rule row.
ALTER TABLE intake_outbox ADD COLUMN IF NOT EXISTS home_epoch BIGINT;

-- Every home epoch comes from one sequence, so a deleted and re-delegated channel never repeats an
-- epoch an older row or intake may still carry.
CREATE SEQUENCE IF NOT EXISTS o_channel_home_epochs;
SELECT setval(
    'o_channel_home_epochs',
    COALESCE((SELECT MAX(epoch) FROM o_channel_homes), 0) + 1,
    false
);

-- END migrations/postgres/0134_channel_home_epochs.sql

-- BEGIN migrations/postgres/0135_channel_clear_boundary_native_correlation.sql sha256=174c706f1328c48b7d46104d63cdd2f3394338b65b07ec7852af00a6f546dba8
-- Native clear correlation on the channel clear boundary row.
--
-- A native clear writes `native_clear_generation` and its opaque ticket in the
-- same statement that advances `clear_generation`, and sets
-- `native_clear_resolved_at` once the clear's effect is durable. Other boundary
-- writers never touch these columns, so a later generation supersedes the ticket.
--
-- No defaults, no backfill: NULL means the row carries no native correlation.
ALTER TABLE channel_session_clear_boundaries
    ADD COLUMN IF NOT EXISTS native_clear_generation BIGINT,
    ADD COLUMN IF NOT EXISTS native_clear_ticket JSONB,
    ADD COLUMN IF NOT EXISTS native_clear_resolved_at TIMESTAMPTZ;

-- A correlation is either absent or carries both its generation and ticket.
ALTER TABLE channel_session_clear_boundaries
    ADD CONSTRAINT chk_channel_clear_boundary_native_complete CHECK (
        (native_clear_generation IS NULL
            AND native_clear_ticket IS NULL
            AND native_clear_resolved_at IS NULL)
        OR (native_clear_generation IS NOT NULL
            AND native_clear_ticket IS NOT NULL)
    );

-- END migrations/postgres/0135_channel_clear_boundary_native_correlation.sql

-- BEGIN migrations/postgres/0136_request_replay_disposition.sql sha256=c8ed6585fadc1ec23945145ff609c4989e9bfc8e36d8f5ae28ef9aacc71a2d48
-- Durable request authority fences replay after a provider start across restore, claim and retries.
-- This migration adds schema and fences; no production writer sets a disposition yet.
ALTER TABLE intake_outbox
    ADD COLUMN IF NOT EXISTS replay_only BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS replay_disposition TEXT,
    ADD COLUMN IF NOT EXISTS replay_source_message_ids TEXT[],
    ADD COLUMN IF NOT EXISTS replay_episode_nonce TEXT,
    ADD COLUMN IF NOT EXISTS replay_owner_incarnation TEXT,
    ADD COLUMN IF NOT EXISTS replay_request_hash TEXT,
    ADD COLUMN IF NOT EXISTS replay_request_key TEXT,
    ADD COLUMN IF NOT EXISTS replay_hold_reason TEXT,
    ADD COLUMN IF NOT EXISTS replay_preserved JSONB;

-- A disposition names every absorbed source; a replay-only receipt never enters delivery or claim.
ALTER TABLE intake_outbox
    ADD CONSTRAINT intake_outbox_replay_disposition_check CHECK (
        CASE WHEN replay_disposition IS NULL
             THEN NOT replay_only
             ELSE replay_disposition IN ('registered_not_started', 'started_unclassified',
                      'startup_failed_no_effect', 'classified_normal', 'withheld')
                  AND replay_source_message_ids IS NOT NULL
                  AND (NOT replay_only OR (status = 'unknown' AND attempt_no = 1))
        END AND (replay_source_message_ids IS NULL OR (
            COALESCE(user_msg_id = ANY(replay_source_message_ids), FALSE)
            AND cardinality(replay_source_message_ids) > 0
            AND array_position(replay_source_message_ids, NULL) IS NULL
            AND array_position(replay_source_message_ids, '') IS NULL))
    ) NOT VALID;

-- Same name as the replaced tuple constraint so the Rust conflict classifier keeps matching it;
-- replay-only receipts sit outside the delivery attempt sequence.
ALTER TABLE intake_outbox DROP CONSTRAINT intake_outbox_unique_message_attempt;
CREATE UNIQUE INDEX intake_outbox_unique_message_attempt
    ON intake_outbox (channel_id, user_msg_id, attempt_no)
    WHERE NOT replay_only;

CREATE UNIQUE INDEX idx_intake_outbox_replay_request_key
    ON intake_outbox (replay_request_key) WHERE replay_request_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_intake_outbox_replay_blocked_sources
    ON intake_outbox USING GIN (replay_source_message_ids)
    WHERE replay_disposition IN ('started_unclassified', 'startup_failed_no_effect', 'withheld');

-- Dispatch projection of the receipt disposition, kept equal to the receipt by the fences below.
ALTER TABLE task_dispatches
    ADD COLUMN IF NOT EXISTS replay_receipt_id BIGINT,
    ADD COLUMN IF NOT EXISTS replay_disposition TEXT;
ALTER TABLE task_dispatches
    ADD CONSTRAINT task_dispatches_replay_projection_check
    CHECK (replay_receipt_id IS NOT NULL OR replay_disposition IS NULL) NOT VALID;
CREATE INDEX IF NOT EXISTS idx_task_dispatches_replay_receipt
    ON task_dispatches (replay_receipt_id)
    WHERE replay_receipt_id IS NOT NULL;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS current_replay_receipt_id BIGINT,
    ADD COLUMN IF NOT EXISTS replay_episode_nonce TEXT;

-- Every state but registered and classified forbids an ordinary automatic rerun.
CREATE OR REPLACE FUNCTION replay_disposition_blocks_rerun(disposition TEXT) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(disposition IN ('started_unclassified', 'startup_failed_no_effect', 'withheld'),
                    FALSE)
$$;

CREATE OR REPLACE FUNCTION replay_sources_blocked(p_provider TEXT, p_channel TEXT, p_sources TEXT[],
                                                  p_except BIGINT DEFAULT NULL)
RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM intake_outbox r
         WHERE r.replay_disposition IN ('started_unclassified', 'startup_failed_no_effect',
                                        'withheld')
           AND r.channel_id = p_channel
           AND r.replay_source_message_ids && p_sources
           AND lower(btrim(r.provider)) = lower(btrim(p_provider))
           AND r.id IS DISTINCT FROM p_except)
$$;

CREATE OR REPLACE FUNCTION replay_dispatch_blocked(p_dispatch_id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM task_dispatches d
                    WHERE d.id = p_dispatch_id
                      AND replay_disposition_blocks_rerun(d.replay_disposition))
$$;

CREATE OR REPLACE FUNCTION replay_session_held(p_receipt_id BIGINT, p_dispatch_id TEXT)
RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
    SELECT replay_dispatch_blocked(p_dispatch_id)
        OR EXISTS (SELECT 1 FROM intake_outbox r
                    WHERE r.id = p_receipt_id
                      AND replay_disposition_blocks_rerun(r.replay_disposition))
$$;

-- Sorted source locks order concurrent authority writes and old-writer admissions alike.
CREATE OR REPLACE FUNCTION replay_lock_sources(p_provider TEXT, p_channel TEXT, p_sources TEXT[])
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE source TEXT;
BEGIN
    FOR source IN SELECT DISTINCT unnest(p_sources) ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(
            jsonb_build_array(lower(btrim(p_provider)), p_channel, source)::TEXT, 6008));
    END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION replay_entry_blocked(p_entry TEXT, p_dispatch TEXT)
RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
    SELECT replay_dispatch_blocked(p_dispatch) OR EXISTS (
        SELECT 1 FROM auto_queue_entry_dispatch_history h
         WHERE h.entry_id = p_entry AND replay_dispatch_blocked(h.dispatch_id))
$$;

-- Forward-only lifecycle; a no-effect start re-enters started only under a new episode nonce.
CREATE OR REPLACE FUNCTION replay_disposition_transition_allowed(old_d TEXT, new_d TEXT,
                                                                 nonce_changed BOOLEAN)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(old_d IS NULL
        OR (old_d = new_d AND (NOT nonce_changed OR old_d = 'registered_not_started'))
        OR (old_d = 'registered_not_started' AND new_d IN ('started_unclassified', 'withheld'))
        OR (old_d = 'started_unclassified'
            AND new_d IN ('withheld', 'startup_failed_no_effect', 'classified_normal'))
        OR (old_d = 'startup_failed_no_effect'
            AND (new_d = 'withheld' OR (new_d = 'started_unclassified' AND nonce_changed))),
        FALSE)
$$;

CREATE OR REPLACE FUNCTION intake_outbox_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    refusal TEXT;
BEGIN
    IF TG_OP = 'INSERT' AND NEW.parent_outbox_id IS NOT NULL THEN
        SELECT COALESCE(p.replay_source_message_ids, NEW.replay_source_message_ids)
          INTO NEW.replay_source_message_ids FROM intake_outbox p WHERE p.id = NEW.parent_outbox_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        PERFORM replay_lock_sources(NEW.provider, NEW.channel_id,
            COALESCE(NEW.replay_source_message_ids, ARRAY[NEW.user_msg_id]));
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF replay_disposition_blocks_rerun(OLD.replay_disposition) THEN
            refusal := format('held request row %s cannot be deleted', OLD.id);
        END IF;
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.parent_outbox_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM intake_outbox p WHERE p.id = NEW.parent_outbox_id
              AND (p.replay_only OR replay_disposition_blocks_rerun(p.replay_disposition)
                   OR replay_sources_blocked(p.provider, p.channel_id, COALESCE(p.replay_source_message_ids, ARRAY[p.user_msg_id])))) THEN
            refusal := format('started or replay-only receipt %s cannot parent a retry', NEW.parent_outbox_id);
        ELSIF replay_sources_blocked(NEW.provider, NEW.channel_id,
                                     COALESCE(NEW.replay_source_message_ids,
                                              ARRAY[NEW.user_msg_id])) THEN
            refusal := format('a source of message %s already started', NEW.user_msg_id);
        END IF;
    ELSIF OLD.replay_disposition IS NULL AND NEW.replay_disposition IS NOT NULL
          AND replay_sources_blocked(NEW.provider, NEW.channel_id,
                                     NEW.replay_source_message_ids, NEW.id) THEN
        refusal := format('row %s cannot register a source that already started', NEW.id);
    ELSIF (OLD.replay_disposition IS NOT NULL OR OLD.replay_source_message_ids IS NOT NULL) AND (
            NEW.replay_source_message_ids IS DISTINCT FROM OLD.replay_source_message_ids
            OR NEW.provider IS DISTINCT FROM OLD.provider
            OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
            OR NEW.user_msg_id IS DISTINCT FROM OLD.user_msg_id
            OR NEW.user_text IS DISTINCT FROM OLD.user_text
            OR NEW.replay_only IS DISTINCT FROM OLD.replay_only
            OR (OLD.replay_disposition IS NOT NULL AND (
                NEW.replay_request_key IS DISTINCT FROM OLD.replay_request_key
                OR NEW.replay_request_hash IS DISTINCT FROM OLD.replay_request_hash
                OR NOT replay_disposition_transition_allowed(
                    OLD.replay_disposition, NEW.replay_disposition,
                    NEW.replay_episode_nonce IS DISTINCT FROM OLD.replay_episode_nonce)))) THEN
        refusal := format('row %s cannot weaken its %s request', OLD.id, OLD.replay_disposition);
    ELSIF OLD.replay_disposition = 'withheld' AND (
            (OLD.replay_preserved IS NOT NULL
                 AND NOT COALESCE(NEW.replay_preserved @> OLD.replay_preserved, FALSE))
            OR (OLD.replay_hold_reason IS NOT NULL
                 AND NEW.replay_hold_reason IS DISTINCT FROM OLD.replay_hold_reason)) THEN
        refusal := format('held row %s keeps its preserved result', OLD.id);
    ELSIF replay_disposition_blocks_rerun(OLD.replay_disposition)
          AND ((NEW.status IN ('pending', 'claimed', 'failed_pre_accept', 'failed_post_accept')
                AND NEW.status IS DISTINCT FROM OLD.status)
               OR NEW.claim_owner IS DISTINCT FROM OLD.claim_owner
               OR NEW.claimed_at IS DISTINCT FROM OLD.claimed_at
               OR NEW.target_instance_id IS DISTINCT FROM OLD.target_instance_id) THEN
        refusal := format('started row %s cannot return to %s', OLD.id, NEW.status);
    ELSIF replay_disposition_blocks_rerun(NEW.replay_disposition)
          AND NOT replay_disposition_blocks_rerun(OLD.replay_disposition)
          AND replay_sources_blocked(NEW.provider, NEW.channel_id, NEW.replay_source_message_ids, NEW.id) THEN
        refusal := format('a source of row %s already started elsewhere', NEW.id);
    ELSIF NEW.status IN ('pending', 'claimed', 'accepted', 'spawned')
          AND (NEW.status IS DISTINCT FROM OLD.status
               OR NEW.claim_owner IS DISTINCT FROM OLD.claim_owner
               OR NEW.target_instance_id IS DISTINCT FROM OLD.target_instance_id)
          AND replay_sources_blocked(NEW.provider, NEW.channel_id,
              COALESCE(NEW.replay_source_message_ids, ARRAY[NEW.user_msg_id]), NEW.id) THEN
        refusal := format('row %s shares a started source', NEW.id);
    END IF;
    IF refusal IS NOT NULL THEN
        RAISE EXCEPTION 'replay_disposition_fence: %', refusal
            USING CONSTRAINT = 'replay_disposition_fence';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_intake_outbox_replay_fence_insert
    BEFORE INSERT ON intake_outbox
    FOR EACH ROW EXECUTE FUNCTION intake_outbox_replay_fence();
CREATE TRIGGER trg_intake_outbox_replay_fence_update
    BEFORE UPDATE ON intake_outbox
    FOR EACH ROW
    EXECUTE FUNCTION intake_outbox_replay_fence();
CREATE TRIGGER trg_intake_outbox_replay_fence_delete
    BEFORE DELETE ON intake_outbox
    FOR EACH ROW WHEN (OLD.replay_disposition IS NOT NULL)
    EXECUTE FUNCTION intake_outbox_replay_fence();

-- The dispatch projection follows its receipt in the same transaction.
CREATE OR REPLACE FUNCTION intake_outbox_replay_project() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE task_dispatches
       SET replay_disposition = NEW.replay_disposition
     WHERE replay_receipt_id = NEW.id
       AND replay_disposition IS DISTINCT FROM NEW.replay_disposition;
    RETURN NULL;
END
$$;

CREATE TRIGGER trg_intake_outbox_replay_project
    AFTER UPDATE OF replay_disposition ON intake_outbox
    FOR EACH ROW WHEN (OLD.replay_disposition IS DISTINCT FROM NEW.replay_disposition)
    EXECUTE FUNCTION intake_outbox_replay_project();

-- A held dispatch keeps its status, claim and receipt; its projection is always the receipt's.
CREATE OR REPLACE FUNCTION task_dispatches_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    refusal TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF replay_disposition_blocks_rerun(OLD.replay_disposition) THEN
            RAISE EXCEPTION 'replay_disposition_fence: held dispatch % cannot be deleted', OLD.id
                USING CONSTRAINT = 'replay_disposition_fence';
        END IF;
        RETURN OLD;
    END IF;
    PERFORM 1 FROM task_dispatches WHERE id = NEW.parent_dispatch_id FOR UPDATE;
    IF TG_OP = 'INSERT' AND (
        replay_dispatch_blocked(NEW.parent_dispatch_id) OR EXISTS (
            SELECT 1 FROM auto_queue_entries e WHERE e.kanban_card_id = NEW.kanban_card_id
             AND replay_entry_blocked(e.id, e.dispatch_id))) THEN
        refusal := format('dispatch %s inherits held request lineage', NEW.id);
    END IF;
    IF NEW.replay_receipt_id IS NOT NULL THEN
        PERFORM 1 FROM intake_outbox r WHERE r.id = NEW.replay_receipt_id FOR SHARE;
        IF NOT FOUND THEN
            refusal := format('dispatch %s names missing receipt %s', NEW.id, NEW.replay_receipt_id);
        END IF;
        NEW.replay_disposition := (SELECT r.replay_disposition FROM intake_outbox r
                                    WHERE r.id = NEW.replay_receipt_id);
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.replay_receipt_id IS NOT NULL
       AND NEW.replay_receipt_id IS DISTINCT FROM OLD.replay_receipt_id THEN
        refusal := format('dispatch %s cannot change its receipt', OLD.id);
    ELSIF TG_OP = 'UPDATE' AND replay_disposition_blocks_rerun(OLD.replay_disposition)
          AND replay_disposition_blocks_rerun(NEW.replay_disposition)
          AND (NEW.status IS DISTINCT FROM OLD.status
               OR NEW.claim_owner IS DISTINCT FROM OLD.claim_owner
               OR NEW.claimed_at IS DISTINCT FROM OLD.claimed_at
               OR NEW.parent_dispatch_id IS DISTINCT FROM OLD.parent_dispatch_id
               OR (OLD.result IS NOT NULL AND LEFT(NEW.result, LENGTH(OLD.result)) IS DISTINCT FROM OLD.result)
               OR NEW.claim_expires_at IS DISTINCT FROM OLD.claim_expires_at) THEN
        refusal := format('held dispatch %s keeps its status and claim', OLD.id);
    END IF;
    IF refusal IS NOT NULL THEN
        RAISE EXCEPTION 'replay_disposition_fence: %', refusal
            USING CONSTRAINT = 'replay_disposition_fence';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_task_dispatches_replay_fence_insert
    BEFORE INSERT ON task_dispatches
    FOR EACH ROW EXECUTE FUNCTION task_dispatches_replay_fence();
CREATE TRIGGER trg_task_dispatches_replay_fence_update
    BEFORE UPDATE ON task_dispatches
    FOR EACH ROW WHEN (OLD.replay_receipt_id IS NOT NULL OR NEW.replay_receipt_id IS NOT NULL)
    EXECUTE FUNCTION task_dispatches_replay_fence();
CREATE TRIGGER trg_task_dispatches_replay_fence_delete
    BEFORE DELETE ON task_dispatches
    FOR EACH ROW WHEN (OLD.replay_receipt_id IS NOT NULL)
    EXECUTE FUNCTION task_dispatches_replay_fence();

-- An entry on a held dispatch keeps its status, link and slot, so it is never requeued.
CREATE OR REPLACE FUNCTION auto_queue_entries_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM task_dispatches d WHERE d.id = OLD.dispatch_id OR EXISTS (
        SELECT 1 FROM auto_queue_entry_dispatch_history h
         WHERE h.entry_id = OLD.id AND h.dispatch_id = d.id) ORDER BY d.id FOR UPDATE;
    IF replay_entry_blocked(OLD.id, OLD.dispatch_id) THEN
        RAISE EXCEPTION 'replay_disposition_fence: entry % keeps held dispatch %',
            OLD.id, OLD.dispatch_id
            USING CONSTRAINT = 'replay_disposition_fence';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_auto_queue_entries_replay_fence_update
    BEFORE UPDATE ON auto_queue_entries
    FOR EACH ROW
    WHEN (NEW.status IS DISTINCT FROM OLD.status
          OR NEW.dispatch_id IS DISTINCT FROM OLD.dispatch_id
          OR NEW.slot_index IS DISTINCT FROM OLD.slot_index
          OR NEW.retry_count IS DISTINCT FROM OLD.retry_count
          OR NEW.run_id IS DISTINCT FROM OLD.run_id)
    EXECUTE FUNCTION auto_queue_entries_replay_fence();
CREATE TRIGGER trg_auto_queue_entries_replay_fence_delete
    BEFORE DELETE ON auto_queue_entries
    FOR EACH ROW EXECUTE FUNCTION auto_queue_entries_replay_fence();

-- No new or re-armed provider-launching outbox action for a held dispatch.
CREATE OR REPLACE FUNCTION dispatch_outbox_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM task_dispatches WHERE id = NEW.dispatch_id FOR UPDATE;
    IF replay_dispatch_blocked(NEW.dispatch_id) THEN
        RAISE EXCEPTION 'replay_disposition_fence: % for held dispatch %',
            NEW.action, NEW.dispatch_id
            USING CONSTRAINT = 'replay_disposition_fence';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_dispatch_outbox_replay_fence_insert
    BEFORE INSERT ON dispatch_outbox
    FOR EACH ROW WHEN (NEW.action IN ('notify', 'followup'))
    EXECUTE FUNCTION dispatch_outbox_replay_fence();
CREATE TRIGGER trg_dispatch_outbox_replay_fence_rearm
    BEFORE UPDATE ON dispatch_outbox
    FOR EACH ROW
    WHEN (NEW.action IN ('notify', 'followup') AND NEW.status = 'pending'
          AND (OLD.status IS DISTINCT FROM 'pending' OR NEW.action IS DISTINCT FROM OLD.action
               OR NEW.dispatch_id IS DISTINCT FROM OLD.dispatch_id
               OR NEW.retry_count < OLD.retry_count
               OR NEW.claim_owner IS DISTINCT FROM OLD.claim_owner))
    EXECUTE FUNCTION dispatch_outbox_replay_fence();

-- A missing current link still retains its durable entry-to-dispatch history.
CREATE OR REPLACE FUNCTION auto_queue_history_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF replay_entry_blocked(NEW.entry_id, NULL)
           AND NOT EXISTS (SELECT 1 FROM auto_queue_entry_dispatch_history h
                            WHERE h.entry_id = NEW.entry_id AND h.dispatch_id = NEW.dispatch_id) THEN
            RAISE EXCEPTION 'replay_disposition_fence: entry % keeps its held lineage', NEW.entry_id
                USING CONSTRAINT = 'replay_disposition_fence';
        END IF;
        RETURN NEW;
    END IF;
    IF replay_dispatch_blocked(OLD.dispatch_id) THEN
        RAISE EXCEPTION 'replay_disposition_fence: history % keeps held dispatch %',
            OLD.id, OLD.dispatch_id USING CONSTRAINT = 'replay_disposition_fence';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER trg_auto_queue_history_replay_fence
    BEFORE INSERT OR UPDATE OR DELETE ON auto_queue_entry_dispatch_history
    FOR EACH ROW EXECUTE FUNCTION auto_queue_history_replay_fence();

-- Destructive session writers must fail in their transaction before a held request is detached.
CREATE OR REPLACE FUNCTION sessions_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fresh_handoff BOOLEAN := FALSE;
BEGIN
    IF NOT replay_session_held(OLD.current_replay_receipt_id, OLD.active_dispatch_id) THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        SELECT EXISTS (SELECT 1 FROM intake_outbox r
                        WHERE r.id = NEW.current_replay_receipt_id
                          AND r.replay_disposition = 'registered_not_started'
                          AND NEW.active_dispatch_id IS NULL
                          AND r.id IS DISTINCT FROM OLD.current_replay_receipt_id
                          AND r.replay_episode_nonce = NEW.replay_episode_nonce
                          AND r.channel_id = OLD.channel_id
                          AND lower(btrim(r.provider)) = lower(btrim(OLD.provider))
                          AND NOT replay_sources_blocked(r.provider, r.channel_id,
                                                        r.replay_source_message_ids))
          INTO fresh_handoff;
        IF NEW.claude_session_id IS NOT DISTINCT FROM OLD.claude_session_id
           AND NEW.raw_provider_session_id IS NOT DISTINCT FROM OLD.raw_provider_session_id
           AND NEW.session_key IS NOT DISTINCT FROM OLD.session_key
           AND NEW.cwd IS NOT DISTINCT FROM OLD.cwd
           AND NEW.instance_id IS NOT DISTINCT FROM OLD.instance_id
           AND NEW.provider IS NOT DISTINCT FROM OLD.provider AND NEW.channel_id IS NOT DISTINCT FROM OLD.channel_id
           AND NEW.identity_kind IS NOT DISTINCT FROM OLD.identity_kind AND NEW.discord_token_hash IS NOT DISTINCT FROM OLD.discord_token_hash
           AND NEW.status NOT IN ('idle', 'disconnected', 'aborted')
           AND (fresh_handoff OR (
               NEW.active_dispatch_id IS NOT DISTINCT FROM OLD.active_dispatch_id
               AND NEW.current_replay_receipt_id IS NOT DISTINCT FROM OLD.current_replay_receipt_id
               AND NEW.replay_episode_nonce IS NOT DISTINCT FROM OLD.replay_episode_nonce
               AND NEW.active_turn_nonce IS NOT DISTINCT FROM OLD.active_turn_nonce
               AND NEW.dispatched_origin_turn_nonce IS NOT DISTINCT FROM OLD.dispatched_origin_turn_nonce)) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'replay_disposition_fence: session % keeps its held request', OLD.id
        USING CONSTRAINT = 'replay_disposition_fence';
END
$$;
CREATE TRIGGER trg_sessions_replay_fence
    BEFORE UPDATE OR DELETE ON sessions
    FOR EACH ROW EXECUTE FUNCTION sessions_replay_fence();

-- END migrations/postgres/0136_request_replay_disposition.sql

-- BEGIN migrations/postgres/0137_request_replay_exact_attempt.sql sha256=d333ad80e1bce3e99c590d406f21682a5ca2ea7a97894663a36ee11a557d0634
-- Exact attempt transitions for replay receipts; 0136 fences stay as they are and only gain refusals.
-- A retried attempt records the nonce it replaced so the session can follow it exactly once.
ALTER TABLE intake_outbox ADD COLUMN IF NOT EXISTS replay_retry_of_nonce TEXT;

-- A start keeps its registered nonce, a retry names the nonce it replaces and its saved projection,
-- a classification keeps its nonce and a hold keeps a preserved result.
CREATE OR REPLACE FUNCTION intake_outbox_replay_exact_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.replay_disposition = 'started_unclassified'
        AND OLD.replay_disposition IS DISTINCT FROM 'started_unclassified'
        AND ((OLD.replay_disposition = 'registered_not_started'
              AND NEW.replay_episode_nonce IS DISTINCT FROM OLD.replay_episode_nonce)
             OR (OLD.replay_disposition = 'startup_failed_no_effect'
                 AND (NEW.replay_retry_of_nonce IS DISTINCT FROM OLD.replay_episode_nonce
                      OR NEW.replay_preserved #>> '{projection,nonce}'
                         IS DISTINCT FROM NEW.replay_episode_nonce))))
       OR (OLD.replay_disposition = 'started_unclassified'
           AND NEW.replay_disposition IN ('startup_failed_no_effect', 'classified_normal', 'withheld')
           AND NEW.replay_episode_nonce IS DISTINCT FROM OLD.replay_episode_nonce)
       OR (NEW.replay_disposition = 'withheld' AND OLD.replay_disposition IS DISTINCT FROM 'withheld'
           AND NEW.replay_preserved IS NULL)
       OR (NEW.replay_retry_of_nonce IS DISTINCT FROM OLD.replay_retry_of_nonce
           AND NOT (OLD.replay_disposition = 'startup_failed_no_effect'
                    AND NEW.replay_disposition = 'started_unclassified'))
       OR (OLD.replay_disposition NOT IN ('registered_not_started', 'startup_failed_no_effect')
           AND NEW.replay_preserved #> '{projection}' IS DISTINCT FROM OLD.replay_preserved #> '{projection}')
    THEN
        RAISE EXCEPTION 'replay_disposition_fence: row % breaks its exact % attempt',
            OLD.id, OLD.replay_disposition
            USING CONSTRAINT = 'replay_disposition_fence';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_intake_outbox_replay_exact_fence ON intake_outbox;
CREATE TRIGGER trg_intake_outbox_replay_exact_fence
    BEFORE UPDATE ON intake_outbox
    FOR EACH ROW WHEN (OLD.replay_disposition IS NOT NULL)
    EXECUTE FUNCTION intake_outbox_replay_exact_fence();

-- 0136 session fence plus one exception: an exact live retry moves the session to its receipt's
-- next nonce and drops only the stale resume selector.
CREATE OR REPLACE FUNCTION sessions_replay_fence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fresh_handoff BOOLEAN := FALSE; exact_retry BOOLEAN := FALSE;
BEGIN
    IF NOT replay_session_held(OLD.current_replay_receipt_id, OLD.active_dispatch_id) THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        SELECT EXISTS (SELECT 1 FROM intake_outbox r
                        WHERE r.id = NEW.current_replay_receipt_id
                          AND r.replay_disposition = 'registered_not_started'
                          AND NEW.active_dispatch_id IS NULL
                          AND r.id IS DISTINCT FROM OLD.current_replay_receipt_id
                          AND r.replay_episode_nonce = NEW.replay_episode_nonce
                          AND r.channel_id = OLD.channel_id
                          AND lower(btrim(r.provider)) = lower(btrim(OLD.provider))
                          AND NOT replay_sources_blocked(r.provider, r.channel_id,
                                                        r.replay_source_message_ids))
          INTO fresh_handoff;
        SELECT EXISTS (SELECT 1 FROM intake_outbox r
                        WHERE r.id = OLD.current_replay_receipt_id
                          AND r.id = NEW.current_replay_receipt_id
                          AND r.replay_disposition = 'started_unclassified'
                          AND r.replay_retry_of_nonce = OLD.replay_episode_nonce
                          AND r.replay_episode_nonce = NEW.replay_episode_nonce)
          INTO exact_retry;
        IF (NEW.claude_session_id IS NOT DISTINCT FROM OLD.claude_session_id
            OR (exact_retry AND NEW.claude_session_id IS NULL))
           AND (NEW.raw_provider_session_id IS NOT DISTINCT FROM OLD.raw_provider_session_id
                OR (exact_retry AND NEW.raw_provider_session_id IS NULL))
           AND NEW.session_key IS NOT DISTINCT FROM OLD.session_key
           AND NEW.cwd IS NOT DISTINCT FROM OLD.cwd
           AND NEW.instance_id IS NOT DISTINCT FROM OLD.instance_id
           AND NEW.provider IS NOT DISTINCT FROM OLD.provider AND NEW.channel_id IS NOT DISTINCT FROM OLD.channel_id
           AND NEW.identity_kind IS NOT DISTINCT FROM OLD.identity_kind AND NEW.discord_token_hash IS NOT DISTINCT FROM OLD.discord_token_hash
           AND NEW.status NOT IN ('idle', 'disconnected', 'aborted')
           AND (fresh_handoff OR (
               NEW.active_dispatch_id IS NOT DISTINCT FROM OLD.active_dispatch_id
               AND NEW.current_replay_receipt_id IS NOT DISTINCT FROM OLD.current_replay_receipt_id
               AND (exact_retry OR NEW.replay_episode_nonce IS NOT DISTINCT FROM OLD.replay_episode_nonce)
               AND NEW.active_turn_nonce IS NOT DISTINCT FROM OLD.active_turn_nonce
               AND NEW.dispatched_origin_turn_nonce IS NOT DISTINCT FROM OLD.dispatched_origin_turn_nonce)) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'replay_disposition_fence: session % keeps its held request', OLD.id
        USING CONSTRAINT = 'replay_disposition_fence';
END
$$;

-- END migrations/postgres/0137_request_replay_exact_attempt.sql

-- BEGIN migrations/postgres/0138_o_piece_delivery.sql sha256=7e834babe7a018f9a7313d1944d530a9c8e9da77e9b50c36ea39fc517274e528
-- Re-post budget of one O piece, shared by every node. A row exists only once the piece's first
-- post went uncertain or an operator adopted it; no row means the piece was never admitted.
CREATE TABLE IF NOT EXISTS o_piece_delivery (
    channel_id TEXT NOT NULL CHECK (channel_id ~ '^[1-9][0-9]*$'),
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    native_key TEXT NOT NULL CHECK (BTRIM(native_key) <> ''),
    kind TEXT NOT NULL CHECK (kind IN ('body', 'tool_result')),
    piece_index BIGINT NOT NULL CHECK (piece_index >= 0),
    payload TEXT NOT NULL CHECK (payload <> ''),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    identity_version INTEGER NOT NULL CHECK (identity_version >= 1),
    split_version INTEGER NOT NULL CHECK (split_version >= 1),
    original_anchor BIGINT NOT NULL CHECK (original_anchor >= 0),
    sender_id BIGINT NOT NULL CHECK (sender_id > 0),
    admitted_by TEXT NOT NULL CHECK (admitted_by IN ('uncertain', 'operator')),
    origin_serial BIGINT NOT NULL CHECK (origin_serial >= 0),
    origin_node TEXT NOT NULL CHECK (BTRIM(origin_node) <> ''),
    failure TEXT CHECK (
        failure IN ('not_found', 'unknown_at_cap', 'rejected', 'cap', 'cap_unknown')
    ),
    -- The last differing payload offered under this key; it never replaces the stored one.
    conflict_sha256 TEXT CHECK (conflict_sha256 ~ '^[0-9a-f]{64}$'),
    conflict_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision >= 1),
    admitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, provider, native_key, kind, piece_index),
    -- Codex tool results are never posted, so they are never re-posted either.
    CONSTRAINT o_piece_delivery_postable_kind CHECK (NOT (provider = 'codex' AND kind = 'tool_result')),
    CONSTRAINT o_piece_delivery_conflict_pair CHECK ((conflict_sha256 IS NULL) = (conflict_at IS NULL))
);

-- Consumed POST slots: slot 0 is the original, 1 and 2 the only further sends. A slot row is a
-- consumed fact and is never deleted or reused, whatever its result.
CREATE TABLE IF NOT EXISTS o_piece_attempts (
    channel_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    native_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    piece_index BIGINT NOT NULL,
    slot SMALLINT NOT NULL CHECK (slot IN (0, 1, 2)),
    grant_id UUID NOT NULL UNIQUE,
    intent TEXT NOT NULL CHECK (
        intent IN ('original', 'prior_retry', 'auto_reconfirm', 'operator_resume')
    ),
    approval_id TEXT CHECK (BTRIM(approval_id) <> ''),
    owner TEXT NOT NULL CHECK (BTRIM(owner) <> ''),
    run_id TEXT NOT NULL CHECK (BTRIM(run_id) <> ''),
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline TIMESTAMPTZ,
    result TEXT CHECK (result IN ('created', 'rejected', 'uncertain', 'not_sent', 'abandoned')),
    settled_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, provider, native_key, kind, piece_index, slot),
    FOREIGN KEY (channel_id, provider, native_key, kind, piece_index)
        REFERENCES o_piece_delivery (channel_id, provider, native_key, kind, piece_index)
        ON DELETE RESTRICT,
    CONSTRAINT o_piece_attempts_original_slot CHECK ((slot = 0) = (intent = 'original')),
    CONSTRAINT o_piece_attempts_approval CHECK (
        (intent = 'operator_resume') = (approval_id IS NOT NULL)
    ),
    -- An open attempt carries the deadline after which another holder may settle it.
    CONSTRAINT o_piece_attempts_settled CHECK (
        (result IS NULL) = (settled_at IS NULL) AND (result IS NOT NULL OR deadline IS NOT NULL)
    )
);

-- A Discord message counts for at most one piece; a piece may collect several (duplicates).
CREATE TABLE IF NOT EXISTS o_piece_receipts (
    channel_id TEXT NOT NULL,
    message_id BIGINT NOT NULL CHECK (message_id > 0),
    provider TEXT NOT NULL,
    native_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    piece_index BIGINT NOT NULL,
    slot SMALLINT CHECK (slot IN (0, 1, 2)),
    author_id BIGINT NOT NULL CHECK (author_id > 0),
    method TEXT NOT NULL CHECK (
        method IN ('post_response', 'local_posted', 'marker', 'nonce', 'exact_match')
    ),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, message_id),
    FOREIGN KEY (channel_id, provider, native_key, kind, piece_index)
        REFERENCES o_piece_delivery (channel_id, provider, native_key, kind, piece_index)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS o_piece_receipts_piece
    ON o_piece_receipts (channel_id, provider, native_key, kind, piece_index);

-- END migrations/postgres/0138_o_piece_delivery.sql

-- BEGIN migrations/postgres/0139_imsg_reply_obligations.sql sha256=f86980b491854f579e65fb1f168db329d863eaa8a8247fc886dfe5ba8a6fd041
-- 0139_imsg_reply_obligations.sql [NEW]
CREATE TABLE imsg_reply_obligations (
    origin_id             TEXT PRIMARY KEY,
    source                TEXT NOT NULL DEFAULT 'imessage',
    agent_id              TEXT NOT NULL,
    channel_id            TEXT NOT NULL,
    person_id             BIGINT NOT NULL,
    request_hash          TEXT NOT NULL,

    opened_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at            TIMESTAMPTZ NOT NULL,

    admission             TEXT NOT NULL DEFAULT 'admitting',
    admission_kind        TEXT,
    admission_result      JSONB,
    delivery_confirmed    BOOLEAN,

    resolution            TEXT NOT NULL DEFAULT 'open',

    admitted_turn_id      TEXT,
    queued_message_id     TEXT,
    episode_key           TEXT,
    inject_nonce          TEXT,

    last_reply_episode    TEXT,
    last_reply_at         TIMESTAMPTZ,

    terminal_seen_at      TIMESTAMPTZ,
    terminal_first_line   TEXT,

    active_send_action    UUID,
    discord_mirror_id     TEXT,

    closed_at             TIMESTAMPTZ,
    closed_reason         TEXT,

    CHECK (source = 'imessage'),
    CHECK (person_id > 0),
    CHECK (LENGTH(origin_id) BETWEEN 1 AND 256),
    CHECK (expires_at = opened_at + INTERVAL '24 hours'),

    CHECK (admission IN (
        'admitting', 'admitted', 'refused', 'unknown'
    )),
    CHECK (admission_kind IS NULL OR admission_kind IN (
        'started', 'queued', 'injected', 'unconfirmed'
    )),
    CHECK (resolution IN (
        'open', 'replied', 'expired', 'closed'
    ))
);

CREATE TABLE imsg_reply_actions (
    id                 UUID PRIMARY KEY,
    origin_id          TEXT NOT NULL
        REFERENCES imsg_reply_obligations(origin_id)
        ON DELETE RESTRICT,

    purpose            TEXT NOT NULL,
    action_key         TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'claimed',

    reply_episode_key  TEXT,
    claimed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at            TIMESTAMPTZ,
    receipt            JSONB,

    UNIQUE (origin_id, id),
    UNIQUE (origin_id, purpose, action_key),

    CHECK (purpose IN (
        'progress', 'final', 'fallback', 'expiry', 'mirror'
    )),
    CHECK (status IN (
        'claimed', 'sent', 'unknown', 'not_sent'
    )),
    CHECK ((status = 'sent') = (sent_at IS NOT NULL))
);

-- A claimed external send must belong to the same obligation.
ALTER TABLE imsg_reply_obligations
ADD CONSTRAINT imsg_active_action_origin_fk
FOREIGN KEY (origin_id, active_send_action)
REFERENCES imsg_reply_actions(origin_id, id)
DEFERRABLE INITIALLY DEFERRED;

-- All unresolved external iMessage sends compete for ONE slot.
CREATE UNIQUE INDEX imsg_one_unresolved_external_send
ON imsg_reply_actions(origin_id)
WHERE purpose IN ('progress', 'final', 'fallback', 'expiry')
  AND status IN ('claimed', 'unknown');

-- Lifetime one-shot actions.
CREATE UNIQUE INDEX imsg_one_fallback
ON imsg_reply_actions(origin_id)
WHERE purpose = 'fallback';

CREATE UNIQUE INDEX imsg_one_expiry
ON imsg_reply_actions(origin_id)
WHERE purpose = 'expiry';

CREATE UNIQUE INDEX imsg_one_mirror
ON imsg_reply_actions(origin_id)
WHERE purpose = 'mirror';

CREATE INDEX imsg_open_expiry
ON imsg_reply_obligations(expires_at)
WHERE resolution = 'open';

CREATE INDEX imsg_open_episode
ON imsg_reply_obligations(episode_key)
WHERE resolution = 'open';

-- END migrations/postgres/0139_imsg_reply_obligations.sql

-- BEGIN migrations/postgres/0140_channel_recovery_custody.sql sha256=2adcb4f9c929b223537b6ea4126a8cf8797a0aaea10314b63490a494bc578bc2
-- One recovery-consumption custody row per channel, shared by every node. A row changes only by
-- revision CAS and is never deleted; no home row reference or cascade reaches it.
CREATE TABLE IF NOT EXISTS channel_recovery_custody (
    channel_id TEXT PRIMARY KEY CHECK (channel_id ~ '^[1-9][0-9]*$'),
    revision BIGINT NOT NULL CHECK (revision >= 1),
    provider TEXT NOT NULL CHECK (BTRIM(provider) <> ''),
    token_hash TEXT NOT NULL CHECK (BTRIM(token_hash) <> ''),
    owner_node TEXT NOT NULL CHECK (BTRIM(owner_node) <> ''),
    process_incarnation TEXT NOT NULL CHECK (BTRIM(process_incarnation) <> ''),
    attempt_id TEXT NOT NULL CHECK (BTRIM(attempt_id) <> ''),
    -- NULL is an explicit absence (a no-home legacy channel), never a placeholder epoch.
    home_epoch BIGINT CHECK (home_epoch >= 1),
    activation_generation BIGINT CHECK (activation_generation >= 0),
    issued_episode JSONB CHECK (jsonb_typeof(issued_episode) = 'object'),
    -- Phase and JSON shapes belong to format_version; a reader that does not know them holds.
    phase TEXT NOT NULL CHECK (BTRIM(phase) <> ''),
    active_operation JSONB CHECK (jsonb_typeof(active_operation) = 'object'),
    canonical_seal JSONB NOT NULL CHECK (jsonb_typeof(canonical_seal) = 'object'),
    ever_scoped BOOLEAN NOT NULL DEFAULT FALSE,
    format_version INTEGER NOT NULL CHECK (format_version >= 1), -- agentdesk-audit: allow-int4 (row format version)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Bounded progress only: no message bodies and no operation history.
    CONSTRAINT channel_recovery_custody_bounded CHECK (
        octet_length(COALESCE(issued_episode::text, '')) <= 4096
        AND octet_length(COALESCE(active_operation::text, '')) <= 16384
        AND octet_length(canonical_seal::text) <= 4096
    )
);

-- Rows that still hold a responsibility, for restart reconstruction.
CREATE INDEX IF NOT EXISTS channel_recovery_custody_open
    ON channel_recovery_custody (channel_id) WHERE phase <> 'closed';

-- END migrations/postgres/0140_channel_recovery_custody.sql

-- BEGIN migrations/postgres/0141_sessions_hosted_execution_archive.sql sha256=8bd647f7c3fd5298e37eeea2d88634fa78ebf93dc35dfc8bf052b159067132e7
-- Keep the returned execution on its canonical row without changing legacy NULL semantics.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS returned_hosted_execution JSONB;

-- END migrations/postgres/0141_sessions_hosted_execution_archive.sql

