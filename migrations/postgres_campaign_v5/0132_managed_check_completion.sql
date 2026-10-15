-- Opt-in verification on existing cards and phase runs; no second queue/ledger.
ALTER TABLE kanban_cards ADD COLUMN verification_contract JSONB;
ALTER TABLE kanban_cards ADD COLUMN verification_state JSONB;

CREATE INDEX idx_cards_verification_review ON kanban_cards(id)
    WHERE verification_contract IS NOT NULL AND status = 'review';

-- One completion predicate, also used by the Rust/JS review disposition.
-- Receipts in the reserved managed-check: namespace are written only by the
-- in-process collector. Public phase-run mutation APIs reject that namespace.
CREATE FUNCTION agentdesk_checks_passed(card kanban_cards) RETURNS BOOLEAN
LANGUAGE plpgsql VOLATILE AS $$
DECLARE requirement JSONB; check_id TEXT; receipt test_phase_runs;
BEGIN
    IF card.verification_contract IS NULL THEN RETURN FALSE; END IF;
    IF card.status <> 'review' OR card.verification_state IS NULL
       OR card.verification_state->>'contract_digest' IS DISTINCT FROM card.verification_contract->>'digest'
       OR card.verification_state->>'assigned_agent_id' IS DISTINCT FROM card.assigned_agent_id
       OR card.verification_state->>'repo_id' IS DISTINCT FROM card.repo_id
       OR card.verification_state->>'pipeline_stage_id' IS DISTINCT FROM card.pipeline_stage_id
       OR card.verification_state->>'candidate_digest' IS NULL
       OR card.verification_state->>'outcome' IS DISTINCT FROM 'passed' THEN RETURN FALSE; END IF;
    IF EXISTS (SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=card.id
               AND d.dispatch_type IN ('implementation','rework') AND d.status IN ('pending','dispatched'))
       OR NOT EXISTS (SELECT 1 FROM task_dispatches d WHERE d.id=card.verification_state->>'dispatch_id'
               AND d.kanban_card_id=card.id AND d.status='completed'
               AND d.dispatch_type IN ('implementation','rework')
               AND d.result IS NOT DISTINCT FROM card.verification_state->>'dispatch_result')
       OR (SELECT d.id FROM task_dispatches d WHERE d.kanban_card_id=card.id
               AND d.dispatch_type IN ('implementation','rework')
               ORDER BY d.created_at DESC, d.id DESC LIMIT 1)
          IS DISTINCT FROM card.verification_state->>'dispatch_id' THEN RETURN FALSE; END IF;
    IF COALESCE(jsonb_array_length(card.verification_contract->'template'->'acceptance'),0) < 1 THEN RETURN FALSE; END IF;
    FOR requirement IN SELECT value FROM jsonb_array_elements(card.verification_contract->'template'->'acceptance') LOOP
        IF COALESCE(jsonb_array_length(requirement->'checks'),0) < 1 THEN RETURN FALSE; END IF;
        FOR check_id IN SELECT jsonb_array_elements_text(requirement->'checks') LOOP
            SELECT * INTO receipt FROM test_phase_runs r
                WHERE r.idempotency_key=card.verification_state->'runs'->>check_id
                  AND r.idempotency_key LIKE 'managed-check:%' AND r.card_id=card.id
                  AND r.head_sha=card.verification_state->>'candidate_digest'
                  AND r.status='passed' AND r.completed_at IS NOT NULL
                  AND r.evidence->>'collector'='protected-command-v1'
                  AND r.evidence->>'contract_digest'=card.verification_contract->>'digest'
                  AND r.evidence->>'dispatch_id'=card.verification_state->>'dispatch_id'
                  AND r.evidence->>'check_id'=check_id
                  AND r.evidence->>'process_reaped'='true'
                  AND r.evidence->>'outcome'='passed';
            IF NOT FOUND THEN RETURN FALSE; END IF;
        END LOOP;
    END LOOP;
    RETURN TRUE;
END $$;

CREATE FUNCTION agentdesk_check_disposition(card kanban_cards) RETURNS TEXT
LANGUAGE sql VOLATILE AS $$
    SELECT CASE
        WHEN card.verification_contract IS NULL THEN 'unmanaged'
        WHEN card.status='done' THEN 'completed'
        WHEN NOT agentdesk_checks_passed(card) THEN
            CASE WHEN card.verification_state->>'outcome' IN ('failed','unverifiable')
                THEN 'await_human' ELSE 'await_checks' END
        WHEN card.verification_contract->'template'->>'review'='local' AND NOT EXISTS (
            SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=card.id
                AND d.id=card.latest_dispatch_id AND d.dispatch_type='review' AND d.status='completed'
                AND d.parent_dispatch_id=card.verification_state->>'dispatch_id'
                AND d.context::jsonb->>'verification_candidate_digest'=card.verification_state->>'candidate_digest'
                AND d.context::jsonb->>'verification_contract_digest'=card.verification_contract->>'digest'
                AND d.result::jsonb->>'verdict' IN ('pass','approved')
        ) THEN 'run_local_review'
        WHEN card.verification_contract->'template'->>'review'='human'
            AND card.verification_state->>'human_approved' IS DISTINCT FROM 'true' THEN 'await_human'
        WHEN card.verification_contract->'template'->>'review' IN ('none','human','local') THEN 'ready_to_commit'
        ELSE 'await_human'
    END
$$;

CREATE FUNCTION agentdesk_guard_verified_completion() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.verification_contract IS NOT NULL THEN
        IF NEW.verification_contract IS DISTINCT FROM OLD.verification_contract THEN
            RAISE EXCEPTION 'managed verification contract is immutable';
        END IF;
        IF NEW.status IS DISTINCT FROM OLD.status AND NEW.status='done' THEN
            -- The card row is already locked by UPDATE. Lock the referenced
            -- result before evaluating the volatile predicate, so a committed
            -- replacement wins over a previously computed eligible decision.
            PERFORM id FROM task_dispatches
                WHERE kanban_card_id=OLD.id ORDER BY id FOR UPDATE;
            IF NEW.verification_state IS DISTINCT FROM OLD.verification_state
               OR NEW.assigned_agent_id IS DISTINCT FROM OLD.assigned_agent_id
               OR NEW.repo_id IS DISTINCT FROM OLD.repo_id
               OR NEW.pipeline_stage_id IS DISTINCT FROM OLD.pipeline_stage_id
               OR NOT agentdesk_checks_passed(OLD)
               OR agentdesk_check_disposition(OLD)<>'ready_to_commit' THEN
                RAISE EXCEPTION 'managed completion requires current complete verification';
            END IF;
        END IF;
        -- This first adapter admits canonical card states only. Renaming a
        -- terminal state or raw force cannot turn an unverified result into
        -- success. Failure/cancellation exits remain independent of checks.
        IF NEW.status NOT IN ('backlog','ready','requested','in_progress','review','done','failed','canceled','cancelled') THEN
            RAISE EXCEPTION 'managed checks require a supported pipeline state';
        END IF;
        IF NEW.assigned_agent_id IS DISTINCT FROM OLD.assigned_agent_id
           OR NEW.repo_id IS DISTINCT FROM OLD.repo_id
           OR NEW.pipeline_stage_id IS DISTINCT FROM OLD.pipeline_stage_id
           OR (NEW.status IS DISTINCT FROM OLD.status AND NEW.status<>'done') THEN
            NEW.verification_state := NULL;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_verified_completion BEFORE UPDATE ON kanban_cards
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_verified_completion();

-- Serialize new attempts/review requests with the card's completion UPDATE.
-- Existing rows are locked by the completion trigger; no I/O occurs under it.
CREATE FUNCTION agentdesk_bind_verified_dispatch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE card kanban_cards;
BEGIN
    SELECT * INTO card FROM kanban_cards WHERE id=NEW.kanban_card_id FOR UPDATE;
    IF card.verification_contract IS NOT NULL THEN
        IF card.status='done' AND NEW.dispatch_type IN ('implementation','rework','review') THEN
            RAISE EXCEPTION 'reopen the managed card before creating a new attempt';
        END IF;
        IF NEW.dispatch_type='review' THEN
            IF agentdesk_check_disposition(card)<>'run_local_review' THEN
                RAISE EXCEPTION 'managed review must wait for current verification';
            END IF;
            NEW.parent_dispatch_id := card.verification_state->>'dispatch_id';
            NEW.context := (COALESCE(NULLIF(NEW.context,'')::jsonb,'{}'::jsonb) || jsonb_build_object(
                'verification_candidate_digest',card.verification_state->>'candidate_digest',
                'verification_contract_digest',card.verification_contract->>'digest',
                'verification_candidate_path',card.verification_state->>'candidate_path'
            ))::text;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bind_verified_dispatch BEFORE INSERT ON task_dispatches
FOR EACH ROW EXECUTE FUNCTION agentdesk_bind_verified_dispatch();

-- Reuse resource_locks for the short copy step. Both admission paths take the
-- same transaction advisory lock, so a writer cannot race candidate capture.
CREATE FUNCTION agentdesk_guard_candidate_capture() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE resource TEXT;
BEGIN
    IF NEW.lock_key NOT LIKE 'managed-capture:%' THEN RETURN NEW; END IF;
    resource := substring(NEW.lock_key FROM 17);
    PERFORM pg_advisory_xact_lock(hashtext('agentdesk.check.workspace'),hashtext(resource));
    IF EXISTS (SELECT 1 FROM node_execution_leases WHERE resource_key=resource
               AND (managed OR expires_at>clock_timestamp())) THEN
        RAISE EXCEPTION 'candidate workspace still has an execution lease';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_candidate_capture BEFORE INSERT OR UPDATE ON resource_locks
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_candidate_capture();

CREATE FUNCTION agentdesk_guard_capture_writer() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.resource_key IS NULL THEN RETURN NEW; END IF;
    PERFORM pg_advisory_xact_lock(hashtext('agentdesk.check.workspace'),hashtext(NEW.resource_key));
    IF EXISTS (SELECT 1 FROM resource_locks WHERE lock_key='managed-capture:'||NEW.resource_key
               AND expires_at>clock_timestamp()) THEN
        RAISE EXCEPTION 'workspace candidate capture is in progress';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_capture_writer BEFORE INSERT ON node_execution_leases
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_capture_writer();
