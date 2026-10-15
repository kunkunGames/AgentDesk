-- Forward convergence of immutable PostgreSQL histories.
-- Generated and verified by scripts/check_migration_convergence.py.
-- Historical receipts are preserved; only this version is recorded.

-- BEGIN migrations/postgres_campaign_v5/0131_managed_execution_lifetime.sql sha256=225c906e3cc19cb304f4afe4ab8841d36aa91898a7538e0085fc3f4968f10dcc
-- An expiring delivery lease cannot prove an asynchronous writer stopped.
-- Keep the existing execution lease as the admission owner; no second queue.
ALTER TABLE node_execution_leases
    ADD COLUMN managed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN resource_key TEXT,
    ADD COLUMN execution_state TEXT NOT NULL DEFAULT 'reserved'
        CHECK (execution_state IN ('reserved', 'running', 'unknown', 'quiesced')),
    ADD COLUMN deadline_at TIMESTAMPTZ,
    ADD COLUMN max_tool_calls INTEGER,
    ADD COLUMN permission_ids TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN provider_session_id TEXT,
    ADD COLUMN runtime_identity JSONB CHECK (runtime_identity IS NULL OR jsonb_typeof(runtime_identity)='object'),
    ADD COLUMN stop_observation JSONB CHECK (stop_observation IS NULL OR jsonb_typeof(stop_observation)='object'),
    ADD COLUMN stop_observed_at TIMESTAMPTZ,
    ADD COLUMN stop_reason TEXT,
    ADD CONSTRAINT managed_execution_requires_runtime CHECK (
        NOT managed OR execution_state='reserved'
        OR (runtime_identity IS NOT NULL AND COALESCE(runtime_identity->>'version'='1',FALSE))
    ),
    ADD CONSTRAINT managed_execution_has_bounds CHECK (
        NOT managed OR (resource_key IS NOT NULL AND deadline_at IS NOT NULL
                        AND max_tool_calls IS NOT NULL
                        AND max_tool_calls BETWEEN 1 AND 10000)
    );
CREATE INDEX node_execution_resource ON node_execution_leases(instance_id, resource_key);

CREATE OR REPLACE FUNCTION node_execution_occupancy(node_id TEXT)
RETURNS TABLE(provider TEXT, channel_id TEXT) LANGUAGE sql VOLATILE AS $$
    SELECT provider, channel_id FROM intake_outbox
     WHERE target_instance_id=node_id
       AND status IN ('pending','claimed','accepted','spawned','dispatched')
    UNION
    SELECT provider, channel_id FROM node_execution_leases
     WHERE instance_id=node_id AND (managed OR expires_at>clock_timestamp())
$$;

CREATE OR REPLACE FUNCTION guard_provider_execution_capacity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- This also holds the node advisory lock through the conflict check/insert.
    PERFORM reserve_node_execution(NEW.instance_id,NEW.provider,NEW.channel_id);
    IF NEW.resource_key IS NOT NULL AND EXISTS (
        SELECT 1 FROM node_execution_leases occupied_lease
         WHERE occupied_lease.instance_id=NEW.instance_id AND occupied_lease.resource_key=NEW.resource_key
           AND (NEW.managed OR occupied_lease.managed)
           AND (occupied_lease.managed OR occupied_lease.expires_at>clock_timestamp())
    ) THEN
        RAISE EXCEPTION 'managed execution resource remains occupied'
          USING ERRCODE='23514', CONSTRAINT='managed_execution_resource_available';
    END IF;
    RETURN NEW;
END
$$;

-- A stale TTL sweeper (including an older binary) must not delete an unknown
-- writer. Only an explicit, nonce-fenced quiescence transition permits release.
CREATE FUNCTION retain_unquiesced_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.managed AND OLD.execution_state IN ('running','unknown') THEN
        RETURN NULL;
    END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER retain_unquiesced_execution BEFORE DELETE ON node_execution_leases
    FOR EACH ROW EXECUTE FUNCTION retain_unquiesced_execution();

-- END migrations/postgres_campaign_v5/0131_managed_execution_lifetime.sql

-- BEGIN migrations/postgres_campaign_v5/0132_managed_check_completion.sql sha256=535c177b61f5feb6bccfd1720c36844331fe258ce22e6097e435d2462b54afe1
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

-- END migrations/postgres_campaign_v5/0132_managed_check_completion.sql

-- BEGIN migrations/postgres_campaign_v5/0133_campaign_execution_guards.sql sha256=bac9054e4fc846cb10ee509d9956436329aaa09896985afb0d4c5db27486128a
-- A Campaign is still the sole goal ledger. Protected task cards pin its work
-- instructions; observed progress and operator pause/cancel remain editable.
CREATE FUNCTION agentdesk_campaign_graph(document JSONB) RETURNS JSONB
LANGUAGE SQL IMMUTABLE AS $$
    SELECT jsonb_build_object('title',document->'title',
        'description',document->'description','round',document->'round',
        'nodes',COALESCE((SELECT jsonb_object_agg(n->>'id',jsonb_build_object(
            'title',n->'title','stage',n->'stage','round',n->'round',
            'details',n->'details','acceptance',n->'acceptance',
            'dependencies',COALESCE((SELECT jsonb_agg(d ORDER BY d) FROM jsonb_array_elements(n->'dependencies') d),'[]'::jsonb)))
            FROM jsonb_array_elements(document->'nodes') n),'{}'::jsonb))
$$;

CREATE FUNCTION agentdesk_guard_campaign_graph() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.document->>'status'='completed' AND EXISTS(
        SELECT 1 FROM kanban_cards WHERE metadata->>'campaign_id'=OLD.id AND metadata->>'managed_campaign'='true'
    ) AND EXISTS(
        SELECT 1 FROM jsonb_array_elements(NEW.document->'nodes') n
        WHERE NOT EXISTS(SELECT 1 FROM kanban_cards k WHERE k.metadata->>'campaign_id'=OLD.id
            AND k.metadata->>'node_id'=n->>'id' AND k.metadata->>'managed_campaign'='true'
            AND k.status='done' AND agentdesk_check_disposition(jsonb_populate_record(k,'{"status":"review"}'::jsonb))='ready_to_commit')
    ) THEN
        RAISE EXCEPTION 'campaign completion requires all protected task receipts';
    END IF;
    IF agentdesk_campaign_graph(OLD.document) IS DISTINCT FROM agentdesk_campaign_graph(NEW.document)
       AND EXISTS(SELECT 1 FROM kanban_cards WHERE metadata->>'campaign_id'=OLD.id
                  AND metadata->>'managed_campaign'='true') THEN
        RAISE EXCEPTION 'supplied campaign instructions are immutable; approve a new goal for new work';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_campaign_graph BEFORE UPDATE ON campaigns
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_campaign_graph();

CREATE FUNCTION agentdesk_guard_campaign_dispatch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE meta JSONB; goal_state TEXT; workspace TEXT; assigned TEXT; requirements JSONB;
        contract JSONB; state JSONB; attempts BIGINT;
BEGIN
    IF NEW.dispatch_type NOT IN ('implementation','rework','review') THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' AND NEW.status NOT IN ('pending','dispatched') THEN RETURN NEW; END IF;
    SELECT metadata,verification_contract->'template'->>'workspace',assigned_agent_id,verification_contract,verification_state
        INTO meta,workspace,assigned,contract,state FROM kanban_cards WHERE id=NEW.kanban_card_id;
    IF meta->>'managed_campaign' IS DISTINCT FROM 'true' THEN RETURN NEW; END IF;
    SELECT document->>'status' INTO goal_state FROM campaigns
        WHERE id=meta->>'campaign_id' FOR SHARE;
    IF goal_state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'campaign is not active; new dispatch admission is paused';
    END IF;
    SELECT execution_requirements INTO requirements FROM agents WHERE id=assigned;
    IF requirements->'execution_limits' IS NULL OR requirements->'execution_limits'='null'::jsonb
       OR NEW.to_agent_id IS DISTINCT FROM assigned THEN
        RAISE EXCEPTION 'campaign dispatch requires its enrolled agent and finite execution limits';
    END IF;
    IF TG_OP='INSERT' AND COALESCE(NULLIF(NEW.context,''),'{}')::jsonb->>'managed_rework'='true' THEN
        SELECT COUNT(DISTINCT required_capabilities->>'dispatch_id') INTO attempts
            FROM test_phase_runs WHERE card_id=NEW.kanban_card_id AND idempotency_key LIKE 'managed-check:%';
        IF meta->>'auto_rework' IS DISTINCT FROM 'true' OR state->>'outcome' IS DISTINCT FROM 'failed'
           OR state->>'candidate_digest' IS DISTINCT FROM COALESCE(NULLIF(NEW.context,''),'{}')::jsonb->>'failed_candidate_digest'
           OR attempts >= (contract->'template'->>'max_candidates')::int
           OR EXISTS(SELECT 1 FROM node_execution_leases WHERE resource_key=workspace AND (managed OR expires_at>clock_timestamp())) THEN
            RAISE EXCEPTION 'automatic rework has stale evidence, exhausted budget or an uncontained writer';
        END IF;
    END IF;
    NEW.context := (COALESCE(NULLIF(NEW.context,''),'{}')::jsonb ||
        jsonb_build_object('managed_campaign',true,'worktree_path',workspace))::text;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_campaign_dispatch BEFORE INSERT OR UPDATE OF status,context ON task_dispatches
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_campaign_dispatch();

CREATE FUNCTION agentdesk_guard_campaign_card() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.metadata->>'managed_campaign'='true' AND
       (EXISTS(SELECT 1 FROM unnest(ARRAY['managed_campaign','campaign_id','node_id','task_key','plan_digest','input_manifest','auto_rework']) key
               WHERE NEW.metadata->key IS DISTINCT FROM OLD.metadata->key)
        OR NEW.assigned_agent_id IS DISTINCT FROM OLD.assigned_agent_id
        OR NEW.repo_id IS DISTINCT FROM OLD.repo_id) THEN
        RAISE EXCEPTION 'managed campaign identity and input manifest are immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_campaign_card BEFORE UPDATE ON kanban_cards
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_campaign_card();

CREATE FUNCTION agentdesk_guard_campaign_agent() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.provider IS DISTINCT FROM OLD.provider OR NEW.execution_requirements IS DISTINCT FROM OLD.execution_requirements)
       AND EXISTS(SELECT 1 FROM kanban_cards WHERE assigned_agent_id=OLD.id AND status<>'done'
                  AND metadata->>'managed_campaign'='true') THEN
        RAISE EXCEPTION 'unfinished campaign tasks pin their managed execution policy';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_campaign_agent BEFORE UPDATE ON agents
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_campaign_agent();

-- END migrations/postgres_campaign_v5/0133_campaign_execution_guards.sql

-- BEGIN migrations/postgres_campaign_v5/0134_campaign_operations.sql sha256=c3d075258cd20ec60012cde59b1169ba6cca86e91ef1a2365680039706cd1846
-- Campaign owns admission for adopted recurring work. The native execution
-- ledger remains routine_runs; old schedules can never admit these jobs.
ALTER TABLE routines ADD COLUMN campaign_id TEXT REFERENCES campaigns(id);
ALTER TABLE routines ADD COLUMN campaign_node_id TEXT;
ALTER TABLE routines ADD CONSTRAINT campaign_operation_identity CHECK
    ((campaign_id IS NULL) = (campaign_node_id IS NULL));
CREATE UNIQUE INDEX routine_campaign_node ON routines(campaign_id,campaign_node_id)
    WHERE campaign_id IS NOT NULL;
ALTER TABLE routine_runs ADD COLUMN campaign_id TEXT REFERENCES campaigns(id);
ALTER TABLE routine_runs ADD COLUMN campaign_node_id TEXT;
CREATE INDEX campaign_operation_runs ON routine_runs(campaign_id,started_at DESC)
    WHERE campaign_id IS NOT NULL;

CREATE FUNCTION agentdesk_guard_campaign_operation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.campaign_id IS NOT NULL AND NEW.status <> 'paused' THEN
        RAISE EXCEPTION 'Campaign-owned work cannot be enabled as an independent Routine';
    END IF;
    IF TG_OP='UPDATE' AND OLD.campaign_id IS NOT NULL AND
       (NEW.campaign_id IS DISTINCT FROM OLD.campaign_id OR
        NEW.campaign_node_id IS DISTINCT FROM OLD.campaign_node_id OR
        NEW.agent_id IS DISTINCT FROM OLD.agent_id OR
        NEW.script_ref IS DISTINCT FROM OLD.script_ref OR
        NEW.schedule IS DISTINCT FROM OLD.schedule OR
        NEW.execution_strategy IS DISTINCT FROM OLD.execution_strategy OR
        NEW.timeout_secs IS DISTINCT FROM OLD.timeout_secs OR
        NEW.max_retries IS DISTINCT FROM OLD.max_retries OR
        NEW.fallback_agent_id IS DISTINCT FROM OLD.fallback_agent_id) THEN
        RAISE EXCEPTION 'Campaign operation binding is immutable; adopt new work for a new contract';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_campaign_operation BEFORE INSERT OR UPDATE ON routines
    FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_campaign_operation();

CREATE FUNCTION agentdesk_stamp_campaign_operation_run() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE owner TEXT; node TEXT; state TEXT;
BEGIN
    SELECT campaign_id,campaign_node_id INTO owner,node FROM routines WHERE id=NEW.routine_id;
    IF owner IS NOT NULL THEN
        SELECT document->>'status' INTO state FROM campaigns WHERE id=owner FOR SHARE;
        IF state IS DISTINCT FROM 'active' THEN
            RAISE EXCEPTION 'Campaign operation admission requires an active Campaign';
        END IF;
        NEW.campaign_id:=owner;
        NEW.campaign_node_id:=node;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER stamp_campaign_operation_run BEFORE INSERT ON routine_runs
    FOR EACH ROW EXECUTE FUNCTION agentdesk_stamp_campaign_operation_run();

-- A recurring native operation has one immutable execution contract. Status,
-- receipts and explicit operator rearming remain editable through Campaign CAS.
CREATE FUNCTION agentdesk_guard_native_campaign_graph() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE binding RECORD; node JSONB;
BEGIN
    SELECT campaign_node_id,agent_id INTO binding FROM routines WHERE campaign_id=OLD.id;
    IF FOUND THEN
        IF agentdesk_campaign_graph(OLD.document) IS DISTINCT FROM agentdesk_campaign_graph(NEW.document) THEN
            RAISE EXCEPTION 'adopted Campaign instructions are immutable';
        END IF;
        SELECT n INTO node FROM jsonb_array_elements(NEW.document->'nodes') n
            WHERE n->>'id'=binding.campaign_node_id;
        IF node->>'assignee' IS DISTINCT FROM binding.agent_id OR
           node->>'provider' IS DISTINCT FROM
               (SELECT n->>'provider' FROM jsonb_array_elements(OLD.document->'nodes') n
                WHERE n->>'id'=binding.campaign_node_id) THEN
            RAISE EXCEPTION 'adopted Campaign native executor is immutable';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_native_campaign_graph BEFORE UPDATE ON campaigns
    FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_native_campaign_graph();

CREATE FUNCTION agentdesk_guard_native_campaign_card() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM routines WHERE campaign_id=NEW.metadata->>'campaign_id') THEN
        RAISE EXCEPTION 'native operation Campaign already has an execution owner';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_native_campaign_card BEFORE INSERT OR UPDATE ON kanban_cards
    FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_native_campaign_card();

CREATE FUNCTION agentdesk_guard_native_campaign_provider() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.provider IS DISTINCT FROM OLD.provider AND
       EXISTS(SELECT 1 FROM routines WHERE agent_id=OLD.id AND campaign_id IS NOT NULL) THEN
        RAISE EXCEPTION 'adopted Campaign provider is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_native_campaign_provider BEFORE UPDATE ON agents
    FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_native_campaign_provider();

-- END migrations/postgres_campaign_v5/0134_campaign_operations.sql

-- BEGIN migrations/postgres_campaign_v5/0135_campaign_graph_runtime.sql sha256=a2bc772c0fc9c1747f395e14315198e67306449be28419452b83356e3bbad236
-- Graph versions and contracts extend Campaign. Execution still belongs to
-- kanban_cards, auto_queue_entries and task_dispatches.
CREATE TABLE campaign_execution_specs (
    campaign_id TEXT PRIMARY KEY REFERENCES campaigns(id),
    spec JSONB NOT NULL,
    active_version BIGINT,
    hold_reason TEXT,
    last_reconciled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE campaign_graph_versions (
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    version BIGINT NOT NULL CHECK(version>0),
    parent_version BIGINT,
    graph JSONB NOT NULL,
    digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('candidate','validated','active','superseded','rejected')),
    failure_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(campaign_id,version),
    UNIQUE(campaign_id,digest)
);
CREATE UNIQUE INDEX campaign_one_active_graph ON campaign_graph_versions(campaign_id) WHERE status='active';
CREATE TABLE campaign_graph_tasks (
    task_key TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    node_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('plan','validate','work')),
    version BIGINT,
    contract_digest TEXT NOT NULL,
    card_id TEXT NOT NULL UNIQUE REFERENCES kanban_cards(id),
    observation JSONB,
    artifact_manifest JSONB,
    accepted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX campaign_graph_tasks_owner ON campaign_graph_tasks(campaign_id);
CREATE TABLE campaign_graph_occurrences (
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    scheduled_at TIMESTAMPTZ NOT NULL,
    child_id TEXT NOT NULL UNIQUE REFERENCES campaign_execution_specs(campaign_id),
    PRIMARY KEY(campaign_id,scheduled_at)
);
-- IDs are provider event IDs, not poll timestamps. Replayed stream frames do
-- not replenish or double-spend the observed budget. Arguments/output are hashed.
CREATE TABLE campaign_graph_progress (
    dispatch_id TEXT NOT NULL REFERENCES task_dispatches(id),
    event_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    output_digest TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(dispatch_id,event_id)
);
CREATE TABLE campaign_graph_failures (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    task_key TEXT NOT NULL REFERENCES campaign_graph_tasks(task_key),
    fingerprint TEXT NOT NULL,
    category TEXT NOT NULL,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(task_key,fingerprint)
);
-- Prevent a second canonical work attempt or stale graph activation from
-- bypassing the controller via Auto Queue recovery or a direct dispatch API.
CREATE FUNCTION agentdesk_guard_graph_dispatch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE t campaign_graph_tasks%ROWTYPE; s campaign_execution_specs%ROWTYPE; c JSONB;
BEGIN
    SELECT * INTO t FROM campaign_graph_tasks WHERE card_id=NEW.kanban_card_id;
    IF NOT FOUND OR NEW.dispatch_type NOT IN ('implementation','rework') THEN RETURN NEW; END IF;
    IF NEW.status NOT IN ('pending','dispatched','running') THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' AND OLD.status NOT IN ('pending','dispatched','running') THEN
        RAISE EXCEPTION 'Terminal graph attempts cannot be replayed';
    END IF;
    SELECT document INTO c FROM campaigns WHERE id=t.campaign_id FOR SHARE;
    SELECT * INTO s FROM campaign_execution_specs WHERE campaign_id=t.campaign_id FOR SHARE;
    PERFORM pg_advisory_xact_lock(hashtext('campaign-task'),hashtext(t.task_key));
    IF c->>'status'<>'active' OR s.hold_reason IS NOT NULL THEN
        RAISE EXCEPTION 'Campaign graph admission is paused or held';
    END IF;
    IF t.role='work' AND NOT EXISTS (
        SELECT 1 FROM campaign_graph_versions v, jsonb_array_elements(v.graph->'nodes') n
        WHERE v.campaign_id=t.campaign_id AND v.version=s.active_version
          AND v.status='active' AND n->>'task_key'=t.task_key
    ) THEN RAISE EXCEPTION 'Task is not part of the active graph'; END IF;
    IF EXISTS (SELECT 1 FROM kv_meta WHERE key='runtime-config' AND
        (expires_at IS NULL OR expires_at>NOW()) AND value::jsonb->'internalTasksEnabled'='true'::jsonb) IS NOT TRUE THEN
        RAISE EXCEPTION 'Internal task admission is disabled';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM kanban_cards k JOIN agents a ON a.id=k.assigned_agent_id
        WHERE k.id=t.card_id AND a.id=NEW.to_agent_id AND a.provider IN ('qwen','opencode')
          AND s.spec->'agents' ? a.id) THEN
        RAISE EXCEPTION 'Graph dispatch must use its enrolled native executor';
    END IF;
    IF TG_OP='INSERT' AND EXISTS (SELECT 1 FROM task_dispatches d
        WHERE d.kanban_card_id=NEW.kanban_card_id AND d.dispatch_type IN ('implementation','rework')) THEN
        RAISE EXCEPTION 'Graph tasks require a reconciled evolution contract before another attempt';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_graph_dispatch BEFORE INSERT OR UPDATE OF status ON task_dispatches
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_graph_dispatch();

-- END migrations/postgres_campaign_v5/0135_campaign_graph_runtime.sql

-- BEGIN migrations/postgres_campaign_v5/0136_campaign_codex_executor.sql sha256=777e4a7794da0570a66a06e8974c0d38c492f6b4905a64ea5bd15e3e156f2d69
-- Extend native Campaign execution to Codex without changing the admission fences.
-- Migration 0135 is already deployed and must remain immutable.
CREATE OR REPLACE FUNCTION agentdesk_guard_graph_dispatch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE t campaign_graph_tasks%ROWTYPE; s campaign_execution_specs%ROWTYPE; c JSONB;
BEGIN
    SELECT * INTO t FROM campaign_graph_tasks WHERE card_id=NEW.kanban_card_id;
    IF NOT FOUND OR NEW.dispatch_type NOT IN ('implementation','rework') THEN RETURN NEW; END IF;
    IF NEW.status NOT IN ('pending','dispatched','running') THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' AND OLD.status NOT IN ('pending','dispatched','running') THEN
        RAISE EXCEPTION 'Terminal graph attempts cannot be replayed';
    END IF;
    SELECT document INTO c FROM campaigns WHERE id=t.campaign_id FOR SHARE;
    SELECT * INTO s FROM campaign_execution_specs WHERE campaign_id=t.campaign_id FOR SHARE;
    PERFORM pg_advisory_xact_lock(hashtext('campaign-task'),hashtext(t.task_key));
    IF c->>'status'<>'active' OR s.hold_reason IS NOT NULL THEN
        RAISE EXCEPTION 'Campaign graph admission is paused or held';
    END IF;
    IF t.role='work' AND NOT EXISTS (
        SELECT 1 FROM campaign_graph_versions v, jsonb_array_elements(v.graph->'nodes') n
        WHERE v.campaign_id=t.campaign_id AND v.version=s.active_version
          AND v.status='active' AND n->>'task_key'=t.task_key
    ) THEN RAISE EXCEPTION 'Task is not part of the active graph'; END IF;
    IF EXISTS (SELECT 1 FROM kv_meta WHERE key='runtime-config' AND
        (expires_at IS NULL OR expires_at>NOW()) AND value::jsonb->'internalTasksEnabled'='true'::jsonb) IS NOT TRUE THEN
        RAISE EXCEPTION 'Internal task admission is disabled';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM kanban_cards k JOIN agents a ON a.id=k.assigned_agent_id
        WHERE k.id=t.card_id AND a.id=NEW.to_agent_id AND a.provider IN ('codex','qwen','opencode')
          AND s.spec->'agents' ? a.id) THEN
        RAISE EXCEPTION 'Graph dispatch must use its enrolled native executor';
    END IF;
    IF TG_OP='INSERT' AND EXISTS (SELECT 1 FROM task_dispatches d
        WHERE d.kanban_card_id=NEW.kanban_card_id AND d.dispatch_type IN ('implementation','rework')) THEN
        RAISE EXCEPTION 'Graph tasks require a reconciled evolution contract before another attempt';
    END IF;
    RETURN NEW;
END $$;

-- END migrations/postgres_campaign_v5/0136_campaign_codex_executor.sql

-- BEGIN migrations/postgres_campaign_v5/0137_campaign_graph_reconciliation.sql sha256=fdec2c62fbe9af6a161ed504130c0d65c59e393b3a1185f31f5d3466f634dae4
-- Operator reconciliation authorizes a new independently validated contract;
-- it never rewrites the original failure, card, dispatch or artifact evidence.
CREATE TABLE campaign_graph_reconciliations (
    failure_id TEXT PRIMARY KEY REFERENCES campaign_graph_failures(id),
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    expected_revision BIGINT NOT NULL,
    evidence TEXT NOT NULL CHECK(length(evidence) BETWEEN 1 AND 8000),
    candidate_version BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY(campaign_id,candidate_version) REFERENCES campaign_graph_versions(campaign_id,version)
);

-- END migrations/postgres_campaign_v5/0137_campaign_graph_reconciliation.sql

-- BEGIN migrations/postgres_campaign_v5/0138_campaign_goal_revisions.sql sha256=24c7b2a4b2e3f8c40d405111f59891874f33befa29f6485b76ff1b3d5e3273a1
-- Operator scope corrections retain immutable before/after contracts and all
-- prior execution usage. They do not replay or migrate business tasks.
CREATE TABLE campaign_goal_revisions (
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    request_id TEXT NOT NULL CHECK(length(request_id) BETWEEN 1 AND 128),
    request_digest TEXT NOT NULL,
    campaign_revision BIGINT NOT NULL,
    previous_goal JSONB NOT NULL,
    revised_goal JSONB NOT NULL,
    evidence TEXT NOT NULL CHECK(length(evidence) BETWEEN 1 AND 8000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(campaign_id,request_id)
);

-- END migrations/postgres_campaign_v5/0138_campaign_goal_revisions.sql

-- BEGIN migrations/postgres_campaign_v5/0139_campaign_dag_templates.sql sha256=29cc9653f622dab7dbc599c5217d1cf4fdf086dce6e16cba18c948ec61b7144d
-- Versioned DAG definitions and evidence extend the existing Campaign owner.
-- Control cards still use Kanban, Auto Queue and the native completion path.
CREATE TABLE campaign_dag_families (
    id TEXT PRIMARY KEY,
    root_campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    scope_digest TEXT NOT NULL,
    scope JSONB NOT NULL,
    bootstrap_campaign_id TEXT REFERENCES campaigns(id),
    review_wait_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(root_campaign_id, scope_digest)
);
CREATE TABLE campaign_dag_members (
    campaign_id TEXT PRIMARY KEY REFERENCES campaign_execution_specs(campaign_id),
    family_id TEXT NOT NULL REFERENCES campaign_dag_families(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX campaign_dag_members_family ON campaign_dag_members(family_id);
CREATE TABLE campaign_dag_samples (
    sequence BIGSERIAL PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES campaign_dag_families(id),
    campaign_id TEXT NOT NULL UNIQUE REFERENCES campaign_execution_specs(campaign_id),
    graph_version BIGINT NOT NULL,
    graph_digest TEXT NOT NULL,
    graph JSONB NOT NULL,
    goal JSONB NOT NULL,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY(campaign_id, graph_version) REFERENCES campaign_graph_versions(campaign_id, version)
);
CREATE INDEX campaign_dag_samples_family ON campaign_dag_samples(family_id, sequence);
CREATE TABLE campaign_dag_reviews (
    id TEXT PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES campaign_dag_families(id),
    owner_campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    task_key TEXT NOT NULL UNIQUE,
    superior_agent_id TEXT NOT NULL REFERENCES agents(id),
    sample_count BIGINT NOT NULL CHECK(sample_count>0),
    threshold BIGINT NOT NULL CHECK(threshold BETWEEN 1 AND 100),
    sample_digest TEXT NOT NULL,
    snapshot JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','promoted','deferred','failed')),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at TIMESTAMPTZ,
    UNIQUE(family_id, sample_count)
);
CREATE UNIQUE INDEX campaign_dag_one_pending_review ON campaign_dag_reviews(family_id) WHERE status='pending';
CREATE TABLE campaign_dag_templates (
    id TEXT PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES campaign_dag_families(id),
    version BIGINT NOT NULL CHECK(version>0),
    schema_version INTEGER NOT NULL CHECK(schema_version=1),
    document JSONB NOT NULL,
    digest TEXT NOT NULL,
    review_id TEXT NOT NULL UNIQUE REFERENCES campaign_dag_reviews(id),
    status TEXT NOT NULL CHECK(status IN ('active','retired','blocked')),
    blocked_reason TEXT,
    blocked_by_campaign_id TEXT REFERENCES campaigns(id),
    blocked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(family_id, version)
);
CREATE UNIQUE INDEX campaign_dag_one_active_template ON campaign_dag_templates(family_id) WHERE status='active';
CREATE TABLE campaign_dag_control_assignments (
    task_key TEXT PRIMARY KEY REFERENCES campaign_graph_tasks(task_key),
    family_id TEXT NOT NULL REFERENCES campaign_dag_families(id),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    purpose TEXT NOT NULL CHECK(purpose IN ('bootstrap_plan','template_review')),
    input_manifest JSONB
);
ALTER TABLE campaign_graph_tasks DROP CONSTRAINT campaign_graph_tasks_role_check;
ALTER TABLE campaign_graph_tasks ADD CONSTRAINT campaign_graph_tasks_role_check
    CHECK(role IN ('plan','validate','work','template_review'));
ALTER TABLE campaign_graph_versions ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1);
ALTER TABLE campaign_graph_versions ADD COLUMN planner_agent_id TEXT REFERENCES agents(id);
ALTER TABLE campaign_graph_versions ADD COLUMN template_id TEXT REFERENCES campaign_dag_templates(id);

-- A superior agent may only execute its server-recorded control assignment.
-- This does not enroll that agent for business nodes or relax replay fences.
CREATE OR REPLACE FUNCTION agentdesk_guard_graph_dispatch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE t campaign_graph_tasks%ROWTYPE; s campaign_execution_specs%ROWTYPE; c JSONB;
BEGIN
    SELECT * INTO t FROM campaign_graph_tasks WHERE card_id=NEW.kanban_card_id;
    IF NOT FOUND OR NEW.dispatch_type NOT IN ('implementation','rework') THEN RETURN NEW; END IF;
    IF NEW.status NOT IN ('pending','dispatched','running') THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' AND OLD.status NOT IN ('pending','dispatched','running') THEN
        RAISE EXCEPTION 'Terminal graph attempts cannot be replayed';
    END IF;
    SELECT document INTO c FROM campaigns WHERE id=t.campaign_id FOR SHARE;
    SELECT * INTO s FROM campaign_execution_specs WHERE campaign_id=t.campaign_id FOR SHARE;
    PERFORM pg_advisory_xact_lock(hashtext('campaign-task'),hashtext(t.task_key));
    IF c->>'status'<>'active' OR s.hold_reason IS NOT NULL THEN
        RAISE EXCEPTION 'Campaign graph admission is paused or held';
    END IF;
    IF t.role='work' AND NOT EXISTS (
        SELECT 1 FROM campaign_graph_versions v, jsonb_array_elements(v.graph->'nodes') n
        WHERE v.campaign_id=t.campaign_id AND v.version=s.active_version
          AND v.status='active' AND n->>'task_key'=t.task_key
    ) THEN RAISE EXCEPTION 'Task is not part of the active graph'; END IF;
    IF EXISTS (SELECT 1 FROM kv_meta WHERE key='runtime-config' AND
        (expires_at IS NULL OR expires_at>NOW()) AND value::jsonb->'internalTasksEnabled'='true'::jsonb) IS NOT TRUE THEN
        RAISE EXCEPTION 'Internal task admission is disabled';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM kanban_cards k JOIN agents a ON a.id=k.assigned_agent_id
        WHERE k.id=t.card_id AND a.id=NEW.to_agent_id AND a.provider IN ('codex','qwen','opencode')
          AND (s.spec->'agents' ? a.id OR EXISTS (
              SELECT 1 FROM campaign_dag_control_assignments ca
              WHERE ca.task_key=t.task_key AND ca.agent_id=a.id
                AND ((t.role='plan' AND ca.purpose='bootstrap_plan')
                  OR (t.role='template_review' AND ca.purpose='template_review'))))
    ) THEN RAISE EXCEPTION 'Graph dispatch must use its enrolled native executor or assigned native superior control agent'; END IF;
    IF TG_OP='INSERT' AND EXISTS (SELECT 1 FROM task_dispatches d
        WHERE d.kanban_card_id=NEW.kanban_card_id AND d.dispatch_type IN ('implementation','rework')) THEN
        RAISE EXCEPTION 'Graph tasks require a reconciled evolution contract before another attempt';
    END IF;
    RETURN NEW;
END $$;

-- END migrations/postgres_campaign_v5/0139_campaign_dag_templates.sql

-- BEGIN migrations/postgres_campaign_v5/0140_campaign_learning_continuous_reviews.sql sha256=425295c352ae719af62ede300ce013362f6d6f8ac42a91c0e2bf0a17c33bf40c
-- Preserve accepted templates while superior agents review the next sample batch.
ALTER TABLE campaign_dag_reviews DROP CONSTRAINT campaign_dag_reviews_status_check;
ALTER TABLE campaign_dag_reviews ADD CONSTRAINT campaign_dag_reviews_status_check
    CHECK(status IN ('pending','promoted','kept','deferred','failed'));
ALTER TABLE campaign_dag_reviews ADD COLUMN decision_digest TEXT;
ALTER TABLE campaign_execution_specs ADD COLUMN preflight JSONB;
ALTER TABLE campaign_graph_tasks ADD COLUMN execution_profile JSONB;
ALTER TABLE campaign_execution_specs ADD COLUMN schedule_admission_started_at TIMESTAMPTZ;
CREATE TABLE campaign_dispatch_observations (
    dispatch_id TEXT PRIMARY KEY REFERENCES task_dispatches(id),
    provider TEXT NOT NULL,
    model TEXT,
    usage JSONB,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE campaign_schedule_revisions (
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    request_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    campaign_revision BIGINT NOT NULL,
    previous_goal JSONB NOT NULL,
    revised_goal JSONB NOT NULL,
    evidence TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(campaign_id,request_id)
);


-- END migrations/postgres_campaign_v5/0140_campaign_learning_continuous_reviews.sql

-- BEGIN migrations/postgres_campaign_v5/0141_campaign_occurrence_budgets.sql sha256=f52e49c033e68d31007343fc7dcb973eb02aca67d23accef37bd66013d933b85
-- Parent lifetime control budgets must not enlarge recurring business work.
ALTER TABLE campaign_execution_specs ADD COLUMN occurrence_budget JSONB;
ALTER TABLE campaign_execution_specs ADD CONSTRAINT campaign_occurrence_budget_object
    CHECK (occurrence_budget IS NULL OR jsonb_typeof(occurrence_budget) = 'object');

-- Preserve the budget at the first audited activation for existing parents.
-- Parents enrolled with a schedule retain their enrollment budget.
UPDATE campaign_execution_specs s SET occurrence_budget = COALESCE(
    (SELECT r.previous_goal->'budget' FROM campaign_schedule_revisions r
     WHERE r.campaign_id=s.campaign_id ORDER BY r.created_at,r.request_id LIMIT 1),
    s.spec->'budget')
WHERE s.spec ? 'schedule';

ALTER TABLE campaign_schedule_revisions ADD COLUMN previous_occurrence_budget JSONB;
ALTER TABLE campaign_schedule_revisions ADD COLUMN revised_occurrence_budget JSONB;

-- END migrations/postgres_campaign_v5/0141_campaign_occurrence_budgets.sql

-- BEGIN migrations/postgres_campaign_v5/0142_campaign_control_ownership.sql sha256=0dc9005b7bfa7002ce4fec2b135159891c3ae2276a79ae0e484f4eab294af94a
-- Audited settlement preserves abandoned controls as history. Prevent every
-- dispatch entry point from first-starting one of these retired ready cards.
-- Existing unknown attempts and terminal receipts remain unchanged.
CREATE FUNCTION agentdesk_guard_graph_control_ownership() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE owner_campaign TEXT; owner_task TEXT; retired BOOLEAN;
BEGIN
    IF NEW.status NOT IN ('pending','dispatched','running') THEN RETURN NEW; END IF;
    SELECT campaign_id,task_key INTO owner_campaign,owner_task FROM campaign_graph_tasks
      WHERE card_id=NEW.kanban_card_id AND role='plan';
    IF NOT FOUND THEN RETURN NEW; END IF;
    -- Campaign first matches replan/publication and the existing graph guard.
    -- Read the observation AFTER this lock, so racing settlement cannot leave
    -- an old pre-lock observation authorizing a first dispatch.
    PERFORM 1 FROM campaigns WHERE id=owner_campaign FOR SHARE;
    PERFORM pg_advisory_xact_lock(hashtext('campaign-task'),hashtext(owner_task));
    SELECT observation->>'outcome'='superseded_unstarted' INTO retired
      FROM campaign_graph_tasks WHERE task_key=owner_task FOR SHARE;
    IF retired THEN
        RAISE EXCEPTION 'Superseded never-dispatched graph control cannot be dispatched';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_graph_control_ownership BEFORE INSERT OR UPDATE ON task_dispatches
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_graph_control_ownership();

-- END migrations/postgres_campaign_v5/0142_campaign_control_ownership.sql

-- BEGIN migrations/postgres_campaign_v5/0143_campaign_review_execution_policy.sql sha256=3003f193e5190e6552ea9dae0ce3b761e0997496404907320c512220f8e7c692
-- Every dispatch entry point must honor the immutable finite review claim.
-- Existing 0139 one-attempt and 0142 control-ownership guards remain unchanged.
CREATE FUNCTION agentdesk_guard_graph_review_policy() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    task campaign_graph_tasks%ROWTYPE;
    review campaign_dag_reviews%ROWTYPE;
    policy JSONB;
    captured JSONB;
    deadline TIMESTAMPTZ;
BEGIN
    IF NEW.dispatch_type NOT IN ('implementation','rework')
       OR NEW.status NOT IN ('pending','dispatched','running') THEN RETURN NEW; END IF;
    SELECT * INTO task FROM campaign_graph_tasks WHERE card_id=NEW.kanban_card_id;
    IF NOT FOUND OR task.role<>'template_review' THEN RETURN NEW; END IF;
    -- Match publication and existing dispatch locks, then family -> review.
    PERFORM 1 FROM campaigns WHERE id=task.campaign_id FOR SHARE;
    PERFORM pg_advisory_xact_lock(hashtext('campaign-task'),hashtext(task.task_key));
    SELECT * INTO review FROM campaign_dag_reviews WHERE task_key=task.task_key;
    IF NOT FOUND THEN RAISE EXCEPTION 'Template review requires a durable claim'; END IF;
    PERFORM 1 FROM campaign_dag_families WHERE id=review.family_id FOR SHARE;
    SELECT * INTO review FROM campaign_dag_reviews WHERE task_key=task.task_key FOR SHARE;
    IF review.status<>'pending' OR task.version IS NOT NULL
       OR task.campaign_id<>review.owner_campaign_id OR NEW.to_agent_id<>review.superior_agent_id
       OR NOT EXISTS(SELECT 1 FROM campaign_dag_control_assignments authority
           WHERE authority.task_key=task.task_key AND authority.family_id=review.family_id
             AND authority.agent_id=review.superior_agent_id AND authority.purpose='template_review') THEN
        RAISE EXCEPTION 'Template review claim is terminal or its control assignment differs';
    END IF;
    policy := review.snapshot->'execution_policy';
    captured := review.snapshot#>'{goal,budget}';
    IF jsonb_typeof(policy) IS DISTINCT FROM 'object'
       OR jsonb_typeof(captured) IS DISTINCT FROM 'object'
       OR jsonb_typeof(captured->'node_timeout_secs') IS DISTINCT FROM 'number'
       OR jsonb_typeof(captured->'max_elapsed_secs') IS DISTINCT FROM 'number'
       OR policy-ARRAY['schema_version','max_dispatches','max_elapsed_secs','max_observed_tool_calls','deadline','goal_digest','captured_budget']<>'{}'::jsonb
       OR jsonb_typeof(policy->'schema_version') IS DISTINCT FROM 'number'
       OR jsonb_typeof(policy->'max_dispatches') IS DISTINCT FROM 'number'
       OR jsonb_typeof(policy->'max_elapsed_secs') IS DISTINCT FROM 'number'
       OR jsonb_typeof(policy->'max_observed_tool_calls') IS DISTINCT FROM 'number'
       OR jsonb_typeof(policy->'deadline') IS DISTINCT FROM 'string'
       OR jsonb_typeof(policy->'goal_digest') IS DISTINCT FROM 'string'
       OR policy->>'goal_digest' !~ '^[0-9a-f]{64}$'
       OR policy->'captured_budget' IS DISTINCT FROM captured THEN
        RAISE EXCEPTION 'Template review lacks a valid frozen finite execution policy';
    END IF;
    deadline := (policy->>'deadline')::timestamptz;
    IF (policy->>'schema_version')::bigint<>1 OR (policy->>'max_dispatches')::bigint<>1
       OR (policy->>'max_elapsed_secs')::bigint<>(captured->>'node_timeout_secs')::bigint
       OR (policy->>'max_elapsed_secs')::bigint NOT BETWEEN 30 AND 3600
       OR (policy->>'max_elapsed_secs')::bigint>(captured->>'max_elapsed_secs')::bigint
       OR (policy->>'max_observed_tool_calls')::bigint<>COALESCE((captured->>'max_observed_tool_calls')::bigint,100)
       OR (policy->>'max_observed_tool_calls')::bigint NOT BETWEEN 20 AND 2000
       OR deadline<>review.created_at+make_interval(secs=>(policy->>'max_elapsed_secs')::integer)
       OR clock_timestamp()>=deadline THEN
        RAISE EXCEPTION 'Template review frozen policy is invalid or its deadline has elapsed';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_graph_review_policy BEFORE INSERT OR UPDATE ON task_dispatches
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_graph_review_policy();

-- Decisions can change status/reason, never the input, policy, assignment or
-- distinct-sample watermark of an existing claim. Legacy rows stay intact.
CREATE FUNCTION agentdesk_guard_review_claim_identity() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.id,NEW.family_id,NEW.owner_campaign_id,NEW.task_key,NEW.superior_agent_id,
        NEW.sample_count,NEW.threshold,NEW.sample_digest,NEW.snapshot,NEW.created_at)
       IS DISTINCT FROM
       (OLD.id,OLD.family_id,OLD.owner_campaign_id,OLD.task_key,OLD.superior_agent_id,
        OLD.sample_count,OLD.threshold,OLD.sample_digest,OLD.snapshot,OLD.created_at) THEN
        RAISE EXCEPTION 'Published template review claim identity is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_review_claim_identity BEFORE UPDATE ON campaign_dag_reviews
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_review_claim_identity();

-- END migrations/postgres_campaign_v5/0143_campaign_review_execution_policy.sql

-- BEGIN migrations/postgres_campaign_v5/0144_campaign_graph_card_contract.sql sha256=d8cd5dafd4a455d38e132f98c97ae77ed88328516b834299bd13db108dead159
-- Native graph publication fixes the executor and prompt before any queue or
-- dispatch can run. Generic Kanban writers may still update lifecycle evidence,
-- but cannot replace the issued graph card's execution contract.
CREATE FUNCTION agentdesk_guard_graph_card_contract() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE card_id=OLD.id) AND (
        NEW.assigned_agent_id IS DISTINCT FROM OLD.assigned_agent_id
        OR NEW.repo_id IS DISTINCT FROM OLD.repo_id
        OR NEW.title IS DISTINCT FROM OLD.title
        OR NEW.description IS DISTINCT FROM OLD.description
        OR NEW.github_issue_url IS DISTINCT FROM OLD.github_issue_url
        OR NEW.github_issue_number IS DISTINCT FROM OLD.github_issue_number
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[
            'campaign_id','node_id','graph_version','graph_task_key',
            'scope_assessment_status','scope_depth',
            'sandbox_preflight','production_mutation_allowed','managed_campaign'
        ]) key WHERE NEW.metadata->key IS DISTINCT FROM OLD.metadata->key)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE='23514',
            CONSTRAINT='campaign_graph_card_contract_immutable',
            MESSAGE='campaign_graph_card_contract_immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_graph_card_contract BEFORE UPDATE ON kanban_cards
FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_graph_card_contract();

-- END migrations/postgres_campaign_v5/0144_campaign_graph_card_contract.sql

-- BEGIN migrations/postgres_campaign_v5/0145_campaign_workflow_manager.sql sha256=09980d2c3b59e6d2095af02d2bd979fb8b10f33c0088a9a6eb17b4dd7b69b596
-- Existing enrolled goals retain their published contracts and execution mode.
-- New public enrollments explicitly opt into the compact manager.
ALTER TABLE campaign_execution_specs ADD COLUMN manager_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE campaign_graph_tasks DROP CONSTRAINT campaign_graph_tasks_role_check;
ALTER TABLE campaign_graph_tasks ADD CONSTRAINT campaign_graph_tasks_role_check
    CHECK(role IN ('plan','validate','work','template_review','manage','finish'));
ALTER TABLE campaign_graph_versions ADD COLUMN manager_task_key TEXT
    REFERENCES campaign_graph_tasks(task_key);
ALTER TABLE campaign_graph_versions ADD CONSTRAINT campaign_graph_version_evolution_owner
    CHECK(failure_id IS NULL OR manager_task_key IS NULL);

-- SQL and Rust compare JSON values rather than inventing a second hash encoding.
CREATE FUNCTION agentdesk_workflow_authority_state(owner TEXT) RETURNS JSONB LANGUAGE SQL STABLE AS $$
WITH spec AS (SELECT * FROM campaign_execution_specs WHERE campaign_id=owner),
keys AS (SELECT n->>'task_key' AS key FROM campaign_graph_versions v JOIN spec s
    ON v.campaign_id=s.campaign_id AND v.version=s.active_version,
    LATERAL jsonb_array_elements(v.graph->'nodes') n),
work AS (SELECT t.*,d.id AS dispatch_id,d.status AS dispatch_status,d.result::text AS raw_result
    FROM campaign_graph_tasks t LEFT JOIN LATERAL (SELECT id,status,result FROM task_dispatches
        WHERE kanban_card_id=t.card_id AND dispatch_type IN ('implementation','rework')
        ORDER BY created_at DESC,id DESC LIMIT 1) d ON TRUE
    WHERE t.campaign_id=owner AND t.role='work' AND t.task_key IN(SELECT key FROM keys))
SELECT jsonb_build_object(
    'tasks',COALESCE((SELECT jsonb_agg(jsonb_build_object('task_key',task_key,'node_id',node_id,
        'contract_digest',contract_digest,'accepted',accepted,'observation',observation,
        'artifact_manifest',artifact_manifest,'dispatch_id',dispatch_id,'dispatch_status',dispatch_status,
        'result_digest',CASE WHEN raw_result IS NOT NULL THEN encode(sha256(convert_to(raw_result,'UTF8')),'hex') END)
        ORDER BY task_key) FROM work),'[]'::jsonb),
    'failures',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',f.id,'task_key',f.task_key,'category',f.category,'evidence',f.evidence) ORDER BY f.id)
        FROM campaign_graph_failures f WHERE f.campaign_id=owner AND (f.task_key IN(SELECT key FROM keys) OR EXISTS(
            SELECT 1 FROM campaign_graph_tasks control,spec WHERE control.task_key=f.task_key
            AND control.role IN ('plan','validate','manage','finish') AND control.version>=spec.active_version))),'[]'::jsonb),
    'unmet_goal_assessments',COALESCE((SELECT jsonb_agg(jsonb_build_object('task_key',t.task_key,'validation',t.observation->'goal_validation') ORDER BY t.task_key)
        FROM campaign_graph_tasks t JOIN spec s ON t.campaign_id=s.campaign_id AND t.version=s.active_version
        WHERE t.role='finish' AND t.accepted AND t.observation->'goal_validation'->'approved'='false'::jsonb
          AND t.execution_profile->'workflow_goal'=s.spec),'[]'::jsonb))
$$;

CREATE FUNCTION agentdesk_guard_workflow_control() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE t campaign_graph_tasks%ROWTYPE; s campaign_execution_specs%ROWTYPE; graph_digest TEXT;
BEGIN
    IF NEW.dispatch_type NOT IN ('implementation','rework') OR NEW.status NOT IN ('pending','dispatched','running') THEN RETURN NEW; END IF;
    SELECT * INTO t FROM campaign_graph_tasks WHERE card_id=NEW.kanban_card_id AND role IN ('manage','finish');
    IF NOT FOUND THEN RETURN NEW; END IF;
    PERFORM 1 FROM campaigns WHERE id=t.campaign_id FOR SHARE;
    SELECT * INTO s FROM campaign_execution_specs WHERE campaign_id=t.campaign_id FOR SHARE;
    PERFORM pg_advisory_xact_lock(hashtext('campaign-task'),hashtext(t.task_key));
    SELECT * INTO t FROM campaign_graph_tasks WHERE task_key=t.task_key FOR SHARE;
    SELECT digest INTO graph_digest FROM campaign_graph_versions
        WHERE campaign_id=t.campaign_id AND version=s.active_version AND status='active';
    IF NOT s.manager_enabled OR t.observation IS NOT NULL OR t.version IS DISTINCT FROM s.active_version
        OR t.execution_profile->'workflow_goal' IS DISTINCT FROM s.spec
        OR t.execution_profile->>'workflow_graph_digest' IS DISTINCT FROM graph_digest
        OR t.execution_profile->'workflow_authority_state' IS DISTINCT FROM agentdesk_workflow_authority_state(t.campaign_id) THEN
        RAISE EXCEPTION 'Workflow control has lost its current goal, graph or result frontier';
    END IF;
    IF t.role='finish' AND NOT EXISTS(SELECT 1 FROM campaign_graph_tasks manager
        WHERE manager.campaign_id=t.campaign_id AND manager.role='manage' AND manager.accepted
          AND manager.observation->'workflow_decision'->>'action'='complete'
          AND manager.observation->>'snapshot_digest'=t.execution_profile->>'workflow_snapshot_digest') THEN
        RAISE EXCEPTION 'Goal verification has no accepted manager completion proposal';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_workflow_control BEFORE INSERT OR UPDATE ON task_dispatches
    FOR EACH ROW EXECUTE FUNCTION agentdesk_guard_workflow_control();

-- END migrations/postgres_campaign_v5/0145_campaign_workflow_manager.sql

