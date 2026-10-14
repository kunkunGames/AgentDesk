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
