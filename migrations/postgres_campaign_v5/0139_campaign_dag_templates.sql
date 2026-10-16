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
