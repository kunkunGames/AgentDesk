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
