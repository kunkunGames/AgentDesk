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
