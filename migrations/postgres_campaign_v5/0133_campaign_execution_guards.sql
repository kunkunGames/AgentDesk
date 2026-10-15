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
