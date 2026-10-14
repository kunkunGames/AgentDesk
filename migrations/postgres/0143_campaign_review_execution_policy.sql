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
