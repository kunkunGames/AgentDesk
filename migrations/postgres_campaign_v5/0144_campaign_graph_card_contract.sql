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
