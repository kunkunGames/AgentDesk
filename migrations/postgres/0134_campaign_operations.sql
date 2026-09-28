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
