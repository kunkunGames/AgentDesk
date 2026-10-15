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
