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
