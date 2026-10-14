-- Preserve accepted templates while superior agents review the next sample batch.
ALTER TABLE campaign_dag_reviews DROP CONSTRAINT campaign_dag_reviews_status_check;
ALTER TABLE campaign_dag_reviews ADD CONSTRAINT campaign_dag_reviews_status_check
    CHECK(status IN ('pending','promoted','kept','deferred','failed'));
ALTER TABLE campaign_dag_reviews ADD COLUMN decision_digest TEXT;
ALTER TABLE campaign_execution_specs ADD COLUMN preflight JSONB;
ALTER TABLE campaign_graph_tasks ADD COLUMN execution_profile JSONB;
ALTER TABLE campaign_execution_specs ADD COLUMN schedule_admission_started_at TIMESTAMPTZ;
CREATE TABLE campaign_dispatch_observations (
    dispatch_id TEXT PRIMARY KEY REFERENCES task_dispatches(id),
    provider TEXT NOT NULL,
    model TEXT,
    usage JSONB,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE campaign_schedule_revisions (
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    request_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    campaign_revision BIGINT NOT NULL,
    previous_goal JSONB NOT NULL,
    revised_goal JSONB NOT NULL,
    evidence TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(campaign_id,request_id)
);

