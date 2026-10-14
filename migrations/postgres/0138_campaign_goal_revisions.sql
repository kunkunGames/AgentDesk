-- Operator scope corrections retain immutable before/after contracts and all
-- prior execution usage. They do not replay or migrate business tasks.
CREATE TABLE campaign_goal_revisions (
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    request_id TEXT NOT NULL CHECK(length(request_id) BETWEEN 1 AND 128),
    request_digest TEXT NOT NULL,
    campaign_revision BIGINT NOT NULL,
    previous_goal JSONB NOT NULL,
    revised_goal JSONB NOT NULL,
    evidence TEXT NOT NULL CHECK(length(evidence) BETWEEN 1 AND 8000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(campaign_id,request_id)
);
