-- Operator reconciliation authorizes a new independently validated contract;
-- it never rewrites the original failure, card, dispatch or artifact evidence.
CREATE TABLE campaign_graph_reconciliations (
    failure_id TEXT PRIMARY KEY REFERENCES campaign_graph_failures(id),
    campaign_id TEXT NOT NULL REFERENCES campaign_execution_specs(campaign_id),
    expected_revision BIGINT NOT NULL,
    evidence TEXT NOT NULL CHECK(length(evidence) BETWEEN 1 AND 8000),
    candidate_version BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY(campaign_id,candidate_version) REFERENCES campaign_graph_versions(campaign_id,version)
);
