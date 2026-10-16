//! Renew only a failed, unaccepted artifact identity in an existing proposal.
//! The published key and receipt stay immutable; QA reviews the new candidate.
use super::super::{Effect, Goal, Graph, contract_digest};
use super::captured;
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn candidate(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    goal: &Goal,
    version: i64,
    key: &str,
    failure: &Value,
) -> Result<Graph> {
    ensure!(
        failure["external_effect_requires_reconciliation"] == false,
        "planning effects require separate reconciliation"
    );
    let result = captured::result(tx, id, key, failure).await?;
    ensure!(
        matches!(
            result["campaign_receipt"]["outcome"].as_str(),
            Some("passed" | "no_work")
        ),
        "identity recovery requires the already checked successful planning receipt"
    );
    let mut graph: Graph = serde_json::from_value(result["campaign_graph"].clone())?;
    graph.validate(goal)?;
    let original: Value = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 AND status='active'",
    ).bind(id).bind(version).fetch_one(&mut **tx).await?;
    let original: Graph = serde_json::from_value(original)?;
    let mut renewed = 0;
    for node in &mut graph.nodes {
        let prior = sqlx::query(
            "SELECT t.campaign_id,t.role,t.accepted,t.observation,t.contract_digest,
             EXISTS(SELECT 1 FROM campaign_graph_failures f WHERE f.task_key=t.task_key
                 AND f.category IN ('code','plan')) AS failed,
             EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=t.card_id
                 AND (d.status IN ('pending','dispatched','running') OR EXISTS(
                     SELECT 1 FROM sessions s WHERE s.active_dispatch_id=d.id
                     AND s.status IN ('working','turn_active','awaiting_bg')))) AS running
             FROM campaign_graph_tasks t WHERE t.task_key=$1",
        )
        .bind(&node.task_key)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(prior) = prior else {
            continue;
        };
        ensure!(
            prior.get::<String, _>("campaign_id") == id,
            "task identity belongs to another campaign"
        );
        let old_digest: String = prior.get("contract_digest");
        let proposed_digest = contract_digest(node)?;
        if old_digest == proposed_digest {
            continue;
        }
        let old = original
            .nodes
            .iter()
            .find(|n| n.task_key == node.task_key)
            .ok_or_else(|| anyhow::anyhow!("changed identity is outside the active graph"))?;
        ensure!(
            prior.get::<String, _>("role") == "work"
                && !prior.get::<bool, _>("accepted")
                && !prior.get::<bool, _>("running")
                && prior.get::<bool, _>("failed")
                && prior.get::<Option<Value>, _>("observation").is_some()
                && old.effect == Effect::ArtifactOnly
                && node.effect == Effect::ArtifactOnly
                && old.id == node.id
                && old.kind == node.kind
                && old.agent == node.agent,
            "only a failed unaccepted artifact task may acquire a new identity; preserve effects and accepted/running contracts"
        );
        super::descendants::require_unpublished(tx, &original, &old.id).await?;
        let old_key = node.task_key.clone();
        node.task_key = format!(
            "artifact-revision:{}",
            contract_digest(&(id, &old_key, &old_digest, &proposed_digest))?
        );
        node.details.push_str(&format!("\nNative identity reconciliation: {old_key} remains the immutable failed artifact attempt. This contract has a distinct identity; independently validate its proposed artifact requirements before execution."));
        renewed += 1;
    }
    ensure!(
        renewed > 0,
        "captured proposal has no safely renewable artifact identity"
    );
    Ok(graph)
}
