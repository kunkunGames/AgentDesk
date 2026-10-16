use super::super::{Effect, Graph, contract_digest};
use super::Reconcile;
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction};

pub(super) async fn candidate(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    version: Option<i64>,
    key: &str,
    request: &Reconcile,
) -> Result<Graph> {
    let version = version.ok_or_else(|| anyhow::anyhow!("missing active business graph"))?;
    let original: serde_json::Value = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 AND status='active'",
    ).bind(id).bind(version).fetch_one(&mut **tx).await?;
    let mut graph: Graph = serde_json::from_value(original)?;
    let node = graph
        .nodes
        .iter()
        .find(|n| n.task_key == key)
        .ok_or_else(|| anyhow::anyhow!("failure is outside the active graph"))?;
    ensure!(
        node.effect == Effect::ArtifactOnly,
        "external effects require a separate effect reconciliation; replay is forbidden"
    );
    let failed_id = node.id.clone();
    super::descendants::require_unpublished(tx, &graph, &failed_id).await?;
    let node = graph
        .nodes
        .iter_mut()
        .find(|n| n.id == failed_id)
        .ok_or_else(|| anyhow::anyhow!("failed node disappeared"))?;
    node.task_key = format!(
        "reconciled:{}",
        contract_digest(&(id, key, &request.failure_id))?
    );
    node.details.push_str(&planning_evidence(&request.evidence));
    Ok(graph)
}

pub(super) fn planning_evidence(evidence: &str) -> String {
    format!(
        "\nOperator reconciliation evidence (verify independently; not a success receipt):\n{evidence}\nPreserve the previous blocked attempt. Re-observe current state and produce fresh outputs; do not assume the correction succeeded."
    )
}
