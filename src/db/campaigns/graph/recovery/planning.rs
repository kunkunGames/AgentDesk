//! Stage a captured proposal after a verified planning environment correction.
//! The blocked receipt stays unaccepted; the independent verifier has authority.
use super::super::{Effect, Graph, Kind};
use super::{Reconcile, artifact::planning_evidence};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Postgres, Transaction};

pub(super) async fn candidate(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    key: &str,
    failure: &Value,
    request: &Reconcile,
) -> Result<Graph> {
    ensure!(
        failure["external_effect_requires_reconciliation"] == false,
        "planning effects require separate reconciliation"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work')
         OR EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1)
         OR EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE campaign_id=$1)",
        )
        .bind(id)
        .fetch_one(&mut **tx)
        .await?,
        "planning recovery is only available before any business graph or occurrence"
    );
    let result = super::captured::result(tx, id, key, failure).await?;
    ensure!(
        result["campaign_receipt"]["outcome"] == "blocked"
            && result["campaign_receipt"]["failure_category"] == "infrastructure",
        "planning recovery requires its captured infrastructure-blocked receipt"
    );
    let mut graph: Graph = serde_json::from_value(result["campaign_graph"].clone())?;
    let mut roots = 0;
    for node in graph.nodes.iter_mut().filter(|n| n.dependencies.is_empty()) {
        ensure!(
            node.kind == Kind::Inspect && node.effect == Effect::ArtifactOnly,
            "recovered planning roots must independently inspect without external effects"
        );
        node.details.push_str(&planning_evidence(&request.evidence));
        roots += 1;
    }
    ensure!(
        roots > 0,
        "captured planning proposal has no inspection root"
    );
    Ok(graph)
}
