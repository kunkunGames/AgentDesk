//! A captured valid proposal may be revalidated after a runtime/schema mismatch.
//! Keep the original failed attempt, lifetime budget and independent QA authority.
use super::super::{Effect, Graph, Kind, result};
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
    ensure!(!sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work')
         OR EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE campaign_id=$1)
         OR EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1 AND status<>'rejected')")
        .bind(id).fetch_one(&mut **tx).await?,
        "transport recovery requires no published business work, occurrence or live candidate");
    let captured = super::captured::result(tx, id, key, failure).await?;
    ensure!(
        matches!(
            captured["completion_source"].as_str(),
            Some("campaign_native_receipt" | "watcher_streaming_final")
        ),
        "captured proposal lacks native runtime provenance"
    );
    result::validate_dispatch(&captured)?;
    ensure!(
        captured["campaign_receipt"]["outcome"] == "passed",
        "transport recovery requires a captured passed planning proposal"
    );
    let mut graph: Graph = serde_json::from_value(captured["campaign_graph"].clone())?;
    let mut roots = 0;
    for node in graph.nodes.iter_mut().filter(|n| n.dependencies.is_empty()) {
        ensure!(
            node.kind == Kind::Inspect && node.effect == Effect::ArtifactOnly,
            "recovered proposal roots must inspect without external effects"
        );
        node.details.push_str(&planning_evidence(&request.evidence));
        roots += 1;
    }
    ensure!(roots > 0, "captured proposal has no inspection root");
    Ok(graph)
}
