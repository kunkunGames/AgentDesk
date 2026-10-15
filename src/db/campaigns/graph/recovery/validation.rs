//! Revalidate a never-executed candidate after lost structured transport.
//! A retained file is evidence of the transport fault, never substituted approval.
use super::super::{Effect, Graph, Kind};
use super::{Reconcile, artifact::planning_evidence};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn candidate(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    key: &str,
    failure: &Value,
    request: &Reconcile,
) -> Result<Graph> {
    ensure!(
        failure["external_effect_requires_reconciliation"] == false,
        "validation effects require separate reconciliation"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work')
         OR EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE campaign_id=$1)"
        )
        .bind(id)
        .fetch_one(&mut **tx)
        .await?,
        "validation recovery is only available before business publication or occurrences"
    );
    let dispatch = failure["dispatch_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("captured validation dispatch required"))?;
    let row = sqlx::query(
        "SELECT d.status,d.result::text AS result,t.version FROM task_dispatches d
         JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
         WHERE d.id=$1 AND t.campaign_id=$2 AND t.task_key=$3 AND t.role='validate'
           AND d.dispatch_type IN ('implementation','rework')",
    )
    .bind(dispatch)
    .bind(id)
    .bind(key)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        row.get::<String, _>("status") == "completed",
        "validation dispatch must be terminal"
    );
    let raw: Option<String> = row.get("result");
    let mut result: Value = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
    if let Some(encoded) = result.as_str() {
        result = serde_json::from_str(encoded)?;
    }
    ensure!(
        result["completion_source"] == "watcher_streaming_final"
            && result["agent_response_present"] == true
            && result["campaign_receipt"].is_null(),
        "validation recovery requires captured loss of the structured streaming result"
    );
    let version: i64 = row
        .get::<Option<i64>, _>("version")
        .ok_or_else(|| anyhow::anyhow!("validation candidate version required"))?;
    let value: Value = sqlx::query_scalar(
        "SELECT graph FROM campaign_graph_versions WHERE campaign_id=$1 AND version=$2 AND status='candidate'")
        .bind(id).bind(version).fetch_one(&mut **tx).await?;
    let mut graph: Graph = serde_json::from_value(value)?;
    for node in graph.nodes.iter_mut().filter(|n| n.dependencies.is_empty()) {
        ensure!(
            node.kind == Kind::Inspect && node.effect == Effect::ArtifactOnly,
            "revalidated roots must inspect without external effects"
        );
        node.details.push_str(&planning_evidence(&request.evidence));
    }
    // Keep the failed validator and candidate digest; admit a distinct candidate
    // and a fresh independent validator rather than replaying a terminal task.
    ensure!(sqlx::query("UPDATE campaign_graph_versions SET status='rejected' WHERE campaign_id=$1 AND version=$2 AND status='candidate'")
        .bind(id).bind(version).execute(&mut **tx).await?.rows_affected() == 1,
        "validation candidate changed");
    Ok(graph)
}
