//! Decode the exact terminal native planning attempt tied to its failure.
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn result(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    key: &str,
    failure: &Value,
) -> Result<Value> {
    let dispatch = failure["dispatch_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("captured planning dispatch required"))?;
    let row = sqlx::query(
        "SELECT d.status,d.result::text AS result FROM task_dispatches d
         JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
         WHERE d.id=$1 AND t.campaign_id=$2 AND t.task_key=$3 AND t.role='plan'
           AND d.dispatch_type IN ('implementation','rework')",
    )
    .bind(dispatch)
    .bind(id)
    .bind(key)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        row.get::<String, _>("status") == "completed",
        "planning dispatch must be terminal and completed"
    );
    let raw: Option<String> = row.get("result");
    let mut result: Value = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
    if let Some(encoded) = result.as_str() {
        result = serde_json::from_str(encoded)?;
    }
    Ok(result)
}
