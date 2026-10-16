//! Observed native provider telemetry, not a claim of per-tool interception.
use sqlx::PgPool;
use std::time::Duration;

/// The native TUI watcher bypasses the stream bridge. Persist its bounded,
/// hash-only batch in one transaction, deduplicated by provider event identity.
pub(crate) async fn watcher_batch(pool: &PgPool, dispatch: &str, events: &[serde_json::Value]) {
    if events.is_empty() || events.len() > 256 {
        return;
    }
    let body = serde_json::Value::Array(events.to_vec());
    let write = async {
        let mut tx = pool.begin().await?;
        sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint)
            SELECT $1,e.event_id,e.fingerprint FROM jsonb_to_recordset($2) AS e(event_id text,fingerprint text)
            WHERE e.fingerprint IS NOT NULL AND length(e.event_id) BETWEEN 1 AND 256
            AND EXISTS(SELECT 1 FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE d.id=$1)
            ON CONFLICT DO NOTHING")
            .bind(dispatch).bind(&body).execute(&mut *tx).await?;
        sqlx::query("UPDATE campaign_graph_progress p SET output_digest=e.output_digest
            FROM jsonb_to_recordset($2) AS e(event_id text,output_digest text)
            WHERE p.dispatch_id=$1 AND p.event_id=e.event_id AND p.output_digest IS NULL AND e.output_digest IS NOT NULL")
            .bind(dispatch).bind(&body).execute(&mut *tx).await?;
        tx.commit().await
    };
    if !matches!(
        tokio::time::timeout(Duration::from_millis(500), write).await,
        Ok(Ok::<_, sqlx::Error>(_))
    ) {
        tracing::warn!(%dispatch,"graph watcher telemetry unavailable; observed counts may be incomplete");
    }
}

pub(crate) async fn tool_use(
    pool: Option<&PgPool>,
    dispatch: Option<&str>,
    event: Option<&str>,
    fingerprint: &str,
) {
    let (Some(pool), Some(dispatch), Some(event)) = (pool, dispatch, event) else {
        return;
    };
    if event.len() > 256 {
        return;
    }
    let result=tokio::time::timeout(Duration::from_millis(500), sqlx::query("INSERT INTO campaign_graph_progress(dispatch_id,event_id,fingerprint) SELECT $1,$2,$3 WHERE EXISTS(SELECT 1 FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE d.id=$1) ON CONFLICT DO NOTHING")
        .bind(dispatch).bind(event).bind(fingerprint).execute(pool)).await;
    if !matches!(result, Ok(Ok(_))) {
        tracing::warn!(%dispatch,"graph telemetry unavailable; observed counts may be incomplete");
    }
}
pub(crate) async fn tool_result(
    pool: Option<&PgPool>,
    dispatch: Option<&str>,
    event: Option<&str>,
    hash: &str,
) {
    let (Some(pool), Some(dispatch), Some(event)) = (pool, dispatch, event) else {
        return;
    };
    if !matches!(tokio::time::timeout(Duration::from_millis(500), sqlx::query("UPDATE campaign_graph_progress SET output_digest=$3 WHERE dispatch_id=$1 AND event_id=$2 AND output_digest IS NULL")
        .bind(dispatch).bind(event).bind(hash).execute(pool)).await, Ok(Ok(_))) {tracing::warn!(%dispatch,"graph result telemetry unavailable");}
}

/// Canonical cancellation can lack completed_at. Freeze terminal lifetime at
/// updated_at instead of charging a cancelled dispatch forever across restarts.
pub(crate) async fn elapsed_seconds(pool: &PgPool, campaign: &str) -> Result<f64, sqlx::Error> {
    sqlx::query_scalar("SELECT COALESCE(SUM(GREATEST(0, EXTRACT(EPOCH FROM (COALESCE(d.completed_at, CASE WHEN d.status IN ('completed','failed','cancelled','rejected') THEN d.updated_at ELSE NOW() END)-d.created_at)))),0)::float8 FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.campaign_id=$1 AND d.dispatch_type IN ('implementation','rework')")
        .bind(campaign).fetch_one(pool).await
}
