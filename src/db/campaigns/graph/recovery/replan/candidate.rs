//! Explicitly invalidate only a pinned successor that has published no business work.
use super::UnpublishedCandidate;
use anyhow::{Result, ensure};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

pub(super) async fn retire(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    active: Option<i64>,
    failure: &PgRow,
    requested: Option<&UnpublishedCandidate>,
) -> Result<()> {
    if failure.get::<String, _>("role") != "work" {
        ensure!(
            requested.is_none(),
            "candidate supersession requires a business planning failure"
        );
        return Ok(());
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM campaign_graph_versions WHERE campaign_id=$1 AND status IN ('candidate','validated')",
    ).bind(id).fetch_one(&mut **tx).await?;
    let Some(requested) = requested else {
        ensure!(count == 0, "another candidate already awaits QA");
        return Ok(());
    };
    ensure!(
        failure.get::<String, _>("category") == "plan",
        "candidate supersession requires a business planning failure"
    );
    ensure!(
        count == 1
            && requested.version > 0
            && requested.digest.len() == 64
            && requested.digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "one digest-pinned unpublished candidate required"
    );
    let latest = sqlx::query(
        "SELECT version,parent_version,digest,status,template_id FROM campaign_graph_versions WHERE campaign_id=$1 ORDER BY version DESC LIMIT 1 FOR UPDATE",
    ).bind(id).fetch_one(&mut **tx).await?;
    ensure!(
        latest.get::<i64, _>("version") == requested.version
            && latest.get::<String, _>("digest") == requested.digest
            && matches!(
                latest.get::<String, _>("status").as_str(),
                "candidate" | "validated"
            )
            && active.is_some()
            && latest.get::<Option<i64>, _>("parent_version") == active
            && latest.get::<Option<String>, _>("template_id").is_none(),
        "candidate identity, parent or approval scope changed"
    );
    ensure!(!sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND version=$2 AND role='work')",
    ).bind(id).bind(requested.version).fetch_one(&mut **tx).await?, "candidate has published business work");
    // Keep the proposed graph, original planner/QA receipts and usage. This
    // explicit invalidation cannot activate or approve a replacement.
    let changed = sqlx::query(
        "UPDATE campaign_graph_versions SET status='rejected' WHERE campaign_id=$1 AND version=$2 AND digest=$3 AND status IN ('candidate','validated')",
    ).bind(id).bind(requested.version).bind(&requested.digest).execute(&mut **tx).await?.rows_affected();
    ensure!(changed == 1, "candidate changed during source correction");
    Ok(())
}
