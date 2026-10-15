//! Durable control ownership and source-correction context for planner turns.
use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub(super) fn pending(state: &Value) -> bool {
    state["tasks"].as_array().is_some_and(|tasks| {
        tasks.iter().any(|task| {
            matches!(task["role"].as_str(), Some("plan" | "manage" | "finish"))
                && task["accepted"] != true
                && task["observation"].is_null()
        })
    })
}

pub(super) async fn settle_superseded_unstarted(pool: &PgPool, campaign: &str) -> Result<()> {
    crate::db::campaigns::graph::recovery::replan::settle_superseded_unstarted_planners(
        pool, campaign,
    )
    .await?;
    crate::db::campaigns::graph::workflow::retirement::settle_stale_unstarted(pool, campaign)
        .await?;
    Ok(())
}

pub(super) async fn observe_retired_validation(
    pool: &PgPool,
    campaign: &str,
    key: &str,
    dispatch: &str,
    version: i64,
    validation: &Value,
) -> Result<bool> {
    let digest = validation["digest"].as_str().unwrap_or("");
    let retired: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_versions v
         WHERE v.campaign_id=$1 AND v.version=$2 AND v.digest=$3 AND v.status='rejected'
         AND EXISTS(SELECT 1 FROM campaign_graph_failures repair
           WHERE repair.campaign_id=v.campaign_id AND repair.evidence ? 'operator_replan'
             AND repair.evidence->'superseded_candidate'->>'version'=v.version::text
             AND repair.evidence->'superseded_candidate'->>'digest'=v.digest))",
    )
    .bind(campaign)
    .bind(version)
    .bind(digest)
    .fetch_one(pool)
    .await?;
    if !retired {
        return Ok(false);
    }
    // Record the assessment as history, not approval of the retired candidate.
    // Raw provider result, proposal, task identity and lifetime usage stay intact.
    sqlx::query("UPDATE campaign_graph_tasks SET observation=$2
        WHERE campaign_id=$3 AND task_key=$1 AND role='validate' AND version=$4
          AND NOT accepted AND observation IS NULL")
        .bind(key).bind(serde_json::json!({"dispatch_id":dispatch,"outcome":"superseded",
            "graph_version":version,"reason":"Digest-pinned operator source correction retired this unexecuted candidate before result observation"}))
        .bind(campaign).bind(version).execute(pool).await?;
    Ok(true)
}
