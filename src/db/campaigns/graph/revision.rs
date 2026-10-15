//! Audited operator correction before any business task is published.
//! Keep the campaign paused, retain usage and ask the planner for a new graph.
pub(crate) mod budget;
use super::{Goal, contract_digest};
use crate::db::campaigns::{self, Campaign, CampaignStatus};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub expected_revision: i64,
    pub request_id: String,
    pub evidence: String,
    pub goal: Goal,
}

pub async fn revise(pool: &PgPool, id: &str, request: &Revision) -> Result<i64> {
    request.goal.validate()?;
    ensure!(
        !request.request_id.trim().is_empty() && request.request_id.len() <= 128,
        "bounded request ID required"
    );
    ensure!(
        !request.evidence.trim().is_empty() && request.evidence.len() <= 8000,
        "bounded correction evidence required"
    );
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internal task admission disabled"
    );
    let digest = contract_digest(request)?;
    let mut tx = pool.begin().await?;
    let c: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    if let Some(prior) = sqlx::query("SELECT request_digest,campaign_revision FROM campaign_goal_revisions WHERE campaign_id=$1 AND request_id=$2")
        .bind(id).bind(&request.request_id).fetch_optional(&mut *tx).await? {
        ensure!(prior.get::<String,_>("request_digest") == digest, "request ID already used with different content");
        return Ok(prior.get("campaign_revision"));
    }
    ensure!(
        c.0.status == CampaignStatus::Paused && c.0.revision == request.expected_revision,
        "pause the campaign at the expected revision first"
    );
    let row = sqlx::query(
        "SELECT spec,hold_reason FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    let previous: Value = row.get("spec");
    let old: Goal = serde_json::from_value(previous.clone())?;
    super::schedule::budget::validate_revision(&mut tx, id, &request.goal.budget).await?;
    ensure!(
        old.observation_window == request.goal.observation_window
            && old.repo == request.goal.repo
            && old.agents == request.goal.agents
            && old.planner == request.goal.planner
            && old.verifier == request.goal.verifier
            && serde_json::to_value(&old.schedule)?
                == serde_json::to_value(&request.goal.schedule)?,
        "scope correction cannot change repository, executors or schedule"
    );
    ensure!(
        previous != serde_json::to_value(&request.goal)?,
        "goal is unchanged"
    );
    // An operator may explicitly adjust finite limits, with both values audited.
    // No lifetime, dispatch, event or evolution counters are reset here.
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='work') OR EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE campaign_id=$1)")
        .bind(id).fetch_one(&mut *tx).await?, "business tasks or scheduled occurrences already exist; preserve their contracts");
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE t.campaign_id=$1 AND (d.status IN ('pending','dispatched','running') OR EXISTS(SELECT 1 FROM sessions s WHERE s.active_dispatch_id=d.id AND s.status IN ('working','turn_active','awaiting_bg'))))")
        .bind(id).fetch_one(&mut *tx).await?, "campaign owns active execution");
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id JOIN auto_queue_runs r ON r.id=e.run_id WHERE t.campaign_id=$1 AND r.status IN ('generated','active','paused'))")
        .bind(id).fetch_one(&mut *tx).await?, "campaign queue cleanup is incomplete");
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_failures WHERE campaign_id=$1 AND category IN ('unknown','infrastructure'))")
        .bind(id).fetch_one(&mut *tx).await?, "unknown execution outcomes require reconciliation");
    // A completed validator may not have been observed after the operator
    // paused admission. Keep its raw dispatch result, but never let an old
    // approval activate a graph against the revised goal.
    let pending = sqlx::query("SELECT t.task_key,d.status,d.result FROM campaign_graph_tasks t LEFT JOIN LATERAL (SELECT status,result FROM task_dispatches WHERE kanban_card_id=t.card_id ORDER BY created_at DESC LIMIT 1) d ON TRUE WHERE t.campaign_id=$1 AND t.observation IS NULL")
        .bind(id).fetch_all(&mut *tx).await?;
    for task in pending {
        ensure!(
            task.get::<Option<String>, _>("status").as_deref() == Some("completed"),
            "unobserved control execution is not completed"
        );
        let raw: Option<String> = task.get("result");
        let mut result: Value = serde_json::from_str(raw.as_deref().unwrap_or("null"))?;
        if let Some(encoded) = result.as_str() {
            result = serde_json::from_str(encoded)?;
        }
        ensure!(
            matches!(
                result
                    .pointer("/campaign_receipt/outcome")
                    .and_then(Value::as_str),
                Some("passed" | "no_work")
            ),
            "unobserved failed control requires reconciliation"
        );
        sqlx::query("UPDATE campaign_graph_tasks SET observation=$2 WHERE task_key=$1 AND observation IS NULL")
            .bind(task.get::<String,_>("task_key"))
            .bind(json!({"outcome":"superseded_without_acceptance","operator_goal_revision":request.request_id}))
            .execute(&mut *tx).await?;
    }
    let plan: Option<String> = sqlx::query_scalar("SELECT task_key FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='plan' ORDER BY created_at DESC LIMIT 1")
        .bind(id).fetch_optional(&mut *tx).await?;
    if let Some(key) = plan {
        let failure = contract_digest(&(id, "operator-goal-revision", &request.request_id))?;
        let evidence = json!({"operator_goal_revision":request.request_id,"reason":request.evidence,
            "previous_goal":previous,"revised_goal":request.goal,
            "instruction":"Correct the graph against the revised goal. Preserve all prior evidence. Independent validation is required; no business task has been published."});
        sqlx::query("INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence) VALUES($1,$2,$3,$1,'plan',$4)")
            .bind(failure).bind(id).bind(key).bind(evidence).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE campaign_graph_versions SET status=CASE WHEN status='active' THEN 'superseded' ELSE 'rejected' END WHERE campaign_id=$1 AND status IN ('candidate','validated','active')")
        .bind(id).execute(&mut *tx).await?;
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2,active_version=NULL,hold_reason=NULL WHERE campaign_id=$1")
        .bind(id).bind(serde_json::to_value(&request.goal)?).execute(&mut *tx).await?;
    let updated = campaigns::replace_on_tx(
        &mut tx,
        id,
        c.0.revision,
        serde_json::from_value(serde_json::to_value(&c.0)?)?,
    )
    .await?;
    sqlx::query("INSERT INTO campaign_goal_revisions(campaign_id,request_id,request_digest,campaign_revision,previous_goal,revised_goal,evidence) VALUES($1,$2,$3,$4,$5,$6,$7)")
        .bind(id).bind(&request.request_id).bind(digest).bind(updated.revision)
        .bind(previous).bind(serde_json::to_value(&request.goal)?).bind(&request.evidence).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(updated.revision)
}
