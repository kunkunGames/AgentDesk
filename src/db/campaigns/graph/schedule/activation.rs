//! Audited conversion to a recurring parent preserves its completed evidence.
use super::*;
use crate::db::campaigns::graph::contract_digest;
use serde_json::Value;
use sqlx::types::Json;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Activation {
    pub expected_revision: i64,
    pub request_id: String,
    pub evidence: String,
    pub schedule: Schedule,
    /// Omission retains an existing policy; first activation pins goal.budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence_budget: Option<super::super::Budget>,
    /// False updates the future policy while preserving the parent's status.
    /// Omit true from the digest to preserve older request-id retries.
    #[serde(default = "default_activate", skip_serializing_if = "is_true")]
    pub activate: bool,
}
fn default_activate() -> bool {
    true
}
fn is_true(value: &bool) -> bool {
    *value
}
pub async fn activate(pool: &PgPool, id: &str, request: &Activation) -> Result<i64> {
    request.schedule.validate()?;
    ensure!(
        !request.request_id.trim().is_empty()
            && request.request_id.len() <= 128
            && !request.evidence.trim().is_empty()
            && request.evidence.len() <= 8000,
        "bounded schedule request and evidence required"
    );
    let digest = contract_digest(request)?;
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internalTasksEnabled is required"
    );
    let mut tx = pool.begin().await?;
    let c: Json<campaigns::Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    if let Some(row)=sqlx::query("SELECT request_digest,campaign_revision FROM campaign_schedule_revisions WHERE campaign_id=$1 AND request_id=$2").bind(id).bind(&request.request_id).fetch_optional(&mut *tx).await? {
        ensure!(row.get::<String,_>("request_digest")==digest,"schedule request ID already has different content");
        return Ok(row.get("campaign_revision"));
    }
    ensure!(
        matches!(
            c.0.status,
            CampaignStatus::Paused | CampaignStatus::Completed
        ) && c.0.revision == request.expected_revision,
        "pause or complete the parent at its expected revision first"
    );
    let row = sqlx::query(
        "SELECT spec,hold_reason,occurrence_budget FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    ensure!(
        row.get::<Option<String>, _>("hold_reason").is_none(),
        "schedule cannot clear an execution hold"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_occurrences WHERE child_id=$1)"
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?,
        "an occurrence cannot become a recurring parent"
    );
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE t.campaign_id=$1 AND (d.status IN('pending','dispatched','running') OR EXISTS(SELECT 1 FROM sessions s WHERE s.active_dispatch_id=d.id AND s.status IN('working','turn_active','awaiting_bg')))) OR EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id JOIN auto_queue_runs r ON r.id=e.run_id WHERE t.campaign_id=$1 AND r.status IN('generated','active','paused'))").bind(id).fetch_one(&mut *tx).await?,"parent still owns execution or an unsettled queue");
    ensure!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM campaign_graph_failures f WHERE f.campaign_id=$1 AND f.category IN('unknown','infrastructure') AND NOT EXISTS(SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id))").bind(id).fetch_one(&mut *tx).await?,"unknown prior outcomes require reconciliation");
    let previous: Value = row.get("spec");
    let mut goal: Goal = serde_json::from_value(previous.clone())?;
    ensure!(
        request.activate || goal.schedule.is_some(),
        "policy-only changes require an existing recurring parent; first activation establishes admission time"
    );
    let previous_budget: Option<Value> = row.get("occurrence_budget");
    let proposed_budget = request
        .occurrence_budget
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let occurrence_budget = serde_json::to_value(budget::resolve(
        &goal,
        proposed_budget.or_else(|| previous_budget.clone()),
    )?)?;
    ensure!(
        goal.observation_window.is_none(),
        "an observation window cannot become a parent"
    );
    goal.schedule = Some(request.schedule.clone());
    goal.validate()?;
    let revised = serde_json::to_value(&goal)?;
    sqlx::query("UPDATE campaign_execution_specs SET spec=$2,occurrence_budget=$3,schedule_admission_started_at=CASE WHEN $4 THEN NOW() ELSE schedule_admission_started_at END,preflight=NULL WHERE campaign_id=$1").bind(id).bind(&revised).bind(&occurrence_budget).bind(request.activate).execute(&mut *tx).await?;
    let mut input: CampaignInput = serde_json::from_value(serde_json::to_value(&c.0)?)?;
    if request.activate {
        input.status = CampaignStatus::Active;
    }
    let changed = campaigns::replace_on_tx(&mut tx, id, c.0.revision, input).await?;
    sqlx::query("INSERT INTO campaign_schedule_revisions(campaign_id,request_id,request_digest,campaign_revision,previous_goal,revised_goal,evidence,previous_occurrence_budget,revised_occurrence_budget) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(id).bind(&request.request_id).bind(digest).bind(changed.revision).bind(previous).bind(revised).bind(&request.evidence).bind(previous_budget).bind(occurrence_budget).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(changed.revision)
}
