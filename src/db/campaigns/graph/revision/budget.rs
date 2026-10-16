//! Audited finite budget increases preserve every published execution contract.
use super::super::{Budget, Goal, contract_digest, learning};
use crate::db::campaigns::{self, Campaign, CampaignStatus};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row, types::Json};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetRevision {
    pub expected_revision: i64,
    pub request_id: String,
    pub evidence: String,
    pub budget: Budget,
}

pub(in crate::db::campaigns::graph) fn validate_increase(old: &Budget, new: &Budget) -> Result<()> {
    ensure!(
        new.max_dispatches >= old.max_dispatches
            && new.max_elapsed_secs >= old.max_elapsed_secs
            && new.max_evolutions >= old.max_evolutions
            && new.node_timeout_secs == old.node_timeout_secs
            && match (old.max_observed_tool_calls, new.max_observed_tool_calls) {
                (Some(before), Some(after)) => after >= before,
                (None, None) => true,
                _ => false,
            },
        "only finite increases are allowed; retain node timeout and tool-limit mode"
    );
    ensure!(
        serde_json::to_value(old)? != serde_json::to_value(new)?,
        "budget is unchanged"
    );
    Ok(())
}

pub async fn extend(pool: &PgPool, id: &str, request: &BudgetRevision) -> Result<i64> {
    ensure!(
        !request.request_id.trim().is_empty() && request.request_id.len() <= 128,
        "bounded request ID required"
    );
    ensure!(
        !request.evidence.trim().is_empty() && request.evidence.len() <= 8000,
        "bounded budget evidence required"
    );
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internal task admission disabled"
    );
    let digest = contract_digest(&("budget-only", request))?;
    let mut tx = pool.begin().await?;
    let c: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    if let Some(prior) = sqlx::query(
        "SELECT request_digest,campaign_revision FROM campaign_goal_revisions WHERE campaign_id=$1 AND request_id=$2",
    ).bind(id).bind(&request.request_id).fetch_optional(&mut *tx).await? {
        ensure!(prior.get::<String, _>("request_digest") == digest,
            "request ID already used with different content");
        return Ok(prior.get("campaign_revision"));
    }
    ensure!(
        c.0.status == CampaignStatus::Paused && c.0.revision == request.expected_revision,
        "pause the campaign at the expected revision first"
    );
    let row = sqlx::query(
        "SELECT spec,hold_reason,occurrence_budget FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    let previous: Value = row.get("spec");
    let old: Goal = serde_json::from_value(previous.clone())?;
    let occurrence_budget = if old.schedule.is_some() {
        Some(serde_json::to_value(
            super::super::schedule::budget::resolve(&old, row.get("occurrence_budget"))?,
        )?)
    } else {
        row.get::<Option<Value>, _>("occurrence_budget")
    };
    validate_increase(&old.budget, &request.budget)?;
    super::super::schedule::budget::validate_revision(&mut tx, id, &request.budget).await?;
    let mut revised = old.clone();
    revised.budget = request.budget.clone();
    revised.validate()?;
    let hold: Option<String> = row.get("hold_reason");
    let held_control = super::super::control_budget::held_role(hold.as_deref());
    ensure!(
        hold.is_none()
            || held_control.is_some()
            || hold.as_deref() == Some("Plan evolution budget exhausted")
            || hold.as_deref()
                == Some(
                    "Cumulative execution time or observed tool budget exhausted; retained across restart and graph revisions"
                ),
        "budget extension cannot clear an execution or effect reconciliation hold"
    );
    ensure!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_failures f WHERE f.campaign_id=$1
         AND f.category IN ('unknown','infrastructure') AND NOT EXISTS(
             SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id))",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?,
        "unknown execution outcomes require reconciliation"
    );
    ensure!(!sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id
         WHERE t.campaign_id=$1 AND (d.status IN ('pending','dispatched','running') OR EXISTS(
             SELECT 1 FROM sessions s WHERE s.active_dispatch_id=d.id AND s.status IN ('working','turn_active','awaiting_bg'))))",
    ).bind(id).fetch_one(&mut *tx).await?, "campaign owns active execution");
    ensure!(!sqlx::query_scalar::<_, bool>(
        "WITH owned AS (SELECT DISTINCT r.id,r.status FROM campaign_graph_tasks t
         JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id JOIN auto_queue_runs r ON r.id=e.run_id
         WHERE t.campaign_id=$1) SELECT EXISTS(SELECT 1 FROM owned WHERE status IN ('generated','active','paused'))
         OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks WHERE run_ids && ARRAY(SELECT id FROM owned))",
    ).bind(id).fetch_one(&mut *tx).await?, "campaign queue cleanup is incomplete");
    ensure!(!sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks WHERE campaign_id=$1 AND observation IS NULL)",
    ).bind(id).fetch_one(&mut *tx).await?, "unobserved work must settle before budget extension");
    let usage = learning::budget::usage_on_connection(&mut tx, id, &old).await?;
    ensure!(
        usage.published_tasks + 2 < i64::from(revised.budget.max_dispatches)
            && usage.elapsed_dispatch_seconds < f64::from(revised.budget.max_elapsed_secs)
            && revised
                .budget
                .max_observed_tool_calls
                .is_none_or(|limit| usage.observed_tool_calls < i64::from(limit)),
        "revised finite budget must leave capacity for independently verified work"
    );
    if let Some(role) = held_control {
        let frame = super::super::control_budget::on_connection(&mut tx, id, &revised).await?;
        ensure!(
            frame.control_wait(role).is_none(),
            "revised finite budget must satisfy the held control and fresh verification reserves"
        );
    }
    if hold.as_deref() == Some("Plan evolution budget exhausted") {
        ensure!(
            revised.budget.max_evolutions > old.budget.max_evolutions,
            "evolution hold requires an explicit evolution increase"
        );
        let plans: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1 AND role='plan'",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        let versions: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(version),0) FROM campaign_graph_versions WHERE campaign_id=$1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            plans < i64::from(revised.budget.max_evolutions) + 1
                && versions < i64::from(revised.budget.max_evolutions) + 1,
            "lifetime planning or version budget remains exhausted"
        );
    }
    // Keep raw failures, attempts, task keys, manifests, graphs and counters.
    sqlx::query(
        "UPDATE campaign_execution_specs SET spec=$2,occurrence_budget=$3,hold_reason=NULL WHERE campaign_id=$1",
    )
    .bind(id)
    .bind(serde_json::to_value(&revised)?)
    .bind(occurrence_budget)
    .execute(&mut *tx)
    .await?;
    let updated = campaigns::replace_on_tx(
        &mut tx,
        id,
        c.0.revision,
        serde_json::from_value(serde_json::to_value(&c.0)?)?,
    )
    .await?;
    sqlx::query("INSERT INTO campaign_goal_revisions(campaign_id,request_id,request_digest,campaign_revision,previous_goal,revised_goal,evidence) VALUES($1,$2,$3,$4,$5,$6,$7)")
        .bind(id).bind(&request.request_id).bind(digest).bind(updated.revision)
        .bind(previous).bind(serde_json::to_value(&revised)?).bind(&request.evidence)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(updated.revision)
}
