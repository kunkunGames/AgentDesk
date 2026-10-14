//! Retire only exhausted, settled occurrences when a later slot is due.
//! Never reset historical counters, certify success, or stop an execution owner.
use super::super::{Goal, workflow::lifecycle};
use crate::db::campaigns::{self, Campaign, CampaignInput, CampaignStatus};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, types::Json};

const EVOLUTION_EXHAUSTED: &str = "Plan evolution budget exhausted";

pub(super) async fn settle_previous(
    tx: &mut Transaction<'_, Postgres>,
    parent: &str,
    next_slot: DateTime<Utc>,
) -> Result<bool> {
    // Admission already owns the parent campaign/spec locks. Lock children in
    // stable order; publication and operator recovery also own the child lock.
    let rows = sqlx::query(
        "SELECT c.document,o.scheduled_at FROM campaign_graph_occurrences o
         JOIN campaigns c ON c.id=o.child_id
         WHERE o.campaign_id=$1 AND c.document->>'status' IN ('active','paused','planned')
         ORDER BY c.id FOR UPDATE OF c",
    )
    .bind(parent)
    .fetch_all(&mut **tx)
    .await?;
    let mut retiring = Vec::new();
    for row in rows {
        let c: Json<Campaign> = row.get("document");
        if c.status != CampaignStatus::Active
            || row.get::<DateTime<Utc>, _>("scheduled_at") >= next_slot
        {
            return Ok(false);
        }
        let state = sqlx::query(
            "SELECT spec,hold_reason FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
        )
        .bind(&c.id)
        .fetch_one(&mut **tx)
        .await?;
        if state.get::<Option<String>, _>("hold_reason").as_deref() != Some(EVOLUTION_EXHAUSTED) {
            return Ok(false);
        }
        let goal: Goal = serde_json::from_value(state.get::<Value, _>("spec"))?;
        let (plans, versions): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*) FILTER(WHERE role='plan'),
             (SELECT COALESCE(MAX(version),0) FROM campaign_graph_versions WHERE campaign_id=$1)
             FROM campaign_graph_tasks WHERE campaign_id=$1",
        )
        .bind(&c.id)
        .fetch_one(&mut **tx)
        .await?;
        // The initial plan/version is not a replan. Both counters remain intact.
        if plans <= i64::from(goal.budget.max_evolutions)
            && versions <= i64::from(goal.budget.max_evolutions)
        {
            return Ok(false);
        }
        if !lock_owners(tx, &c.id).await? || !lifecycle::settled(tx, &c.id).await? {
            return Ok(false);
        }
        let unsettled: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t WHERE t.campaign_id=$1
             AND t.observation IS NULL)
             OR EXISTS(SELECT 1 FROM campaign_graph_failures f WHERE f.campaign_id=$1
             AND f.category IN ('unknown','infrastructure') AND NOT EXISTS(
                 SELECT 1 FROM campaign_graph_reconciliations r WHERE r.failure_id=f.id))",
        )
        .bind(&c.id)
        .fetch_one(&mut **tx)
        .await?;
        if unsettled || has_unverified_effect(tx, &c.id).await? {
            return Ok(false);
        }
        retiring.push(c.0);
    }
    for c in retiring {
        let mut input: CampaignInput = serde_json::from_value(serde_json::to_value(&c)?)?;
        // Cancelled is a truthful terminal outcome, never an accepted Finish.
        // Retain the hold reason, graph, failures, receipts and spent budget.
        input.status = CampaignStatus::Cancelled;
        campaigns::replace_retaining_history_on_tx(tx, &c.id, c.revision, input).await?;
    }
    Ok(true)
}

async fn lock_owners(tx: &mut Transaction<'_, Postgres>, id: &str) -> Result<bool> {
    let keys: Vec<String> = sqlx::query_scalar(
        "SELECT task_key FROM campaign_graph_tasks WHERE campaign_id=$1 ORDER BY task_key",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    for key in keys {
        let locked: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtext('campaign-task'),hashtext($1))",
        )
        .bind(&key)
        .fetch_one(&mut **tx)
        .await?;
        if !locked {
            return Ok(false);
        }
        let card: Option<String> = sqlx::query_scalar(
            "SELECT t.card_id FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
             WHERE t.campaign_id=$1 AND t.task_key=$2 FOR UPDATE OF t,k SKIP LOCKED",
        )
        .bind(id)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
        // A skipped lock is an owner, never proof that the task disappeared.
        if card.is_none() {
            return Ok(false);
        }
    }
    let runs: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT e.run_id FROM auto_queue_entries e JOIN campaign_graph_tasks t
         ON t.card_id=e.kanban_card_id WHERE t.campaign_id=$1 ORDER BY e.run_id",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    for run in runs {
        let locked: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext('aq_run:' || $1))")
                .bind(&run)
                .fetch_one(&mut **tx)
                .await?;
        if !locked {
            return Ok(false);
        }
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM auto_queue_entries WHERE run_id=$1")
                .bind(&run)
                .fetch_one(&mut **tx)
                .await?;
        let entries: Vec<String> = sqlx::query_scalar(
            "SELECT e.id FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
             WHERE e.run_id=$1 FOR UPDATE OF e,r SKIP LOCKED",
        )
        .bind(run)
        .fetch_all(&mut **tx)
        .await?;
        if entries.len() as i64 != count {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn has_unverified_effect(tx: &mut Transaction<'_, Postgres>, id: &str) -> Result<bool> {
    // Every published work contract must be known. External producers also
    // require accepted direct independent verification before a fresh batch.
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t
         WHERE t.campaign_id=$1 AND t.role='work' AND NOT EXISTS(
             SELECT 1 FROM campaign_graph_versions v,
                 jsonb_array_elements(v.graph->'nodes') n
             WHERE v.campaign_id=t.campaign_id AND v.version=t.version
                 AND n->>'task_key'=t.task_key AND (
                     n->>'effect'='artifact_only' OR (
                         n->>'effect'='external' AND t.accepted AND (n->>'kind'='verify' OR EXISTS(
                             SELECT 1 FROM jsonb_array_elements(v.graph->'nodes') verify
                             JOIN campaign_graph_tasks checked ON checked.task_key=verify->>'task_key'
                             WHERE verify->>'kind'='verify' AND verify->>'agent'<>n->>'agent'
                                 AND verify->'dependencies' ? (n->>'id')
                                 AND checked.campaign_id=t.campaign_id AND checked.accepted
                         ))
                     )
                 )
         ))",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?)
}
