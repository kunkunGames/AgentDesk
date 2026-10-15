//! Separate ordinary lifetime enforcement from each frozen optional review.
//! Raw telemetry, historical assignments and reconciliation holds never change.
use crate::db::campaigns::graph::{Goal, contract_digest};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};

pub const DEFAULT_REVIEW_TOOL_LIMIT: u32 = 100;

// A role label alone cannot waive ordinary enforcement. Bind every excluded
// task to its durable claim and the immutable superior control assignment.
pub const REVIEW_TASK_SQL: &str = "t.role='template_review' AND t.version IS NULL AND EXISTS(
    SELECT 1 FROM campaign_dag_reviews review
    JOIN campaign_dag_control_assignments authority ON authority.task_key=review.task_key
    WHERE review.task_key=t.task_key AND review.owner_campaign_id=t.campaign_id
      AND authority.purpose='template_review' AND authority.family_id=review.family_id
      AND authority.agent_id=review.superior_agent_id)";

pub const UNSETTLED_TASK_SQL: &str = "EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=t.card_id AND (
    d.status IN ('pending','dispatched','running') OR EXISTS(SELECT 1 FROM sessions s WHERE s.active_dispatch_id=d.id
        AND s.status IN ('working','turn_active','awaiting_bg','awaiting_user'))))
    OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
        WHERE e.kanban_card_id=t.card_id AND (e.status IN ('pending','dispatched')
          OR (r.status IN ('generated','active','paused','pending','restoring') AND NOT EXISTS(
            SELECT 1 FROM auto_queue_entries other WHERE other.run_id=r.id AND other.kanban_card_id IS DISTINCT FROM t.card_id))))
    OR EXISTS(SELECT 1 FROM auto_queue_slots slot JOIN auto_queue_entries e ON e.run_id=slot.assigned_run_id
        WHERE e.kanban_card_id=t.card_id AND (e.status IN ('pending','dispatched') OR NOT EXISTS(
          SELECT 1 FROM auto_queue_entries other WHERE other.run_id=e.run_id AND other.kanban_card_id IS DISTINCT FROM t.card_id)))
    OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup JOIN auto_queue_entries e ON e.run_id=ANY(cleanup.run_ids)
        WHERE e.kanban_card_id=t.card_id AND (NOT EXISTS(SELECT 1 FROM auto_queue_entries other
          WHERE other.run_id=e.run_id AND other.kanban_card_id IS DISTINCT FROM t.card_id)
          OR EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=t.card_id AND d.id=ANY(cleanup.dispatch_ids))))
    OR EXISTS(SELECT 1 FROM sessions s WHERE s.status IN ('working','turn_active','awaiting_bg','awaiting_user')
        AND (string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||t.card_id]
            OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
                WHERE e.kanban_card_id=t.card_id AND s.thread_channel_id IN (r.unified_thread_id,r.unified_thread_channel_id)
                  AND (e.status IN ('pending','dispatched') OR NOT EXISTS(SELECT 1 FROM auto_queue_entries other
                    WHERE other.run_id=r.id AND other.kanban_card_id IS DISTINCT FROM t.card_id)))))";

const INVALID_POLICY: &str =
    "Pending review has no valid frozen finite policy; original snapshot and assignment retained";
const TOOL_LIMIT: &str =
    "Frozen per-review observed tool budget exhausted; cancellation and ownership cleanup required";
const LATE_COMPLETION: &str =
    "Review completed after its frozen deadline; original terminal receipt retained";
const MISSING_END: &str =
    "Review deadline elapsed without a canonical completion timestamp; original receipt retained";
const DEADLINE: &str =
    "Frozen per-review deadline elapsed; cancellation and ownership cleanup required";

/// Only exact controller-produced policy facts have this classification.
/// Provider unknown/effect/infrastructure outcomes remain reconciliation holds.
pub async fn known_policy_failure(pool: &PgPool, key: &str) -> Result<Option<String>> {
    let reasons = [
        INVALID_POLICY,
        TOOL_LIMIT,
        LATE_COMPLETION,
        MISSING_END,
        DEADLINE,
    ];
    Ok(sqlx::query_scalar(&format!(
        "SELECT review.reason FROM campaign_dag_reviews review
        JOIN campaign_graph_tasks t ON t.task_key=review.task_key
        WHERE review.task_key=$1 AND review.status='failed' AND review.reason=ANY($2)
          AND ({REVIEW_TASK_SQL})"
    ))
    .bind(key)
    .bind(&reasons[..])
    .fetch_optional(pool)
    .await?
    .flatten())
}

#[derive(Clone, Debug, Serialize)]
pub struct OrdinaryUsage {
    pub scope: &'static str,
    pub published_tasks: i64,
    pub elapsed_dispatch_seconds: f64,
    pub observed_tool_calls: i64,
}

pub async fn ordinary_usage(pool: &PgPool, id: &str, goal: &Goal) -> Result<OrdinaryUsage> {
    let mut connection = pool.acquire().await?;
    usage_on_connection(&mut connection, id, goal).await
}

pub async fn usage_on_connection(
    connection: &mut sqlx::PgConnection,
    id: &str,
    goal: &Goal,
) -> Result<OrdinaryUsage> {
    let selection = if goal.schedule.is_some() {
        format!(" AND NOT ({REVIEW_TASK_SQL})")
    } else {
        String::new()
    };
    let published_tasks = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM campaign_graph_tasks t WHERE t.campaign_id=$1{selection}"
    ))
    .bind(id)
    .fetch_one(&mut *connection)
    .await?;
    let elapsed_dispatch_seconds = sqlx::query_scalar(&format!(
        "SELECT COALESCE(SUM(GREATEST(0,EXTRACT(EPOCH FROM
        (COALESCE(d.completed_at,CASE WHEN d.status IN ('completed','failed','cancelled','rejected')
          THEN d.updated_at ELSE NOW() END)-d.created_at)))),0)::float8
        FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
        WHERE t.campaign_id=$1 AND d.dispatch_type IN ('implementation','rework'){selection}"
    ))
    .bind(id)
    .fetch_one(&mut *connection)
    .await?;
    let observed_tool_calls = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM campaign_graph_progress p
        JOIN task_dispatches d ON d.id=p.dispatch_id
        JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
        WHERE t.campaign_id=$1{selection}"
    ))
    .bind(id)
    .fetch_one(&mut *connection)
    .await?;
    Ok(OrdinaryUsage {
        scope: if goal.schedule.is_some() {
            "recurring_parent_ordinary_lifetime"
        } else {
            "campaign_lifetime"
        },
        published_tasks,
        elapsed_dispatch_seconds,
        observed_tool_calls,
    })
}

pub async fn publication_count(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    goal: &Goal,
) -> Result<i64> {
    let selection = if goal.schedule.is_some() {
        format!(" AND NOT ({REVIEW_TASK_SQL})")
    } else {
        String::new()
    };
    Ok(sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM campaign_graph_tasks t WHERE t.campaign_id=$1{selection}"
    ))
    .bind(id)
    .fetch_one(&mut **tx)
    .await?)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReviewPolicy {
    pub schema_version: u32,
    pub max_dispatches: u32,
    pub max_elapsed_secs: u32,
    pub max_observed_tool_calls: u32,
    pub deadline: DateTime<Utc>,
    pub goal_digest: String,
    pub captured_budget: Value,
}

impl ReviewPolicy {
    pub fn freeze(goal: &Goal, claimed_at: DateTime<Utc>) -> Result<Self> {
        goal.validate()?;
        Ok(Self {
            schema_version: 1,
            max_dispatches: 1,
            max_elapsed_secs: goal.budget.node_timeout_secs,
            max_observed_tool_calls: goal
                .budget
                .max_observed_tool_calls
                .unwrap_or(DEFAULT_REVIEW_TOOL_LIMIT),
            deadline: claimed_at
                + chrono::Duration::seconds(i64::from(goal.budget.node_timeout_secs)),
            goal_digest: contract_digest(goal)?,
            captured_budget: serde_json::to_value(&goal.budget)?,
        })
    }

    pub fn from_snapshot(snapshot: &Value, claimed_at: DateTime<Utc>) -> Result<Self> {
        // Missing legacy policy is intentionally not derived from a revised
        // live goal. Its original snapshot and assignment remain immutable.
        ensure!(
            snapshot.get("execution_policy").is_some(),
            "Pending review lacks a frozen finite execution policy"
        );
        let policy: Self = serde_json::from_value(snapshot["execution_policy"].clone())?;
        let captured: Goal = serde_json::from_value(snapshot["goal"].clone())?;
        ensure!(
            policy == Self::freeze(&captured, claimed_at)?,
            "Frozen review execution policy differs from its captured goal and claim time"
        );
        Ok(policy)
    }
}

pub async fn admit_publication(tx: &mut Transaction<'_, Postgres>, key: &str) -> Result<()> {
    let family: String =
        sqlx::query_scalar("SELECT family_id FROM campaign_dag_reviews WHERE task_key=$1")
            .bind(key)
            .fetch_one(&mut **tx)
            .await?;
    // Claim/decision/publication use family -> review order. The campaign/task
    // locks are already held by the ordinary publication owner.
    sqlx::query("SELECT id FROM campaign_dag_families WHERE id=$1 FOR SHARE")
        .bind(family)
        .fetch_one(&mut **tx)
        .await?;
    let row = sqlx::query("SELECT snapshot,created_at,status,NOW() AS now FROM campaign_dag_reviews WHERE task_key=$1 FOR SHARE")
        .bind(key).fetch_one(&mut **tx).await?;
    let policy = ReviewPolicy::from_snapshot(&row.get("snapshot"), row.get("created_at"))?;
    ensure!(
        row.get::<String, _>("status") == "pending"
            && row.get::<DateTime<Utc>, _>("now") < policy.deadline,
        "Review claim is terminal or its frozen deadline has elapsed"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_tasks WHERE task_key=$1")
            .bind(key)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        count < i64::from(policy.max_dispatches),
        "Frozen per-review task budget exhausted"
    );
    Ok(())
}

pub async fn admit_decision(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    snapshot: &Value,
    claimed_at: DateTime<Utc>,
) -> Result<()> {
    let policy = ReviewPolicy::from_snapshot(snapshot, claimed_at)?;
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT NOW()")
        .fetch_one(&mut **tx)
        .await?;
    let completed_at: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT d.completed_at FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.task_key=$1 AND d.dispatch_type IN ('implementation','rework') AND d.status='completed' ORDER BY d.created_at DESC,d.id DESC LIMIT 1")
        .bind(key).fetch_optional(&mut **tx).await?.flatten();
    ensure!(
        completed_at.map_or(now < policy.deadline, |end| end <= policy.deadline),
        "Review has no canonical completion within its frozen deadline"
    );
    let tools: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_progress p JOIN task_dispatches d ON d.id=p.dispatch_id JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.task_key=$1")
        .bind(key).fetch_one(&mut **tx).await?;
    ensure!(
        tools <= i64::from(policy.max_observed_tool_calls),
        "Frozen per-review observed tool budget exceeded"
    );
    Ok(())
}

pub async fn expire_pending(pool: &PgPool, id: &str) -> Result<Vec<String>> {
    let keys: Vec<String> = sqlx::query_scalar("SELECT task_key FROM campaign_dag_reviews WHERE owner_campaign_id=$1 AND status='pending' ORDER BY created_at LIMIT 64")
        .bind(id).fetch_all(pool).await?;
    let mut expired = Vec::new();
    for key in keys {
        let mut tx = pool.begin().await?;
        let family: String =
            sqlx::query_scalar("SELECT family_id FROM campaign_dag_reviews WHERE task_key=$1")
                .bind(&key)
                .fetch_one(&mut *tx)
                .await?;
        sqlx::query("SELECT id FROM campaign_dag_families WHERE id=$1 FOR UPDATE")
            .bind(family)
            .fetch_one(&mut *tx)
            .await?;
        let row = sqlx::query("SELECT snapshot,created_at,status,NOW() AS now FROM campaign_dag_reviews WHERE task_key=$1 FOR UPDATE")
            .bind(&key).fetch_one(&mut *tx).await?;
        if row.get::<String, _>("status") != "pending" {
            continue;
        }
        let policy = ReviewPolicy::from_snapshot(&row.get("snapshot"), row.get("created_at"));
        let dispatch = sqlx::query("SELECT d.status,d.completed_at FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.task_key=$1 AND d.dispatch_type IN ('implementation','rework') ORDER BY d.created_at DESC,d.id DESC LIMIT 1")
            .bind(&key).fetch_optional(&mut *tx).await?;
        let tools: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM campaign_graph_progress p JOIN task_dispatches d ON d.id=p.dispatch_id JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id WHERE t.task_key=$1")
            .bind(&key).fetch_one(&mut *tx).await?;
        let now: DateTime<Utc> = row.get("now");
        let reason = match policy {
            Err(_) => Some(INVALID_POLICY),
            Ok(policy) => {
                let completed = dispatch
                    .as_ref()
                    .is_some_and(|d| d.get::<String, _>("status") == "completed");
                let completed_at = dispatch
                    .as_ref()
                    .and_then(|d| d.get::<Option<DateTime<Utc>>, _>("completed_at"));
                if tools > i64::from(policy.max_observed_tool_calls)
                    || (!completed && tools >= i64::from(policy.max_observed_tool_calls))
                {
                    Some(TOOL_LIMIT)
                } else if completed {
                    if completed_at.is_some_and(|end| end > policy.deadline) {
                        Some(LATE_COMPLETION)
                    } else if completed_at.is_none() && now >= policy.deadline {
                        Some(MISSING_END)
                    } else {
                        None
                    }
                } else if now >= policy.deadline {
                    Some(DEADLINE)
                } else {
                    None
                }
            }
        };
        if let Some(reason) = reason {
            sqlx::query("UPDATE campaign_dag_reviews SET status='failed',reason=$2,decided_at=NOW() WHERE task_key=$1 AND status='pending'")
                .bind(&key).bind(reason).execute(&mut *tx).await?;
            expired.push(key);
        }
        tx.commit().await?;
    }
    Ok(expired)
}

/// Failed read-only claims still own real processes/queues until cancellation
/// settles. Return their keys even after the claim's status was made terminal.
pub async fn failed_keys(pool: &PgPool, id: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(&format!("SELECT r.task_key FROM campaign_dag_reviews r JOIN campaign_graph_tasks t ON t.task_key=r.task_key WHERE r.owner_campaign_id=$1 AND r.status='failed' AND ({REVIEW_TASK_SQL}) AND ({UNSETTLED_TASK_SQL}) ORDER BY r.created_at LIMIT 64"))
        .bind(id).fetch_all(pool).await?)
}

pub async fn failed_owners_unsettled(pool: &PgPool, id: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM campaign_dag_reviews r JOIN campaign_graph_tasks t ON t.task_key=r.task_key WHERE r.owner_campaign_id=$1 AND r.status='failed' AND ({REVIEW_TASK_SQL}) AND ({UNSETTLED_TASK_SQL}))"))
        .bind(id).fetch_one(pool).await?)
}

pub async fn review_card_runs(pool: &PgPool, key: &str) -> Result<(String, Vec<String>)> {
    let card: String = sqlx::query_scalar(&format!(
        "SELECT t.card_id FROM campaign_graph_tasks t WHERE t.task_key=$1 AND ({REVIEW_TASK_SQL})"
    ))
    .bind(key)
    .fetch_one(pool)
    .await?;
    // Candidate identities only. Cancellation rechecks exclusive ownership
    // under run and entry locks; this read never authorizes a mutation.
    let runs = sqlx::query_scalar("SELECT DISTINCT r.id
        FROM campaign_graph_tasks t JOIN auto_queue_entries e ON e.kanban_card_id=t.card_id
        JOIN auto_queue_runs r ON r.id=e.run_id
        WHERE t.task_key=$1 AND (r.status IN ('generated','active','paused','pending','restoring')
          OR e.status IN ('pending','dispatched')
          OR EXISTS(SELECT 1 FROM auto_queue_slots slot WHERE slot.assigned_run_id=r.id)
          OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup WHERE r.id=ANY(cleanup.run_ids)))")
        .bind(key).fetch_all(pool).await?;
    Ok((card, runs))
}

pub async fn unsettled(pool: &PgPool, key: &str) -> Result<bool> {
    // Historical thread mappings and quiescent reusable TUI sessions are not
    // evidence of an open turn. Actual live turns, slots and cleanup stay fenced.
    Ok(sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t WHERE t.task_key=$1 AND ({UNSETTLED_TASK_SQL}))"))
        .bind(key).fetch_one(pool).await?)
}
