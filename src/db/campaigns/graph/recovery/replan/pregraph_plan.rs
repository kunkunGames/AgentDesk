//! Prove the complete, linear operator repair history before any graph exists.
use super::{FailureScope, contract_digest, pregraph_capacity};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

fn one<'a>(rows: &'a [Value], field: &str, key: &str) -> Result<&'a Value> {
    let found: Vec<_> = rows.iter().filter(|row| row[field] == key).collect();
    ensure!(
        found.len() == 1,
        "one canonical pregraph operator audit required"
    );
    Ok(found[0])
}

fn failure<'a>(rows: &'a [PgRow], task: &str, category: &str) -> Result<&'a PgRow> {
    let found: Vec<_> = rows
        .iter()
        .filter(|row| {
            row.get::<String, _>("task_key") == task && row.get::<String, _>("category") == category
        })
        .collect();
    ensure!(
        found.len() == 1,
        "one exact failure per pregraph planning attempt required"
    );
    Ok(found[0])
}

fn audit_link(
    id: &str,
    goal: &Value,
    original: &PgRow,
    repair: &PgRow,
    reconciliations: &[Value],
    revisions: &[Value],
    successor: &PgRow,
) -> Result<()> {
    let original_id: String = original.get("id");
    let captured: Value = original.get("evidence");
    let evidence: Value = repair.get("evidence");
    let request_id = evidence["operator_replan"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("canonical pregraph repair request required"))?;
    let reconciliation = one(reconciliations, "failure_id", &original_id)?;
    let revision = one(revisions, "request_id", request_id)?;
    let expected_revision = reconciliation["expected_revision"]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("canonical pregraph reconciliation revision required"))?;
    let operator_evidence = evidence["operator_evidence"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("canonical pregraph operator evidence required"))?;
    let request = super::super::Replan {
        expected_revision,
        request_id: request_id.into(),
        failure_id: original_id.clone(),
        evidence: operator_evidence.into(),
        budget: None,
        supersede_candidate: None,
        unattempted_effect: None,
        settled_effect: None,
        rejected_validation: None,
        cancelled_validation: None,
    };
    let repair_id = contract_digest(&(id, "operator-replan", request_id))?;
    ensure!(
        repair.get::<String, _>("id") == repair_id
            && repair.get::<String, _>("fingerprint") == repair_id
            && successor.get::<String, _>("task_key") == format!("{id}:plan:{repair_id}")
            && evidence["reconciled_failure_id"] == original_id
            && evidence["original_failure"] == captured
            && evidence["external_effect_requires_reconciliation"] == false
            && [
                "unattempted_effect",
                "settled_effect",
                "settled_effect_authority",
                "rejected_validation",
                "cancelled_validation",
                "superseded_candidate"
            ]
            .iter()
            .all(|key| evidence.get(*key) == Some(&Value::Null))
            && reconciliation["campaign_id"] == id
            && revision["campaign_id"] == id
            && reconciliation["evidence"] == operator_evidence
            && revision["evidence"] == operator_evidence
            && revision["request_digest"] == contract_digest(&("operator-replan", &request))?
            && revision["campaign_revision"].as_i64() == expected_revision.checked_add(1)
            && revision["previous_goal"] == *goal
            && revision["revised_goal"] == *goal,
        "pregraph recovery requires an unchanged-goal canonical operator reconciliation chain"
    );
    let start: DateTime<Utc> = original.get("created_at");
    let end: DateTime<Utc> = successor.get("created_at");
    for recorded in [
        repair.get::<DateTime<Utc>, _>("created_at"),
        serde_json::from_value(reconciliation["created_at"].clone())?,
        serde_json::from_value(revision["created_at"].clone())?,
    ] {
        ensure!(
            start <= recorded && recorded <= end,
            "pregraph operator audit chronology changed"
        );
    }
    Ok(())
}

pub(super) async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    scope: FailureScope<'_>,
) -> Result<()> {
    ensure!(
        scope.active.is_none()
            && scope.goal.schedule.is_none()
            && scope.failure.get::<String, _>("category") == "unknown"
            && scope.unattempted_effect.is_none()
            && scope.settled_effect.is_none()
            && scope.rejected_validation.is_none()
            && scope.cancelled_validation.is_none(),
        "only pregraph capacity failures without effect or validation claims may recover"
    );
    let empty: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM campaign_graph_versions WHERE campaign_id=$1)
         AND (SELECT COUNT(*) FROM campaign_graph_tasks WHERE campaign_id=$1) BETWEEN 1 AND $2",
    )
    .bind(id)
    .bind(i64::from(crate::db::campaigns::graph::MAX_AUDITED_EVOLUTIONS) + 1)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        empty,
        "pregraph recovery requires no graph and bounded planning history"
    );
    // Follow the publication lock order and fence a new dispatch for every
    // historical card, not just the latest failure being reconciled.
    let tasks = sqlx::query(
        "SELECT t.task_key,t.role,t.version,t.accepted,t.artifact_manifest,t.created_at
         FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
         WHERE t.campaign_id=$1 ORDER BY t.created_at,t.task_key FOR UPDATE OF k FOR SHARE OF t",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    let failures = sqlx::query(
        "SELECT f.id,f.task_key,f.category,f.fingerprint,f.evidence,f.created_at,
            t.role,t.version,t.accepted,t.contract_digest
         FROM campaign_graph_failures f JOIN campaign_graph_tasks t ON t.task_key=f.task_key
         WHERE f.campaign_id=$1 AND t.campaign_id=$1 FOR SHARE OF f,t",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    let reconciliations: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM campaign_graph_reconciliations r WHERE campaign_id=$1 FOR SHARE",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    let revisions: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM campaign_goal_revisions r WHERE campaign_id=$1 FOR SHARE",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        !tasks.is_empty()
            && failures.len() == tasks.len() * 2 - 1
            && reconciliations.len() + 1 == tasks.len()
            && revisions.len() + 1 == tasks.len()
            && tasks.last().is_some_and(|task| {
                task.get::<String, _>("task_key") == scope.failure.get::<String, _>("task_key")
            }),
        "latest pregraph planner must own the complete unbranched repair history"
    );
    let goal = serde_json::to_value(scope.goal)?;
    for (index, task) in tasks.iter().enumerate() {
        let key: String = task.get("task_key");
        ensure!(
            task.get::<String, _>("role") == "plan"
                && !task.get::<bool, _>("accepted")
                && task.get::<Option<Value>, _>("artifact_manifest").is_none()
                && task.get::<Option<i64>, _>("version") == (index > 0).then_some(0)
                && (index != 0 || key == format!("{id}:plan:initial")),
            "pregraph frontier must contain only its initial and audited repair planners"
        );
        let original = failure(&failures, &key, "unknown")?;
        let captured: Value = original.get("evidence");
        pregraph_capacity::validate(
            tx,
            id,
            FailureScope {
                failure: original,
                evidence: &captured,
                ..scope
            },
        )
        .await?;
        if let Some(next) = tasks.get(index + 1) {
            audit_link(
                id,
                &goal,
                original,
                failure(&failures, &key, "plan")?,
                &reconciliations,
                &revisions,
                next,
            )?;
        } else {
            ensure!(
                captured == *scope.evidence
                    && !reconciliations
                        .iter()
                        .any(|r| r["failure_id"] == original.get::<String, _>("id")),
                "only the latest unresolved pregraph capacity failure may recover"
            );
        }
    }
    super::super::settled_effect::require_no_campaign_owners(tx, id).await?;
    ensure!(
        super::super::super::super::workflow::lifecycle::settled(tx, id).await?,
        "initial planner execution, delivery and durable cleanup must settle first"
    );
    Ok(())
}
