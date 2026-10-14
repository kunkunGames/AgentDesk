//! Retain effects except exact audited non-attempts or settled failed attempts.
use super::super::{recovery::replan, result};
use anyhow::Result;
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

pub(super) struct PinSelection {
    pub pinned: Vec<PgRow>,
    pub settled: Vec<replan::SettledExternalEffect>,
}

pub(super) async fn pinned_tasks(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> Result<PinSelection> {
    // Keep the existing artifact/verification and live-owner selection. An
    // accepted contract cannot be released by an audit of an earlier attempt.
    let rows = sqlx::query("SELECT t.task_key,t.contract_digest,t.card_id,t.accepted
        FROM campaign_graph_tasks t JOIN campaign_graph_versions v
          ON v.campaign_id=t.campaign_id AND v.version=t.version
        CROSS JOIN LATERAL jsonb_array_elements(v.graph->'nodes') n
        WHERE t.campaign_id=$1 AND t.role='work' AND n->>'task_key'=t.task_key AND (
          EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=t.card_id AND (
            d.status IN ('pending','dispatched','running') OR COALESCE(n->>'effect','external')<>'artifact_only'))
          OR (t.accepted AND (COALESCE(n->>'effect','external')<>'artifact_only' OR n->>'kind'='verify'
            OR EXISTS(SELECT 1 FROM jsonb_array_elements(v.graph->'nodes') verifier
              JOIN campaign_graph_tasks checked ON checked.task_key=verifier->>'task_key'
              WHERE verifier->>'kind'='verify' AND verifier->'dependencies' ? t.node_id AND checked.accepted))))")
        .bind(id).fetch_all(&mut **tx).await?;
    let mut pinned = Vec::new();
    let mut settled = Vec::new();
    for row in rows {
        let mut retain = row.get::<bool, _>("accepted");
        let mut task_settlements = Vec::new();
        if !retain {
            let dispatches = sqlx::query("SELECT id,status,result::text AS result FROM task_dispatches WHERE kanban_card_id=$1 FOR SHARE")
                .bind(row.get::<String, _>("card_id")).fetch_all(&mut **tx).await?;
            for dispatch in dispatches {
                let raw: Option<String> = dispatch.get("result");
                if dispatch.get::<Option<String>, _>("status").as_deref() != Some("completed") {
                    retain = true;
                    break;
                }
                if let Some(audit) = replan::settled_effect::released_proof(
                    tx,
                    id,
                    &row.get::<String, _>("task_key"),
                    &dispatch.get::<String, _>("id"),
                )
                .await?
                {
                    task_settlements.push(audit);
                } else if !attested(
                    tx,
                    id,
                    &row.get::<String, _>("task_key"),
                    &dispatch.get::<String, _>("id"),
                    raw.as_deref().unwrap_or("null"),
                )
                .await?
                {
                    retain = true;
                    break;
                }
            }
        }
        if retain {
            pinned.push(row);
        } else {
            settled.extend(task_settlements);
        }
    }
    Ok(PinSelection { pinned, settled })
}

async fn attested(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    key: &str,
    dispatch: &str,
    raw: &str,
) -> Result<bool> {
    let proofs = sqlx::query("SELECT original.evidence AS original,repair.evidence->'unattempted_effect' AS audit
        FROM campaign_graph_failures original
        JOIN campaign_graph_reconciliations resolved ON resolved.failure_id=original.id AND resolved.campaign_id=original.campaign_id
        JOIN campaign_graph_failures repair ON repair.campaign_id=original.campaign_id
          AND repair.evidence->>'reconciled_failure_id'=original.id
        WHERE original.campaign_id=$1 AND original.task_key=$2 AND original.evidence->>'dispatch_id'=$3
          AND repair.category='plan' AND repair.evidence ? 'operator_replan'
          AND repair.evidence->'unattempted_effect'->>'dispatch_id'=$3")
        .bind(id).bind(key).bind(dispatch).fetch_all(&mut **tx).await?;
    let Ok(mut captured) = serde_json::from_str::<Value>(raw) else {
        return Ok(false);
    };
    if let Some(encoded) = captured.as_str() {
        let Ok(value) = serde_json::from_str(encoded) else {
            return Ok(false);
        };
        captured = value;
    }
    if result::validate_dispatch(&captured).is_err() {
        return Ok(false);
    }
    for proof in proofs {
        let audit: Value = proof.get("audit");
        if let Ok(audit) = serde_json::from_value::<replan::UnattemptedEffect>(audit)
            && replan::validate_unattempted_effect(
                &audit,
                dispatch,
                &proof.get::<Value, _>("original"),
                raw,
                &captured,
            )
            .is_ok()
        {
            return Ok(true);
        }
    }
    Ok(false)
}
