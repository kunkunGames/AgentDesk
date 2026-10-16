//! Revalidate canonical execution identity under the same admission lock order.
use super::{AUTHORITY, SettledExternalEffect, ValidationContext, validate};
use crate::db::campaigns::graph::{Goal, learning};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};

async fn captured(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    key: &str,
    dispatch: &str,
) -> Result<(String, String, Value)> {
    // Updating the card fences new dispatch foreign-key insertion, while task
    // and dispatch SHARE locks block acceptance/result changes through commit.
    let task = sqlx::query(
        "SELECT t.contract_digest,t.accepted,k.id FROM campaign_graph_tasks t
        JOIN kanban_cards k ON k.id=t.card_id WHERE t.campaign_id=$1 AND t.task_key=$2
        FOR UPDATE OF k FOR SHARE OF t",
    )
    .bind(campaign)
    .bind(key)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        !task.get::<bool, _>("accepted"),
        "accepted external work remains pinned"
    );
    let row = sqlx::query(
        "SELECT d.id,d.status,d.result::text AS result,d.completed_at IS NOT NULL AS ended
        FROM task_dispatches d JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
        WHERE t.campaign_id=$1 AND t.task_key=$2
        ORDER BY d.created_at DESC,d.id DESC LIMIT 1 FOR SHARE OF d",
    )
    .bind(campaign)
    .bind(key)
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        row.get::<String, _>("id") == dispatch
            && row.get::<Option<String>, _>("status").as_deref() == Some("completed")
            && row.get::<bool, _>("ended"),
        "settled effects require the latest completed dispatch and actual terminal timestamp"
    );
    require_owners_settled(tx, campaign, Some(key)).await?;
    let raw: Option<String> = row.get("result");
    let raw = raw.ok_or_else(|| anyhow::anyhow!("retained native result required"))?;
    let mut receipt: Value = serde_json::from_str(&raw)?;
    if let Some(encoded) = receipt.as_str() {
        receipt = serde_json::from_str(encoded)?;
    }
    Ok((task.get("contract_digest"), raw, receipt))
}

pub(crate) async fn require_no_campaign_owners(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
) -> Result<()> {
    require_owners_settled(tx, campaign, None).await
}

async fn require_owners_settled(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    key: Option<&str>,
) -> Result<()> {
    // NULL or unfamiliar states are not evidence of quiescence. The common
    // owned-work predicate includes working sessions, slots and cleanup debt.
    let unsettled = sqlx::query_scalar::<_, bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM campaign_graph_tasks t WHERE t.campaign_id=$1 AND ($2::text IS NULL OR t.task_key=$2) AND (
          ({}) OR EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=t.card_id
            AND (d.status IS NULL OR d.status NOT IN ('completed','failed','cancelled')))
          OR (t.observation IS NULL AND EXISTS(SELECT 1 FROM task_dispatches d
            WHERE d.kanban_card_id=t.card_id AND d.dispatch_type IN ('implementation','rework')))
          OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
            WHERE e.kanban_card_id=t.card_id AND (e.status IS NULL OR e.status NOT IN ('done','failed','user_cancelled','skipped')
              OR r.status IS NULL OR r.status NOT IN ('completed','failed','cancelled')))
          OR EXISTS(SELECT 1 FROM auto_queue_slots slot JOIN auto_queue_entries e ON e.run_id=slot.assigned_run_id
            WHERE e.kanban_card_id=t.card_id)
          OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup JOIN task_dispatches d ON d.id=ANY(cleanup.dispatch_ids)
            WHERE d.kanban_card_id=t.card_id)
          OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup
            CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.card_rollback_tasks)='array'
              THEN cleanup.card_rollback_tasks ELSE '[]'::jsonb END) rollback
            WHERE rollback->>'card_id'=t.card_id OR EXISTS(SELECT 1 FROM task_dispatches d
              WHERE d.kanban_card_id=t.card_id AND d.id=rollback->>'dispatch_id'))
          OR EXISTS(SELECT 1 FROM sessions s WHERE (s.status IS NULL OR s.status NOT IN ('idle','disconnected'))
            AND (EXISTS(SELECT 1 FROM task_dispatches d WHERE d.id=s.active_dispatch_id AND d.kanban_card_id=t.card_id)
              OR string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||t.card_id]
              OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
                WHERE e.kanban_card_id=t.card_id AND s.thread_channel_id IN (r.unified_thread_id,r.unified_thread_channel_id))))))",
        learning::budget::UNSETTLED_TASK_SQL
    )).bind(campaign).bind(key).fetch_one(&mut **tx).await?;
    ensure!(
        !unsettled,
        "settled effect still has unknown state, active ownership or cleanup debt"
    );
    Ok(())
}

pub(crate) async fn authenticated_request_proof(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    key: &str,
    goal: &Goal,
    original: &Value,
    audit: &SettledExternalEffect,
    fresh_admission: bool,
) -> Result<()> {
    let (contract, raw, receipt) = captured(tx, campaign, key, &audit.dispatch_id).await?;
    validate(
        audit,
        ValidationContext {
            campaign,
            task: key,
            contract: &contract,
            goal,
            dispatch: &audit.dispatch_id,
            original,
            raw: &raw,
            captured: &receipt,
            fresh_admission,
        },
    )
}

/// A persisted server-authenticated repair can release only its exact retained
/// latest terminal failed obligation. All other attempts remain pinned.
pub(crate) async fn released_proof(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
    key: &str,
    dispatch: &str,
) -> Result<Option<SettledExternalEffect>> {
    let proofs = sqlx::query("SELECT original.evidence AS original,repair.evidence->'settled_effect' AS audit
        FROM campaign_graph_failures original
        JOIN campaign_graph_reconciliations resolved ON resolved.failure_id=original.id AND resolved.campaign_id=original.campaign_id
        JOIN campaign_graph_failures repair ON repair.campaign_id=original.campaign_id
          AND repair.evidence->>'reconciled_failure_id'=original.id
        WHERE original.campaign_id=$1 AND original.task_key=$2 AND original.evidence->>'dispatch_id'=$3
          AND repair.category='plan' AND repair.evidence ? 'operator_replan'
          AND repair.evidence->>'settled_effect_authority'=$4
          AND repair.evidence->'settled_effect'->>'dispatch_id'=$3")
        .bind(campaign).bind(key).bind(dispatch).bind(AUTHORITY).fetch_all(&mut **tx).await?;
    if proofs.is_empty() {
        return Ok(None);
    }
    let goal: Value =
        sqlx::query_scalar("SELECT spec FROM campaign_execution_specs WHERE campaign_id=$1")
            .bind(campaign)
            .fetch_one(&mut **tx)
            .await?;
    let goal: Goal = serde_json::from_value(goal)?;
    for proof in proofs {
        if let Ok(audit) = serde_json::from_value::<SettledExternalEffect>(proof.get("audit"))
            && authenticated_request_proof(
                tx,
                campaign,
                key,
                &goal,
                &proof.get::<Value, _>("original"),
                &audit,
                false,
            )
            .await
            .is_ok()
        {
            return Ok(Some(audit));
        }
    }
    Ok(None)
}
