//! Settle only abandoned, never-dispatched planner controls under a durable replan.
use anyhow::Result;
use sqlx::{PgPool, Postgres, Row, Transaction};

pub(crate) async fn settle_superseded_unstarted_planners(
    pool: &PgPool,
    campaign: &str,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    // Match publication/replan and dispatch admission lock order. A first
    // dispatch must observe the settlement after this campaign lock releases.
    sqlx::query("SELECT id FROM campaigns WHERE id=$1 FOR UPDATE")
        .bind(campaign)
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("SELECT campaign_id FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(campaign)
        .fetch_one(&mut *tx)
        .await?;
    let changed = settle_on_tx(&mut tx, campaign).await?;
    tx.commit().await?;
    Ok(changed)
}

pub(super) async fn settle_on_tx(
    tx: &mut Transaction<'_, Postgres>,
    campaign: &str,
) -> Result<u64> {
    // All three records must come from the same audited operator request.
    // A graph version or task age alone never relinquishes control ownership.
    let audit = sqlx::query(
        "SELECT repair.id, revision.request_id, revision.created_at
         FROM campaign_goal_revisions revision
         JOIN campaign_graph_failures repair ON repair.campaign_id=revision.campaign_id
           AND repair.category='plan' AND repair.evidence->>'operator_replan'=revision.request_id
           AND repair.evidence->>'operator_evidence'=revision.evidence
         JOIN campaign_graph_reconciliations reconciled ON reconciled.campaign_id=revision.campaign_id
           AND reconciled.failure_id=repair.evidence->>'reconciled_failure_id'
           AND reconciled.evidence=revision.evidence
           AND reconciled.expected_revision+1=revision.campaign_revision
         WHERE revision.campaign_id=$1
         ORDER BY revision.campaign_revision DESC, revision.request_id DESC LIMIT 1",
    )
    .bind(campaign)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(audit) = audit else { return Ok(0) };
    let repair: String = audit.get("id");
    let request: String = audit.get("request_id");
    let audited_at: chrono::DateTime<chrono::Utc> = audit.get("created_at");
    let candidates = sqlx::query(
        "SELECT t.task_key,t.card_id FROM campaign_graph_tasks t
         JOIN kanban_cards k ON k.id=t.card_id
         WHERE t.campaign_id=$1 AND t.role='plan' AND NOT t.accepted
           AND t.observation IS NULL AND t.artifact_manifest IS NULL
           AND t.created_at<$2 AND t.task_key<>$3
         ORDER BY t.task_key FOR UPDATE OF t,k SKIP LOCKED",
    )
    .bind(campaign)
    .bind(audited_at)
    .bind(format!("{campaign}:plan:{repair}"))
    .fetch_all(&mut **tx)
    .await?;
    let mut changed = 0;
    for candidate in candidates {
        let card: String = candidate.get("card_id");
        // Card row locking also fences new queue references through their FK.
        // Busy queue rows are left to their owner, never cancelled or inferred
        // terminal. Recheck the complete proof after acquiring these locks.
        let locked = sqlx::query(
            "SELECT e.id FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
             WHERE e.kanban_card_id=$1 FOR UPDATE OF e,r SKIP LOCKED",
        )
        .bind(&card)
        .fetch_all(&mut **tx)
        .await?;
        let entries: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM auto_queue_entries WHERE kanban_card_id=$1")
                .bind(&card)
                .fetch_one(&mut **tx)
                .await?;
        if entries == 0 || locked.len() as i64 != entries {
            continue;
        }
        changed += sqlx::query(
            "UPDATE campaign_graph_tasks t SET observation=jsonb_build_object(
               'outcome','superseded_unstarted','operator_replan',$2::text,
               'repair_failure_id',$3::text,'graph_version',t.version,
               'reason','Never-dispatched planner with only completed skipped queues superseded by an audited operator replan')
             FROM kanban_cards k WHERE t.task_key=$1 AND k.id=t.card_id
               AND t.role='plan' AND NOT t.accepted AND t.observation IS NULL
               AND t.artifact_manifest IS NULL AND k.status='ready'
               AND k.latest_dispatch_id IS NULL AND k.started_at IS NULL
               AND k.completed_at IS NULL AND k.active_thread_id IS NULL
               AND (k.channel_thread_map IS NULL OR k.channel_thread_map='{}'::jsonb)
               AND NOT EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=k.id)
               AND NOT EXISTS(SELECT 1 FROM auto_queue_entries e
                 LEFT JOIN auto_queue_runs r ON r.id=e.run_id WHERE e.kanban_card_id=k.id
                 AND (e.status IS DISTINCT FROM 'skipped' OR r.status IS DISTINCT FROM 'completed'
                   OR e.dispatch_id IS NOT NULL OR e.dispatched_at IS NOT NULL
                   OR e.retry_count<>0 OR e.slot_index IS NOT NULL))
               AND NOT EXISTS(SELECT 1 FROM auto_queue_slots slot JOIN auto_queue_entries e
                 ON e.run_id=slot.assigned_run_id WHERE e.kanban_card_id=k.id)
               AND NOT EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup
                 JOIN auto_queue_entries e ON e.run_id=ANY(cleanup.run_ids) WHERE e.kanban_card_id=k.id)
               AND NOT EXISTS(SELECT 1 FROM sessions s
                 WHERE s.status IN ('working','turn_active','awaiting_bg','awaiting_user','idle','connected')
                   AND (string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||k.id]
                     OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
                       WHERE e.kanban_card_id=k.id AND s.thread_channel_id IN
                         (r.unified_thread_id,r.unified_thread_channel_id))))",
        )
        .bind(candidate.get::<String, _>("task_key"))
        .bind(&request)
        .bind(&repair)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    }
    Ok(changed)
}
