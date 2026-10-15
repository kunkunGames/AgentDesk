//! Retire a stale frozen control only when its canonical queue never started.
//! Started attempts and uncertain owners remain the responsibility of their owner.
use crate::db::{
    auto_queue::{self, EntryStatusUpdateOptions},
    campaigns::{
        Campaign, CampaignStatus,
        graph::{Goal, contract_digest},
    },
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};

/// Publication and first dispatch serialize on campaign, spec and task tokens.
/// Queue tokens/rows are acquired without waiting: a busy owner is never cancelled.
/// Only a generated private run qualifies. Active/paused/restoring runs retain
/// ownership even if their dispatch has not yet been attached: slot allocation
/// outside the run-token protocol cannot be retired safely by this observer.
pub(crate) async fn settle_stale_unstarted(pool: &PgPool, id: &str) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let campaign: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let enabled: bool = sqlx::query_scalar(
        "SELECT manager_enabled FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if campaign.0.status != CampaignStatus::Active || !enabled {
        return Ok(0);
    }
    let keys: Vec<String> = sqlx::query_scalar(
        "SELECT task_key FROM campaign_graph_tasks WHERE campaign_id=$1
         AND role IN ('manage','finish') AND NOT accepted AND observation IS NULL
         AND artifact_manifest IS NULL ORDER BY task_key LIMIT 64",
    )
    .bind(id)
    .fetch_all(&mut *tx)
    .await?;
    let mut changed = 0;
    for key in keys {
        let token: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtext('campaign-task'),hashtext($1))",
        )
        .bind(&key)
        .fetch_one(&mut *tx)
        .await?;
        if !token {
            continue;
        }
        let task = sqlx::query(
            "SELECT t.card_id,t.version,t.execution_profile,v.digest AS original_graph_digest
             FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
             JOIN campaign_graph_versions v ON v.campaign_id=t.campaign_id AND v.version=t.version
             WHERE t.campaign_id=$1 AND t.task_key=$2 AND t.role IN ('manage','finish')
               AND NOT t.accepted AND t.observation IS NULL AND t.artifact_manifest IS NULL
             FOR UPDATE OF t,k SKIP LOCKED",
        )
        .bind(id)
        .bind(&key)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(task) = task else { continue };
        let card: String = task.get("card_id");
        let version: i64 = task.get("version");
        let profile: Value = task.get("execution_profile");
        let original_graph_digest: String = task.get("original_graph_digest");
        if card != crate::db::campaigns::graph::store::card_id(&key)
            || !bound_profile(&profile, version, &original_graph_digest)?
        {
            continue;
        }
        let runs: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT run_id FROM auto_queue_entries WHERE kanban_card_id=$1 ORDER BY id",
        )
        .bind(&card)
        .fetch_all(&mut *tx)
        .await?;
        // The publisher creates one entry in one private run. Additional or
        // missing queue provenance is not evidence of a never-started control.
        if runs.len() != 1 {
            continue;
        }
        let Some(run) = &runs[0] else { continue };
        let run_token: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext('aq_run:' || $1))")
                .bind(run)
                .fetch_one(&mut *tx)
                .await?;
        if !run_token {
            continue;
        }
        // A run row lock fences new entries through their FK. SKIP LOCKED also
        // preserves a queue writer that took its row before the run token.
        let entries = sqlx::query(
            "SELECT e.id FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
             WHERE e.kanban_card_id=$1 AND r.id=$2 FOR UPDATE OF e,r SKIP LOCKED",
        )
        .bind(&card)
        .bind(run)
        .fetch_all(&mut *tx)
        .await?;
        if entries.len() != 1 {
            continue;
        }
        // This is the incumbent replan/control no-attempt proof, extended to
        // every dispatch type and every retained queue/cleanup owner. Re-read
        // Goal, active graph and authority in the same statement as this proof.
        let proof = sqlx::query(
            "WITH owned AS (SELECT * FROM auto_queue_entries WHERE kanban_card_id=$2),
             frontier_state AS (SELECT s.spec,s.active_version,v.digest,
                agentdesk_workflow_authority_state(s.campaign_id) AS authority
                FROM campaign_execution_specs s JOIN campaign_graph_versions v
                  ON v.campaign_id=s.campaign_id AND v.version=s.active_version AND v.status='active'
                WHERE s.campaign_id=$3 AND s.manager_enabled)
             SELECT frontier_state.*, EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
                WHERE t.task_key=$1 AND t.campaign_id=$3 AND t.card_id=$2
                  AND t.role IN ('manage','finish') AND NOT t.accepted AND t.observation IS NULL
                  AND t.artifact_manifest IS NULL AND k.status='ready'
                  AND k.latest_dispatch_id IS NULL AND k.started_at IS NULL AND k.completed_at IS NULL
                  AND k.requested_at IS NULL AND k.active_thread_id IS NULL
                  AND (k.channel_thread_map IS NULL OR k.channel_thread_map='{}'::jsonb)
                  AND k.metadata->>'graph_task_key'=t.task_key AND k.metadata->>'campaign_id'=t.campaign_id
                  AND k.metadata->>'node_id'=t.node_id AND k.metadata->'graph_version'=to_jsonb(t.version)
                  AND NOT EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=k.id)
                  AND (SELECT COUNT(*) FROM owned)=1
                  AND NOT EXISTS(SELECT 1 FROM owned e LEFT JOIN auto_queue_runs r ON r.id=e.run_id
                    WHERE e.run_id IS DISTINCT FROM $4 OR e.agent_id IS DISTINCT FROM k.assigned_agent_id
                      OR e.status IS DISTINCT FROM 'pending' OR e.dispatch_id IS NOT NULL
                      OR e.dispatched_at IS NOT NULL OR e.completed_at IS NOT NULL
                      OR e.retry_count<>0 OR e.slot_index IS NOT NULL
                      OR e.reason IS DISTINCT FROM 'validated graph task' OR e.priority_rank IS DISTINCT FROM 1
                      OR e.thread_group IS DISTINCT FROM 0 OR e.batch_phase IS DISTINCT FROM 0
                      OR r.status IS DISTINCT FROM 'generated' OR r.completed_at IS NOT NULL
                      OR r.agent_id IS DISTINCT FROM k.assigned_agent_id
                      OR r.ai_model IS DISTINCT FROM 'campaign-graph-v1' OR r.review_mode IS DISTINCT FROM 'disabled'
                      OR r.unified_thread IS DISTINCT FROM false OR r.unified_thread_id IS NOT NULL
                      OR r.unified_thread_channel_id IS NOT NULL
                      OR r.max_concurrent_threads IS DISTINCT FROM 1 OR r.thread_group_count IS DISTINCT FROM 1)
                  AND NOT EXISTS(SELECT 1 FROM auto_queue_entries e WHERE e.run_id=$4
                    AND e.kanban_card_id IS DISTINCT FROM k.id)
                  AND NOT EXISTS(SELECT 1 FROM auto_queue_entry_dispatch_history h JOIN owned e ON e.id=h.entry_id)
                  AND NOT EXISTS(SELECT 1 FROM auto_queue_entry_transitions h JOIN owned e ON e.id=h.entry_id)
                  AND NOT EXISTS(SELECT 1 FROM auto_queue_phase_gates WHERE run_id=$4)
                  AND NOT EXISTS(SELECT 1 FROM auto_queue_slots WHERE assigned_run_id=$4)
                  AND NOT EXISTS(SELECT 1 FROM sessions s
                    WHERE string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||k.id])
                  AND NOT EXISTS(SELECT 1 FROM dispatch_queue WHERE kanban_card_id=k.id)
                  AND NOT EXISTS(SELECT 1 FROM dispatch_outbox WHERE card_id=k.id)
                  AND NOT EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup WHERE
                    $4=ANY(cleanup.run_ids)
                    OR cleanup.card_rollback_source='campaign_review_scope:'||k.id
                    OR jsonb_typeof(cleanup.card_rollback_tasks) IS DISTINCT FROM 'array'
                    OR jsonb_typeof(cleanup.pending_emits) IS DISTINCT FROM 'array'
                    OR jsonb_typeof(cleanup.released_slots) IS DISTINCT FROM 'array'
                    OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.card_rollback_tasks)='array'
                        THEN cleanup.card_rollback_tasks ELSE '[]'::jsonb END) rollback WHERE rollback->>'card_id'=k.id)
                    OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.pending_emits)='array'
                        THEN cleanup.pending_emits ELSE '[]'::jsonb END) emit
                        WHERE emit->>'card_id'=k.id OR emit->>'kanban_card_id'=k.id)
                    OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.released_slots)='array'
                        THEN cleanup.released_slots ELSE '[]'::jsonb END) released JOIN owned e
                        ON e.agent_id=released->>'agent_id' AND e.slot_index::text=released->>'slot_index'))
                ) AS unstarted FROM frontier_state",
        ).bind(&key).bind(&card).bind(id).bind(run).fetch_optional(&mut *tx).await?;
        let Some(proof) = proof else { continue };
        if !proof.get::<bool, _>("unstarted") {
            continue;
        }
        let goal: Value = proof.get("spec");
        let current_version: i64 = proof.get("active_version");
        let graph_digest: String = proof.get("digest");
        let authority: Value = proof.get("authority");
        let goal_changed = profile["workflow_goal"] != goal;
        let graph_changed = version != current_version || original_graph_digest != graph_digest;
        let authority_changed = profile["workflow_authority_state"] != authority;
        if !goal_changed && !graph_changed && !authority_changed {
            continue;
        }

        // The shared entry writer retains its canonical audit. Suppress its
        // opportunistic run finalizer: retirement may not release slots, clear
        // phase gates or send completion notifications. Only this proven run closes.
        let suppressed: bool = sqlx::query_scalar(
            "SELECT COALESCE(current_setting('agentdesk.suppress_terminal_entry_finalize',true)='on',false)",
        ).fetch_one(&mut *tx).await?;
        auto_queue::runs::set_terminal_entry_finalize_suppressed_on_pg_tx(&mut tx, true)
            .await
            .map_err(anyhow::Error::msg)?;
        let entry = auto_queue::update_entry_status_on_pg_tx(
            &mut tx,
            &entries[0].get::<String, _>("id"),
            auto_queue::ENTRY_STATUS_SKIPPED,
            "workflow_stale_unstarted",
            &EntryStatusUpdateOptions::default(),
        )
        .await
        .map_err(anyhow::Error::msg)?;
        ensure!(
            entry.changed && entry.run_id == *run,
            "stale workflow entry changed during retirement"
        );
        auto_queue::runs::set_terminal_entry_finalize_suppressed_on_pg_tx(&mut tx, suppressed)
            .await
            .map_err(anyhow::Error::msg)?;
        let closed = sqlx::query(
            "UPDATE auto_queue_runs SET status='completed',completed_at=NOW()
             WHERE id=$1 AND status='generated' AND completed_at IS NULL",
        )
        .bind(run)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure!(closed == 1, "stale workflow run changed during retirement");
        let observation = json!({"outcome":"superseded","unstarted":true,
            "graph_version":version,"snapshot_digest":profile["workflow_snapshot_digest"],
            "original_execution_profile":profile,
            "superseding_goal_digest":contract_digest(&serde_json::from_value::<Goal>(goal)?)?,
            "superseding_graph_version":current_version,"superseding_graph_digest":graph_digest,
            "superseding_authority_state":authority,
            "changed":{"goal":goal_changed,"graph":graph_changed,"authority":authority_changed},
            "reason":"Stored Goal, active graph or result authority changed before this frozen control ever started; its exclusive untouched queue is retired"});
        let retired = sqlx::query(
            "UPDATE campaign_graph_tasks SET observation=$2 WHERE campaign_id=$3 AND task_key=$1
             AND role IN ('manage','finish') AND NOT accepted AND observation IS NULL AND artifact_manifest IS NULL",
        ).bind(&key).bind(observation).bind(id).execute(&mut *tx).await?.rows_affected();
        ensure!(
            retired == 1,
            "stale workflow task changed during retirement"
        );
        changed += retired;
    }
    tx.commit().await?;
    Ok(changed)
}

fn bound_profile(profile: &Value, version: i64, graph_digest: &str) -> Result<bool> {
    let Ok(goal) = serde_json::from_value::<Goal>(profile["workflow_goal"].clone()) else {
        return Ok(false);
    };
    let authority = &profile["workflow_authority_state"];
    Ok(profile["schema_version"] == 1
        && profile["graph_version"] == version
        && profile["goal_digest"] == contract_digest(&goal)?
        && profile["workflow_graph_digest"] == graph_digest
        && profile["workflow_snapshot_digest"]
            .as_str()
            .is_some_and(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
        && authority
            .as_object()
            .is_some_and(|object| object.len() == 3)
        && ["tasks", "failures", "unmet_goal_assessments"]
            .iter()
            .all(|key| authority[*key].is_array()))
}
