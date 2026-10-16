//! Completion observes existing owners and cleanup; it never deletes ownership.
use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Postgres, Row, Transaction};

pub(crate) async fn settled(tx: &mut Transaction<'_, Postgres>, id: &str) -> Result<bool> {
    // Ownership and retained cleanup payloads share one PostgreSQL snapshot.
    let mut rows=sqlx::query("WITH tasks AS (SELECT card_id FROM campaign_graph_tasks WHERE campaign_id=$1),
        attempts AS (SELECT d.id,d.status FROM task_dispatches d JOIN tasks t ON t.card_id=d.kanban_card_id),
        entries AS (SELECT e.* FROM auto_queue_entries e WHERE e.kanban_card_id IN(SELECT card_id FROM tasks)
            OR e.dispatch_id IN(SELECT id FROM attempts)),
        runs AS (SELECT DISTINCT r.id,r.status,r.unified_thread_id,r.unified_thread_channel_id
            FROM auto_queue_runs r JOIN entries e ON e.run_id=r.id),
        ownership AS (
        SELECT EXISTS(SELECT 1 FROM attempts WHERE status IN ('pending','dispatched','running'))
        OR EXISTS(SELECT 1 FROM runs WHERE status IS NULL OR status NOT IN ('completed','failed','cancelled'))
        OR EXISTS(SELECT 1 FROM entries WHERE status IS NULL OR status NOT IN ('done','failed','user_cancelled','skipped'))
        OR EXISTS(SELECT 1 FROM auto_queue_slots WHERE assigned_run_id IN(SELECT id FROM runs))
        OR EXISTS(SELECT 1 FROM sessions s WHERE (s.status IS NULL OR s.status NOT IN ('idle','disconnected')) AND (
            s.active_dispatch_id IN(SELECT id FROM attempts)
            OR EXISTS(SELECT 1 FROM tasks t WHERE string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||t.card_id])
            OR EXISTS(SELECT 1 FROM runs r WHERE s.thread_channel_id IN(r.unified_thread_id,r.unified_thread_channel_id))))
        OR EXISTS(SELECT 1 FROM dispatch_queue WHERE kanban_card_id IN(SELECT card_id FROM tasks))
        OR EXISTS(SELECT 1 FROM dispatch_outbox WHERE (card_id IN(SELECT card_id FROM tasks) OR dispatch_id IN(SELECT id FROM attempts))
            AND (status IS NULL OR status NOT IN ('done','acknowledged')))
        OR EXISTS(SELECT 1 FROM auto_queue_run_cleanup_tasks cleanup WHERE
            cleanup.run_ids && ARRAY(SELECT id FROM runs) OR cleanup.dispatch_ids && ARRAY(SELECT id FROM attempts)
            OR cleanup.card_rollback_source IN(SELECT 'campaign_review_scope:'||card_id FROM tasks)
            OR jsonb_typeof(cleanup.card_rollback_tasks) IS DISTINCT FROM 'array'
            OR jsonb_typeof(cleanup.pending_emits) IS DISTINCT FROM 'array'
            OR jsonb_typeof(cleanup.released_slots) IS DISTINCT FROM 'array'
            OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.card_rollback_tasks)='array'
                THEN cleanup.card_rollback_tasks ELSE '[]'::jsonb END) rollback
                WHERE rollback->>'card_id' IN(SELECT card_id FROM tasks) OR rollback->>'dispatch_id' IN(SELECT id FROM attempts))
            OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.pending_emits)='array'
                THEN cleanup.pending_emits ELSE '[]'::jsonb END) emit
                WHERE emit->>'kanban_card_id' IN(SELECT card_id FROM tasks) OR emit->>'card_id' IN(SELECT card_id FROM tasks)
                    OR emit->>'dispatch_id' IN(SELECT id FROM attempts))
            OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.released_slots)='array'
                THEN cleanup.released_slots ELSE '[]'::jsonb END) released JOIN entries e
                ON e.agent_id=released->>'agent_id' AND e.slot_index::text=released->>'slot_index')) AS busy)
        SELECT ownership.busy,cleanup.* FROM ownership
        LEFT JOIN auto_queue_run_cleanup_tasks cleanup ON true ORDER BY cleanup.id")
        .bind(id).fetch(&mut **tx);
    while let Some(row) = rows.try_next().await? {
        if row.get::<bool, _>("busy")
            || (row.get::<Option<i64>, _>("id").is_some()
                && !crate::services::auto_queue::cleanup_tasks::task_payload_is_known(&row))
        {
            return Ok(false);
        }
    }
    Ok(true)
}
