//! Bounded raw native rows from one read-only repeatable-read snapshot.
use super::control_history;
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{FromRow, Postgres, Transaction};

#[derive(FromRow)]
pub(super) struct Campaign {
    pub id: String,
    pub status: Option<String>,
    pub active_version: Option<i64>,
    pub hold_reason: Option<String>,
    pub spec: Value,
}
#[derive(FromRow)]
pub(super) struct Task {
    pub campaign_id: String,
    pub task_key: String,
    pub card_id: String,
    pub version: Option<i64>,
    pub role: String,
    pub contract_digest: String,
    pub accepted: bool,
    pub observed: bool,
    pub requires_reconciliation: bool,
    pub effect: String,
    pub assigned_agent_id: Option<String>,
    pub latest_dispatch_id: Option<String>,
    pub provider: Option<String>,
    pub node: Option<Value>,
    pub active_graph: bool,
}
#[derive(FromRow)]
pub(super) struct Dispatch {
    pub id: String,
    pub card_id: String,
    pub status: Option<String>,
    pub agent_id: Option<String>,
    pub ended: bool,
    pub latest: bool,
    pub raw: Option<String>,
    pub oversized: bool,
    pub chronology_known: bool,
    pub historical_artifact_retired: bool,
    pub control_reconciliation: Option<Value>,
}
#[derive(FromRow)]
pub(super) struct Entry {
    pub id: String,
    pub card_id: Option<String>,
    pub run_id: Option<String>,
    pub agent_id: Option<String>,
    pub dispatch_id: Option<String>,
    pub status: Option<String>,
    pub slot_index: Option<i64>,
    pub thread_group: Option<i64>,
}
#[derive(FromRow)]
pub(super) struct Run {
    pub id: String,
    pub agent_id: Option<String>,
    pub status: Option<String>,
}
#[derive(FromRow)]
pub(super) struct Slot {
    pub agent_id: String,
    pub slot_index: i64,
    pub assigned_run_id: Option<String>,
    pub assigned_thread_group: Option<i64>,
}
#[derive(FromRow)]
pub(super) struct Session {
    pub id: i64,
    pub status: Option<String>,
    pub agent_id: Option<String>,
    pub provider: Option<String>,
    pub active_dispatch_id: Option<String>,
    pub card_ids: Vec<String>,
    pub workspace_conflict: bool,
}
#[derive(FromRow)]
pub(super) struct Audit {
    pub task_key: String,
    pub campaign_id: String,
    pub dispatch_id: String,
    pub original: Value,
    pub settled: Option<Value>,
    pub unattempted: Option<Value>,
    pub authority: Option<String>,
    pub admitted_goal: Option<Value>,
    pub oversized: bool,
}
pub(super) struct Snapshot {
    pub campaigns: Vec<Campaign>,
    pub tasks: Vec<Task>,
    pub dispatches: Vec<Dispatch>,
    pub entries: Vec<Entry>,
    pub runs: Vec<Run>,
    pub slots: Vec<Slot>,
    pub sessions: Vec<Session>,
    pub cleanup: Vec<i64>,
    pub legacy_queue: Vec<i64>,
    pub delivery_debt: Vec<i64>,
    pub unsupported_stops: Vec<String>,
    pub audits: Vec<Audit>,
    pub truncated: bool,
}

pub(super) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    campaigns: &[String],
    selected: &str,
) -> Result<Snapshot> {
    let owners = sqlx::query_as::<_, Campaign>(
        "SELECT c.id,c.document->>'status' AS status,s.active_version,s.hold_reason,s.spec
        FROM campaigns c JOIN campaign_execution_specs s ON s.campaign_id=c.id
        WHERE c.id=ANY($1) ORDER BY c.id",
    )
    .bind(campaigns)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        owners.len() == campaigns.len(),
        "requested campaign owner scope is unavailable"
    );
    let tasks = sqlx::query_as::<_, Task>("SELECT t.campaign_id,t.task_key,t.card_id,t.version,t.role,t.contract_digest,t.accepted,
        t.observation IS NOT NULL AS observed,
        COALESCE(t.observation->'external_effect_requires_reconciliation'='true'::jsonb,false) AS requires_reconciliation,
        COALESCE(n.node->>'effect','external') AS effect,k.assigned_agent_id,k.latest_dispatch_id,a.provider,
        CASE WHEN t.task_key=$2 AND octet_length(n.node::text)<=65536 THEN n.node END AS node,
        COALESCE(v.status='active',false) AS active_graph
        FROM campaign_graph_tasks t JOIN kanban_cards k ON k.id=t.card_id
        LEFT JOIN agents a ON a.id=k.assigned_agent_id
        LEFT JOIN campaign_graph_versions v ON v.campaign_id=t.campaign_id AND v.version=t.version
        LEFT JOIN LATERAL (SELECT node FROM jsonb_array_elements(v.graph->'nodes') node
          WHERE node->>'task_key'=t.task_key LIMIT 1) n ON TRUE
        WHERE t.campaign_id=ANY($1) ORDER BY t.task_key LIMIT 257")
        .bind(campaigns).bind(selected).fetch_all(&mut **tx).await?;
    let cards: Vec<_> = tasks.iter().map(|t| t.card_id.clone()).collect();
    let selected_card = tasks
        .iter()
        .find(|t| t.task_key == selected)
        .map(|t| t.card_id.as_str())
        .unwrap_or("");
    let dispatch_sql = "SELECT d.id,d.kanban_card_id AS card_id,d.status,d.to_agent_id AS agent_id,
        d.completed_at IS NOT NULL AS ended,
        d.id=(SELECT newer.id FROM task_dispatches newer WHERE newer.kanban_card_id=d.kanban_card_id
          ORDER BY newer.created_at DESC,newer.id DESC LIMIT 1) AS latest,
        d.created_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM task_dispatches unknown
          WHERE unknown.kanban_card_id=d.kanban_card_id AND unknown.created_at IS NULL) AS chronology_known,
        EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN campaign_graph_versions v ON v.campaign_id=t.campaign_id AND v.version=t.version
          JOIN campaign_graph_failures f ON f.campaign_id=t.campaign_id AND f.task_key=t.task_key
          JOIN campaign_graph_reconciliations retired ON retired.campaign_id=f.campaign_id AND retired.failure_id=f.id
          WHERE t.card_id=d.kanban_card_id AND t.role='work' AND t.observation IS NOT NULL
            AND t.observation=f.evidence
            AND t.observation->'external_effect_requires_reconciliation'='false'::jsonb
            AND f.category IN ('unknown','infrastructure') AND f.evidence->>'dispatch_id'=d.id
            AND f.evidence->>'task_key'=t.task_key AND f.evidence->'external_effect_requires_reconciliation'='false'::jsonb
            AND f.evidence#>>'{runtime,contract_digest}'=t.contract_digest AND f.evidence#>>'{runtime,role}'='work'
            AND f.evidence#>>'{runtime,graph_version}'=t.version::text AND f.evidence#>>'{runtime,dispatch_status}'=d.status
            AND EXISTS(SELECT 1 FROM jsonb_array_elements(v.graph->'nodes') n WHERE n->>'task_key'=t.task_key AND n->>'effect'='artifact_only')) AS historical_artifact_retired,
        __CONTROL_RECONCILIATION__ AS control_reconciliation,
        CASE WHEN octet_length(d.result::text)<=65536 THEN d.result::text END AS raw,
        COALESCE(octet_length(d.result::text)>65536,false) AS oversized
        FROM task_dispatches d WHERE d.kanban_card_id=ANY($1) ORDER BY d.id LIMIT 513"
        .replace("__CONTROL_RECONCILIATION__", control_history::PROOF_SQL);
    let dispatches = sqlx::query_as::<_, Dispatch>(&dispatch_sql)
        .bind(&cards)
        .fetch_all(&mut **tx)
        .await?;
    let dispatch_ids: Vec<_> = dispatches.iter().map(|d| d.id.clone()).collect();
    // Read every entry in related runs, including foreign cards. A scope-only
    // count would silently bless a mixed run as the current owner's run.
    let entries = sqlx::query_as::<_, Entry>("SELECT e.id,e.kanban_card_id AS card_id,e.run_id,e.agent_id,e.dispatch_id,e.status,e.slot_index,e.thread_group
        FROM auto_queue_entries e WHERE e.kanban_card_id=ANY($1) OR e.dispatch_id=ANY($2)
          OR e.run_id IN(SELECT linked.run_id FROM auto_queue_entries linked
            WHERE linked.kanban_card_id=ANY($1) OR linked.dispatch_id=ANY($2))
        ORDER BY e.id LIMIT 513")
        .bind(&cards).bind(&dispatch_ids).fetch_all(&mut **tx).await?;
    let run_ids: Vec<_> = entries.iter().filter_map(|e| e.run_id.clone()).collect();
    let runs = sqlx::query_as::<_, Run>(
        "SELECT id,agent_id,status FROM auto_queue_runs WHERE id=ANY($1) ORDER BY id LIMIT 257",
    )
    .bind(&run_ids)
    .fetch_all(&mut **tx)
    .await?;
    let slots = sqlx::query_as::<_, Slot>(
        "SELECT slot.agent_id,slot.slot_index,slot.assigned_run_id,slot.assigned_thread_group
        FROM auto_queue_slots slot WHERE slot.assigned_run_id=ANY($1)
          OR EXISTS(SELECT 1 FROM auto_queue_entries e WHERE e.kanban_card_id=ANY($2)
            AND e.agent_id=slot.agent_id AND e.slot_index=slot.slot_index)
        ORDER BY slot.agent_id,slot.slot_index LIMIT 257",
    )
    .bind(&run_ids)
    .bind(&cards)
    .fetch_all(&mut **tx)
    .await?;
    let sessions = sqlx::query_as::<_, Session>("SELECT s.id::bigint,s.status,s.agent_id,s.provider,s.active_dispatch_id,
        EXISTS(SELECT 1 FROM unnest(string_to_array(replace(s.cwd,chr(92),'/'),'/')) component
          WHERE component LIKE 'card-%' AND component<>'card-'||$2) AS workspace_conflict,
        ARRAY(SELECT k.id FROM kanban_cards k WHERE (
          EXISTS(SELECT 1 FROM task_dispatches d WHERE d.id=s.active_dispatch_id AND d.kanban_card_id=k.id)
          OR string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||k.id]
          OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
            WHERE e.kanban_card_id=k.id AND s.thread_channel_id IN(r.unified_thread_id,r.unified_thread_channel_id))) ORDER BY k.id LIMIT 33) AS card_ids
        FROM sessions s WHERE EXISTS(SELECT 1 FROM campaign_graph_tasks t WHERE t.campaign_id=ANY($1) AND (
          EXISTS(SELECT 1 FROM task_dispatches d WHERE d.id=s.active_dispatch_id AND d.kanban_card_id=t.card_id)
          OR string_to_array(replace(s.cwd,chr(92),'/'),'/') @> ARRAY['card-'||t.card_id]
          OR EXISTS(SELECT 1 FROM auto_queue_entries e JOIN auto_queue_runs r ON r.id=e.run_id
            WHERE e.kanban_card_id=t.card_id AND s.thread_channel_id IN(r.unified_thread_id,r.unified_thread_channel_id))))
        ORDER BY s.id LIMIT 513")
        .bind(campaigns).bind(selected_card).fetch_all(&mut **tx).await?;
    let cleanup = sqlx::query_scalar::<_, i64>("SELECT cleanup.id FROM auto_queue_run_cleanup_tasks cleanup
        WHERE cleanup.run_ids && $1 OR cleanup.dispatch_ids && $2
          OR jsonb_typeof(cleanup.card_rollback_tasks) IS DISTINCT FROM 'array'
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(cleanup.card_rollback_tasks)='array'
            THEN cleanup.card_rollback_tasks ELSE '[]'::jsonb END) rollback
            WHERE rollback->>'card_id'=ANY($3) OR rollback->>'dispatch_id'=ANY($2)
              OR jsonb_typeof(rollback) IS DISTINCT FROM 'object')
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(cleanup.pending_emits) emit WHERE emit->>'card_id'=ANY($3))
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(cleanup.released_slots) released
            JOIN auto_queue_entries e ON e.agent_id=released->>'agent_id' AND e.slot_index::text=released->>'slot_index'
            WHERE e.kanban_card_id=ANY($3)) ORDER BY cleanup.id LIMIT 257")
        .bind(&run_ids).bind(&dispatch_ids).bind(&cards).fetch_all(&mut **tx).await?;
    // The frozen runtime has no trusted owner-stop/settlement producer. Any
    // such event is unknown evidence, never an invented positive settlement.
    let unsupported_stops = sqlx::query_scalar::<_, String>("SELECT DISTINCT event.dispatch_id FROM dispatch_events event
        WHERE event.dispatch_id=ANY($1) AND event.transition_source LIKE 'campaign_review_owner_stop%' ORDER BY event.dispatch_id LIMIT 257")
        .bind(&dispatch_ids).fetch_all(&mut **tx).await?;
    let legacy_queue = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM dispatch_queue WHERE kanban_card_id=ANY($1) ORDER BY id LIMIT 257",
    )
    .bind(&cards)
    .fetch_all(&mut **tx)
    .await?;
    let delivery_debt = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM dispatch_outbox WHERE (card_id=ANY($1) OR dispatch_id=ANY($2))
        AND (status IS NULL OR status NOT IN ('done','acknowledged')) ORDER BY id LIMIT 257",
    )
    .bind(&cards)
    .bind(&dispatch_ids)
    .fetch_all(&mut **tx)
    .await?;
    let audits = sqlx::query_as::<_, Audit>("SELECT original.task_key,original.campaign_id,original.evidence->>'dispatch_id' AS dispatch_id,
        CASE WHEN octet_length(original.evidence::text)<=65536 THEN original.evidence ELSE '{}'::jsonb END AS original,
        CASE WHEN octet_length(repair.evidence::text)<=65536 AND jsonb_typeof(repair.evidence->'settled_effect')='object' THEN repair.evidence->'settled_effect' END AS settled,
        CASE WHEN octet_length(repair.evidence::text)<=65536 AND jsonb_typeof(repair.evidence->'unattempted_effect')='object' THEN repair.evidence->'unattempted_effect' END AS unattempted,
        repair.evidence->>'settled_effect_authority' AS authority,
        revision.revised_goal AS admitted_goal,
        octet_length(original.evidence::text)>65536 OR octet_length(repair.evidence::text)>65536 AS oversized
        FROM campaign_graph_failures original JOIN campaign_graph_reconciliations resolved
          ON resolved.failure_id=original.id AND resolved.campaign_id=original.campaign_id
        JOIN campaign_graph_failures repair ON repair.campaign_id=original.campaign_id
          AND repair.evidence->>'reconciled_failure_id'=original.id
        LEFT JOIN campaign_goal_revisions revision ON revision.campaign_id=repair.campaign_id
          AND revision.request_id=repair.evidence->>'operator_replan'
        WHERE original.campaign_id=ANY($1) AND repair.category='plan' AND repair.evidence ? 'operator_replan'
          AND (jsonb_typeof(repair.evidence->'settled_effect')='object' OR jsonb_typeof(repair.evidence->'unattempted_effect')='object')
        ORDER BY original.id,repair.id LIMIT 33")
        .bind(campaigns).fetch_all(&mut **tx).await?;
    let bytes: usize = dispatches
        .iter()
        .filter_map(|d| d.raw.as_ref())
        .map(String::len)
        .chain(
            dispatches
                .iter()
                .filter_map(|d| d.control_reconciliation.as_ref())
                .map(|proof| proof.to_string().len()),
        )
        .sum();
    let truncated = tasks.len() > 256
        || dispatches.len() > 512
        || entries.len() > 512
        || runs.len() > 256
        || slots.len() > 256
        || sessions.len() > 512
        || cleanup.len() > 256
        || unsupported_stops.len() > 256
        || audits.len() > 32
        || legacy_queue.len() > 256
        || delivery_debt.len() > 256
        || bytes > 2 * 1024 * 1024;
    Ok(Snapshot {
        campaigns: owners,
        tasks,
        dispatches,
        entries,
        runs,
        slots,
        sessions,
        cleanup,
        legacy_queue,
        delivery_debt,
        unsupported_stops,
        audits,
        truncated,
    })
}
