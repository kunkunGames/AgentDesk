use super::contract::Contract;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

pub(super) struct Work {
    pub card: String,
    pub contract: Contract,
    pub state: Option<Value>,
    pub dispatch: String,
    pub result: Option<String>,
    pub agent: Option<String>,
    pub repo: Option<String>,
    pub stage: Option<String>,
}

pub(super) async fn enroll(pool: &PgPool, card: &str, contract: &Contract) -> anyhow::Result<()> {
    let (repo, agent): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT repo_id,assigned_agent_id FROM kanban_cards WHERE id=$1")
            .bind(card)
            .fetch_one(pool)
            .await?;
    super::validate_assignment(pool, repo.as_deref(), agent.as_deref()).await?;
    let contract = serde_json::to_value(contract)?;
    let mut tx = pool.begin().await?;
    // Dispatch insertion takes this same card lock. Read dispatch membership
    // in a fresh statement after acquiring it, including any insertion that
    // won the race with enrollment. No filesystem I/O runs under this lock.
    sqlx::query("SELECT id FROM kanban_cards WHERE id=$1 FOR UPDATE")
        .bind(card)
        .execute(&mut *tx)
        .await?;
    let changed=sqlx::query("UPDATE kanban_cards SET verification_contract=$2,updated_at=NOW() WHERE id=$1 AND status IN ('backlog','ready') AND (verification_contract IS NULL OR verification_contract=$2) AND repo_id IS NOT DISTINCT FROM $3 AND assigned_agent_id IS NOT DISTINCT FROM $4 AND NOT EXISTS(SELECT 1 FROM task_dispatches WHERE kanban_card_id=$1)")
        .bind(card).bind(contract).bind(repo).bind(agent).execute(&mut *tx).await?.rows_affected();
    anyhow::ensure!(
        changed == 1,
        "verification must be enrolled on an idle card before dispatch"
    );
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn disposition(pool: &PgPool, card: &str) -> Result<String, String> {
    sqlx::query_scalar("SELECT agentdesk_check_disposition(k) FROM kanban_cards k WHERE id=$1")
        .bind(card)
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())
}

// Selection and reloading share the same admission predicate. Applying a batch
// limit to all review cards first would let incomplete implementations occupy
// every sweep forever, preventing an independent candidate from being checked.
const WORK_SCOPE: &str = "FROM kanban_cards k JOIN LATERAL (SELECT id,result,status FROM task_dispatches WHERE kanban_card_id=k.id AND dispatch_type IN ('implementation','rework') ORDER BY created_at DESC,id DESC LIMIT 1) d ON d.status='completed' WHERE k.status='review' AND k.verification_contract IS NOT NULL AND NOT EXISTS(SELECT 1 FROM task_dispatches a WHERE a.kanban_card_id=k.id AND a.dispatch_type IN ('implementation','rework') AND a.status IN ('pending','dispatched'))";

pub(super) async fn pending_cards(pool: &PgPool) -> anyhow::Result<Vec<String>> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT k.id {WORK_SCOPE} ORDER BY k.updated_at,k.id LIMIT 16"
    ))
    .fetch_all(pool)
    .await?)
}

pub(super) async fn load(pool: &PgPool, card: &str) -> anyhow::Result<Option<Work>> {
    let query = format!(
        "SELECT k.verification_contract,k.verification_state,k.assigned_agent_id,k.repo_id,k.pipeline_stage_id,d.id AS dispatch_id,d.result {WORK_SCOPE} AND k.id=$1"
    );
    let row = sqlx::query(&query).bind(card).fetch_optional(pool).await?;
    row.map(|r| {
        Ok(Work {
            card: card.into(),
            contract: serde_json::from_value(r.get("verification_contract"))?,
            state: r.get("verification_state"),
            dispatch: r.get("dispatch_id"),
            result: r.get("result"),
            agent: r.get("assigned_agent_id"),
            repo: r.get("repo_id"),
            stage: r.get("pipeline_stage_id"),
        })
    })
    .transpose()
}

pub(super) fn attempt_state(work: &Work) -> Value {
    json!({"contract_digest":work.contract.digest,"dispatch_id":work.dispatch,"dispatch_result":work.result,
        "assigned_agent_id":work.agent,"repo_id":work.repo,"pipeline_stage_id":work.stage,"runs":{},"outcome":"running"})
}

pub(super) fn state(work: &Work, candidate: &super::snapshot::Candidate) -> Value {
    let mut state = attempt_state(work);
    state["candidate_digest"] = json!(candidate.digest);
    state["candidate_path"] = json!(candidate.path);
    state
}

pub(super) async fn current(pool: &PgPool, work: &Work) -> anyhow::Result<bool> {
    Ok(load(pool, &work.card).await?.is_some_and(|now| {
        now.dispatch == work.dispatch
            && now.result == work.result
            && now.contract.digest == work.contract.digest
            && now.agent == work.agent
            && now.repo == work.repo
            && now.stage == work.stage
    }))
}

pub(super) async fn save(pool: &PgPool, work: &Work, state: &Value) -> anyhow::Result<bool> {
    write_state(pool, work, Some(state), None).await
}

async fn write_state(
    pool: &PgPool,
    work: &Work,
    state: Option<&Value>,
    reason: Option<&str>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    // Card before dispatch is also the completion guard's lock order.
    sqlx::query("SELECT id FROM kanban_cards WHERE id=$1 FOR UPDATE")
        .bind(&work.card)
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query("SELECT result,status FROM task_dispatches WHERE id=$1 FOR UPDATE")
        .bind(&work.dispatch)
        .fetch_one(&mut *tx)
        .await?;
    if row.get::<String, _>("status") != "completed"
        || row.get::<Option<String>, _>("result") != work.result
    {
        return Ok(false);
    }
    let n=sqlx::query("UPDATE kanban_cards SET verification_state=$2,blocked_reason=COALESCE($9,blocked_reason),updated_at=NOW() WHERE id=$1 AND status='review' AND verification_contract->>'digest'=$3 AND assigned_agent_id IS NOT DISTINCT FROM $4 AND repo_id IS NOT DISTINCT FROM $5 AND verification_state IS NOT DISTINCT FROM $7 AND pipeline_stage_id IS NOT DISTINCT FROM $8 AND NOT EXISTS(SELECT 1 FROM task_dispatches d WHERE d.kanban_card_id=$1 AND d.dispatch_type IN ('implementation','rework') AND (d.status IN ('pending','dispatched') OR (d.created_at,d.id)>(SELECT created_at,id FROM task_dispatches WHERE id=$6)))")
        .bind(&work.card).bind(state).bind(&work.contract.digest).bind(&work.agent).bind(&work.repo).bind(&work.dispatch).bind(&work.state).bind(&work.stage).bind(reason).execute(&mut *tx).await?.rows_affected();
    tx.commit().await?;
    Ok(n == 1)
}

pub(super) async fn mark_wait(pool: &PgPool, work: &Work, reason: &str) -> anyhow::Result<bool> {
    write_state(pool, work, work.state.as_ref(), Some(reason)).await
}

pub(super) async fn unverifiable(pool: &PgPool, work: &Work, reason: &str) -> anyhow::Result<bool> {
    let mut state = work
        .state
        .as_ref()
        .filter(|state| {
            state["dispatch_id"] == work.dispatch
                && state["dispatch_result"] == json!(work.result)
                && state["contract_digest"] == work.contract.digest
                && state["pipeline_stage_id"] == json!(work.stage)
        })
        .cloned()
        .unwrap_or_else(|| attempt_state(work));
    state["outcome"] = json!("unverifiable");
    state["reason"] = json!(reason.chars().take(500).collect::<String>());
    write_state(pool, work, Some(&state), Some("verification_unverifiable")).await
}

/// A verifier cannot write implementation inputs or create descendants. A lost
/// collector is retryable in a fresh private sandbox; this never frees a writer.
pub(super) async fn expire(pool: &PgPool, key: &str) -> anyhow::Result<bool> {
    let mut tx = super::super::store::lock(pool, key)
        .await
        .map_err(anyhow::Error::msg)?;
    let Some(run) = super::super::store::current(&mut tx, key)
        .await
        .map_err(anyhow::Error::msg)?
    else {
        return Ok(false);
    };
    let live:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource_locks WHERE lock_key=$1 AND holder_job_id=$2 AND expires_at>clock_timestamp())")
        .bind(&run.resource_lock_key).bind(&run.holder_job_id).fetch_one(&mut *tx).await?;
    if run.status != "running" || live {
        return Ok(false);
    }
    sqlx::query("UPDATE test_phase_runs SET status='failed',evidence=jsonb_build_object('outcome','unverifiable','reason','collector_lease_expired'),completed_at=NOW(),updated_at=NOW() WHERE id=$1")
        .bind(&run.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}
