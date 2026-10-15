//! A failed assertion can request a bounded new implementation through the
//! existing dispatch/FSM. Unknown execution or unverifiable checks never retry.
use super::store;
use serde_json::{Value, json};
use sqlx::PgPool;

pub(super) async fn schedule(pool: &PgPool, work: &store::Work) -> anyhow::Result<bool> {
    let allowed:bool=sqlx::query_scalar("SELECT COALESCE(metadata->>'managed_campaign'='true' AND metadata->>'auto_rework'='true' AND verification_state->>'outcome'='failed',false) FROM kanban_cards WHERE id=$1")
        .bind(&work.card).fetch_one(pool).await?;
    if !allowed {
        return Ok(false);
    }
    let state = work
        .state
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("failed check lacks state"))?;
    let attempts:i64=sqlx::query_scalar("SELECT COUNT(DISTINCT required_capabilities->>'dispatch_id') FROM test_phase_runs WHERE card_id=$1 AND idempotency_key LIKE 'managed-check:%'")
        .bind(&work.card).fetch_one(pool).await?;
    if attempts >= i64::from(work.contract.template.max_candidates) {
        return Ok(false);
    }
    let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_execution_leases WHERE resource_key=$1 AND (managed OR expires_at>clock_timestamp()))")
        .bind(work.contract.template.workspace.to_string_lossy().as_ref()).fetch_one(pool).await?;
    if active {
        return Ok(false);
    }
    let agent = work
        .agent
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("managed rework requires an assigned agent"))?;
    let reports:Vec<Value>=sqlx::query_scalar("SELECT evidence FROM test_phase_runs WHERE card_id=$1 AND required_capabilities->>'dispatch_id'=$2 ORDER BY created_at,id")
        .bind(&work.card).bind(&work.dispatch).fetch_all(pool).await?;
    let key =
        super::contract::digest(format!("{}:{}", work.contract.digest, work.dispatch).as_bytes());
    let context = json!({"parent_dispatch_id":work.dispatch,"managed_rework":true,
        "failed_candidate_digest":state["candidate_digest"],"contract_digest":work.contract.digest,
        "instruction":"Fix the failing acceptance checks. Preserve all immutable dependency inputs.",
        "check_receipts":reports,"acceptance":work.contract.template.acceptance});
    crate::dispatch::create_dispatch_core_with_id(
        pool,
        &format!("managed-rework:{key}"),
        &work.card,
        agent,
        "rework",
        "Repair protected acceptance checks",
        &context,
    )
    .await?;
    Ok(true)
}
