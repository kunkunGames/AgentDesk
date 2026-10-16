//! Server-owned verification driven by the existing policy reconciliation tick.
//! Storage, subprocesses and FSM effects are kept in separate modules.
mod artifacts;
mod capture;
pub(crate) use crate::services::managed_checks::contract;
mod driver;
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod native_tests;
#[cfg(test)]
mod pg_tests;
mod process;
mod rework;
pub(crate) mod snapshot;
mod store;

use super::TestPhaseRunRequest;
pub(crate) use artifacts::{VerifiedArtifact, completed_artifact};
pub(crate) use driver::kick;
use serde_json::{Value, json};
use sqlx::PgPool;
pub(crate) use store::disposition;

#[cfg(all(test, target_os = "linux"))]
pub(crate) async fn test_drive(
    pool: &PgPool,
    engine: &crate::engine::PolicyEngine,
    card: &str,
) -> anyhow::Result<()> {
    driver::drive(pool, engine, card).await
}

pub(crate) async fn validate_assignment(
    pool: &PgPool,
    repo: Option<&str>,
    agent: Option<&str>,
) -> anyhow::Result<()> {
    crate::pipeline::ensure_loaded();
    let pipeline = crate::pipeline::resolve_for_card_pg(pool, repo, agent).await;
    anyhow::ensure!(
        pipeline.is_terminal("done")
            && !pipeline.is_terminal("review")
            && pipeline
                .states
                .iter()
                .all(|s| !s.terminal || s.id == "done"),
        "managed checks currently require the canonical review/done pipeline"
    );
    Ok(())
}

/// Both FSM entry points use the same interpretation of managed review.
pub(crate) fn review_gate_passes(disposition: &str, legacy_verdict: Option<&str>) -> bool {
    match disposition {
        "unmanaged" => matches!(legacy_verdict, Some("pass") | Some("approved")),
        "ready_to_commit" => true,
        _ => false,
    }
}

pub(super) fn reject_public_write(request: &TestPhaseRunRequest) -> Result<(), String> {
    if request.phase_key.trim().starts_with("managed-check:")
        || request
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.trim().starts_with("managed-check:"))
    {
        return Err("managed check receipts are writable only by the protected collector".into());
    }
    Ok(())
}

pub(crate) async fn enroll(
    pool: &PgPool,
    card: &str,
    template_name: &str,
) -> anyhow::Result<Value> {
    let path = std::env::var_os("AGENTDESK_CHECK_REGISTRY")
        .ok_or_else(|| anyhow::anyhow!("protected check registry is not configured"))?;
    let name = template_name.to_owned();
    let contract = tokio::task::spawn_blocking(move || {
        let template = contract::load_template(std::path::Path::new(&path), &name)?;
        snapshot::enroll(template)
    })
    .await?
    .map_err(anyhow::Error::msg)?;
    store::enroll(pool, card, &contract).await?;
    Ok(json!({"card_id":card,"contract_digest":contract.digest}))
}

pub(crate) async fn inspect(pool: &PgPool, card: &str) -> anyhow::Result<Value> {
    use sqlx::Row;
    let row=sqlx::query("SELECT verification_contract,verification_state,agentdesk_check_disposition(k) AS disposition FROM kanban_cards k WHERE id=$1")
        .bind(card).fetch_one(pool).await?;
    Ok(
        json!({"contract":row.get::<Option<Value>,_>("verification_contract"),"state":row.get::<Option<Value>,_>("verification_state"),"disposition":row.get::<String,_>("disposition")}),
    )
}

pub(crate) async fn approve(
    pool: &PgPool,
    card: &str,
    candidate: &str,
    dispatch: &str,
    contract: &str,
    reason: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !reason.trim().is_empty() && reason.len() <= 1000,
        "human approval needs a reason"
    );
    let changed=sqlx::query("UPDATE kanban_cards k SET verification_state=verification_state || jsonb_build_object('human_approved',true,'approval_reason',$3::text,'approval_authority','server_admin'),updated_at=NOW() WHERE id=$1 AND verification_contract->'template'->>'review'='human' AND verification_state->>'candidate_digest'=$2 AND verification_state->>'dispatch_id'=$4 AND verification_contract->>'digest'=$5 AND agentdesk_checks_passed(k)")
        .bind(card).bind(candidate).bind(reason).bind(dispatch).bind(contract).execute(pool).await?.rows_affected();
    anyhow::ensure!(
        changed == 1,
        "human approval has a stale candidate or incomplete checks"
    );
    Ok(())
}
