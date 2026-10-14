//! Audited recovery of a read-only final verifier never approves or repeats work.
mod audit;
pub(crate) use audit::quota_result;

use super::super::{contract_digest, workflow};
use crate::db::campaigns::{self, Campaign, CampaignStatus};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};

pub(crate) const AUTHORITY: &str = "configured_server_admin_bearer";

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Recovery {
    pub expected_revision: i64,
    pub request_id: String,
    pub failure_id: String,
    pub evidence: String,
    pub failed_finish: FailedFinish,
    pub operator_attestation: OperatorAttestation,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailedFinish {
    pub dispatch_id: String,
    pub graph_version: i64,
    pub graph_digest: String,
    pub raw_result_sha256: String,
    pub contract_digest: String,
    /// SHA256 of canonical JSON for ordered (event_id,fingerprint,output_digest).
    /// Telemetry contains hashes, not arguments or an OS exit certificate.
    pub progress_sha256: String,
}

/// These are explicit administrator attestations, not inferred from telemetry.
/// The shared admin credential does not isolate providers from the operator.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAttestation {
    pub read_only_execution_verified: bool,
    pub no_unknown_effects: bool,
    pub provider_cause_repaired: bool,
}

#[derive(Debug, Serialize)]
pub struct Recovered {
    pub revision: i64,
    pub recovery_id: String,
    pub independent_goal_validation_required: bool,
    pub business_contracts_preserved: bool,
}

pub async fn request(pool: &PgPool, id: &str, request: &Recovery) -> Result<Recovered> {
    on_admin_boundary(pool, id, request, false).await
}

/// The HTTP owner verifies the explicitly configured administrator Bearer first.
pub(crate) async fn request_as_server_admin(
    pool: &PgPool,
    id: &str,
    request: &Recovery,
) -> Result<Recovered> {
    on_admin_boundary(pool, id, request, true).await
}

async fn on_admin_boundary(
    pool: &PgPool,
    id: &str,
    request: &Recovery,
    server_admin: bool,
) -> Result<Recovered> {
    ensure!(
        server_admin,
        "failed finish recovery requires configured server administrator authentication"
    );
    audit::request_shape(request)?;
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internal task admission disabled"
    );
    let digest = contract_digest(&("operator-finish-recovery", request))?;
    let recovery_id = contract_digest(&(id, "operator-finish-recovery", &request.request_id))?;
    let mut tx = pool.begin().await?;
    // Match publication/admission: campaign, execution spec, then card/task.
    let campaign: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let state = sqlx::query(
        "SELECT spec,active_version,hold_reason,manager_enabled
         FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if let Some(prior) = sqlx::query(
        "SELECT request_digest,campaign_revision FROM campaign_goal_revisions
         WHERE campaign_id=$1 AND request_id=$2",
    )
    .bind(id)
    .bind(&request.request_id)
    .fetch_optional(&mut *tx)
    .await?
    {
        ensure!(
            prior.get::<String, _>("request_digest") == digest,
            "request ID already used with different content"
        );
        return Ok(Recovered {
            revision: prior.get("campaign_revision"),
            recovery_id,
            independent_goal_validation_required: true,
            business_contracts_preserved: true,
        });
    }
    ensure!(
        campaign.0.status == CampaignStatus::Paused
            && campaign.0.revision == request.expected_revision,
        "pause the campaign at the expected revision first"
    );
    ensure!(
        state.get::<bool, _>("manager_enabled")
            && state.get::<Option<i64>, _>("active_version")
                == Some(request.failed_finish.graph_version)
            && state.get::<Option<String>, _>("hold_reason").as_deref()
                == Some("Execution outcome requires reconciliation; automatic replay is disabled"),
        "failed finish recovery requires the held managed active graph"
    );
    let goal: super::super::Goal = serde_json::from_value(state.get("spec"))?;
    goal.validate()?;
    let captured = audit::captured(&mut tx, id, &goal, request).await?;
    super::replan::settled_effect::require_no_campaign_owners(&mut tx, id).await?;
    ensure!(
        workflow::lifecycle::settled(&mut tx, id).await?,
        "campaign execution, delivery and durable cleanup must settle first"
    );
    let frontier = workflow::store::snapshot_on_connection(&mut tx, id).await?;
    frontier.validate_completion_frontier()?;
    ensure!(
        frontier
            .graph
            .nodes
            .iter()
            .all(|node| frontier.accepted(&node.task_key)),
        "failed finish recovery cannot admit unpublished or unaccepted business work"
    );
    let budget = super::super::control_budget::on_connection(&mut tx, id, &goal).await?;
    ensure!(
        budget.control_wait("manage").is_none(),
        "insufficient lifetime budget for manager and independent goal verification"
    );
    // The retained failure changes the immutable frontier, yielding new control
    // identities. The original failed finish stays unaccepted and untouched.
    let repair = json!({"operator_finish_recovery":request.request_id,
        "reconciled_failure_id":request.failure_id,"operator_evidence":request.evidence,
        "original_failure":captured,"failed_finish":request.failed_finish,
        "operator_attestation":request.operator_attestation,"finish_recovery_authority":AUTHORITY,
        "external_effect_requires_reconciliation":false,
        "instruction":"The configured administrator attests that retained tool commands were independently audited as read-only with no unknown effects, and the provider cause was repaired. Hash-only telemetry alone does not prove those claims. Preserve the failed finish, raw result, nullable completed_at, all failures, accepted work, artifacts and lifetime usage. This audit grants neither Goal.success approval nor business replay. Reassess the current frozen frontier through the existing manager. Reuse accepted work unchanged; propose complete without failure_id when justified. A new independently assigned final verifier must cover every unchanged Goal.success criterion with current accepted proof paths and hashes before completion. Do not replan the business DAG, repeat the failed card, replenish targets or create business effects."});
    sqlx::query(
        "INSERT INTO campaign_graph_reconciliations(failure_id,campaign_id,expected_revision,evidence)
         VALUES($1,$2,$3,$4)",
    )
    .bind(&request.failure_id)
    .bind(id)
    .bind(request.expected_revision)
    .bind(&request.evidence)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
         VALUES($1,$2,$3,$1,'plan',$4)",
    )
    .bind(&recovery_id)
    .bind(id)
    .bind(captured["task_key"].as_str())
    .bind(repair)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE campaign_execution_specs SET hold_reason=NULL WHERE campaign_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let updated = campaigns::replace_on_tx(
        &mut tx,
        id,
        campaign.0.revision,
        serde_json::from_value(serde_json::to_value(&campaign.0)?)?,
    )
    .await?;
    let unchanged: Value = state.get("spec");
    sqlx::query(
        "INSERT INTO campaign_goal_revisions(campaign_id,request_id,request_digest,campaign_revision,
         previous_goal,revised_goal,evidence) VALUES($1,$2,$3,$4,$5,$5,$6)",
    )
    .bind(id)
    .bind(&request.request_id)
    .bind(digest)
    .bind(updated.revision)
    .bind(unchanged)
    .bind(&request.evidence)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Recovered {
        revision: updated.revision,
        recovery_id,
        independent_goal_validation_required: true,
        business_contracts_preserved: true,
    })
}

pub(crate) fn validate_history_request(
    campaign: &str,
    request: &Recovery,
    request_digest: &str,
    recovery_id: &str,
) -> Result<()> {
    audit::request_shape(request)?;
    ensure!(
        contract_digest(&("operator-finish-recovery", request))? == request_digest
            && contract_digest(&(campaign, "operator-finish-recovery", &request.request_id))?
                == recovery_id,
        "native finish recovery request digest mismatch"
    );
    Ok(())
}
