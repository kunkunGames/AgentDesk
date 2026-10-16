//! An audited operator repair returns design to the existing planner, never to a retry loop.
mod cancelled_validation;
mod candidate;
mod control;
mod effect;
mod guards;
pub(crate) mod settled_effect;
use super::super::{Budget, Goal, contract_digest, revision::budget};
use crate::db::campaigns::{self, Campaign, CampaignStatus};
use anyhow::{Result, ensure};
pub use cancelled_validation::CancelledValidation;
pub(crate) use control::settle_superseded_unstarted_planners;
pub use effect::UnattemptedEffect;
pub(crate) use effect::validate as validate_unattempted_effect;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
pub use settled_effect::SettledExternalEffect;
use sqlx::{PgPool, Row, types::Json};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Replan {
    pub expected_revision: i64,
    pub request_id: String,
    pub failure_id: String,
    pub evidence: String,
    pub budget: Option<Budget>,
    pub supersede_candidate: Option<UnpublishedCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unattempted_effect: Option<UnattemptedEffect>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_effect: Option<SettledExternalEffect>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_validation: Option<RejectedValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_validation: Option<CancelledValidation>,
}

/// Pins an operator-observed rejected QA result to this planning request.
/// The raw hash seals the current snapshot, not unrecorded historical bytes.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RejectedValidation {
    pub dispatch_id: String,
    pub graph_version: i64,
    pub graph_digest: String,
    pub raw_result_sha256: String,
    pub contract_digest: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnpublishedCandidate {
    pub version: i64,
    pub digest: String,
}

#[derive(Debug, Serialize)]
pub struct PlannedRepair {
    pub revision: i64,
    pub repair_failure_id: String,
    pub planner_agent_id: String,
    pub independent_validation_required: bool,
    pub goal_scope_preserved: bool,
}

pub async fn request(pool: &PgPool, id: &str, request: &Replan) -> Result<PlannedRepair> {
    request_on_admin_boundary(pool, id, request, false).await
}

/// Called only after the HTTP boundary verifies a configured admin Bearer.
/// This is shared-administrator authority, not separate provider isolation.
pub(crate) async fn request_as_server_admin(
    pool: &PgPool,
    id: &str,
    request: &Replan,
) -> Result<PlannedRepair> {
    request_on_admin_boundary(pool, id, request, true).await
}

async fn request_on_admin_boundary(
    pool: &PgPool,
    id: &str,
    request: &Replan,
    server_admin: bool,
) -> Result<PlannedRepair> {
    ensure!(
        request.unattempted_effect.is_none() || request.settled_effect.is_none(),
        "unattempted and settled effect audits are mutually exclusive"
    );
    ensure!(
        request.rejected_validation.is_none()
            || (request.unattempted_effect.is_none()
                && request.settled_effect.is_none()
                && request.cancelled_validation.is_none()),
        "rejected validation cannot claim business effect resolution"
    );
    ensure!(
        request.cancelled_validation.is_none()
            || (request.unattempted_effect.is_none() && request.settled_effect.is_none()),
        "cancelled validation cannot claim business effect resolution"
    );
    ensure!(
        request.settled_effect.is_none() || server_admin,
        "settled effects require configured server administrator authentication"
    );
    ensure!(
        !request.request_id.trim().is_empty() && request.request_id.len() <= 128,
        "bounded request ID required"
    );
    ensure!(
        !request.evidence.trim().is_empty() && request.evidence.len() <= 8000,
        "bounded independently verifiable reconciliation evidence required"
    );
    ensure!(
        !request.failure_id.is_empty() && request.failure_id.len() <= 128,
        "bounded captured failure ID required"
    );
    ensure!(
        crate::services::settings::internal_tasks_enabled(pool)
            .await
            .map_err(anyhow::Error::msg)?,
        "internal task admission disabled"
    );
    let digest = contract_digest(&("operator-replan", request))?;
    let repair_id = contract_digest(&(id, "operator-replan", &request.request_id))?;
    let mut tx = pool.begin().await?;
    let c: Json<Campaign> =
        sqlx::query_scalar("SELECT document FROM campaigns WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let state = sqlx::query("SELECT spec,active_version,hold_reason FROM campaign_execution_specs WHERE campaign_id=$1 FOR UPDATE")
        .bind(id).fetch_one(&mut *tx).await?;
    let previous: Value = state.get("spec");
    let mut goal: Goal = serde_json::from_value(previous.clone())?;
    if let Some(prior) = sqlx::query("SELECT request_digest,campaign_revision FROM campaign_goal_revisions WHERE campaign_id=$1 AND request_id=$2")
        .bind(id).bind(&request.request_id).fetch_optional(&mut *tx).await? {
        ensure!(prior.get::<String,_>("request_digest") == digest, "request ID already used with different content");
        if let Some(audit) = &request.settled_effect {
            let proof = sqlx::query("SELECT original.task_key,original.evidence FROM campaign_graph_failures original
                JOIN campaign_graph_failures repair ON repair.campaign_id=original.campaign_id
                    AND repair.evidence->>'reconciled_failure_id'=original.id
                WHERE original.id=$1 AND original.campaign_id=$2 AND repair.id=$3
                    AND repair.evidence->>'settled_effect_authority'=$4")
                .bind(&request.failure_id).bind(id).bind(&repair_id).bind(settled_effect::AUTHORITY)
                .fetch_one(&mut *tx).await?;
            settled_effect::authenticated_request_proof(&mut tx, id, &proof.get::<String,_>("task_key"),
                &goal, &proof.get::<Value,_>("evidence"), audit, false).await?;
        }
        return Ok(PlannedRepair { revision: prior.get("campaign_revision"), repair_failure_id: repair_id,
            planner_agent_id: goal.planner, independent_validation_required: true, goal_scope_preserved: true });
    }
    ensure!(
        c.0.status == CampaignStatus::Paused && c.0.revision == request.expected_revision,
        "pause the campaign at the expected revision first"
    );
    let failure = sqlx::query("SELECT f.task_key,f.category,f.evidence,t.role,t.version,t.accepted,t.contract_digest FROM campaign_graph_failures f
        JOIN campaign_graph_tasks t ON t.task_key=f.task_key WHERE f.id=$1 AND f.campaign_id=$2")
        .bind(&request.failure_id).bind(id).fetch_one(&mut *tx).await?;
    let original: Value = failure.get("evidence");
    let execution_failure = guards::admission(
        state.get::<Option<String>, _>("hold_reason").as_deref(),
        &failure,
    )?;
    ensure!(
        (original["external_effect_requires_reconciliation"] == false
            && request.unattempted_effect.is_none()
            && request.settled_effect.is_none())
            || (original["external_effect_requires_reconciliation"] == true
                && (request.unattempted_effect.is_some() || request.settled_effect.is_some())),
        "unknown external effects require an explicit dispatch-pinned operator audit; artifact failures cannot claim effect resolution"
    );
    if request.settled_effect.is_some()
        || request.rejected_validation.is_some()
        || request.cancelled_validation.is_some()
    {
        settled_effect::require_no_campaign_owners(&mut tx, id).await?;
    }
    guards::settled(&mut tx, id, &request.failure_id, execution_failure).await?;
    guards::failure_scope(
        &mut tx,
        id,
        guards::FailureScope {
            active: state.get("active_version"),
            failure: &failure,
            evidence: &original,
            unattempted_effect: request.unattempted_effect.as_ref(),
            settled_effect: request.settled_effect.as_ref(),
            goal: &goal,
            rejected_validation: request.rejected_validation.as_ref(),
            cancelled_validation: request.cancelled_validation.as_ref(),
        },
    )
    .await?;
    candidate::retire(
        &mut tx,
        id,
        state.get("active_version"),
        &failure,
        request.supersede_candidate.as_ref(),
    )
    .await?;
    if let Some(expanded) = &request.budget {
        budget::validate_increase(&goal.budget, expanded)?;
        super::super::schedule::budget::validate_revision(&mut tx, id, expanded).await?;
        goal.budget = expanded.clone();
    }
    goal.validate()?;
    guards::capacity(&mut tx, pool, id, &goal).await?;
    let repair = json!({"operator_replan":request.request_id,"reconciled_failure_id":request.failure_id,
        "operator_evidence":request.evidence,"original_failure":original,
        "unattempted_effect":request.unattempted_effect,
        "settled_effect":request.settled_effect,
        "settled_effect_authority":request.settled_effect.as_ref().map(|_| settled_effect::AUTHORITY),
        "rejected_validation":request.rejected_validation,
        "cancelled_validation":request.cancelled_validation,
        "superseded_candidate":request.supersede_candidate,
        "external_effect_requires_reconciliation":false,
        "instruction":"The operator requests an agent-authored correction, not approval or replay. Independently verify the operator evidence. The existing planner must repair only the failed subgraph against the unchanged objective, success conditions and constraints. Preserve accepted/in-flight contracts, actual artifacts, original failures and lifetime usage. Use new task keys for changed failed contracts. The corrected DAG requires independent QA before any new work or external effect. A settled_effect audit is administrator-attested current state, never acceptance or a historical command success. Bind its audit_sha256 in settled_effect_gates to the graph-local node.id of new read-only Inspect and independent Verify nodes; inspection and verification must never contain task_key values; every new external effect must follow that target QA. Inspect every source-defined configured target and ownership, and recheck exact expected/current target preconditions immediately before each remaining mutation; matching targets invoke no mutator. Retain missing historical child exits as missing."});
    sqlx::query("INSERT INTO campaign_graph_reconciliations(failure_id,campaign_id,expected_revision,evidence)
        VALUES($1,$2,$3,$4)").bind(&request.failure_id).bind(id).bind(request.expected_revision)
        .bind(&request.evidence).execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO campaign_graph_failures(id,campaign_id,task_key,fingerprint,category,evidence)
        VALUES($1,$2,$3,$1,'plan',$4)",
    )
    .bind(&repair_id)
    .bind(id)
    .bind(failure.get::<String, _>("task_key"))
    .bind(repair)
    .execute(&mut *tx)
    .await?;
    if failure.get::<String, _>("role") == "validate" {
        // A failed pre-business validation is not approval. Its graph and raw attempt remain.
        sqlx::query("UPDATE campaign_graph_versions SET status='rejected' WHERE campaign_id=$1 AND version=$2 AND status='candidate'")
            .bind(id).bind(failure.get::<Option<i64>,_>("version")).execute(&mut *tx).await?;
    }
    sqlx::query(
        "UPDATE campaign_execution_specs SET spec=$2,hold_reason=NULL WHERE campaign_id=$1",
    )
    .bind(id)
    .bind(serde_json::to_value(&goal)?)
    .execute(&mut *tx)
    .await?;
    let updated = campaigns::replace_on_tx(
        &mut tx,
        id,
        c.0.revision,
        serde_json::from_value(serde_json::to_value(&c.0)?)?,
    )
    .await?;
    sqlx::query("INSERT INTO campaign_goal_revisions(campaign_id,request_id,request_digest,campaign_revision,previous_goal,revised_goal,evidence)
        VALUES($1,$2,$3,$4,$5,$6,$7)").bind(id).bind(&request.request_id).bind(digest)
        .bind(updated.revision).bind(previous).bind(serde_json::to_value(&goal)?).bind(&request.evidence)
        .execute(&mut *tx).await?;
    control::settle_on_tx(&mut tx, id).await?;
    tx.commit().await?;
    Ok(PlannedRepair {
        revision: updated.revision,
        repair_failure_id: repair_id,
        planner_agent_id: goal.planner,
        independent_validation_required: true,
        goal_scope_preserved: true,
    })
}
