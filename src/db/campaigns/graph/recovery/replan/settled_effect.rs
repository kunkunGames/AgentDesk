//! A settled partial execution is neither an unattempted effect nor acceptance.
//!
//! The server validates integrity, retained execution identity and ownership.
//! Source-defined target coverage/current state are an explicit administrator
//! attestation. The shared admin credential does not isolate a provider from
//! an operator; fresh independent target QA remains mandatory before effects.
mod durable;
mod gates;

use super::super::super::{Goal, contract_digest, digest, learning, result};
use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub(crate) use durable::{authenticated_request_proof, released_proof, require_no_campaign_owners};
pub(crate) use gates::validate_graph;

const MAX_AUDIT_BYTES: usize = 32 * 1024;
const MAX_ADMISSION_AGE_SECS: i64 = 3600;
pub(crate) const AUTHORITY: &str = "configured_server_admin_bearer";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SettledExternalEffect {
    pub dispatch_id: String,
    pub result_sha256: String,
    pub audit_sha256: String,
    pub operator_attestation: OperatorAttestation,
    pub bundle: SettledEffectBundle,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAttestation {
    pub complete_target_coverage: bool,
    pub current_state_verified: bool,
    pub no_unknown_effects: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SettledEffectBundle {
    pub schema_version: u8,
    pub campaign_id: String,
    pub task_key: String,
    pub contract_digest: String,
    pub goal_scope_sha256: String,
    pub source_sha256: String,
    pub configuration_sha256: String,
    /// Canonical digest of every ordered (target ID, expected state digest).
    /// Completeness against the source/configuration is administrator-attested,
    /// not inferred from comparing two caller-provided lists.
    pub target_set_sha256: String,
    pub configured_targets: Vec<ConfiguredTarget>,
    pub observed_at: String,
    pub targets: Vec<SettledTarget>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredTarget {
    pub id: String,
    pub expected_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SettledTarget {
    pub id: String,
    pub state: TargetState,
    /// Each state digest covers source-defined content, links and modes.
    pub expected_sha256: String,
    pub current_sha256: String,
    /// Retained attributable before/after/command evidence; a missing historical
    /// child exit remains missing and must never be represented as success.
    pub prior_evidence_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TargetState {
    Converged,
    PendingDelta,
}

fn sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) struct ValidationContext<'a> {
    pub campaign: &'a str,
    pub task: &'a str,
    pub contract: &'a str,
    pub goal: &'a Goal,
    pub dispatch: &'a str,
    pub original: &'a Value,
    pub raw: &'a str,
    pub captured: &'a Value,
    pub fresh_admission: bool,
}

pub(crate) fn validate(
    audit: &SettledExternalEffect,
    context: ValidationContext<'_>,
) -> Result<()> {
    let ValidationContext {
        campaign,
        task,
        contract,
        goal,
        dispatch,
        original,
        raw,
        captured,
        fresh_admission,
    } = context;
    ensure!(
        !audit.dispatch_id.is_empty()
            && audit.dispatch_id.len() <= 128
            && audit.dispatch_id == dispatch
            && original["dispatch_id"] == dispatch
            && original["task_key"] == task,
        "settled effect audit must bind the latest captured task dispatch"
    );
    ensure!(
        sha256(&audit.result_sha256)
            && audit.result_sha256 == digest(raw.as_bytes())
            && sha256(&audit.audit_sha256)
            && audit.audit_sha256 == contract_digest(&audit.bundle)?,
        "settled effect audit requires exact retained native result and bundle SHA256 pins"
    );
    ensure!(
        audit.operator_attestation.complete_target_coverage
            && audit.operator_attestation.current_state_verified
            && audit.operator_attestation.no_unknown_effects,
        "administrator must attest complete source-defined coverage and determinate current target states"
    );
    let bundle = &audit.bundle;
    ensure!(
        serde_json::to_vec(bundle)?.len() <= MAX_AUDIT_BYTES
            && bundle.schema_version == 1
            && bundle.campaign_id == campaign
            && bundle.task_key == task
            && bundle.contract_digest == contract
            && sha256(&bundle.contract_digest)
            && bundle.goal_scope_sha256 == learning::scope_digest(goal)?
            && sha256(&bundle.source_sha256)
            && sha256(&bundle.configuration_sha256),
        "settled target bundle must preserve the exact campaign, task contract, goal scope and source/configuration identities"
    );
    let observed = DateTime::parse_from_rfc3339(&bundle.observed_at)?.with_timezone(&Utc);
    let age = Utc::now().signed_duration_since(observed).num_seconds();
    ensure!(age >= -60, "settled target observation is in the future");
    if fresh_admission {
        ensure!(
            age <= MAX_ADMISSION_AGE_SECS,
            "settled target observation is stale"
        );
    }
    // Once admitted, expiry alone must not pin historical effects again after
    // slow planning. New effects are gated by a fresh Inspect/Verify graph.
    ensure!(
        (1..=64).contains(&bundle.configured_targets.len())
            && bundle.configured_targets.len() == bundle.targets.len(),
        "bounded complete target set required"
    );
    let mut configured = std::collections::BTreeMap::new();
    for target in &bundle.configured_targets {
        ensure!(
            !target.id.trim().is_empty()
                && target.id.len() <= 240
                && !target.id.contains(['\0', '\n', '\r'])
                && sha256(&target.expected_sha256)
                && configured
                    .insert(&target.id, &target.expected_sha256)
                    .is_none(),
            "configured source manifest requires unique target identities and expected-state pins"
        );
    }
    let mut ids = BTreeSet::new();
    for target in &bundle.targets {
        ensure!(
            !target.id.trim().is_empty()
                && target.id.len() <= 240
                && !target.id.contains(['\0', '\n', '\r'])
                && ids.insert(&target.id)
                && sha256(&target.expected_sha256)
                && sha256(&target.current_sha256)
                && sha256(&target.prior_evidence_sha256),
            "each configured target requires unique identity and exact current, expected and retained evidence hashes"
        );
        ensure!(
            configured.get(&target.id).copied() == Some(&target.expected_sha256),
            "current settled evidence must cover the exact configured target manifest"
        );
        ensure!(
            (target.state == TargetState::Converged)
                == (target.expected_sha256 == target.current_sha256),
            "target state must distinguish convergence from a remaining delta"
        );
    }
    ensure!(
        sha256(&bundle.target_set_sha256)
            && bundle.target_set_sha256 == contract_digest(&bundle.configured_targets)?,
        "settled target universe digest does not match the complete ordered target manifest"
    );
    ensure!(
        original["external_effect_requires_reconciliation"] == true
            && original["failure"]["category"] == "code",
        "only a retained blocked code execution can receive a settled partial-effect audit"
    );
    result::validate_dispatch(captured)?;
    ensure!(
        matches!(
            captured["completion_source"].as_str(),
            Some("campaign_native_receipt" | "watcher_streaming_final")
        ) && captured["campaign_receipt"]["outcome"] == "blocked"
            && captured["campaign_receipt"]["failure_category"] == "code",
        "settled effects require a retained native blocked/code receipt; unknown execution cannot be attested into success"
    );
    Ok(())
}
