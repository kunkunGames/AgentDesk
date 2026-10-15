//! An operator audit can resolve a proven unattempted effect, never an unknown attempt.
use super::super::super::{digest, result};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An explicit operator attestation, bound to the exact retained native result.
/// The audit hash identifies independently reviewable command/effect evidence;
/// it is not an agent label, a success receipt or permission to replay a card.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnattemptedEffect {
    pub dispatch_id: String,
    pub result_sha256: String,
    pub audit_sha256: String,
}

fn sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn validate(
    audit: &UnattemptedEffect,
    dispatch: &str,
    original: &Value,
    raw: &str,
    captured: &Value,
) -> Result<()> {
    ensure!(
        !audit.dispatch_id.is_empty()
            && audit.dispatch_id.len() <= 128
            && audit.dispatch_id == dispatch,
        "operator effect audit must bind the latest captured dispatch"
    );
    ensure!(
        sha256(&audit.result_sha256)
            && audit.result_sha256 == digest(raw.as_bytes())
            && sha256(&audit.audit_sha256),
        "operator effect audit requires exact native result and audit SHA256 pins"
    );
    ensure!(
        original["external_effect_requires_reconciliation"] == true
            && original["failure"]["category"] == "plan",
        "only an external planning rejection can receive an unattempted-effect audit"
    );
    result::validate_dispatch(captured)?;
    ensure!(
        matches!(
            captured["completion_source"].as_str(),
            Some("campaign_native_receipt" | "watcher_streaming_final")
        ) && captured["campaign_receipt"]["outcome"] == "blocked"
            && captured["campaign_receipt"]["failure_category"] == "plan",
        "a retained native blocked planning receipt is required; missing or uncertain execution cannot be attested away"
    );
    Ok(())
}
