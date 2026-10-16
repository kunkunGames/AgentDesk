//! Shared provider envelope contract for prompts and native receipt ingestion.
//! Extra explanations belong inside the receipt, never beside authority fields.
use anyhow::{Result, ensure};
use serde_json::Value;

pub const RESULT_KEYS: &[&str] = &[
    "campaign_receipt",
    "campaign_graph",
    "graph_validation",
    "dag_template_review",
    "workflow_decision",
    "goal_validation",
];
pub const MAX_OBSERVATIONS: usize = 32;
pub const MAX_COMMAND_BYTES: usize = 2000;
pub const MAX_EVIDENCE_BYTES: usize = 4000;
/// Complete provider file, including its DAG and receipt; declared artifacts
/// have separate handoff limits.
pub const MAX_RESULT_FILE_BYTES: u64 = 256 * 1024;

pub fn file_limits() -> Value {
    serde_json::json!({"max_file_bytes": MAX_RESULT_FILE_BYTES, "encoding": "UTF-8"})
}

pub fn redact(value: &str) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)(bearer\s+|(?:token|password|api[_-]?key|secret)\s*[:=]\s*)[^\s,;]+",
        )
        .expect("fixed receipt redaction regex") // agentdesk-audit: allow-unwrap -- fixed literal; confidentiality regression exercises initialization.
    });
    re.replace_all(
        &value.chars().take(MAX_EVIDENCE_BYTES).collect::<String>(),
        "[redacted]",
    )
    .into_owned()
}

/// Only bounded, redacted business evidence enters the template corpus.
/// Full provider transcripts and configuration remain with their owners.
pub fn sample_evidence(result: &Value) -> Value {
    let receipt = &result["campaign_receipt"];
    let observations: Vec<Value> = receipt["observations"]
        .as_array()
        .into_iter()
        .flatten()
        .take(MAX_OBSERVATIONS)
        .map(|o| {
            serde_json::json!({"command":redact(o["command"].as_str().unwrap_or("")),
            "exit_code":o["exit_code"],"evidence":redact(o["evidence"].as_str().unwrap_or(""))})
        })
        .collect();
    serde_json::json!({"outcome":receipt["outcome"],"summary":redact(receipt["summary"].as_str().unwrap_or("")),"observations":observations})
}

pub fn validate_envelope(proposed: &Value) -> Result<()> {
    let object = proposed
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("result must be an object"))?;
    ensure!(
        object.keys().all(|k| RESULT_KEYS.contains(&k.as_str())),
        "result contains reserved completion fields; only {} are allowed at the top level",
        RESULT_KEYS.join(", ")
    );
    ensure!(
        proposed["campaign_receipt"].is_object(),
        "missing Campaign receipt"
    );
    Ok(())
}

pub fn instructions() -> String {
    format!(
        "Completion JSON contract: the only permitted top-level keys are {}. Do not add status, summary, work_outcome, verification_scope or any other top-level field. Put scope and limitations inside campaign_receipt.summary or observations[].evidence. The complete .agentdesk-campaign-result.json file, including campaign_graph and campaign_receipt, must be at most {MAX_RESULT_FILE_BYTES} UTF-8 bytes on disk. This is separate from declared artifact limits. Write compact JSON, check the actual file byte size, parse it and assert the allowed keys before ending the turn. Programmatically retain immutable node contracts; keep NEW instructions concise and put detailed evidence in declared artifacts. If a diagnostic graph cannot fit, omit that unaccepted graph from a bounded blocked receipt; never omit pinned nodes from a graph or raise the limit. Successful passed/no_work receipts need 1..={MAX_OBSERVATIONS} observations, each with a nonempty command (at most {MAX_COMMAND_BYTES} UTF-8 bytes), integer exit_code=0 and nonempty evidence (at most {MAX_EVIDENCE_BYTES} UTF-8 bytes). Preserve failures truthfully; use blocked for unresolved errors. Keep long commands and raw evidence in declared artifacts and reference the exact artifact command there. No result wrapper. End the turn after writing and checking the receipt so the native completion owner can collect it.\n",
        RESULT_KEYS.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn envelope_rejects_authority_and_unadvertised_scope_but_preserves_nested_evidence() {
        let mut result = json!({"campaign_receipt":{"summary":"checked","verification_scope":"planning only"},"graph_validation":{"approved":true}});
        validate_envelope(&result).unwrap();
        for key in ["status", "work_outcome", "verification_scope", "summary"] {
            result[key] = json!("not permitted");
            assert!(validate_envelope(&result).is_err(), "{key}");
            result.as_object_mut().unwrap().remove(key);
        }
        assert!(validate_envelope(&json!({"campaign_graph":{}})).is_err());
        assert!(validate_envelope(&json!([])).is_err());
    }

    #[test]
    fn sample_evidence_redacts_credentials_and_preserves_business_outcome() {
        let sampled = sample_evidence(&json!({"campaign_receipt": {
            "outcome": "passed",
            "summary": "verified token=fixture-value",
            "observations": [{"command": "check password:fixture-password",
                "exit_code": 0, "evidence": "Bearer fixture-credential; api_key=fixture-key"}]
        }}));
        assert_eq!(sampled["outcome"], "passed");
        assert_eq!(sampled["observations"][0]["exit_code"], 0);
        let serialized = sampled.to_string();
        for credential in [
            "fixture-value",
            "fixture-password",
            "fixture-credential",
            "fixture-key",
        ] {
            assert!(!serialized.contains(credential));
        }
        assert!(serialized.contains("[redacted]"));
    }
}
