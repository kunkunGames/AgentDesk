//! Runtime completion metadata is separate from the strict provider envelope.
use anyhow::{Result, ensure};
use serde_json::Value;

pub const CAPTURE_SUMMARY: &str =
    "Native Campaign result captured; independent graph checks pending";

/// Validate a persisted dispatch without allowing runtime fields in provider files.
/// Recognized capture metadata carries no graph or business acceptance authority.
pub fn validate_dispatch(value: &Value) -> Result<()> {
    let mut envelope = value.clone();
    if ["summary", "work_outcome", "completion_source"]
        .iter()
        .any(|key| !value[*key].is_null())
    {
        ensure!(
            value["summary"] == CAPTURE_SUMMARY && value["work_outcome"] == "artifact",
            "unexpected native capture metadata"
        );
        ensure!(
            value["completion_source"].is_null()
                || matches!(
                    value["completion_source"].as_str(),
                    Some("campaign_native_receipt" | "watcher_streaming_final")
                ),
            "unrecognized native completion source"
        );
        let object = envelope
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("result must be an object"))?;
        for key in ["summary", "work_outcome", "completion_source"] {
            object.remove(key);
        }
    }
    super::receipt::validate_envelope(&envelope)?;
    super::schema::validate_result(&envelope)
}

pub(super) fn captured_schema_failure(error: &Value) -> bool {
    error.as_str().is_some_and(|error| {
        ["completion_source", "summary", "work_outcome"]
            .iter()
            .any(|key| error.starts_with(&format!("unknown field `{key}`, expected ")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_metadata_is_validated_separately_and_cannot_relax_provider_schema() {
        let native = json!({"campaign_receipt":{"outcome":"passed","summary":"checked",
            "observations":[{"command":"inspect","exit_code":0,"evidence":"observed"}]}});
        validate_dispatch(&native).unwrap();
        for source in [
            None,
            Some("campaign_native_receipt"),
            Some("watcher_streaming_final"),
        ] {
            let mut captured = native.clone();
            captured["summary"] = json!(CAPTURE_SUMMARY);
            captured["work_outcome"] = json!("artifact");
            if let Some(source) = source {
                captured["completion_source"] = json!(source);
            }
            validate_dispatch(&captured).unwrap();
            assert!(super::super::receipt::validate_envelope(&captured).is_err());
            for (key, forged) in [
                ("status", json!("completed")),
                ("summary", json!("business passed")),
                ("work_outcome", json!("passed")),
                ("completion_source", json!("operator_override")),
            ] {
                let mut wrong = captured.clone();
                wrong[key] = forged;
                assert!(validate_dispatch(&wrong).is_err(), "{key}");
            }
        }
        let mut wrong = native;
        wrong["graph_validation"] = json!({"digest":"d","approved":"true","reason":"checked"});
        assert!(validate_dispatch(&wrong).is_err());
    }
}
