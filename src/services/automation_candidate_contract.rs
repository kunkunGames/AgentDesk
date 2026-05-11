use serde_json::Value;

pub const PIPELINE_STAGE_ID: &str = "automation-candidate";
pub const MARKER_METADATA_KEY: &str = "automation_candidate";
pub const MARKER_ENABLED_KEY: &str = "enabled";
pub const MARKER_LOOP_ENABLED_KEY: &str = "loop_enabled";
pub const PROGRAM_METADATA_KEY: &str = "program";
pub const PROGRAM_REPO_DIR_KEY: &str = "repo_dir";
pub const PROGRAM_ALLOWED_WRITE_PATHS_KEY: &str = "allowed_write_paths";
pub const PROGRAM_METRIC_NAME_KEY: &str = "metric_name";
pub const PROGRAM_METRIC_TARGET_KEY: &str = "metric_target";
pub const PROGRAM_METRIC_DIRECTION_KEY: &str = "metric_direction";
pub const PROGRAM_FINAL_GATE_KEY: &str = "final_gate";
pub const PROGRAM_ITERATION_BUDGET_KEY: &str = "iteration_budget";
pub const PROGRAM_CURRENT_ITERATION_KEY: &str = "current_iteration";
pub const PROGRAM_DESCRIPTION_KEY: &str = "description";

pub const REQUIRED_PROGRAM_FIELDS: [&str; 4] = [
    PROGRAM_REPO_DIR_KEY,
    PROGRAM_ALLOWED_WRITE_PATHS_KEY,
    PROGRAM_METRIC_NAME_KEY,
    PROGRAM_METRIC_TARGET_KEY,
];

#[derive(Debug, Clone, serde::Serialize)]
pub struct AutomationCandidateDiscriminator {
    pub pipeline_stage_id: &'static str,
    pub metadata_enabled_path: &'static str,
    pub metadata_loop_enabled_path: &'static str,
    pub required_program_fields: Vec<&'static str>,
}

pub fn discriminator() -> AutomationCandidateDiscriminator {
    AutomationCandidateDiscriminator {
        pipeline_stage_id: PIPELINE_STAGE_ID,
        metadata_enabled_path: "metadata.automation_candidate.enabled",
        metadata_loop_enabled_path: "metadata.automation_candidate.loop_enabled",
        required_program_fields: REQUIRED_PROGRAM_FIELDS.to_vec(),
    }
}

pub fn has_complete_loop_contract(metadata: &Value) -> bool {
    marker_bool(metadata, MARKER_ENABLED_KEY)
        && marker_bool(metadata, MARKER_LOOP_ENABLED_KEY)
        && program_non_empty_string(metadata, PROGRAM_REPO_DIR_KEY)
        && program_non_empty_string(metadata, PROGRAM_METRIC_NAME_KEY)
        && program_has_metric_target(metadata)
        && program_has_allowed_write_paths(metadata)
}

fn marker_bool(metadata: &Value, key: &str) -> bool {
    metadata
        .get(MARKER_METADATA_KEY)
        .and_then(|value| value.get(key))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn program<'a>(metadata: &'a Value) -> Option<&'a Value> {
    metadata.get(PROGRAM_METADATA_KEY)
}

fn program_non_empty_string(metadata: &Value, key: &str) -> bool {
    program(metadata)
        .and_then(|value| value.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
}

fn program_has_allowed_write_paths(metadata: &Value) -> bool {
    program(metadata)
        .and_then(|value| value.get(PROGRAM_ALLOWED_WRITE_PATHS_KEY))
        .and_then(Value::as_array)
        .is_some_and(|paths| {
            !paths.is_empty()
                && paths.iter().all(|path| {
                    path.as_str()
                        .map(str::trim)
                        .is_some_and(|value| !value.is_empty())
                })
        })
}

fn program_has_metric_target(metadata: &Value) -> bool {
    program(metadata)
        .and_then(|value| value.get(PROGRAM_METRIC_TARGET_KEY))
        .is_some_and(|value| match value {
            Value::Number(number) => number.as_f64().is_some_and(f64::is_finite),
            _ => !value.is_null(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_metadata() -> Value {
        serde_json::json!({
            "automation_candidate": {
                "enabled": true,
                "loop_enabled": true
            },
            "program": {
                "repo_dir": "/repo",
                "allowed_write_paths": ["src/services"],
                "metric_name": "failure_count",
                "metric_target": 0
            }
        })
    }

    #[test]
    fn complete_loop_contract_requires_marker_and_program() {
        assert!(has_complete_loop_contract(&valid_metadata()));

        let mut missing_marker = valid_metadata();
        missing_marker["automation_candidate"]["loop_enabled"] = serde_json::Value::Bool(false);
        assert!(!has_complete_loop_contract(&missing_marker));

        let mut missing_program = valid_metadata();
        missing_program["program"]["repo_dir"] = serde_json::Value::String(String::new());
        assert!(!has_complete_loop_contract(&missing_program));

        let mut empty_paths = valid_metadata();
        empty_paths["program"]["allowed_write_paths"] = serde_json::json!([]);
        assert!(!has_complete_loop_contract(&empty_paths));
    }
}
