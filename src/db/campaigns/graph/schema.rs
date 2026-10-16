//! Formal schema is generated from the same types used to parse native DAGs.
//! Semantic/byte-size checks remain in Graph::validate and receipt ingestion.
use super::{
    Graph,
    learning::template::{Review, SCHEMA_VERSION},
};
use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// Keep semantic guidance outside document(): historical execution profiles pin
// the entire generated schema, including descriptions, for audited recovery.
pub const GRAPH_REFERENCE_RULES: &str = "Graph reference contract: settled_effect_gates[].inspection and .verification must name distinct existing graph-local node.id (Task.id) values, never persistent task_key values. When recovering an authenticated settled-effect audit, bind its audit_sha256 (64 lowercase hex characters) to a read-only Inspect root and directly dependent independent Verify node. Preserve existing committed gates and their task contracts. Introducing new external-effect contracts under that recovery requires fresh unpublished inspection and verification, and those effects must depend on that target QA. Without an authenticated settled-effect audit, do not invent gates or audit hashes.";

#[derive(JsonSchema, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Passed,
    Blocked,
    NoWork,
}
#[derive(JsonSchema, Deserialize, Serialize)]
struct Observation {
    command: String,
    exit_code: i64,
    evidence: String,
}
#[derive(JsonSchema, Deserialize, Serialize)]
struct Receipt {
    outcome: Outcome,
    summary: String,
    #[serde(default)]
    observations: Vec<Observation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_category: Option<String>,
}
#[derive(JsonSchema, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GraphValidation {
    digest: String,
    approved: bool,
    reason: String,
}
#[derive(JsonSchema, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeResult {
    campaign_receipt: Receipt,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    campaign_graph: Option<Graph>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    graph_validation: Option<GraphValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dag_template_review: Option<Review>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workflow_decision: Option<super::workflow::Decision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    goal_validation: Option<super::workflow::GoalValidation>,
}
pub fn document() -> Result<Value> {
    let mut graph = serde_json::to_value(schemars::schema_for!(Graph))?;
    graph["$id"] = json!("urn:agentdesk:campaign:graph:1");
    let mut result = serde_json::to_value(schemars::schema_for!(NativeResult))?;
    result["$id"] = json!("urn:agentdesk:campaign:result:1");
    Ok(
        json!({"schema_version":SCHEMA_VERSION,"graph":graph,"result":result,"result_limits":super::receipt::file_limits(),
        "semantic_validation":"Graph::validate, pinned contract checks and independent artifact verification are required; schema compliance is not execution success."}),
    )
}
pub fn graph_prompt() -> Result<String> {
    Ok(format!(
        "DAG JSON Schema v{SCHEMA_VERSION} (generated from the native Rust contract):\n{}\nAll semantic graph and artifact rules still apply.",
        serde_json::to_string(&document()?["graph"])?
    ))
}
/// Parse authority fields with the exact types that generate the published
/// schema. Additional receipt evidence stays with its existing receipt owner.
pub fn validate_result(value: &Value) -> Result<()> {
    serde_json::from_value::<NativeResult>(value.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_schema_exposes_strict_native_fields_and_versioned_contracts() {
        let schema = document().unwrap();
        assert_eq!(schema["schema_version"], 1);
        assert_eq!(schema["graph"]["additionalProperties"], false);
        assert_eq!(schema["graph"]["properties"]["nodes"]["maxItems"], 32);
        assert_eq!(
            schema["graph"]["$defs"]["Task"]["additionalProperties"],
            false
        );
        assert_eq!(
            schema["graph"]["$defs"]["Task"]["properties"]["outputs"]["maxItems"],
            super::super::MAX_TASK_OUTPUTS
        );
        assert_eq!(schema["result"]["additionalProperties"], false);
        assert_eq!(
            schema["result_limits"]["max_file_bytes"],
            super::super::receipt::MAX_RESULT_FILE_BYTES
        );
        assert_eq!(schema["result_limits"]["encoding"], "UTF-8");
        let properties = schema["result"]["properties"].as_object().unwrap();
        assert_eq!(properties.len(), super::super::receipt::RESULT_KEYS.len());
        assert!(
            super::super::receipt::RESULT_KEYS
                .iter()
                .all(|key| properties.contains_key(*key))
        );
    }

    #[test]
    fn settled_effect_guidance_preserves_historical_schema_digest() {
        let schema = document().unwrap();
        // Native schema v1 from the installed budget8 release. New prompt
        // guidance must not invalidate retained initial-plan or Finish profiles.
        assert_eq!(
            super::super::contract_digest(&schema).unwrap(),
            "3011c2cce468be37030f665c0b433d694fd9a8e6e917bd6ec955800856e883b5"
        );
        for contract in ["graph", "result"] {
            let properties = &schema[contract]["$defs"]["SettledEffectGate"]["properties"];
            assert_eq!(
                properties,
                &json!({"audit_sha256":{"type":"string"},
                    "inspection":{"type":"string"},"verification":{"type":"string"}})
            );
        }
    }
}
