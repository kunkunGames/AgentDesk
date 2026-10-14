//! A template is a plan definition, never a prior run's completion state.
use super::super::{Goal, Graph, contract_digest};
use anyhow::{Result, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;
pub const INPUT_FILE: &str = ".agentdesk-dag-review-input.json";

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Template {
    #[schemars(range(min = 1, max = 1))]
    pub schema_version: u32,
    #[schemars(length(min = 1, max = 160))]
    pub name: String,
    #[schemars(length(min = 1, max = 8000))]
    pub description: String,
    pub graph: Graph,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Promote,
    Keep,
    Defer,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub sample_digest: String,
    pub decision: Decision,
    #[schemars(length(min = 1, max = 8000))]
    pub reason: String,
    pub template: Option<Template>,
}

/// Expand only tokens from the original definition. Binding values are opaque;
/// embedded braces, shell syntax and model prose are never recursively evaluated.
fn bind(text: &str, goal: &Goal, window: &str) -> Result<String> {
    let mut output = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let end = tail
            .find("}}")
            .ok_or_else(|| anyhow::anyhow!("unterminated template binding"))?;
        let value = match &tail[..end] {
            "objective" => goal.objective.clone(),
            "repo" => goal.repo.clone(),
            "window" => window.to_owned(),
            "success_json" => serde_json::to_string(&goal.success)?,
            "constraints_json" => serde_json::to_string(&goal.constraints)?,
            _ => anyhow::bail!("unknown template binding: {}", &tail[..end]),
        };
        output.push_str(&value);
        rest = &tail[end + 2..];
    }
    ensure!(!rest.contains("}}"), "unmatched template binding");
    output.push_str(rest);
    Ok(output)
}
impl Template {
    pub fn instantiate(
        &self,
        goal: &Goal,
        family: &str,
        occurrence: &str,
        window: &str,
    ) -> Result<Graph> {
        ensure!(
            self.schema_version == SCHEMA_VERSION,
            "unsupported DAG template schema version"
        );
        ensure!(
            !self.name.trim().is_empty()
                && self.name.len() <= 160
                && !self.description.trim().is_empty()
                && self.description.len() <= 8000,
            "bounded template name and description required"
        );
        let mut graph = self.graph.clone();
        for node in &mut graph.nodes {
            ensure!(
                node.task_key == "{{task_key}}",
                "template task keys must use the server binding"
            );
            node.task_key = format!("tpl-{}", contract_digest(&(family, occurrence, &node.id))?);
            node.agent = match node.agent.as_str() {
                "{{planner}}" => goal.planner.clone(),
                "{{verifier}}" => goal.verifier.clone(),
                _ => anyhow::bail!("template agents must bind to planner or verifier roles"),
            };
            node.title = bind(&node.title, goal, window)?;
            node.details = bind(&node.details, goal, window)?;
            for condition in &mut node.acceptance {
                *condition = bind(condition, goal, window)?;
            }
        }
        graph.validate(goal)?;
        Ok(graph)
    }
}

pub fn instructions() -> &'static str {
    "Read the frozen .agentdesk-dag-review-input.json and verify its sample_digest. Compare active_template, the new independently verified completed DAGs, quality_baseline and failure_history for this exact goal scope. Do not treat planning approval as execution evidence. Account for outcome diversity, missing scope, failures and no-work branches. Return dag_template_review with {sample_digest,decision:promote|keep|defer,reason,template:null|{schema_version:1,name,description,graph}} alongside campaign_receipt. Keep the existing template with template:null when it remains suitable; defer with template:null when evidence is insufficient. Promote a new version only when the reusable steps preserve the goal, observed business-effect coverage, artifact handoff and independent verification. The existing template continues serving new occurrences during review. Template nodes use task_key=\"{{task_key}}\" and agent=\"{{planner}}\" or \"{{verifier}}\". Text bindings are {{objective}}, {{repo}}, {{window}}, {{success_json}}, {{constraints_json}}; IDs, dependencies and artifact paths remain literal. Never embed a completed receipt, stale source revision, prior PR/target ID or run state. Tasks must freshly inspect current inputs and reconcile effects before mutation. Preserve all graph rules. Defer with a concrete reason when branching or target-specific steps cannot yet be generalized. This card is read-only planning; no business effects or service changes."
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn template_bindings_are_finite_and_never_evaluate_bound_values() {
        let goal: Goal = serde_json::from_value(serde_json::json!({"objective":"literal {{not_a_token}}", "success":["checked"],"constraints":[],"repo":"a/b","planner":"a","verifier":"b","agents":["a","b"],"budget":{"max_dispatches":3,"max_elapsed_secs":60,"node_timeout_secs":30,"max_evolutions":0}})).unwrap();
        assert_eq!(
            bind("{{objective}} / {{repo}}", &goal, "now").unwrap(),
            "literal {{not_a_token}} / a/b"
        );
        for bad in ["{{unknown}}", "{{objective", "x}}"] {
            assert!(bind(bad, &goal, "now").is_err());
        }
    }
}
