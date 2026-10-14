//! Compact manager decisions are proposals bound to one observed work frontier.
//! The executor retains authority over contracts, effects, budgets and completion.
use super::{Goal, Graph, Kind, contract_digest};
use anyhow::{Result, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
pub(crate) mod lifecycle;
pub(crate) mod retirement;
pub(crate) mod store;
#[cfg(test)]
mod tests;

pub const INPUT_FILE: &str = ".agentdesk-campaign-workflow-input.json";

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Continue,
    Revise,
    Wait,
    Complete,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub snapshot_digest: String,
    pub action: Action,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake_after_secs: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Criterion {
    pub index: usize,
    pub met: bool,
    pub observation: usize,
    pub task_key: String,
    pub path: String,
    pub sha256: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GoalValidation {
    pub snapshot_digest: String,
    pub approved: bool,
    pub criteria: Vec<Criterion>,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Frontier {
    pub campaign_id: String,
    pub goal_digest: String,
    pub goal: Goal,
    pub version: i64,
    pub graph_digest: String,
    pub graph: Graph,
    pub tasks: Vec<Value>,
    pub failures: Vec<Value>,
    pub unmet_goal_assessments: Vec<Value>,
}

impl Frontier {
    pub fn authority_state(&self) -> Value {
        let tasks: Vec<Value> = self
            .tasks
            .iter()
            .cloned()
            .map(|mut task| {
                if let Some(object) = task.as_object_mut() {
                    object.remove("receipt");
                }
                task
            })
            .collect();
        serde_json::json!({"tasks":tasks,"failures":self.failures,"unmet_goal_assessments":self.unmet_goal_assessments})
    }
    pub fn digest(&self) -> Result<String> {
        contract_digest(self)
    }

    pub fn task(&self, key: &str) -> Option<&Value> {
        self.tasks.iter().find(|task| task["task_key"] == key)
    }

    pub fn accepted(&self, key: &str) -> bool {
        self.task(key).is_some_and(|task| task["accepted"] == true)
    }

    pub fn ready_verification(&self) -> bool {
        self.graph.nodes.iter().any(|node| {
            node.kind == Kind::Verify
                && self.task(&node.task_key).is_none()
                && node.dependencies.iter().all(|id| {
                    self.graph
                        .nodes
                        .iter()
                        .find(|parent| parent.id == *id)
                        .is_some_and(|parent| self.accepted(&parent.task_key))
                })
        })
    }

    pub fn running(&self) -> bool {
        self.tasks.iter().any(|task| {
            self.graph
                .nodes
                .iter()
                .any(|node| task["task_key"] == node.task_key)
                && task["accepted"] != true
                && task["observation"].is_null()
        })
    }

    pub fn has_results(&self) -> bool {
        self.tasks.iter().any(|task| !task["observation"].is_null())
    }

    pub fn validate_decision(&self, decision: &Decision, graph: Option<&Graph>) -> Result<()> {
        ensure!(
            decision.snapshot_digest == self.digest()?,
            "workflow frontier changed"
        );
        ensure!(
            !decision.reason.trim().is_empty() && decision.reason.len() <= 4000,
            "workflow decision requires bounded causal evidence"
        );
        ensure!(
            (decision.action == Action::Revise) == graph.is_some(),
            "only a revision can carry a replacement graph"
        );
        ensure!(
            decision.failure_id.is_none() || decision.action == Action::Revise,
            "only a revision can select a captured failure"
        );
        ensure!(
            if decision.action == Action::Wait {
                decision
                    .wake_after_secs
                    .is_some_and(|secs| (1..=3600).contains(&secs))
            } else {
                decision.wake_after_secs.is_none()
            },
            "wait requires a bounded persisted wake deadline"
        );
        if let Some(failure) = &decision.failure_id {
            ensure!(
                self.failures.iter().any(|item| item["id"] == *failure
                    && matches!(item["category"].as_str(), Some("code" | "plan"))),
                "workflow repair must select its captured recoverable failure"
            );
        }
        if let Some(next) = graph {
            next.validate(&self.goal)?;
            ensure!(
                contract_digest(next)? != self.graph_digest,
                "workflow revision must change remaining work"
            );
            // Normal progress may change only tasks that have never been published.
            // Failure recovery keeps its separately audited ownership policy.
            for task in &self.tasks {
                if decision.failure_id.is_some()
                    && task["accepted"] != true
                    && !task["observation"].is_null()
                {
                    continue;
                }
                let key = task["task_key"].as_str().unwrap_or("");
                let node = next
                    .nodes
                    .iter()
                    .find(|node| node.task_key == key)
                    .ok_or_else(|| anyhow::anyhow!("workflow revision removed a published task"))?;
                ensure!(
                    task["contract_digest"] == contract_digest(node)?,
                    "workflow revision changed a published task contract"
                );
            }
        }
        if decision.action == Action::Continue {
            ensure!(
                self.graph
                    .nodes
                    .iter()
                    .any(|node| self.task(&node.task_key).is_none()
                        && node.dependencies.iter().all(|id| self
                            .graph
                            .nodes
                            .iter()
                            .find(|parent| parent.id == *id)
                            .is_some_and(|parent| self.accepted(&parent.task_key)))),
                "a finished graph requires goal verification or a revision"
            );
        }
        if decision.action == Action::Complete {
            self.validate_completion_frontier()?;
        }
        Ok(())
    }

    pub fn validate_completion_frontier(&self) -> Result<()> {
        ensure!(!self.running(), "workflow work is still unsettled");
        ensure!(
            self.graph.nodes.iter().all(|node| {
                self.task(&node.task_key).is_none() || self.accepted(&node.task_key)
            }),
            "published workflow work has not been accepted"
        );
        ensure!(
            self.graph
                .nodes
                .iter()
                .any(|node| node.kind == Kind::Verify && self.accepted(&node.task_key)),
            "goal completion requires independent accepted verification"
        );
        for node in self
            .graph
            .nodes
            .iter()
            .filter(|node| node.kind != Kind::Verify && self.accepted(&node.task_key))
        {
            ensure!(
                self.graph
                    .nodes
                    .iter()
                    .any(|check| check.kind == Kind::Verify
                        && check.agent != node.agent
                        && check.dependencies.contains(&node.id)
                        && self.accepted(&check.task_key)),
                "accepted producer still needs independent verification"
            );
        }
        Ok(())
    }

    pub fn validate_goal(&self, validation: &GoalValidation, result: &Value) -> Result<()> {
        ensure!(
            validation.approved,
            "independent goal verification did not approve completion"
        );
        self.validate_goal_assessment(validation, result)
    }

    pub fn validate_goal_assessment(
        &self,
        validation: &GoalValidation,
        result: &Value,
    ) -> Result<()> {
        self.validate_completion_frontier()?;
        ensure!(
            validation.snapshot_digest == self.digest()?,
            "goal verification frontier changed"
        );
        ensure!(
            !validation.reason.trim().is_empty() && validation.reason.len() <= 4000,
            "independent goal verification did not approve completion"
        );
        ensure!(
            validation.criteria.len() == self.goal.success.len(),
            "goal verification omitted success criteria"
        );
        let observations = result["campaign_receipt"]["observations"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("goal verification observations missing"))?;
        let mut covered = BTreeSet::new();
        for criterion in &validation.criteria {
            ensure!(
                criterion.index < self.goal.success.len()
                    && covered.insert(criterion.index)
                    && (!validation.approved || criterion.met),
                "goal success criteria must each be met exactly once"
            );
            ensure!(
                !criterion.evidence.trim().is_empty() && criterion.evidence.len() <= 4000,
                "goal criterion lacks bounded evidence"
            );
            ensure!(
                criterion.observation < observations.len(),
                "goal criterion observation is missing"
            );
            let node = self
                .graph
                .nodes
                .iter()
                .find(|node| node.task_key == criterion.task_key)
                .ok_or_else(|| anyhow::anyhow!("goal proof is outside the active graph"))?;
            ensure!(
                node.kind == Kind::Verify && self.accepted(&node.task_key),
                "goal proof requires accepted independent verification"
            );
            let task = self
                .task(&node.task_key)
                .ok_or_else(|| anyhow::anyhow!("goal proof task is missing"))?;
            ensure!(
                node.outputs
                    .iter()
                    .any(|output| output.path == criterion.path)
                    && task["artifact_manifest"]["files"][&criterion.path] == criterion.sha256,
                "goal proof artifact differs from accepted output"
            );
        }
        Ok(())
    }
}
