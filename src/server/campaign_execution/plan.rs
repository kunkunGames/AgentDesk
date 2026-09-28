use crate::db::campaigns::{self, Campaign};
use crate::server::test_phase_runs::checks::contract::{digest, relative};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub graph_digest: String,
    pub repo: String,
    pub agent_id: String,
    pub nodes: BTreeMap<String, Node>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Node {
    pub task_key: String,
    pub check_template: String,
    #[serde(default)]
    pub auto_rework: bool,
    #[serde(default)]
    pub inputs: Vec<Input>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Input {
    pub dependency: String,
    pub source: String,
    pub destination: String,
}

/// Exclude observed progress, retain every operator-approved work instruction.
/// Sorted node/dependency order makes this independent of UI ordering.
pub(super) fn graph(campaign: &Campaign) -> Value {
    let nodes: BTreeMap<_, _> = campaign
        .nodes
        .iter()
        .map(|n| {
            let n = &n.input;
            let dependencies: BTreeSet<_> = n.dependencies.iter().collect();
            (
                n.id.clone(),
                json!({"title":n.title,"stage":n.stage,"round":n.round,
            "details":n.details,"acceptance":n.acceptance,"dependencies":dependencies}),
            )
        })
        .collect();
    json!({"title":campaign.title,"description":campaign.description,"round":campaign.round,"nodes":nodes})
}

impl Plan {
    pub(super) fn validate(&self, campaign: &Campaign) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.graph_digest == digest(&serde_json::to_vec(&graph(campaign))?),
            "campaign instructions differ from the approved graph digest"
        );
        anyhow::ensure!(
            !self.repo.trim().is_empty() && !self.agent_id.trim().is_empty(),
            "campaign requires a repository and an agent"
        );
        anyhow::ensure!(
            !self.nodes.is_empty() && self.nodes.len() == campaign.nodes.len(),
            "plan must cover the entire campaign"
        );
        let mut keys = BTreeSet::new();
        let mut templates = BTreeSet::new();
        for node in &campaign.nodes {
            let plan = self
                .nodes
                .get(&node.input.id)
                .ok_or_else(|| anyhow::anyhow!("missing node plan"))?;
            campaigns::validate_id(&plan.task_key)?;
            anyhow::ensure!(
                keys.insert(&plan.task_key) && templates.insert(&plan.check_template),
                "task keys and check workspaces must be exclusive"
            );
            anyhow::ensure!(
                !plan.check_template.is_empty() && plan.inputs.len() <= 64,
                "invalid node input budget"
            );
            let mut paths = BTreeSet::new();
            let mut dependencies = BTreeSet::new();
            for input in &plan.inputs {
                relative(&input.source).map_err(anyhow::Error::msg)?;
                relative(&input.destination).map_err(anyhow::Error::msg)?;
                anyhow::ensure!(
                    paths.insert(&input.destination),
                    "duplicate input destination"
                );
                anyhow::ensure!(
                    node.input.dependencies.contains(&input.dependency),
                    "undeclared input dependency"
                );
                dependencies.insert(&input.dependency);
            }
            anyhow::ensure!(
                dependencies.len() == node.input.dependencies.len(),
                "every dependency must supply at least one actual file"
            );
        }
        Ok(())
    }

    pub(super) fn digest(&self) -> anyhow::Result<String> {
        Ok(digest(&serde_json::to_vec(self)?))
    }
}

pub(super) fn card_id(task: &str) -> String {
    format!("campaign-task:{}", digest(task.as_bytes()))
}
