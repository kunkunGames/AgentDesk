//! Goal-first execution contracts. Model proposals are data, never authority.
pub(crate) mod control_budget;
pub(crate) mod effect_ownership;
pub(crate) mod learning;
pub(crate) mod metrics;
pub(crate) mod planning_input;
pub(crate) mod preflight;
pub(crate) mod progress;
pub(crate) mod receipt;
pub(crate) mod recovery;
pub(crate) mod result;
pub(crate) mod revision;
pub(crate) mod schedule;
pub(crate) mod schema;
pub(crate) mod store;
pub(crate) mod workflow;
use anyhow::{Result, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// The same bounded handoff contract is published to planners and enforced by
/// capture, stored-artifact verification and pinned-input verification.
pub const MAX_ARTIFACT_FILE_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_ARTIFACT_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_TASK_OUTPUTS: usize = 16;
pub const MAX_TASK_INPUTS: usize = 32;
pub const MAX_INITIAL_EVOLUTIONS: u32 = 3;
/// Additional lifetime planning requires an explicit audited operator budget.
pub const MAX_AUDITED_EVOLUTIONS: u32 = 16;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Goal {
    pub objective: String,
    pub success: Vec<String>,
    pub constraints: Vec<String>,
    pub repo: String,
    pub planner: String,
    pub verifier: String,
    pub agents: BTreeSet<String>,
    pub budget: Budget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<schedule::Schedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_window: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prerequisites: Vec<preflight::Prerequisite>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub max_dispatches: u32,
    pub max_elapsed_secs: u32,
    pub node_timeout_secs: u32,
    pub max_evolutions: u32,
    #[serde(default)]
    pub max_observed_tool_calls: Option<u32>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Graph {
    #[schemars(length(min = 2, max = 32))]
    pub nodes: Vec<Task>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 16))]
    pub settled_effect_gates: Vec<SettledEffectGate>,
}

/// A settled failed attempt is historical evidence, never an executable retry.
/// These source-audit identities bind fresh business target QA before new effects.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SettledEffectGate {
    pub audit_sha256: String,
    pub inspection: String,
    pub verification: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Task {
    #[schemars(length(min = 1, max = 64), regex(pattern = r"^[A-Za-z0-9_.-]+$"))]
    pub id: String,
    #[schemars(length(min = 5, max = 200))]
    pub task_key: String,
    #[schemars(length(min = 1, max = 160))]
    pub title: String,
    pub agent: String,
    pub kind: Kind,
    #[serde(default)]
    pub effect: Effect,
    #[schemars(length(min = 1, max = 16000))]
    pub details: String,
    #[schemars(length(min = 1, max = 12))]
    pub acceptance: Vec<String>,
    pub dependencies: BTreeSet<String>,
    #[schemars(length(max = 32))]
    pub inputs: Vec<Input>,
    #[schemars(length(min = 1, max = 16))]
    pub outputs: Vec<Output>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Inspect,
    Execute,
    Verify,
}
/// Native executor contract, not a claim of OS-level containment.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    ArtifactOnly,
    #[default]
    External,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub dependency: String,
    pub source: String,
    pub destination: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub path: String,
    pub format: Format,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Text,
    Json,
    Python,
}

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn contract_digest(value: &impl Serialize) -> Result<String> {
    // serde_json::Value canonicalizes object keys (no preserve_order feature).
    Ok(digest(&serde_json::to_vec(&serde_json::to_value(value)?)?))
}
pub fn relative(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && path.len() <= 240
            && !path.contains(['\\', ':', '\0'])
            && path
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".." && p != ".git"),
        "invalid artifact path"
    );
    Ok(())
}
impl Goal {
    pub fn validate(&self) -> Result<()> {
        if let Some(window) = &self.observation_window {
            chrono::DateTime::parse_from_rfc3339(window)?;
        }
        preflight::validate(&self.prerequisites, &self.agents)?;
        if let Some(schedule) = &self.schedule {
            schedule.validate()?;
        }
        ensure!(
            !self.objective.trim().is_empty() && self.objective.len() <= 16000,
            "invalid objective"
        );
        ensure!(
            !self.success.is_empty() && self.success.len() <= 16 && self.constraints.len() <= 32,
            "goal requires bounded success criteria"
        );
        ensure!(
            self.success
                .iter()
                .chain(&self.constraints)
                .all(|s| !s.trim().is_empty() && s.len() <= 2000),
            "invalid goal criterion"
        );
        ensure!(
            self.repo.len() <= 200
                && self.repo.split('/').count() == 2
                && self.repo.split('/').all(|part| !part.is_empty()
                    && part != "."
                    && part != ".."
                    && part
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))),
            "repository must be owner/name"
        );
        ensure!(
            self.agents.len() >= 2
                && self.agents.len() <= 8
                && self.agents.contains(&self.planner)
                && self.agents.contains(&self.verifier)
                && self.planner != self.verifier,
            "independent enrolled planner/verifier required"
        );
        let b = &self.budget;
        ensure!(
            (3..=200).contains(&b.max_dispatches)
                && (60..=86400).contains(&b.max_elapsed_secs)
                && (30..=3600).contains(&b.node_timeout_secs)
                && b.node_timeout_secs <= b.max_elapsed_secs
                && b.max_evolutions <= MAX_AUDITED_EVOLUTIONS,
            "invalid finite budget"
        );
        ensure!(
            b.max_observed_tool_calls
                .is_none_or(|n| (20..=2000).contains(&n)),
            "invalid observed tool budget"
        );
        Ok(())
    }
}
impl Graph {
    pub fn validate(&self, goal: &Goal) -> Result<()> {
        goal.validate()?;
        ensure!(
            (2..=32).contains(&self.nodes.len()),
            "graph requires 2..32 nodes"
        );
        let mut nodes = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for n in &self.nodes {
            super::validate_id(&n.id)?;
            ensure!(
                n.id.len() <= 64 && nodes.insert(n.id.clone(), n).is_none(),
                "duplicate or oversized node ID"
            );
            ensure!(
                (5..=200).contains(&n.task_key.len()) && keys.insert(&n.task_key),
                "invalid or duplicate logical task key"
            );
            ensure!(goal.agents.contains(&n.agent), "unregistered executor");
            ensure!(
                !n.title.trim().is_empty()
                    && n.title.len() <= 160
                    && !n.details.trim().is_empty()
                    && n.details.len() <= 16000,
                "invalid node instructions"
            );
            ensure!(
                !n.acceptance.is_empty()
                    && n.acceptance.len() <= 12
                    && n.acceptance
                        .iter()
                        .all(|a| !a.trim().is_empty() && a.len() <= 2000),
                "missing acceptance"
            );
            ensure!(
                !n.outputs.is_empty() && n.outputs.len() <= MAX_TASK_OUTPUTS,
                "node {} has {} outputs; required 1..{}",
                n.id,
                n.outputs.len(),
                MAX_TASK_OUTPUTS
            );
            ensure!(
                n.inputs.len() <= MAX_TASK_INPUTS,
                "node {} has {} inputs; maximum is {}",
                n.id,
                n.inputs.len(),
                MAX_TASK_INPUTS
            );
            let mut paths = BTreeSet::new();
            for o in &n.outputs {
                relative(&o.path)?;
                ensure!(paths.insert(&o.path), "duplicate output");
            }
            for i in &n.inputs {
                relative(&i.source)?;
                relative(&i.destination)?;
                ensure!(paths.insert(&i.destination), "input/output path collision");
            }
        }
        let mut audits = BTreeSet::new();
        ensure!(
            self.settled_effect_gates.len() <= 16,
            "too many settled effect gates"
        );
        for gate in &self.settled_effect_gates {
            ensure!(
                gate.audit_sha256.len() == 64
                    && gate
                        .audit_sha256
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    && audits.insert(&gate.audit_sha256)
                    && gate.inspection != gate.verification
                    && nodes.contains_key(&gate.inspection)
                    && nodes.contains_key(&gate.verification),
                "invalid or duplicate settled effect gate"
            );
        }
        let mut ready = BTreeSet::new();
        loop {
            let previous = ready.len();
            for n in &self.nodes {
                if n.dependencies.iter().all(|d| ready.contains(d)) {
                    ready.insert(n.id.clone());
                }
            }
            if ready.len() == previous {
                break;
            }
        }
        ensure!(
            ready.len() == self.nodes.len(),
            "cyclic or missing dependency"
        );
        for n in &self.nodes {
            let mut supplied = BTreeSet::new();
            for i in &n.inputs {
                ensure!(
                    n.dependencies.contains(&i.dependency),
                    "undeclared input dependency"
                );
                let source = nodes
                    .get(&i.dependency)
                    .ok_or_else(|| anyhow::anyhow!("missing input producer"))?;
                ensure!(
                    source.outputs.iter().any(|o| o.path == i.source),
                    "input has no declared producer output"
                );
                supplied.insert(i.dependency.clone());
            }
            ensure!(
                supplied == n.dependencies,
                "every edge requires an actual output mapping"
            );
            if n.kind == Kind::Verify {
                ensure!(
                    n.dependencies.iter().any(|d| nodes[d].agent != n.agent),
                    "verifier must inspect another executor's artifact"
                );
            } else {
                for dependency in &n.dependencies {
                    if nodes[dependency].kind != Kind::Verify {
                        ensure!(
                            self.nodes.iter().any(|v| v.kind == Kind::Verify
                                && v.agent != nodes[dependency].agent
                                && v.dependencies.contains(dependency)
                                && n.dependencies.contains(&v.id)),
                            "consumer must depend on independent verification of its input"
                        );
                    }
                }
                ensure!(
                    self.nodes.iter().any(|v| v.kind == Kind::Verify
                        && v.agent != n.agent
                        && v.dependencies.contains(&n.id)),
                    "work requires a directly dependent independent verifier"
                );
            }
        }
        Ok(())
    }
}
