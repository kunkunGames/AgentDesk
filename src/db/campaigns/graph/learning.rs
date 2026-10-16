//! Evidence-backed reuse belongs to Campaign, not a second scheduler or queue.
pub(crate) mod budget;
mod members;
pub(crate) mod policy;
pub(crate) mod reviews;
pub(crate) mod samples;
pub(crate) mod template;
pub use members::{ControlAssignment, family, initial_assignment, inspection};

use super::{Goal, contract_digest};
use anyhow::Result;
use serde_json::{Value, json};

/// Budgets and schedule windows are run policy, not a reusable business scope.
/// Goal/source requirements, executors and constraints must still match exactly.
pub fn scope(goal: &Goal) -> Value {
    let mut scope = json!({"objective":goal.objective,"success":goal.success,"constraints":goal.constraints,
        "repo":goal.repo,"planner":goal.planner,"verifier":goal.verifier,"agents":goal.agents});
    if !goal.prerequisites.is_empty() {
        scope["prerequisites"] = json!(goal.prerequisites);
    }
    scope
}
pub fn scope_digest(goal: &Goal) -> Result<String> {
    contract_digest(&scope(goal))
}
