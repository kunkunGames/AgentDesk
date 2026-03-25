//! Data-driven pipeline engine (#106 P1-P2).
//!
//! Loads pipeline definition from YAML and provides lookup methods
//! used by `kanban.rs` for transition validation.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

/// Global singleton pipeline config, loaded once at startup.
static PIPELINE: OnceLock<PipelineConfig> = OnceLock::new();

/// Load pipeline from YAML file. Called once during server startup.
pub fn load(path: &Path) -> Result<()> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let config: PipelineConfig =
        serde_yaml::from_str(&content).with_context(|| format!("parsing {}", path.display()))?;
    config.validate()?;
    PIPELINE
        .set(config)
        .map_err(|_| anyhow::anyhow!("pipeline already loaded"))?;
    Ok(())
}

/// Get the loaded pipeline config. Panics if not yet loaded.
pub fn get() -> &'static PipelineConfig {
    PIPELINE
        .get()
        .expect("pipeline not loaded — call pipeline::load() at startup")
}

/// Try to get the loaded pipeline config. Returns None if not yet loaded.
pub fn try_get() -> Option<&'static PipelineConfig> {
    PIPELINE.get()
}

// ── Schema ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    pub version: u32,
    pub states: Vec<StateConfig>,
    pub transitions: Vec<TransitionConfig>,
    #[serde(default)]
    pub gates: HashMap<String, GateConfig>,
    #[serde(default)]
    pub hooks: HashMap<String, HookBindings>,
    #[serde(default)]
    pub clocks: HashMap<String, ClockConfig>,
    #[serde(default)]
    pub timeouts: HashMap<String, TimeoutConfig>,
}

#[derive(Debug, Deserialize)]
pub struct StateConfig {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub terminal: bool,
}

#[derive(Debug, Deserialize)]
pub struct TransitionConfig {
    pub from: String,
    pub to: String,
    #[serde(rename = "type")]
    pub transition_type: TransitionType,
    #[serde(default)]
    pub gates: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum TransitionType {
    Free,
    Gated,
    ForceOnly,
}

#[derive(Debug, Deserialize)]
pub struct GateConfig {
    #[serde(rename = "type")]
    pub gate_type: String,
    #[serde(default)]
    pub check: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HookBindings {
    #[serde(default)]
    pub on_enter: Vec<String>,
    #[serde(default)]
    pub on_exit: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClockConfig {
    pub set: String,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TimeoutConfig {
    pub duration: String,
    pub clock: String,
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub on_exhaust: Option<String>,
    #[serde(default)]
    pub condition: Option<String>,
}

// ── Lookup methods ───────────────────────────────────────────────

impl PipelineConfig {
    /// Find transition rule for from → to.
    pub fn find_transition(&self, from: &str, to: &str) -> Option<&TransitionConfig> {
        self.transitions
            .iter()
            .find(|t| t.from == from && t.to == to)
    }

    /// Check if a state is terminal (no outbound transitions allowed).
    pub fn is_terminal(&self, state: &str) -> bool {
        self.states.iter().any(|s| s.id == state && s.terminal)
    }

    /// Check if a state is valid.
    pub fn is_valid_state(&self, state: &str) -> bool {
        self.states.iter().any(|s| s.id == state)
    }

    /// Get clock field to set when entering a state.
    pub fn clock_for_state(&self, state: &str) -> Option<&ClockConfig> {
        self.clocks.get(state)
    }

    /// Get hook bindings for a state.
    pub fn hooks_for_state(&self, state: &str) -> Option<&HookBindings> {
        self.hooks.get(state)
    }

    /// Validate internal consistency.
    fn validate(&self) -> Result<()> {
        let state_ids: Vec<&str> = self.states.iter().map(|s| s.id.as_str()).collect();

        // All transition from/to must reference valid states
        for t in &self.transitions {
            if !state_ids.contains(&t.from.as_str()) {
                anyhow::bail!("transition from unknown state: {}", t.from);
            }
            if !state_ids.contains(&t.to.as_str()) {
                anyhow::bail!("transition to unknown state: {}", t.to);
            }
        }

        // All gate references must exist in gates map
        for t in &self.transitions {
            for g in &t.gates {
                if !self.gates.contains_key(g) {
                    anyhow::bail!(
                        "transition {}→{} references unknown gate: {}",
                        t.from,
                        t.to,
                        g
                    );
                }
            }
        }

        // Clock fields must reference valid states
        for (state, _) in &self.clocks {
            if !state_ids.contains(&state.as_str()) {
                anyhow::bail!("clock for unknown state: {}", state);
            }
        }

        Ok(())
    }
}
