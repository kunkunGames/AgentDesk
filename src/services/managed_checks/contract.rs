//! Operator-owned, finite check templates. No commands arrive in model output.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Template {
    pub workspace: PathBuf,
    pub artifact_root: PathBuf,
    pub allowed_changes: Vec<String>,
    pub acceptance: Vec<Acceptance>,
    pub checks: BTreeMap<String, Check>,
    pub review: Review,
    pub max_candidates: u32,
    pub max_check_attempts: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Acceptance {
    pub id: String,
    pub checks: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Review {
    None,
    Local,
    Human,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Check {
    pub executable: PathBuf,
    pub wrapper: PathBuf,
    pub executable_sha256: String,
    pub wrapper_sha256: String,
    pub timeout_secs: u64,
    pub kind: CheckKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CheckKind {
    Assertion,
    TestSuite {
        suites: Vec<String>,
        min_tests: u64,
        max_skipped: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Contract {
    pub template: Template,
    pub baseline: BTreeMap<String, String>,
    pub digest: String,
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn relative(path: &str) -> Result<(), String> {
    if path.is_empty()
        || path.contains('\\')
        || path.starts_with(".git/")
        || path == ".git"
        || Path::new(path)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(format!("invalid candidate-relative path: {path}"));
    }
    Ok(())
}

impl Template {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.workspace.is_absolute()
            || !self.artifact_root.is_absolute()
            || self.artifact_root.starts_with(&self.workspace)
            || !(1..=12).contains(&self.max_candidates)
            || !(1..=3).contains(&self.max_check_attempts)
            || self.acceptance.is_empty()
            || self.acceptance.len() > 64
            || self.checks.is_empty()
            || self.checks.len() > 16
        {
            return Err("invalid verification paths, coverage, or finite budgets".into());
        }
        for path in &self.allowed_changes {
            relative(path)?;
        }
        if self
            .workspace
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
            || self
                .artifact_root
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
        {
            return Err("verification paths must be normalized".into());
        }
        let mut ids = BTreeSet::new();
        for acceptance in &self.acceptance {
            if acceptance.id.is_empty()
                || !ids.insert(&acceptance.id)
                || acceptance.checks.is_empty()
                || acceptance
                    .checks
                    .iter()
                    .any(|id| !self.checks.contains_key(id))
            {
                return Err("every unique mandatory acceptance needs registered checks".into());
            }
        }
        for (id, check) in &self.checks {
            if id.is_empty()
                || id.len() > 64
                || !check.executable.is_absolute()
                || !check.wrapper.is_absolute()
                || !(1..=900).contains(&check.timeout_secs)
                || [&check.executable_sha256, &check.wrapper_sha256]
                    .iter()
                    .any(|hash| hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                return Err("invalid protected check identity or deadline".into());
            }
            if let CheckKind::TestSuite {
                suites, min_tests, ..
            } = &check.kind
                && (suites.is_empty()
                    || suites.iter().any(String::is_empty)
                    || *min_tests == 0
                    || suites.iter().collect::<BTreeSet<_>>().len() != suites.len())
            {
                return Err(
                    "test checks require unique suites and a positive executed count".into(),
                );
            }
        }
        Ok(())
    }
}

pub(crate) fn load_template(path: &Path, name: &str) -> Result<Template, String> {
    let content = crate::services::platform::managed_process::read_protected_config(path)
        .map_err(|error| error.to_string())?;
    let mut registry: BTreeMap<String, Template> =
        serde_json::from_str(&content).map_err(|error| error.to_string())?;
    let template = registry
        .remove(name)
        .ok_or("verification template is not registered")?;
    template.validate()?;
    Ok(template)
}

impl Contract {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.template.validate()?;
        if digest(
            &serde_json::to_vec(&(&self.template, &self.baseline)).map_err(|e| e.to_string())?,
        ) != self.digest
        {
            return Err("verification contract digest mismatch".into());
        }
        Ok(())
    }
}
