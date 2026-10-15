//! Explicit prerequisites are checked before spending a planner or work turn.
//! Reports are observations, never permission to replay an unknown effect.
use super::{Goal, MAX_ARTIFACT_FILE_BYTES, digest};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::{collections::BTreeSet, path::Path};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Prerequisite {
    File { path: String, sha256: String },
    Binary { name: String },
    Skill { agent_id: String, name: String },
}
pub fn validate(items: &[Prerequisite], agents: &BTreeSet<String>) -> Result<()> {
    ensure!(items.len() <= 32, "at most 32 explicit prerequisites");
    for item in items {
        match item {
            Prerequisite::File { path, sha256 } => ensure!(
                Path::new(path).is_absolute()
                    && path.len() <= 1024
                    && !path.contains('\0')
                    && sha256.len() == 64
                    && sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "absolute prerequisite file and SHA256 required"
            ),
            Prerequisite::Binary { name } => ensure!(
                identifier(name),
                "prerequisite binary must be a simple executable name"
            ),
            Prerequisite::Skill { agent_id, name } => ensure!(
                agents.contains(agent_id) && identifier(name),
                "prerequisite skill must belong to an enrolled executor"
            ),
        }
    }
    Ok(())
}
fn identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 160
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        && name != "."
        && name != ".."
}
pub async fn check(pool: &PgPool, id: &str, goal: &Goal) -> Result<bool> {
    if goal.prerequisites.is_empty() {
        return Ok(true);
    }
    let mut skills = Vec::new();
    for agent in &goal.agents {
        let row = sqlx::query("SELECT id,provider FROM agents WHERE id=$1")
            .bind(agent)
            .fetch_one(pool)
            .await?;
        skills.push((row.get::<String, _>("id"), row.get::<String, _>("provider")));
    }
    let requirements = goal.prerequisites.clone();
    let repo = goal.repo.clone();
    let report = tokio::task::spawn_blocking(move || {
        let project = crate::services::git::resolve_repo_dir_for_id(Some(&repo))
            .ok()
            .flatten();
        inspect(&requirements, &skills, project.as_deref())
    })
    .await?;
    let ready = report["ready"] == true;
    sqlx::query("UPDATE campaign_execution_specs SET preflight=$2 WHERE campaign_id=$1 AND preflight IS DISTINCT FROM $2")
        .bind(id).bind(report).execute(pool).await?;
    Ok(ready)
}
fn inspect(items: &[Prerequisite], agents: &[(String, String)], project: Option<&str>) -> Value {
    let checks: Vec<Value> = items
        .iter()
        .map(|item| {
            let (ready, code) = match item {
                Prerequisite::File { path, sha256 } => match std::fs::metadata(path) {
                    Ok(meta) if meta.is_file() && meta.len() <= MAX_ARTIFACT_FILE_BYTES => {
                        match std::fs::read(path) {
                            Ok(bytes) if bytes.len() as u64 == meta.len() => (
                                digest(&bytes).eq_ignore_ascii_case(sha256),
                                "source_digest_mismatch",
                            ),
                            _ => (false, "source_unreadable_or_changed"),
                        }
                    }
                    _ => (false, "source_unavailable_or_oversized"),
                },
                Prerequisite::Binary { name } => (which::which(name).is_ok(), "binary_unavailable"),
                Prerequisite::Skill { agent_id, name } => {
                    let present = agents
                        .iter()
                        .find(|(id, _)| id == agent_id)
                        .and_then(|(_, provider)| {
                            crate::services::provider::ProviderKind::from_str(provider)
                        })
                        .is_some_and(|provider| {
                            crate::services::discord::scan_skills(&provider, project)
                                .iter()
                                .any(|(skill, _)| skill == name)
                        });
                    (present, "executor_skill_unavailable")
                }
            };
            json!({"requirement":item,"ready":ready,"reason_code":if ready {"ready"} else {code}})
        })
        .collect();
    json!({"ready":checks.iter().all(|c|c["ready"]==true),"checks":checks})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn changed_source_and_missing_binary_are_actionable_without_execution() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.txt");
        std::fs::write(&path, "original").unwrap();
        let requirements = vec![Prerequisite::File {
            path: path.to_string_lossy().into(),
            sha256: digest(b"original"),
        }];
        assert_eq!(inspect(&requirements, &[], None)["ready"], true);
        std::fs::write(&path, "changed").unwrap();
        assert_eq!(
            inspect(&requirements, &[], None)["checks"][0]["reason_code"],
            "source_digest_mismatch"
        );
        let missing = vec![Prerequisite::Binary {
            name: "agentdesk-definitely-absent-prerequisite".into(),
        }];
        assert_eq!(inspect(&missing, &[], None)["ready"], false);
    }
}
