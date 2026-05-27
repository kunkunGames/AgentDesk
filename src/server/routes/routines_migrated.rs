use serde_json::Value;

use crate::error::{AppError, AppResult};
use crate::services::operator_connectors::{
    OptionalConnectorState, optional_connector_status_by_id,
};

pub(super) fn is_migrated_launchd_script_ref(script_ref: &str) -> bool {
    script_ref.starts_with("migrated-launchd/")
}

pub(super) fn validate_migrated_launchd_activation(
    routine: &crate::services::routines::store::RoutineRecord,
    metadata: Option<&Value>,
    routine_dirs: &[std::path::PathBuf],
) -> AppResult<()> {
    if !is_migrated_launchd_script_ref(&routine.script_ref) {
        return Ok(());
    }
    validate_migrated_launchd_entrypoint(&routine.script_ref, metadata, routine_dirs)?;
    validate_migrated_launchd_required_paths(
        &routine.script_ref,
        metadata,
        routine.checkpoint.as_ref(),
    )?;
    validate_migrated_launchd_required_connectors(
        &routine.script_ref,
        metadata,
        routine.checkpoint.as_ref(),
    )
}

fn validate_migrated_launchd_entrypoint(
    script_ref: &str,
    metadata: Option<&Value>,
    routine_dirs: &[std::path::PathBuf],
) -> AppResult<()> {
    let relative = migrated_launchd_entrypoint_relative_path(script_ref, metadata)?;
    let mut candidates = Vec::new();
    if let Some(root) = crate::config::runtime_root() {
        push_entrypoint_candidate(&mut candidates, root.join(&relative));
    }
    if let Ok(cwd) = std::env::current_dir() {
        push_entrypoint_candidate(&mut candidates, cwd.join(&relative));
    }
    for dir in routine_dirs {
        push_entrypoint_candidate(&mut candidates, dir.join(&relative));
        if let Some(parent) = dir.parent() {
            push_entrypoint_candidate(&mut candidates, parent.join(&relative));
        }
    }
    if candidates.iter().any(|candidate| candidate.is_file()) {
        return Ok(());
    }
    let checked = candidates
        .iter()
        .map(|candidate| candidate.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(AppError::conflict(format!(
        "migrated routine {script_ref} is invalid: shell entrypoint not found ({checked})"
    )))
}

fn push_entrypoint_candidate(
    candidates: &mut Vec<std::path::PathBuf>,
    candidate: std::path::PathBuf,
) {
    if !candidates.iter().any(|existing| existing == &candidate) {
        candidates.push(candidate);
    }
}

fn migrated_launchd_entrypoint_relative_path(
    script_ref: &str,
    metadata: Option<&Value>,
) -> AppResult<std::path::PathBuf> {
    for pointer in ["/migrated_launchd/entrypoint", "/portable/entrypoint"] {
        let Some(value) = metadata.and_then(|metadata| metadata.pointer(pointer)) else {
            continue;
        };
        let raw = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AppError::conflict(format!(
                    "migrated routine metadata field {pointer} must be a non-empty string"
                ))
            })?;
        return validate_relative_entrypoint_path(raw);
    }

    let stem = std::path::Path::new(script_ref)
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            AppError::conflict(format!(
                "migrated routine {script_ref} is invalid: script_ref has no file stem"
            ))
        })?;
    Ok(std::path::PathBuf::from("scripts")
        .join("launchd-migrated")
        .join(format!("{stem}.sh")))
}

fn validate_relative_entrypoint_path(raw_path: &str) -> AppResult<std::path::PathBuf> {
    let normalized = raw_path.trim().replace('\\', "/");
    let path = std::path::PathBuf::from(&normalized);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::Prefix(_)
                    | std::path::Component::RootDir
            )
        })
    {
        return Err(AppError::conflict(
            "migrated routine metadata entrypoint must be a repo-relative path",
        ));
    }
    Ok(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredPathKind {
    Any,
    File,
    Dir,
}

impl RequiredPathKind {
    fn from_value(value: Option<&Value>) -> AppResult<Self> {
        let Some(value) = value else {
            return Ok(Self::Any);
        };
        let Some(kind) = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Err(AppError::conflict(
                "migrated routine required path kind must be a non-empty string",
            ));
        };
        match kind {
            "any" | "path" => Ok(Self::Any),
            "file" => Ok(Self::File),
            "dir" | "directory" => Ok(Self::Dir),
            other => Err(AppError::conflict(format!(
                "unsupported migrated routine required path kind '{other}'"
            ))),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Any => "path",
            Self::File => "file",
            Self::Dir => "directory",
        }
    }

    fn exists(self, path: &std::path::Path) -> bool {
        match self {
            Self::Any => path.exists(),
            Self::File => path.is_file(),
            Self::Dir => path.is_dir(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequiredPath {
    label: String,
    path: std::path::PathBuf,
    kind: RequiredPathKind,
}

fn validate_migrated_launchd_required_paths(
    script_ref: &str,
    metadata: Option<&Value>,
    checkpoint: Option<&Value>,
) -> AppResult<()> {
    let mut required_paths = migrated_launchd_builtin_required_paths(script_ref)?;
    required_paths.extend(migrated_required_paths_from_metadata(metadata)?);
    required_paths.extend(migrated_required_paths_from_checkpoint(checkpoint)?);
    for required in required_paths {
        if required.kind.exists(&required.path) {
            continue;
        }
        return Err(AppError::conflict(format!(
            "migrated routine {script_ref} is invalid: required {} '{}' not found: {}",
            required.kind.label(),
            required.label,
            required.path.display()
        )));
    }
    Ok(())
}

fn migrated_launchd_builtin_required_paths(script_ref: &str) -> AppResult<Vec<RequiredPath>> {
    let mut paths = Vec::new();
    if matches!(
        script_ref,
        "migrated-launchd/memento-daily-report.js"
            | "migrated-launchd/memento-hygiene.js"
            | "migrated-launchd/memory-merge.js"
    ) {
        let workdir = migrated_env_path("AGENTDESK_MIGRATED_AGENTFACTORY_WORKDIR")
            .or_else(|| {
                crate::config::runtime_root()
                    .map(|root| root.join("workspaces").join("agentfactory"))
            })
            .ok_or_else(|| {
                AppError::conflict(format!(
                    "migrated routine {script_ref} is invalid: AGENTDESK_MIGRATED_AGENTFACTORY_WORKDIR is unavailable"
                ))
            })?;
        paths.push(RequiredPath {
            label: "AGENTDESK_MIGRATED_AGENTFACTORY_WORKDIR".to_string(),
            path: workdir,
            kind: RequiredPathKind::Dir,
        });
    }
    if script_ref == "migrated-launchd/memory-merge.js" {
        let skill = migrated_env_path("AGENTDESK_MEMORY_MERGE_SKILL")
            .or_else(|| {
                crate::config::runtime_root()
                    .map(|root| root.join("skills").join("memory-merge").join("SKILL.md"))
            })
            .ok_or_else(|| {
                AppError::conflict(
                    "migrated routine memory-merge is invalid: AGENTDESK_MEMORY_MERGE_SKILL is unavailable",
                )
            })?;
        paths.push(RequiredPath {
            label: "AGENTDESK_MEMORY_MERGE_SKILL".to_string(),
            path: skill,
            kind: RequiredPathKind::File,
        });
    }
    Ok(paths)
}

fn migrated_required_paths_from_checkpoint(
    checkpoint: Option<&Value>,
) -> AppResult<Vec<RequiredPath>> {
    migrated_required_paths_from_value("checkpoint", checkpoint)
}

fn migrated_required_paths_from_metadata(metadata: Option<&Value>) -> AppResult<Vec<RequiredPath>> {
    migrated_required_paths_from_value("metadata", metadata)
}

fn migrated_required_paths_from_value(
    source: &str,
    value: Option<&Value>,
) -> AppResult<Vec<RequiredPath>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let mut paths = Vec::new();
    for pointer in [
        "/migrated_launchd/required_paths",
        "/portable/required_paths",
    ] {
        let Some(value) = value.pointer(pointer) else {
            continue;
        };
        let Some(entries) = value.as_array() else {
            return Err(AppError::conflict(format!(
                "migrated routine {source} field {pointer} must be an array"
            )));
        };
        for entry in entries {
            if let Some(required) = parse_migrated_required_path(entry)? {
                paths.push(required);
            }
        }
    }
    Ok(paths)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequiredConnector {
    id: String,
    label: String,
}

fn validate_migrated_launchd_required_connectors(
    script_ref: &str,
    metadata: Option<&Value>,
    checkpoint: Option<&Value>,
) -> AppResult<()> {
    let mut required_connectors = migrated_required_connectors_from_value("metadata", metadata)?;
    required_connectors.extend(migrated_required_connectors_from_value(
        "checkpoint",
        checkpoint,
    )?);
    for required in required_connectors {
        let Some(status) = optional_connector_status_by_id(&required.id) else {
            return Err(AppError::conflict(format!(
                "migrated routine {script_ref} is invalid: unsupported required connector '{}'",
                required.id
            )));
        };
        if status.state == OptionalConnectorState::Ready {
            continue;
        }
        let setup = if status.setup_actions.is_empty() {
            String::new()
        } else {
            format!(" setup: {}", status.setup_actions.join(" | "))
        };
        return Err(AppError::conflict(format!(
            "migrated routine {script_ref} is invalid: required connector '{}' is {}: {}{}",
            required.label,
            status.state.as_str(),
            status.detail,
            setup
        )));
    }
    Ok(())
}

fn migrated_required_connectors_from_value(
    source: &str,
    value: Option<&Value>,
) -> AppResult<Vec<RequiredConnector>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let mut connectors = Vec::new();
    for pointer in [
        "/migrated_launchd/required_connectors",
        "/portable/required_connectors",
    ] {
        let Some(value) = value.pointer(pointer) else {
            continue;
        };
        let Some(entries) = value.as_array() else {
            return Err(AppError::conflict(format!(
                "migrated routine {source} field {pointer} must be an array"
            )));
        };
        for entry in entries {
            if let Some(required) = parse_migrated_required_connector(entry)? {
                connectors.push(required);
            }
        }
    }
    Ok(connectors)
}

fn parse_migrated_required_connector(value: &Value) -> AppResult<Option<RequiredConnector>> {
    if let Some(id) = value.as_str() {
        let id = id.trim();
        if id.is_empty() {
            return Err(AppError::conflict(
                "migrated routine required connector must not be empty",
            ));
        }
        return Ok(Some(RequiredConnector {
            id: id.to_string(),
            label: id.to_string(),
        }));
    }

    let Some(object) = value.as_object() else {
        return Err(AppError::conflict(
            "migrated routine required connector must be a string or object",
        ));
    };
    if object
        .get("required")
        .and_then(Value::as_bool)
        .is_some_and(|required| !required)
    {
        return Ok(None);
    }
    let id = object
        .get("id")
        .or_else(|| object.get("connector"))
        .or_else(|| object.get("capability"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            AppError::conflict("migrated routine required connector object needs a non-empty id")
        })?;
    let label = object
        .get("label")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(id);
    Ok(Some(RequiredConnector {
        id: id.to_string(),
        label: label.to_string(),
    }))
}

fn parse_migrated_required_path(value: &Value) -> AppResult<Option<RequiredPath>> {
    if let Some(path) = value.as_str() {
        let raw_path = path.trim();
        if raw_path.is_empty() {
            return Err(AppError::conflict(
                "migrated routine required path must not be empty",
            ));
        }
        return Ok(Some(RequiredPath {
            label: raw_path.to_string(),
            path: expand_migrated_required_path(raw_path)?,
            kind: RequiredPathKind::Any,
        }));
    }

    let Some(object) = value.as_object() else {
        return Err(AppError::conflict(
            "migrated routine required path must be a string or object",
        ));
    };
    if object
        .get("required")
        .and_then(Value::as_bool)
        .is_some_and(|required| !required)
    {
        return Ok(None);
    }
    let raw_path = object
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            AppError::conflict("migrated routine required path object needs a non-empty path")
        })?;
    let label = object
        .get("label")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .unwrap_or(raw_path);
    Ok(Some(RequiredPath {
        label: label.to_string(),
        path: expand_migrated_required_path(raw_path)?,
        kind: RequiredPathKind::from_value(object.get("kind"))?,
    }))
}

fn expand_migrated_required_path(raw_path: &str) -> AppResult<std::path::PathBuf> {
    let trimmed = raw_path.trim();
    if trimmed == "~" {
        return dirs::home_dir().ok_or_else(|| {
            AppError::conflict("migrated routine required path uses ~ but home is unavailable")
        });
    }
    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = dirs::home_dir().ok_or_else(|| {
            AppError::conflict("migrated routine required path uses ~ but home is unavailable")
        })?;
        return Ok(home.join(rest));
    }
    if let Some(rest) = trimmed.strip_prefix("${")
        && let Some((name, suffix)) = rest.split_once('}')
    {
        return expand_migrated_env_required_path(name, suffix);
    }
    if let Some(rest) = trimmed.strip_prefix('$') {
        let name_len = rest
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(char::len_utf8)
            .sum::<usize>();
        if name_len > 0 {
            let (name, suffix) = rest.split_at(name_len);
            return expand_migrated_env_required_path(name, suffix);
        }
    }
    Ok(std::path::PathBuf::from(trimmed))
}

fn expand_migrated_env_required_path(
    env_name: &str,
    suffix: &str,
) -> AppResult<std::path::PathBuf> {
    let base = migrated_env_path(env_name).ok_or_else(|| {
        AppError::conflict(format!(
            "migrated routine required path references unset env var {env_name}"
        ))
    })?;
    let suffix = suffix.trim_start_matches(['/', '\\']);
    if suffix.is_empty() {
        Ok(base)
    } else {
        Ok(base.join(suffix))
    }
}

fn migrated_env_path(env_name: &str) -> Option<std::path::PathBuf> {
    std::env::var_os(env_name)
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;
    use std::sync::{LazyLock, Mutex, MutexGuard};

    use super::{
        validate_migrated_launchd_entrypoint, validate_migrated_launchd_required_connectors,
        validate_migrated_launchd_required_paths,
    };

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner())
    }

    #[test]
    fn migrated_launchd_entrypoint_validation_uses_repo_relative_fallback() {
        validate_migrated_launchd_entrypoint("migrated-launchd/memory-merge.js", None, &[])
            .expect("repo checkout includes migrated shell entrypoint");
    }

    #[test]
    fn migrated_launchd_entrypoint_validation_uses_metadata_override() {
        let metadata = json!({
            "migrated_launchd": {
                "entrypoint": "scripts/queue-stability-batch.sh"
            }
        });

        validate_migrated_launchd_entrypoint(
            "migrated-launchd/queue-stability-batch.js",
            Some(&metadata),
            &[],
        )
        .expect("queue stability routine uses a repo-local non-launchd-migrated entrypoint");
    }

    #[test]
    fn migrated_launchd_entrypoint_validation_uses_configured_routine_dirs() {
        let temp = tempfile::tempdir().unwrap();
        let routine_dir = temp.path().join("routines");
        let entrypoint_dir = temp.path().join("scripts").join("launchd-migrated");
        std::fs::create_dir_all(&routine_dir).unwrap();
        std::fs::create_dir_all(&entrypoint_dir).unwrap();
        std::fs::write(
            entrypoint_dir.join("operator-only-test-entrypoint.sh"),
            "#!/bin/sh\n",
        )
        .unwrap();
        let metadata = json!({
            "migrated_launchd": {
                "entrypoint": "scripts/launchd-migrated/operator-only-test-entrypoint.sh"
            }
        });

        validate_migrated_launchd_entrypoint(
            "migrated-launchd/operator-only-test-entrypoint.js",
            Some(&metadata),
            &[routine_dir],
        )
        .expect("entrypoint beside configured routine directory should be accepted");
    }

    #[test]
    fn migrated_launchd_required_path_validation_rejects_missing_path() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing-vault");
        let checkpoint = json!({
            "migrated_launchd": {
                "required_paths": [
                    { "label": "operator vault", "path": missing, "kind": "dir" }
                ]
            }
        });

        let err = validate_migrated_launchd_required_paths(
            "migrated-launchd/agent-feedback-briefing.js",
            None,
            Some(&checkpoint),
        )
        .expect_err("missing required path must block migrated routine enablement");

        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert!(
            err.message().contains(
                "migrated routine migrated-launchd/agent-feedback-briefing.js is invalid"
            )
        );
        assert!(
            err.message()
                .contains("required directory 'operator vault' not found")
        );
    }

    #[test]
    fn migrated_launchd_required_path_validation_accepts_existing_file() {
        let temp = tempfile::tempdir().unwrap();
        let prompt = temp.path().join("prompt.md");
        std::fs::write(&prompt, "prompt").unwrap();
        let checkpoint = json!({
            "portable": {
                "required_paths": [
                    { "label": "operator prompt", "path": prompt, "kind": "file" },
                    { "label": "optional legacy path", "path": temp.path().join("missing"), "required": false }
                ]
            }
        });

        validate_migrated_launchd_required_paths(
            "migrated-launchd/agent-feedback-briefing.js",
            None,
            Some(&checkpoint),
        )
        .expect("existing required file should pass");
    }

    #[test]
    fn migrated_launchd_required_connector_validation_blocks_missing_connector() {
        let _lock = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing-skills");
        let previous = std::env::var_os("AGENTDESK_OBSIDIAN_SKILL_ROOT");
        unsafe { std::env::set_var("AGENTDESK_OBSIDIAN_SKILL_ROOT", &missing) };
        let metadata = json!({
            "migrated_launchd": {
                "required_connectors": ["obsidian_skill_root"]
            }
        });

        let err = validate_migrated_launchd_required_connectors(
            "migrated-launchd/ai-integrated-briefing.js",
            Some(&metadata),
            None,
        )
        .expect_err("missing required connector must block enablement");

        match previous {
            Some(value) => unsafe { std::env::set_var("AGENTDESK_OBSIDIAN_SKILL_ROOT", value) },
            None => unsafe { std::env::remove_var("AGENTDESK_OBSIDIAN_SKILL_ROOT") },
        }
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert!(
            err.message()
                .contains("required connector 'obsidian_skill_root' is missing_path")
        );
    }

    #[test]
    fn migrated_launchd_required_connector_validation_accepts_ready_connector() {
        let _lock = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let previous = std::env::var_os("AGENTDESK_OBSIDIAN_SKILL_ROOT");
        unsafe { std::env::set_var("AGENTDESK_OBSIDIAN_SKILL_ROOT", temp.path()) };
        let metadata = json!({
            "migrated_launchd": {
                "required_connectors": [
                    { "id": "obsidian_skill_root", "label": "Obsidian skills" }
                ]
            }
        });

        validate_migrated_launchd_required_connectors(
            "migrated-launchd/ai-integrated-briefing.js",
            Some(&metadata),
            None,
        )
        .expect("ready connector should pass validation");

        match previous {
            Some(value) => unsafe { std::env::set_var("AGENTDESK_OBSIDIAN_SKILL_ROOT", value) },
            None => unsafe { std::env::remove_var("AGENTDESK_OBSIDIAN_SKILL_ROOT") },
        }
    }
}
