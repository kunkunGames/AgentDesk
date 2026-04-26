use std::path::{Path, PathBuf};

/// `~/.adk/{env}/config/provider-cli-registry.json`
pub fn registry_path(root: &Path) -> PathBuf {
    root.join("config").join("provider-cli-registry.json")
}

/// `~/.adk/{env}/state/provider-cli-migration-{provider}.json`
pub fn migration_state_path(root: &Path, provider: &str) -> PathBuf {
    root.join("state")
        .join(format!("provider-cli-migration-{provider}.json"))
}

/// `~/.adk/{env}/runtime/provider-cli-launch/{session_key}.json`
pub fn launch_artifact_path(root: &Path, session_key: &str) -> PathBuf {
    root.join("runtime")
        .join("provider-cli-launch")
        .join(format!("{}.json", sanitize_file_component(session_key)))
}

/// `~/.adk/{env}/runtime/provider-cli-diagnostics/{timestamp}.json`
pub fn diagnostics_snapshot_path(root: &Path, timestamp_ms: u128) -> PathBuf {
    root.join("runtime")
        .join("provider-cli-diagnostics")
        .join(format!("{timestamp_ms}.json"))
}

/// `~/.adk/{env}/runtime/provider-cli-smoke/{provider}-{channel}.json`
pub fn smoke_result_path(root: &Path, provider: &str, channel: &str) -> PathBuf {
    root.join("runtime")
        .join("provider-cli-smoke")
        .join(format!("{provider}-{channel}.json"))
}

fn sanitize_file_component(raw: &str) -> String {
    let mut component: String = raw
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    while component.starts_with('.') {
        component.remove(0);
    }
    if component.is_empty() {
        "default".to_string()
    } else {
        component
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_artifact_path_sanitizes_session_key_component() {
        let root = Path::new("/tmp/adk-root");
        let path = launch_artifact_path(root, "../codex/live session");

        assert_eq!(
            path,
            root.join("runtime")
                .join("provider-cli-launch")
                .join("codex_live_session.json")
        );
    }
}
