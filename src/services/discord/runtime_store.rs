use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const AGENTDESK_ROOT_DIR_ENV: &str = "AGENTDESK_ROOT_DIR";
const LEGACY_ROOT_DIR_ENV: &str = "REMOTECC_ROOT_DIR";

pub(super) fn agentdesk_root() -> Option<PathBuf> {
    // Primary: AGENTDESK_ROOT_DIR, fallback: legacy REMOTECC_ROOT_DIR
    for key in [AGENTDESK_ROOT_DIR_ENV, LEGACY_ROOT_DIR_ENV] {
        if let Ok(override_root) = std::env::var(key) {
            let trimmed = override_root.trim();
            if !trimmed.is_empty() {
                return Some(PathBuf::from(trimmed));
            }
        }
    }

    dirs::home_dir().map(|h| h.join(".agentdesk"))
}

pub(super) fn runtime_root() -> Option<PathBuf> {
    agentdesk_root().map(|root| root.join("runtime"))
}

pub(super) fn workspace_root() -> Option<PathBuf> {
    agentdesk_root().map(|root| root.join("workspace"))
}

pub(super) fn worktrees_root() -> Option<PathBuf> {
    agentdesk_root().map(|root| root.join("worktrees"))
}

pub(super) fn bot_settings_path() -> Option<PathBuf> {
    agentdesk_root().map(|root| {
        legacy_fallback(root.join("config").join("bot_settings.json"), root.join("bot_settings.json"))
    })
}

pub(super) fn role_map_path() -> Option<PathBuf> {
    agentdesk_root().map(|root| {
        legacy_fallback(root.join("config").join("role_map.json"), root.join("role_map.json"))
    })
}

pub(super) fn org_schema_path() -> Option<PathBuf> {
    agentdesk_root().map(|root| {
        legacy_fallback(root.join("config").join("org.yaml"), root.join("org.yaml"))
    })
}

pub(super) fn discord_uploads_root() -> Option<PathBuf> {
    runtime_root().map(|root| root.join("discord_uploads"))
}

pub(super) fn discord_inflight_root() -> Option<PathBuf> {
    runtime_root().map(|root| root.join("discord_inflight"))
}

pub(super) fn discord_restart_reports_root() -> Option<PathBuf> {
    runtime_root().map(|root| root.join("discord_restart_reports"))
}

pub(super) fn discord_pending_queue_root() -> Option<PathBuf> {
    runtime_root().map(|root| root.join("discord_pending_queue"))
}

pub(super) fn discord_handoff_root() -> Option<PathBuf> {
    runtime_root().map(|root| root.join("discord_handoff"))
}

pub(super) fn shared_agent_memory_root() -> Option<PathBuf> {
    agentdesk_root().map(|root| root.join("shared_agent_memory"))
}

/// Path to the generation counter file.
pub fn generation_path() -> Option<PathBuf> {
    agentdesk_root().map(|root| {
        legacy_fallback(root.join("runtime").join("generation"), root.join("generation"))
    })
}

/// Load the current generation counter (returns 0 if file missing/corrupt).
pub fn load_generation() -> u64 {
    generation_path()
        .and_then(|p| fs::read_to_string(p).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// Increment the generation counter and return the new value.
pub fn increment_generation() -> u64 {
    let current = load_generation();
    let next = current + 1;
    if let Some(path) = generation_path() {
        let _ = atomic_write(&path, &next.to_string());
    }
    next
}

pub(super) fn last_message_root() -> Option<PathBuf> {
    runtime_root().map(|root| root.join("last_message"))
}

/// Save the last processed message ID for a channel.
pub(super) fn save_last_message_id(provider: &str, channel_id: u64, message_id: u64) {
    let Some(root) = last_message_root() else { return };
    let dir = root.join(provider);
    let _ = fs::create_dir_all(&dir);
    let path = dir.join(format!("{}.txt", channel_id));
    let _ = atomic_write(&path, &message_id.to_string());
}

/// Save all last_message_ids from a map (used during SIGTERM).
pub(super) fn save_all_last_message_ids(provider: &str, ids: &std::collections::HashMap<u64, u64>) {
    for (channel_id, message_id) in ids {
        save_last_message_id(provider, *channel_id, *message_id);
    }
}

/// Shared mutex for tests that manipulate AGENTDESK_ROOT_DIR env var.
/// All test modules must use this to avoid env var races.
#[cfg(test)]
pub(super) fn test_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

pub(super) fn atomic_write(path: &Path, data: &str) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp).map_err(|e| e.to_string())?;
    file.write_all(data.as_bytes()).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    fs::rename(&tmp, path).map_err(|e| e.to_string())
}

/// Temporary helper for folder restructuring migration.
/// Returns new_path if it exists, otherwise legacy_path if it exists, otherwise new_path.
/// TODO: Remove after 2026-03-26 (legacy fallback cleanup)
fn legacy_fallback(new_path: PathBuf, legacy_path: PathBuf) -> PathBuf {
    if new_path.exists() {
        new_path
    } else if legacy_path.exists() {
        legacy_path
    } else {
        new_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Acquire the shared env lock to avoid races between tests that mutate
    /// AGENTDESK_ROOT_DIR.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        test_env_lock().lock().unwrap()
    }

    #[test]
    fn test_agentdesk_root_env_override() {
        let _lock = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let override_path = tmp.path().join("custom_root");
        fs::create_dir_all(&override_path).unwrap();

        let prev_legacy = std::env::var_os(LEGACY_ROOT_DIR_ENV);
        unsafe { std::env::remove_var(LEGACY_ROOT_DIR_ENV) };
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, override_path.to_str().unwrap()) };
        let root = agentdesk_root().expect("should return Some");
        assert_eq!(root, override_path);

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
        if let Some(v) = prev_legacy { unsafe { std::env::set_var(LEGACY_ROOT_DIR_ENV, v) }; }
    }

    #[test]
    fn test_agentdesk_root_env_empty_falls_back() {
        let _lock = env_lock();
        let prev_legacy = std::env::var_os(LEGACY_ROOT_DIR_ENV);
        unsafe { std::env::remove_var(LEGACY_ROOT_DIR_ENV) };
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, "   ") };
        // Empty/whitespace-only override should fall back to home-based default
        let root = agentdesk_root().expect("should return Some");
        let expected = dirs::home_dir().unwrap().join(".agentdesk");
        assert_eq!(root, expected);

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
        if let Some(v) = prev_legacy { unsafe { std::env::set_var(LEGACY_ROOT_DIR_ENV, v) }; }
    }

    #[test]
    fn test_bot_settings_path_prefers_new_location() {
        let _lock = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, root.to_str().unwrap()) };

        // Create both new and legacy paths
        let new_path = root.join("config").join("bot_settings.json");
        let legacy_path = root.join("bot_settings.json");
        fs::create_dir_all(new_path.parent().unwrap()).unwrap();
        fs::write(&new_path, "new").unwrap();
        fs::write(&legacy_path, "legacy").unwrap();

        let result = bot_settings_path().expect("should return Some");
        assert_eq!(result, new_path, "Should prefer config/ path when both exist");

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
    }

    #[test]
    fn test_legacy_fallback_when_new_missing() {
        let _lock = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, root.to_str().unwrap()) };

        // Create only legacy path (no config/ directory)
        let legacy_path = root.join("bot_settings.json");
        fs::write(&legacy_path, "legacy").unwrap();

        let result = bot_settings_path().expect("should return Some");
        assert_eq!(result, legacy_path, "Should fall back to legacy path when new path doesn't exist");

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
    }

    #[test]
    fn test_runtime_paths_consistent() {
        let _lock = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, root.to_str().unwrap()) };

        // All path functions should return paths under the root
        let paths: Vec<(&str, Option<PathBuf>)> = vec![
            ("runtime_root", runtime_root()),
            ("workspace_root", workspace_root()),
            ("worktrees_root", worktrees_root()),
            ("discord_uploads_root", discord_uploads_root()),
            ("discord_inflight_root", discord_inflight_root()),
            ("discord_restart_reports_root", discord_restart_reports_root()),
            ("discord_pending_queue_root", discord_pending_queue_root()),
            ("discord_handoff_root", discord_handoff_root()),
            ("shared_agent_memory_root", shared_agent_memory_root()),
            ("last_message_root", last_message_root()),
        ];

        for (name, path) in paths {
            let p = path.unwrap_or_else(|| panic!("{} should return Some", name));
            assert!(
                p.starts_with(&root),
                "{} path {:?} should be under root {:?}",
                name, p, root,
            );
        }

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
    }

    #[test]
    fn test_generation_roundtrip() {
        let _lock = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, root.to_str().unwrap()) };

        // generation_path uses legacy_fallback — create the runtime dir
        let runtime_dir = root.join("runtime");
        fs::create_dir_all(&runtime_dir).unwrap();

        // Initially 0 (file missing)
        assert_eq!(load_generation(), 0);

        // Increment should return 1
        assert_eq!(increment_generation(), 1);
        assert_eq!(load_generation(), 1);

        // Increment again
        assert_eq!(increment_generation(), 2);
        assert_eq!(load_generation(), 2);

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
    }

    #[test]
    fn test_fallback_returns_new_when_neither_exists() {
        let _lock = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        unsafe { std::env::set_var(AGENTDESK_ROOT_DIR_ENV, root.to_str().unwrap()) };

        // Neither config/bot_settings.json nor bot_settings.json exists
        let result = bot_settings_path().expect("should return Some");
        let expected_new = root.join("config").join("bot_settings.json");
        assert_eq!(result, expected_new, "Should return new path when neither exists");

        unsafe { std::env::remove_var(AGENTDESK_ROOT_DIR_ENV) };
    }
}
