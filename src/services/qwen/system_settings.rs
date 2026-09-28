//! Per-turn tool restrictions extend the operator's native Qwen settings.
//! Replacing the whole settings document would discard model identity and
//! execution limits. The temporary overlay is private and removed on drop.
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct QwenSystemSettingsOverride {
    file: tempfile::NamedTempFile,
    _directory: tempfile::TempDir,
}

impl QwenSystemSettingsOverride {
    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }
}

pub(crate) fn create_system_settings_override(
    allowed_core_tools: Option<&[String]>,
) -> Result<Option<QwenSystemSettingsOverride>, String> {
    let base = std::env::var_os(super::QWEN_CODE_SYSTEM_SETTINGS_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    create_override(allowed_core_tools, base.as_deref())
}

fn create_override(
    allowed_core_tools: Option<&[String]>,
    base: Option<&Path>,
) -> Result<Option<QwenSystemSettingsOverride>, String> {
    let Some(allowed_core_tools) = allowed_core_tools.filter(|tools| !tools.is_empty()) else {
        return Ok(None);
    };
    let mut payload = if let Some(path) = base {
        const MAX_SETTINGS_BYTES: u64 = 1024 * 1024;
        let file = std::fs::File::open(path)
            .map_err(|e| format!("Failed to open configured Qwen system settings: {e}"))?;
        let mut bytes = Vec::new();
        file.take(MAX_SETTINGS_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| format!("Failed to read configured Qwen system settings: {e}"))?;
        if bytes.len() as u64 > MAX_SETTINGS_BYTES {
            return Err("Configured Qwen system settings exceed 1 MiB".into());
        }
        serde_json::from_slice::<Value>(&bytes)
            .map_err(|e| format!("Invalid configured Qwen system settings: {e}"))?
    } else {
        json!({})
    };
    let settings = payload
        .as_object_mut()
        .ok_or("Configured Qwen system settings must be an object")?;
    let tools = settings
        .entry("tools")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or("Configured Qwen tools settings must be an object")?;
    tools.insert("core".into(), json!(allowed_core_tools));
    // Some CLI versions replace their system settings file while loading it.
    // A private parent keeps the overlay private even if that replacement
    // chooses a more permissive file mode.
    let directory = tempfile::Builder::new()
        .prefix("agentdesk-qwen-")
        .tempdir()
        .map_err(|e| format!("Failed to create private Qwen settings directory: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("Failed to protect Qwen settings directory: {e}"))?;
    }
    let mut file = tempfile::Builder::new()
        .prefix("agentdesk-qwen-settings-")
        .suffix(".json")
        .tempfile_in(directory.path())
        .map_err(|e| format!("Failed to create private Qwen settings overlay: {e}"))?;
    serde_json::to_writer_pretty(&mut file, &payload)
        .map_err(|e| format!("Failed to write Qwen settings overlay: {e}"))?;
    file.flush()
        .map_err(|e| format!("Failed to flush Qwen settings overlay: {e}"))?;
    Ok(Some(QwenSystemSettingsOverride {
        file,
        _directory: directory,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restricted_turn_retains_limits_model_and_deny_settings_and_cleans_up() {
        let mut base = tempfile::NamedTempFile::new().unwrap();
        let original = json!({"model": {"maxWallTimeSeconds": 900, "maxToolCalls": 60},
            "modelProviders": {"openai": [{"id": "fixture/model", "envKey": "FIXTURE_KEY_NAME"}]},
            "tools": {"core": ["all"], "exclude": ["agent"]}});
        serde_json::to_writer(&mut base, &original).unwrap();
        let overlay = create_override(Some(&["read_file".into()]), Some(base.path()))
            .unwrap()
            .unwrap();
        let path = overlay.path().to_path_buf();
        let actual: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(actual["model"], original["model"]);
        assert_eq!(actual["modelProviders"], original["modelProviders"]);
        assert_eq!(actual["tools"]["exclude"], original["tools"]["exclude"]);
        assert_eq!(actual["tools"]["core"], json!(["read_file"]));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        let parent = path.parent().unwrap().to_path_buf();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&parent).unwrap().permissions().mode() & 0o077,
                0
            );
            // Simulate a native CLI's atomic rewrite: parent access remains private.
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(
                std::fs::metadata(&parent).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        drop(overlay);
        assert!(!parent.exists());
        assert!(!path.exists());
        assert!(base.path().exists());
    }

    #[test]
    fn invalid_or_missing_operator_settings_cannot_silently_drop_limits() {
        let base = tempfile::NamedTempFile::new().unwrap();
        for contents in ["not json", "[]", "{\"tools\":[]}"] {
            std::fs::write(base.path(), contents).unwrap();
            assert!(create_override(Some(&["read_file".into()]), Some(base.path())).is_err());
        }
        let missing = base.path().with_extension("missing");
        assert!(create_override(Some(&["read_file".into()]), Some(&missing)).is_err());
    }
}
