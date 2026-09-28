//! Snapshot protected operator config before native startup. The provider auth
//! catalog still accepts credential keys only; it cannot supply startup flags.
use crate::services::platform::managed_process::{self, WorkspaceIdentity};
use crate::services::provider_auth_profile::{
    ProviderAuthOverlay, home_env_for, profile_env_key_allowed,
};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Identity {
    workspace: WorkspaceIdentity,
    executable: [u64; 7],
}

pub(super) fn prepare(
    binary: &str,
    workspace: &str,
    overlay: &ProviderAuthOverlay,
) -> Result<(ProviderAuthOverlay, Identity), String> {
    let workspace = managed_process::inspect_workspace(Path::new(workspace))
        .map_err(|error| format!("managed workspace admission: {error}"))?;
    let home = overlay
        .home
        .as_deref()
        .filter(|_| !overlay.is_default())
        .ok_or("managed OpenCode requires a named protected auth profile")?;
    let expected_home = home_env_for(&overlay.provider, home);
    if overlay.env.iter().any(|(key, value)| {
        !profile_env_key_allowed(&overlay.provider, key) && expected_home.get(key) != Some(value)
    }) {
        return Err(
            "managed OpenCode accepts only resolved profile credentials and home variables".into(),
        );
    }
    let config =
        managed_process::read_protected_config(&home.join("xdg-config/opencode/opencode.json"))
            .map_err(|error| format!("managed OpenCode protected config: {error}"))?;
    let parsed = serde_json::from_str(&config)
        .map_err(|_| "managed OpenCode requires a valid protected JSON config")?;
    super::managed_session::validate_runtime_config(&parsed, None)?;
    let mut resolved = overlay.clone();
    resolved
        .env
        .insert("OPENCODE_CONFIG_CONTENT".into(), config);
    Ok((
        resolved,
        Identity {
            workspace,
            executable: executable_identity(Path::new(binary))?,
        },
    ))
}

#[cfg(target_os = "linux")]
fn executable_identity(binary: &Path) -> Result<[u64; 7], String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(binary)
        .map_err(|error| format!("managed executable identity: {error}"))?;
    Ok([
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime() as u64,
        metadata.mtime_nsec() as u64,
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
    ])
}

#[cfg(not(target_os = "linux"))]
fn executable_identity(_: &Path) -> Result<[u64; 7], String> {
    Err("managed executable identity requires the Linux template".into())
}
