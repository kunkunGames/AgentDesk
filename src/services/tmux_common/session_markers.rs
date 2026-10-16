//! Session markers.

use super::*;

/// Return whether a provider wrapper was created for the requested auth
/// profile.  A legacy wrapper without a marker is safe to reuse only for the
/// implicit default profile; named profiles must start a fresh provider
/// session rather than silently continue under another account.
pub(crate) fn tmux_session_auth_profile_matches(
    session_name: &str,
    requested_profile_id: &str,
) -> bool {
    let marker = resolve_session_temp_path(session_name, TMUX_AUTH_PROFILE_TEMP_EXT)
        .and_then(|path| std::fs::read_to_string(path).ok())
        .map(|value| value.trim().to_string());
    match marker.as_deref() {
        Some(profile_id) => profile_id == requested_profile_id,
        None => requested_profile_id == crate::services::provider_auth_profile::DEFAULT_PROFILE_ID,
    }
}

/// Persist the selected profile beside a newly-created provider wrapper so warm
/// follow-ups are never routed across account boundaries.
pub(crate) fn write_tmux_session_auth_profile(
    session_name: &str,
    profile_id: &str,
) -> Result<(), String> {
    std::fs::write(
        session_temp_path(session_name, TMUX_AUTH_PROFILE_TEMP_EXT),
        profile_id.trim(),
    )
    .map_err(|error| format!("write tmux auth profile marker: {error}"))
}

pub(crate) fn write_tmux_runtime_kind_marker(
    tmux_session_name: &str,
    runtime_kind: crate::services::agent_protocol::RuntimeHandoffKind,
) -> Result<(), String> {
    with_tmux_source_authority(tmux_session_name, |authority| {
        write_tmux_runtime_kind_marker_under_source_authority(authority, runtime_kind)
    })
}

pub(crate) fn write_tmux_runtime_kind_marker_under_source_authority(
    authority: &TmuxSourceAuthority<'_>,
    runtime_kind: crate::services::agent_protocol::RuntimeHandoffKind,
) -> Result<(), String> {
    use crate::services::agent_protocol::RuntimeHandoffKind;
    if !matches!(runtime_kind, RuntimeHandoffKind::ProcessBackend) {
        ensure_tmux_hosting_admitted_under_source_authority(authority)?;
    } else {
        let current = resolve_tmux_runtime_kind_marker(authority.session());
        if current.is_none() && source_requires_native_preservation_under_authority(authority) {
            return Err("unresolved native or unknown hosting kind requires canonical release before mode change".into());
        }
    }
    let path = session_temp_path(authority.session(), TMUX_RUNTIME_KIND_TEMP_EXT);
    persist_runtime_kind_atomically(Path::new(&path), runtime_kind)
        .map_err(|error| format!("persist runtime hosting kind: {error}"))
}

/// Preparation admission is distinct from any provider input or process spawn.
/// A historical native intent needs a canonical owner release/reset contract;
/// registry absence alone never permits a TUI downgrade or source destruction.
pub(crate) fn require_tmux_hosting_admission(session_name: &str) -> Result<(), String> {
    with_tmux_source_authority(
        session_name,
        ensure_tmux_hosting_admitted_under_source_authority,
    )
}

fn ensure_tmux_hosting_admitted_under_source_authority(
    authority: &TmuxSourceAuthority<'_>,
) -> Result<(), String> {
    if source_requires_native_preservation_under_authority(authority) {
        return Err(
            "native or unknown hosting intent requires canonical release before tmux admission"
                .into(),
        );
    }
    Ok(())
}

pub(crate) fn source_requires_native_preservation_under_authority(
    authority: &TmuxSourceAuthority<'_>,
) -> bool {
    use crate::services::session_backend::{NativeRegistryPresence, process_session_evidence};
    if process_session_evidence(authority.session()).registration != NativeRegistryPresence::Absent
    {
        return true;
    }
    let canonical = session_temp_path(authority.session(), TMUX_RUNTIME_KIND_TEMP_EXT);
    if !matches!(Path::new(&canonical).try_exists(), Ok(false)) {
        return hosting_intent_at_path_requires_preservation(Path::new(&canonical));
    }
    let legacy = legacy_tmp_session_path(authority.session(), TMUX_RUNTIME_KIND_TEMP_EXT);
    hosting_intent_at_path_requires_preservation(Path::new(&legacy))
}

pub(crate) fn hosting_intent_at_path_requires_preservation(path: &Path) -> bool {
    use crate::services::agent_protocol::RuntimeHandoffKind;
    match std::fs::read_to_string(path) {
        Ok(raw) => match RuntimeHandoffKind::from_str(&raw) {
            Some(RuntimeHandoffKind::ProcessBackend) | None => true,
            Some(_) => false,
        },
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

/// Admit native preparation only after the hosting kind survives a Hub crash.
/// The caller's authority prevents same-session recovery or preparation from
/// observing an intermediate marker. The kind deliberately proves no PID,
/// native liveness, descendant quiescence, or permission to adopt/replay work.
pub(crate) fn persist_process_backend_hosting_intent(
    authority: &TmuxSourceAuthority<'_>,
) -> Result<(), String> {
    write_tmux_runtime_kind_marker_under_source_authority(
        authority,
        crate::services::agent_protocol::RuntimeHandoffKind::ProcessBackend,
    )
    .map_err(|error| format!("persist native hosting intent: {error}"))
}

fn persist_runtime_kind_atomically(
    path: &Path,
    kind: crate::services::agent_protocol::RuntimeHandoffKind,
) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("hosting marker parent unavailable"))?;
    // Same-directory exclusive temporary file, so replacement is atomic and
    // failures cannot leave a truncated value which falls back to legacy mode.
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    staged.write_all(kind.as_str().as_bytes())?;
    staged.flush()?;
    #[cfg(test)]
    fail_hosting_intent_write_if_requested(HostingIntentWriteStage::FileSync)?;
    staged.as_file().sync_all()?;
    #[cfg(test)]
    fail_hosting_intent_write_if_requested(HostingIntentWriteStage::Replace)?;
    staged.persist(path).map_err(|error| error.error)?;
    #[cfg(unix)]
    {
        #[cfg(test)]
        fail_hosting_intent_write_if_requested(HostingIntentWriteStage::ParentSync)?;
        // Replacement and its directory entry must be durable before effects.
        crate::services::discord::runtime_store::fsync_parent_dir(path)?;
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HostingIntentWriteStage {
    FileSync,
    Replace,
    ParentSync,
}

#[cfg(test)]
thread_local! {
    static HOSTING_INTENT_WRITE_FAILURE: std::cell::Cell<Option<HostingIntentWriteStage>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn with_hosting_intent_write_failure_for_test<R>(
    stage: HostingIntentWriteStage,
    operation: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<HostingIntentWriteStage>);
    impl Drop for Restore {
        fn drop(&mut self) {
            HOSTING_INTENT_WRITE_FAILURE.with(|fault| fault.set(self.0));
        }
    }
    let _restore = Restore(HOSTING_INTENT_WRITE_FAILURE.with(|fault| fault.replace(Some(stage))));
    operation()
}

#[cfg(test)]
fn fail_hosting_intent_write_if_requested(stage: HostingIntentWriteStage) -> std::io::Result<()> {
    if HOSTING_INTENT_WRITE_FAILURE.with(|fault| fault.get() == Some(stage)) {
        return Err(std::io::Error::other(format!(
            "injected hosting intent {stage:?} failure"
        )));
    }
    Ok(())
}

pub(crate) fn resolve_tmux_runtime_kind_marker(
    tmux_session_name: &str,
) -> Option<crate::services::agent_protocol::RuntimeHandoffKind> {
    let path = resolve_session_temp_path(tmux_session_name, TMUX_RUNTIME_KIND_TEMP_EXT)?;
    let raw = std::fs::read_to_string(path).ok()?;
    crate::services::agent_protocol::RuntimeHandoffKind::from_str(&raw)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::services::agent_protocol::RuntimeHandoffKind;
    use std::io::Read;

    #[test]
    fn native_hosting_marker_atomically_replaces_legacy_source_and_syncs_real_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("owned.runtime-kind");
        std::fs::write(&path, RuntimeHandoffKind::LegacyTmuxWrapper.as_str()).unwrap();
        let mut prior = File::open(&path).unwrap();
        persist_runtime_kind_atomically(&path, RuntimeHandoffKind::ProcessBackend).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            RuntimeHandoffKind::ProcessBackend.as_str()
        );
        let mut prior_bytes = String::new();
        prior.read_to_string(&mut prior_bytes).unwrap();
        assert_eq!(prior_bytes, RuntimeHandoffKind::LegacyTmuxWrapper.as_str());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn durable_native_intent_refuses_late_legacy_stamp_and_generic_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let _root =
            crate::config::TestEnvVarGuard::set_path("AGENTDESK_ROOT_DIR", directory.path());
        let session = format!("AgentDesk-native-intent-{}", uuid::Uuid::new_v4().simple());
        write_tmux_runtime_kind_marker(&session, RuntimeHandoffKind::LegacyTmuxWrapper).unwrap();
        with_tmux_source_authority(&session, |authority| {
            persist_process_backend_hosting_intent(authority)
        })
        .unwrap();
        let output = session_temp_path(&session, "jsonl");
        std::fs::write(&output, "owned source").unwrap();
        assert!(require_tmux_hosting_admission(&session).is_err());
        assert!(
            write_tmux_runtime_kind_marker(&session, RuntimeHandoffKind::LegacyTmuxWrapper)
                .is_err()
        );
        with_tmux_source_authority(&session, |authority| {
            assert!(
                write_tmux_runtime_kind_marker_under_source_authority(
                    authority,
                    RuntimeHandoffKind::CodexTui
                )
                .is_err()
            );
        });
        cleanup_session_temp_files(&session);
        assert_eq!(std::fs::read_to_string(&output).unwrap(), "owned source");
        assert_eq!(
            resolve_tmux_runtime_kind_marker(&session),
            Some(RuntimeHandoffKind::ProcessBackend)
        );
        // Retired adapter markers remain unknown ownership evidence. They
        // cannot authorize a downgrade, a new native owner, or generic cleanup.
        let adapter_session = format!("{session}-adapter");
        let adapter_marker = session_temp_path(&adapter_session, TMUX_RUNTIME_KIND_TEMP_EXT);
        std::fs::write(&adapter_marker, "claude_e_adapter").unwrap();
        assert_eq!(resolve_tmux_runtime_kind_marker(&adapter_session), None);
        assert!(require_tmux_hosting_admission(&adapter_session).is_err());
        for kind in [
            RuntimeHandoffKind::LegacyTmuxWrapper,
            RuntimeHandoffKind::ProcessBackend,
        ] {
            assert!(write_tmux_runtime_kind_marker(&adapter_session, kind).is_err());
        }
        cleanup_session_temp_files(&adapter_session);
        assert_eq!(
            std::fs::read_to_string(&adapter_marker).unwrap(),
            "claude_e_adapter"
        );
    }
}
