//! A warm process can only serve requests with the settings used to spawn it.

use std::hash::{Hash, Hasher};

use crate::services::provider_auth_profile::{DEFAULT_PROFILE_ID, ProviderAuthOverlay};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OpenCodeServerKey {
    pub(super) bin: String,
    pub(super) working_dir: String,
    pub(super) managed: bool,
    pub(super) managed_identity: Option<super::managed_runtime::Identity>,
    // ProviderAuthOverlay's Debug implementation redacts environment values.
    // Keep this identity internal; health projections must not expose it or a
    // digest of credentials. A profile name alone does not identify a process.
    overlay: ProviderAuthOverlay,
}

impl Hash for OpenCodeServerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bin.hash(state);
        self.working_dir.hash(state);
        self.managed.hash(state);
        self.managed_identity.hash(state);
        self.overlay.profile_id.hash(state);
        self.overlay.provider.as_str().hash(state);
        self.overlay.env.hash(state);
        self.overlay.unset.hash(state);
        self.overlay.home.hash(state);
    }
}

pub(super) fn warm_server_key(
    bin: &str,
    working_dir: &str,
    overlay: &ProviderAuthOverlay,
) -> OpenCodeServerKey {
    let mut overlay = overlay.clone();
    if overlay.profile_id.trim().is_empty() {
        overlay.profile_id = DEFAULT_PROFILE_ID.to_string();
    }
    OpenCodeServerKey {
        bin: bin.to_string(),
        working_dir: std::fs::canonicalize(working_dir)
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|_| working_dir.to_string()),
        overlay,
        managed: false,
        managed_identity: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::services::provider::ProviderKind;

    #[test]
    fn changed_environment_cannot_reuse_same_profile_runtime() {
        let mut overlay = ProviderAuthOverlay::default_for(ProviderKind::OpenCode);
        overlay.profile_id = "local-worker".to_string();
        overlay
            .env
            .insert("OPENCODE_CONFIG_CONTENT".into(), "first-policy".into());
        let mut pool = HashMap::new();
        pool.insert(warm_server_key("opencode", ".", &overlay), "old-runtime");

        assert_eq!(
            pool.get(&warm_server_key("opencode", ".", &overlay)),
            Some(&"old-runtime")
        );
        overlay
            .env
            .insert("OPENCODE_CONFIG_CONTENT".into(), "second-policy".into());
        assert!(!pool.contains_key(&warm_server_key("opencode", ".", &overlay)));
    }

    #[test]
    fn environment_removal_and_home_change_invalidate_runtime() {
        let overlay = ProviderAuthOverlay::default_for(ProviderKind::OpenCode);
        let original = warm_server_key("opencode", ".", &overlay);
        let mut without_inherited = overlay.clone();
        without_inherited
            .unset
            .insert("OPENCODE_CONFIG".to_string());
        assert_ne!(
            original,
            warm_server_key("opencode", ".", &without_inherited)
        );

        let mut different_home = overlay;
        different_home.home = Some("isolated-provider-home".into());
        assert_ne!(original, warm_server_key("opencode", ".", &different_home));
    }

    #[test]
    fn runtime_identity_debug_does_not_reveal_environment_values() {
        let mut overlay = ProviderAuthOverlay::default_for(ProviderKind::OpenCode);
        overlay.env.insert(
            "OPENCODE_CONFIG_CONTENT".into(),
            "private-policy-value".into(),
        );
        let display = format!("{:?}", warm_server_key("opencode", ".", &overlay));
        assert!(!display.contains("private-policy-value"));
        assert!(display.contains("<redacted>"));
    }

    #[test]
    fn managed_launch_cannot_reuse_an_unmanaged_runtime() {
        let overlay = ProviderAuthOverlay::default_for(ProviderKind::OpenCode);
        let legacy = warm_server_key("opencode", ".", &overlay);
        let mut managed = legacy.clone();
        managed.managed = true;
        let pool = HashMap::from([(legacy, "legacy-runtime")]);
        assert!(!pool.contains_key(&managed));
    }
}
