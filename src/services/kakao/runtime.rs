//! A fixed credential owner, independent of cluster leader election.
use crate::config::ClusterConfig;
use crate::services::cluster::node_registry::resolve_self_instance_id_without_config;

use super::KakaoError;

const OWNER_ENV: &str = "AGENTDESK_KAKAO_CREDENTIAL_OWNER_INSTANCE_ID";
const OWNER_ERROR: &str = "Kakao requires its explicitly configured credential-owning node";

pub(crate) fn require_calendar_owner(config: &ClusterConfig) -> Result<(), KakaoError> {
    let owner = std::env::var(OWNER_ENV).ok();
    let environment_id = std::env::var("AGENTDESK_INSTANCE_ID").ok();
    let declared = [config.instance_id.as_deref(), environment_id.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|id| !id.is_empty());
    validate_calendar_owner(
        config.enabled,
        owner.as_deref(),
        declared,
        &resolve_self_instance_id_without_config(),
    )
}

// Also fence message access to the shared token runtime when an owner is pinned.
pub(crate) fn require_pinned_owner() -> Result<(), KakaoError> {
    let Ok(owner) = std::env::var(OWNER_ENV) else {
        return Ok(());
    };
    validate_pin(&owner, &resolve_self_instance_id_without_config())
}

fn validate_pin(owner: &str, registered: &str) -> Result<(), KakaoError> {
    if owner.trim().is_empty() || owner.trim() != registered {
        return Err(KakaoError::InvalidConfiguration(OWNER_ERROR));
    }
    Ok(())
}

fn validate_calendar_owner(
    clustered: bool,
    owner: Option<&str>,
    declared: Option<&str>,
    registered: &str,
) -> Result<(), KakaoError> {
    if let Some(owner) = owner {
        validate_pin(owner, registered)?;
    }
    if clustered && (owner.is_none() || declared != Some(registered)) {
        return Err(KakaoError::InvalidConfiguration(OWNER_ERROR));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_admits_only_explicit_persistent_owner() {
        assert!(validate_calendar_owner(true, Some("mac"), Some("mac"), "mac").is_ok());
        for (owner, declared, registered) in [
            (None, Some("mac"), "mac"),
            (Some(""), Some("mac"), "mac"),
            (Some("mac"), Some("windows"), "windows"),
            (Some("mac"), None, "mac"),
            (Some("mac"), Some("new-id"), "mac"),
        ] {
            assert!(validate_calendar_owner(true, owner, declared, registered).is_err());
        }
    }

    #[test]
    fn standalone_remains_supported_and_explicit_pin_never_fails_over() {
        assert!(validate_calendar_owner(false, None, None, "single-node").is_ok());
        assert!(validate_calendar_owner(false, Some("other"), None, "single-node").is_err());
        assert!(validate_pin("mac", "windows").is_err());
        assert!(validate_pin(" ", "windows").is_err());
    }
}
