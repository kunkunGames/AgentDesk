//! Managed leases survive a service restart, so their node owner must do so too.
use crate::config::ClusterConfig;

pub(super) fn declared<'a>(
    configured: Option<&'a str>,
    environment: Option<&'a str>,
) -> Option<&'a str> {
    [configured, environment]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

pub(crate) fn require_stable(config: &ClusterConfig, registered: &str) -> Result<(), String> {
    validate(
        config.instance_id.as_deref(),
        std::env::var("AGENTDESK_INSTANCE_ID").ok().as_deref(),
        registered,
    )
}

fn validate(
    configured: Option<&str>,
    environment: Option<&str>,
    registered: &str,
) -> Result<(), String> {
    let declared = declared(configured, environment).ok_or(
        "managed execution requires a persistent cluster.instance_id or AGENTDESK_INSTANCE_ID; hostname/PID fallback cannot own restart-safe leases",
    )?;
    if declared != registered {
        return Err("managed node identity changed after bootstrap; restore the registered identity or restart before admitting work".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_identity_uses_bootstrap_precedence_and_rejects_pid_fallback() {
        assert_eq!(
            declared(Some(" configured "), Some("environment")),
            Some("configured")
        );
        assert_eq!(
            declared(Some(" "), Some(" environment ")),
            Some("environment")
        );
        assert!(validate(None, None, "host-12345").is_err());
        assert!(validate(Some(" "), Some(" "), "single-node").is_err());
        assert!(validate(Some("stable-node"), Some("unused"), "stable-node").is_ok());
        assert!(validate(None, Some("stable-node"), "stable-node").is_ok());
    }

    #[test]
    fn config_reload_cannot_rebind_retained_work_to_another_node() {
        assert!(validate(Some("replacement"), Some("registered"), "registered").is_err());
        assert!(validate(Some("registered"), Some("replacement"), "registered").is_ok());
        assert!(validate(None, Some("replacement"), "registered").is_err());
    }
}
