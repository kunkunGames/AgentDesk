//! Settled-effect attestation requires an explicitly configured admin Bearer.
//! Ambient loopback access and auth-less installs cannot authorize it. This is
//! shared server-admin authority, not an independently isolated operator role.
use crate::{
    config::Config,
    error::{AppError, AppResult, ErrorCode},
};
use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};

pub(super) fn require(config: &Config, headers: &HeaderMap) -> AppResult<()> {
    require_for(config, headers, "settled external effects")
}

pub(super) fn require_finish_recovery(config: &Config, headers: &HeaderMap) -> AppResult<()> {
    require_for(config, headers, "failed final verifier recovery")
}

fn require_for(config: &Config, headers: &HeaderMap, purpose: &str) -> AppResult<()> {
    let valid = config
        .server
        .auth_token
        .as_deref()
        .filter(|s| !s.is_empty())
        .zip(
            headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer ")),
        )
        .is_some_and(|(expected, supplied)| {
            crate::utils::auth::constant_time_token_eq(expected, supplied)
        });
    if !valid {
        return Err(AppError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Policy,
            format!("{purpose} require an explicit configured server administrator Bearer token"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settled_effect_auth_requires_configured_explicit_admin_bearer() {
        let mut config = Config::default();
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test-admin".parse().unwrap());
        for token in [None, Some("")] {
            config.server.auth_token = token.map(str::to_owned);
            assert!(require(&config, &headers).is_err());
            assert!(require_finish_recovery(&config, &headers).is_err());
        }
        config.server.auth_token = Some("test-admin".into());
        assert!(require(&config, &headers).is_ok());
        assert!(require_finish_recovery(&config, &headers).is_ok());
        for value in [
            None,
            Some("Bearer wrong"),
            Some("Basic test-admin"),
            Some("Bearer test-admin "),
        ] {
            headers.remove("authorization");
            if let Some(value) = value {
                headers.insert("authorization", value.parse().unwrap());
            }
            headers.insert("origin", "http://127.0.0.1:8791".parse().unwrap());
            headers.insert("referer", "http://localhost:8791".parse().unwrap());
            headers.insert("x-agent-id", "operator".parse().unwrap());
            assert!(require(&config, &headers).is_err());
            assert!(require_finish_recovery(&config, &headers).is_err());
        }
    }
}
