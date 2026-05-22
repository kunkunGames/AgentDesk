use regex::Regex;
use std::sync::LazyLock;
use url::Url;

static BEARER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(Authorization:\s*(?:Bearer|Bot)\s+)[^\s]+").unwrap());
static ASSIGNMENT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API[_-]?KEY)[A-Z0-9_]*\s*=\s*)[^\s]+")
        .unwrap()
});
static POSTGRES_DSN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"postgres(?:ql)?://[^\s]+").unwrap());

pub(crate) fn mask_dsn_password(input: &str) -> String {
    let Ok(mut url) = Url::parse(input) else {
        return input.to_string();
    };
    if !matches!(url.scheme(), "postgres" | "postgresql") || url.password().is_none() {
        return input.to_string();
    }
    let _ = url.set_password(Some("***"));
    url.to_string()
}

pub(crate) fn redact_known_secrets(input: &str) -> String {
    let redacted = POSTGRES_DSN_RE.replace_all(input, |captures: &regex::Captures<'_>| {
        mask_dsn_password(captures.get(0).map(|m| m.as_str()).unwrap_or_default())
    });
    let redacted = BEARER_RE.replace_all(&redacted, "${1}***");
    ASSIGNMENT_RE.replace_all(&redacted, "${1}***").into_owned()
}

#[cfg(test)]
mod tests {
    use super::{mask_dsn_password, redact_known_secrets};

    #[test]
    fn mask_dsn_password_redacts_postgres_password() {
        assert_eq!(
            mask_dsn_password("postgres://agent:secret@db.internal:5432/agentdesk"),
            "postgres://agent:***@db.internal:5432/agentdesk"
        );
    }

    #[test]
    fn redact_known_secrets_masks_bearer_bot_and_assignments() {
        let redacted = redact_known_secrets(
            "Authorization: Bearer live-token\nAuthorization: Bot discord-token\nDATABASE_URL=postgres://u:p@h/db\nOPENAI_API_KEY=sk-live",
        );

        assert!(redacted.contains("Authorization: Bearer ***"));
        assert!(redacted.contains("Authorization: Bot ***"));
        assert!(redacted.contains("DATABASE_URL=***"));
        assert!(redacted.contains("OPENAI_API_KEY=***"));
        assert!(!redacted.contains("live-token"));
        assert!(!redacted.contains("discord-token"));
        assert!(!redacted.contains("sk-live"));
    }
}
