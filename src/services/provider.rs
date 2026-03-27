use crate::utils::format::safe_prefix;
use std::process::Command;

/// Tmux session name prefix — always "AgentDesk".
pub const TMUX_SESSION_PREFIX: &str = "AgentDesk";

/// Tmux session name suffix for dev/release isolation.
/// Dev environment (`~/.adk/dev`) appends "-dev"; release has no suffix.
pub fn tmux_env_suffix() -> &'static str {
    use std::sync::OnceLock;
    static SUFFIX: OnceLock<String> = OnceLock::new();
    SUFFIX.get_or_init(|| match std::env::var("AGENTDESK_ROOT_DIR").ok() {
        Some(root) if root.contains(".adk/dev") => "-dev".to_string(),
        _ => String::new(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    Claude,
    Codex,
    Unsupported(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCapabilities {
    pub binary_name: &'static str,
    pub supports_structured_output: bool,
    pub supports_resume: bool,
    pub supports_tool_stream: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderRuntimeProbe {
    pub provider: ProviderKind,
    pub capabilities: ProviderCapabilities,
    pub binary_path: Option<String>,
    pub version: Option<String>,
}

impl ProviderKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Unsupported(s) => s.as_str(),
        }
    }

    pub fn display_name(&self) -> &str {
        match self {
            Self::Claude => "Claude",
            Self::Codex => "Codex",
            Self::Unsupported(s) => s.as_str(),
        }
    }

    pub fn counterpart(&self) -> Self {
        match self {
            Self::Claude => Self::Codex,
            Self::Codex => Self::Claude,
            Self::Unsupported(_) => self.clone(),
        }
    }

    pub fn capabilities(&self) -> Option<ProviderCapabilities> {
        match self {
            Self::Claude => Some(ProviderCapabilities {
                binary_name: "claude",
                supports_structured_output: true,
                supports_resume: true,
                supports_tool_stream: true,
            }),
            Self::Codex => Some(ProviderCapabilities {
                binary_name: "codex",
                supports_structured_output: true,
                supports_resume: true,
                supports_tool_stream: true,
            }),
            Self::Unsupported(_) => None,
        }
    }

    pub fn resolve_runtime_path(&self) -> Option<String> {
        match self {
            Self::Claude => crate::services::claude::resolve_claude_path(),
            Self::Codex => crate::services::codex::resolve_codex_path(),
            Self::Unsupported(_) => None,
        }
    }

    pub fn probe_runtime(&self) -> Option<ProviderRuntimeProbe> {
        let capabilities = self.capabilities()?;
        let binary_path = self.resolve_runtime_path();
        let version = binary_path.as_ref().and_then(|path| {
            let mut command = Command::new(path);
            crate::services::platform::apply_runtime_path(&mut command);
            command
                .arg("--version")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| {
                    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if stdout.is_empty() {
                        None
                    } else {
                        Some(stdout)
                    }
                })
        });
        Some(ProviderRuntimeProbe {
            provider: self.clone(),
            capabilities,
            binary_path,
            version,
        })
    }

    /// Parse a known provider string. Returns None for unknown providers.
    pub fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "claude" => Some(Self::Claude),
            "codex" => Some(Self::Codex),
            _ => None,
        }
    }

    /// Parse a provider string, returning Unsupported for unknown providers.
    pub fn from_str_or_unsupported(raw: &str) -> Self {
        Self::from_str(raw).unwrap_or_else(|| Self::Unsupported(raw.trim().to_string()))
    }

    /// Returns true if this is a known, supported provider.
    pub fn is_supported(&self) -> bool {
        !matches!(self, Self::Unsupported(_))
    }

    pub fn is_channel_supported(&self, channel_name: Option<&str>, is_dm: bool) -> bool {
        if is_dm {
            return self.is_supported();
        }

        let Some(channel_name) = channel_name else {
            return matches!(self, Self::Claude);
        };

        if channel_name.ends_with("-cdx") {
            return matches!(self, Self::Codex);
        }

        if channel_name.ends_with("-cc") {
            return matches!(self, Self::Claude);
        }

        matches!(self, Self::Claude)
    }

    pub fn build_tmux_session_name(&self, channel_name: &str) -> String {
        let sanitized: String = channel_name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        let trimmed = safe_prefix(&sanitized, 44);
        format!(
            "{}-{}-{}{}",
            TMUX_SESSION_PREFIX,
            self.as_str(),
            trimmed,
            tmux_env_suffix()
        )
    }
}

pub fn parse_provider_and_channel_from_tmux_name(
    session_name: &str,
) -> Option<(ProviderKind, String)> {
    let prefix = format!("{}-", TMUX_SESSION_PREFIX);
    let stripped = session_name.strip_prefix(&prefix)?;
    let suffix = tmux_env_suffix();
    let without_suffix = if !suffix.is_empty() {
        stripped.strip_suffix(suffix).unwrap_or(stripped)
    } else {
        stripped
    };
    if let Some(rest) = without_suffix.strip_prefix("claude-") {
        return Some((ProviderKind::Claude, rest.to_string()));
    }
    if let Some(rest) = without_suffix.strip_prefix("codex-") {
        return Some((ProviderKind::Codex, rest.to_string()));
    }
    Some((ProviderKind::Claude, without_suffix.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{ProviderKind, parse_provider_and_channel_from_tmux_name};

    #[test]
    fn test_provider_channel_support() {
        assert!(ProviderKind::Claude.is_channel_supported(Some("mac-mini"), false));
        assert!(ProviderKind::Claude.is_channel_supported(Some("cookingheart-dev-cc"), false));
        assert!(!ProviderKind::Claude.is_channel_supported(Some("cookingheart-dev-cdx"), false));
        assert!(ProviderKind::Codex.is_channel_supported(Some("cookingheart-dev-cdx"), false));
        assert!(!ProviderKind::Codex.is_channel_supported(Some("cookingheart-dev-cc"), false));
        assert!(ProviderKind::Codex.is_channel_supported(None, true));
    }

    #[test]
    fn test_unsupported_provider() {
        let p = ProviderKind::from_str_or_unsupported("gpt");
        assert!(!p.is_supported());
        assert_eq!(p.as_str(), "gpt");
        assert_eq!(p.display_name(), "gpt");
        assert!(!p.is_channel_supported(Some("test-cc"), false));
        assert!(!p.is_channel_supported(Some("test"), false));
        assert!(!p.is_channel_supported(None, true));
    }

    #[test]
    fn test_from_str_or_unsupported_known() {
        assert_eq!(
            ProviderKind::from_str_or_unsupported("claude"),
            ProviderKind::Claude
        );
        assert_eq!(
            ProviderKind::from_str_or_unsupported("Codex"),
            ProviderKind::Codex
        );
    }

    #[test]
    fn test_tmux_name_parse_supports_provider_aware_names() {
        assert_eq!(
            parse_provider_and_channel_from_tmux_name("AgentDesk-claude-cookingheart-dev-cc"),
            Some((ProviderKind::Claude, "cookingheart-dev-cc".to_string()))
        );
        assert_eq!(
            parse_provider_and_channel_from_tmux_name("AgentDesk-codex-cookingheart-dev-cdx"),
            Some((ProviderKind::Codex, "cookingheart-dev-cdx".to_string()))
        );
        assert_eq!(
            parse_provider_and_channel_from_tmux_name("AgentDesk-mac-mini"),
            Some((ProviderKind::Claude, "mac-mini".to_string()))
        );
    }

    #[test]
    fn test_provider_from_str_claude() {
        assert_eq!(ProviderKind::from_str("claude"), Some(ProviderKind::Claude));
    }

    #[test]
    fn test_provider_from_str_codex() {
        assert_eq!(ProviderKind::from_str("codex"), Some(ProviderKind::Codex));
    }

    #[test]
    fn test_provider_from_str_case_insensitive() {
        assert_eq!(ProviderKind::from_str("Claude"), Some(ProviderKind::Claude));
        assert_eq!(ProviderKind::from_str("CLAUDE"), Some(ProviderKind::Claude));
        assert_eq!(ProviderKind::from_str("CODEX"), Some(ProviderKind::Codex));
        assert_eq!(ProviderKind::from_str("Codex"), Some(ProviderKind::Codex));
    }

    #[test]
    fn test_provider_from_str_unknown() {
        assert_eq!(ProviderKind::from_str("gpt"), None);
        assert_eq!(ProviderKind::from_str(""), None);
    }

    #[test]
    fn test_build_tmux_session_name() {
        let name = ProviderKind::Claude.build_tmux_session_name("my-channel");
        assert!(name.starts_with("AgentDesk-claude-"));
        assert!(name.contains("my-channel"));

        let name2 = ProviderKind::Codex.build_tmux_session_name("dev-cdx");
        assert!(name2.starts_with("AgentDesk-codex-"));
        assert!(name2.contains("dev-cdx"));
    }

    #[test]
    fn test_parse_provider_and_channel_from_tmux_name() {
        let channel = "my-test-channel";
        let session = ProviderKind::Claude.build_tmux_session_name(channel);
        let (provider, parsed_channel) =
            parse_provider_and_channel_from_tmux_name(&session).unwrap();
        assert_eq!(provider, ProviderKind::Claude);
        assert_eq!(parsed_channel, channel);

        let session2 = ProviderKind::Codex.build_tmux_session_name(channel);
        let (provider2, parsed_channel2) =
            parse_provider_and_channel_from_tmux_name(&session2).unwrap();
        assert_eq!(provider2, ProviderKind::Codex);
        assert_eq!(parsed_channel2, channel);
    }

    #[test]
    fn test_is_channel_supported_cc_suffix() {
        assert!(ProviderKind::Claude.is_channel_supported(Some("dev-cc"), false));
        assert!(!ProviderKind::Codex.is_channel_supported(Some("dev-cc"), false));
    }

    #[test]
    fn test_is_channel_supported_cdx_suffix() {
        assert!(ProviderKind::Codex.is_channel_supported(Some("dev-cdx"), false));
        assert!(!ProviderKind::Claude.is_channel_supported(Some("dev-cdx"), false));
    }

    #[test]
    fn test_counterpart_provider() {
        assert_eq!(ProviderKind::Claude.counterpart(), ProviderKind::Codex);
        assert_eq!(ProviderKind::Codex.counterpart(), ProviderKind::Claude);

        let unsupported = ProviderKind::Unsupported("gpt".to_string());
        assert_eq!(
            unsupported.counterpart(),
            ProviderKind::Unsupported("gpt".to_string())
        );
    }

    #[test]
    fn test_provider_capabilities_known_providers_support_agent_contract() {
        for provider in [ProviderKind::Claude, ProviderKind::Codex] {
            let capabilities = provider.capabilities().expect("supported provider");
            assert!(capabilities.supports_structured_output);
            assert!(capabilities.supports_resume);
            assert!(capabilities.supports_tool_stream);
            assert!(!capabilities.binary_name.is_empty());
        }
    }
}
