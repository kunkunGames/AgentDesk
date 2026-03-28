use crate::services::provider::ProviderKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelOption {
    pub label: &'static str,
    pub value: &'static str,
    pub feature_summary: &'static str,
    pub specialty_domain: &'static str,
    pub token_pricing: &'static str,
}

impl ModelOption {
    pub fn inline_description(&self) -> String {
        format!(
            "{} | {} | {}",
            self.feature_summary, self.specialty_domain, self.token_pricing
        )
    }

    fn matches_query(&self, needle: &str) -> bool {
        self.value.to_ascii_lowercase().contains(needle)
            || self.label.to_ascii_lowercase().contains(needle)
            || self.feature_summary.to_ascii_lowercase().contains(needle)
            || self.specialty_domain.to_ascii_lowercase().contains(needle)
            || self.token_pricing.to_ascii_lowercase().contains(needle)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProviderModelCatalog {
    pub display_name: &'static str,
    pub default_feature_summary: &'static str,
    pub default_specialty_domain: &'static str,
    pub default_token_pricing: &'static str,
    pub options: &'static [ModelOption],
}

impl ProviderModelCatalog {
    pub fn default_inline_description(&self) -> String {
        format!(
            "{} | {} | {}",
            self.default_feature_summary, self.default_specialty_domain, self.default_token_pricing
        )
    }
}

const CLAUDE_OPTIONS: &[ModelOption] = &[
    ModelOption {
        label: "Default",
        value: "default",
        feature_summary: "Uses the runtime default for the current Claude Code account tier",
        specialty_domain: "Daily coding or deep reasoning, depending on tier policy",
        token_pricing: "Pricing follows the underlying default model",
    },
    ModelOption {
        label: "Sonnet",
        value: "sonnet",
        feature_summary: "Latest Sonnet alias for balanced work",
        specialty_domain: "Daily coding, balanced intelligence-cost-speed",
        token_pricing: "Input $3 / Output $15 per 1M tokens",
    },
    ModelOption {
        label: "Opus",
        value: "opus",
        feature_summary: "Latest Opus alias for highest-intelligence work",
        specialty_domain: "Complex reasoning, architecture, hard debugging",
        token_pricing: "Input $5 / Output $25 per 1M tokens",
    },
    ModelOption {
        label: "Haiku",
        value: "haiku",
        feature_summary: "Fastest and most cost-efficient Claude alias",
        specialty_domain: "Simple tasks, quick replies, lightweight coding",
        token_pricing: "Input $1 / Output $5 per 1M tokens",
    },
    ModelOption {
        label: "Sonnet 1M",
        value: "sonnet[1m]",
        feature_summary: "Sonnet alias with the 1M context window",
        specialty_domain: "Long-context coding, research, long-running sessions",
        token_pricing: "Uses Sonnet pricing: Input $3 / Output $15 per 1M tokens",
    },
    ModelOption {
        label: "Opus 1M",
        value: "opus[1m]",
        feature_summary: "Opus alias with the 1M context window",
        specialty_domain: "Large-context reasoning, architecture, deep analysis",
        token_pricing: "Uses Opus pricing: Input $5 / Output $25 per 1M tokens",
    },
    ModelOption {
        label: "Opus Plan",
        value: "opusplan",
        feature_summary: "Hybrid planning mode with Opus planning and Sonnet execution",
        specialty_domain: "Plan-first workflows that continue into implementation",
        token_pricing: "No single fixed rate; planning uses Opus and execution uses Sonnet pricing",
    },
];

const CODEX_OPTIONS: &[ModelOption] = &[
    ModelOption {
        label: "Default",
        value: "default",
        feature_summary: "Uses the current Codex CLI config or profile default",
        specialty_domain: "General coding, research, agentic execution",
        token_pricing: "Pricing follows the underlying default model",
    },
    ModelOption {
        label: "GPT-5.4",
        value: "gpt-5.4",
        feature_summary: "Frontier reasoning and coding flagship",
        specialty_domain: "Complex professional work, agentic coding",
        token_pricing: "Input $2.50 / Output $15.00 per 1M tokens",
    },
    ModelOption {
        label: "GPT-5.4 Mini",
        value: "gpt-5.4-mini",
        feature_summary: "Strong mini-tier model for coding, computer use, and subagents",
        specialty_domain: "High-volume coding, cost-sensitive agentic work",
        token_pricing: "Input $0.75 / Output $4.50 per 1M tokens",
    },
    ModelOption {
        label: "GPT-5.3 Codex",
        value: "gpt-5.3-codex",
        feature_summary: "Most capable agentic coding model in the Codex lineup",
        specialty_domain: "Long-running coding, tool use, complex execution",
        token_pricing: "Input $1.75 / Output $14.00 per 1M tokens",
    },
    ModelOption {
        label: "GPT-5.3 Codex Spark",
        value: "gpt-5.3-codex-spark",
        feature_summary: "Ultra-low-latency real-time coding research preview",
        specialty_domain: "Immediate feedback loops, interactive coding iteration",
        token_pricing: "Public API input/output pricing is not published",
    },
    ModelOption {
        label: "GPT-5.2 Codex",
        value: "gpt-5.2-codex",
        feature_summary: "Long-horizon agentic coding model",
        specialty_domain: "Large codebases, long-running coding tasks",
        token_pricing: "Input $1.75 / Output $14.00 per 1M tokens",
    },
    ModelOption {
        label: "GPT-5.1 Codex",
        value: "gpt-5.1-codex",
        feature_summary: "GPT-5.1-based agentic coding model",
        specialty_domain: "Daily Codex coding, legacy compatibility",
        token_pricing: "Input $1.25 / Output $10.00 per 1M tokens",
    },
    ModelOption {
        label: "GPT-5 Codex",
        value: "gpt-5-codex",
        feature_summary: "Legacy GPT-5-based Codex model",
        specialty_domain: "General Codex coding, legacy fallback",
        token_pricing: "Input $1.25 / Output $10.00 per 1M tokens",
    },
    ModelOption {
        label: "GPT-5 Codex Mini",
        value: "gpt-5-codex-mini",
        feature_summary: "Small legacy Codex variant",
        specialty_domain: "Lightweight Codex tasks, usage stretch",
        token_pricing: "Public API input/output pricing needs confirmation",
    },
];

const GEMINI_OPTIONS: &[ModelOption] = &[
    ModelOption {
        label: "Default",
        value: "default",
        feature_summary: "Uses the current Gemini CLI default or provider-managed routing",
        specialty_domain: "General multimodal work, low-friction CLI usage",
        token_pricing: "Pricing follows the underlying default model",
    },
    ModelOption {
        label: "Gemini 2.5 Pro",
        value: "gemini-2.5-pro",
        feature_summary: "State-of-the-art multipurpose thinking model",
        specialty_domain: "Complex reasoning, coding, large codebase and document analysis",
        token_pricing: "Input $1.25 / Output $10.00 per 1M tokens (<=200K), Input $2.50 / Output $15.00 per 1M tokens (>200K)",
    },
    ModelOption {
        label: "Gemini 2.5 Flash",
        value: "gemini-2.5-flash",
        feature_summary: "Hybrid reasoning model with 1M context and thinking budget",
        specialty_domain: "Low-latency reasoning, high-volume agents",
        token_pricing: "Input $0.30 / Output $2.50 per 1M tokens",
    },
    ModelOption {
        label: "Gemini 2.5 Flash-Lite",
        value: "gemini-2.5-flash-lite",
        feature_summary: "Smallest and most cost-efficient Gemini 2.5 model",
        specialty_domain: "High-throughput batch work, lightweight tasks",
        token_pricing: "Input $0.10 / Output $0.40 per 1M tokens",
    },
];

const CLAUDE_CATALOG: ProviderModelCatalog = ProviderModelCatalog {
    display_name: "Claude",
    default_feature_summary: "Uses the runtime default for the current Claude Code account tier",
    default_specialty_domain: "Daily coding or deep reasoning, depending on tier policy",
    default_token_pricing: "Pricing follows the underlying default model",
    options: CLAUDE_OPTIONS,
};

const CODEX_CATALOG: ProviderModelCatalog = ProviderModelCatalog {
    display_name: "Codex",
    default_feature_summary: "Uses the active Codex CLI config or profile default",
    default_specialty_domain: "General coding, research, agentic execution",
    default_token_pricing: "Pricing follows the underlying default model",
    options: CODEX_OPTIONS,
};

const GEMINI_CATALOG: ProviderModelCatalog = ProviderModelCatalog {
    display_name: "Gemini",
    default_feature_summary: "Uses the active Gemini CLI default or provider-managed routing",
    default_specialty_domain: "General multimodal work, low-friction CLI usage",
    default_token_pricing: "Pricing follows the underlying default model",
    options: GEMINI_OPTIONS,
};

pub fn catalog_for_provider(provider: &ProviderKind) -> Option<&'static ProviderModelCatalog> {
    catalog_for_name(provider.as_str())
}

pub fn catalog_for_name(provider: &str) -> Option<&'static ProviderModelCatalog> {
    match provider.trim().to_ascii_lowercase().as_str() {
        "claude" => Some(&CLAUDE_CATALOG),
        "codex" => Some(&CODEX_CATALOG),
        "gemini" => Some(&GEMINI_CATALOG),
        _ => None,
    }
}

pub fn model_details_for_provider(provider: &ProviderKind, value: &str) -> Option<String> {
    model_details_for_name(provider.as_str(), value)
}

pub fn model_details_for_name(provider_name: &str, value: &str) -> Option<String> {
    let catalog = catalog_for_name(provider_name)?;
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("default") {
        return Some(catalog.default_inline_description());
    }

    catalog
        .options
        .iter()
        .find(|option| option.value.eq_ignore_ascii_case(trimmed))
        .map(ModelOption::inline_description)
}

pub fn normalize_model_override(
    provider: &ProviderKind,
    raw: &str,
) -> Result<Option<String>, String> {
    normalize_model_override_for_name(provider.as_str(), raw)
}

pub fn normalize_model_override_for_name(
    provider_name: &str,
    raw: &str,
) -> Result<Option<String>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("Model name cannot be empty.".to_string());
    }

    let lowered = trimmed.to_ascii_lowercase();
    if matches!(lowered.as_str(), "default" | "clear" | "none" | "reset") {
        return Ok(None);
    }

    let Some(catalog) = catalog_for_name(provider_name) else {
        return Err(format!(
            "Model catalog is not configured for provider `{}`.",
            provider_name
        ));
    };

    if let Some(option) = catalog
        .options
        .iter()
        .find(|option| option.value.eq_ignore_ascii_case(trimmed))
    {
        return Ok((option.value != "default").then(|| option.value.to_string()));
    }

    if provider_name.eq_ignore_ascii_case("claude") && is_allowed_claude_raw_model(trimmed) {
        return Ok(Some(trimmed.to_string()));
    }

    Err(format!(
        "Unknown model `{}` for {}. Try: {}",
        trimmed,
        catalog.display_name,
        example_values(catalog)
    ))
}

pub fn render_catalog_values(provider: &ProviderKind) -> Option<String> {
    catalog_for_provider(provider).map(|catalog| {
        catalog
            .options
            .iter()
            .map(|option| format!("`{}`", option.value))
            .collect::<Vec<_>>()
            .join(", ")
    })
}

pub fn matches_catalog_query(option: &ModelOption, partial: &str) -> bool {
    let needle = partial.trim().to_ascii_lowercase();
    needle.is_empty() || option.matches_query(&needle)
}

fn example_values(catalog: &ProviderModelCatalog) -> String {
    catalog
        .options
        .iter()
        .take(5)
        .map(|option| option.value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn is_allowed_claude_raw_model(raw: &str) -> bool {
    let base = raw.strip_suffix("[1m]").unwrap_or(raw);
    let lower = base.to_ascii_lowercase();
    if !(lower.starts_with("claude-opus-")
        || lower.starts_with("claude-sonnet-")
        || lower.starts_with("claude-haiku-"))
    {
        return false;
    }

    lower
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '.'))
}

#[cfg(test)]
mod tests {
    use super::{
        ProviderModelCatalog, catalog_for_name, matches_catalog_query, model_details_for_name,
        normalize_model_override_for_name, render_catalog_values,
    };
    use crate::services::provider::ProviderKind;

    fn has_value(catalog: &ProviderModelCatalog, value: &str) -> bool {
        catalog.options.iter().any(|option| option.value == value)
    }

    #[test]
    fn test_catalogs_cover_three_providers() {
        let claude = catalog_for_name("claude").expect("claude catalog");
        let codex = catalog_for_name("codex").expect("codex catalog");
        let gemini = catalog_for_name("gemini").expect("gemini catalog");

        assert!(has_value(claude, "sonnet"));
        assert!(has_value(codex, "gpt-5.4"));
        assert!(has_value(gemini, "gemini-2.5-pro"));
    }

    #[test]
    fn test_default_keywords_clear_override() {
        assert_eq!(
            normalize_model_override_for_name("codex", "default").unwrap(),
            None
        );
        assert_eq!(
            normalize_model_override_for_name("gemini", "clear").unwrap(),
            None
        );
    }

    #[test]
    fn test_normalizes_known_curated_values() {
        assert_eq!(
            normalize_model_override_for_name("codex", "GPT-5.4-MINI").unwrap(),
            Some("gpt-5.4-mini".to_string())
        );
        assert_eq!(
            normalize_model_override_for_name("claude", "opusplan").unwrap(),
            Some("opusplan".to_string())
        );
    }

    #[test]
    fn test_accepts_claude_raw_model_names() {
        assert_eq!(
            normalize_model_override_for_name("claude", "claude-opus-4-6[1m]").unwrap(),
            Some("claude-opus-4-6[1m]".to_string())
        );
        assert_eq!(
            normalize_model_override_for_name("claude", "claude-sonnet-4-6-20251001").unwrap(),
            Some("claude-sonnet-4-6-20251001".to_string())
        );
    }

    #[test]
    fn test_unknown_value_returns_examples() {
        let err = normalize_model_override_for_name("codex", "gpt-5-mini").unwrap_err();
        assert!(err.contains("Unknown model"));
        assert!(err.contains("gpt-5.4"));
    }

    #[test]
    fn test_render_catalog_values_is_backtick_joined() {
        let rendered = render_catalog_values(&ProviderKind::Codex).unwrap();
        assert!(rendered.contains("`default`"));
        assert!(rendered.contains("`gpt-5.3-codex`"));
    }

    #[test]
    fn test_model_details_render_three_part_description() {
        let details = model_details_for_name("codex", "gpt-5.4").expect("codex details");
        assert!(details.contains(" | "));
        assert!(details.contains("Frontier reasoning and coding flagship"));
        assert!(details.contains("Input $2.50 / Output $15.00"));
    }

    #[test]
    fn test_default_model_details_render_three_part_description() {
        let details = model_details_for_name("gemini", "default").expect("default details");
        assert!(details.contains(" | "));
        assert!(details.contains("provider-managed routing"));
    }

    #[test]
    fn test_catalog_query_matches_pricing_metadata() {
        let catalog = catalog_for_name("claude").expect("claude catalog");
        let opus = catalog
            .options
            .iter()
            .find(|option| option.value == "opus")
            .expect("opus option");
        assert!(matches_catalog_query(opus, "$25"));
    }
}
