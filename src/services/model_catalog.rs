use crate::services::provider::ProviderKind;

// Model refreshes should stay data-only by default.
// When model ids or metadata change, update the catalog entries here and keep
// `/model` UI and command flow unchanged unless a separate UI change is requested.

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
        label: "claude-sonnet-4-6 (sonnet)",
        value: "sonnet",
        feature_summary: "속도와 지능의 균형이 가장 좋은 일상형",
        specialty_domain: "일상 코딩·분석·구현",
        token_pricing: "$3 / $15",
    },
    ModelOption {
        label: "claude-opus-4-6 (opus)",
        value: "opus",
        feature_summary: "가장 높은 지능의 상위형",
        specialty_domain: "복잡한 설계·깊은 추론·에이전트 빌드",
        token_pricing: "$5 / $25",
    },
    ModelOption {
        label: "claude-haiku-4-5 (haiku)",
        value: "haiku",
        feature_summary: "가장 빠른 경량형",
        specialty_domain: "간단 작업·대량 처리",
        token_pricing: "$1 / $5",
    },
    ModelOption {
        label: "claude-sonnet-4-6[1m] (sonnet[1m])",
        value: "sonnet[1m]",
        feature_summary: "1M 컨텍스트 장문맥형, 기본 Sonnet 단가 유지",
        specialty_domain: "큰 코드베이스·긴 세션",
        token_pricing: "$3 / $15",
    },
    ModelOption {
        label: "claude-opus-4-6[1m] (opus[1m])",
        value: "opus[1m]",
        feature_summary: "1M 컨텍스트 최상위 장문맥형, 기본 Opus 단가 유지",
        specialty_domain: "초대형 코드베이스·장기 설계",
        token_pricing: "$5 / $25",
    },
    ModelOption {
        label: "opusplan",
        value: "opusplan",
        feature_summary: "계획은 Opus, 실행은 Sonnet으로 자동 분리",
        specialty_domain: "아키텍처 계획→구현 분리 작업",
        token_pricing: "혼합 과금(Opus $5 / $25 + Sonnet $3 / $15, 단계별)",
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
        label: "gpt-5.4",
        value: "gpt-5.4",
        feature_summary: "플래그십 코딩+추론+도구사용 통합형",
        specialty_domain: "복잡한 코딩·에이전트 워크플로우",
        token_pricing: "$2.50 / $15",
    },
    ModelOption {
        label: "gpt-5.4-mini",
        value: "gpt-5.4-mini",
        feature_summary: "빠르고 효율적인 경량형",
        specialty_domain: "서브에이전트·고속 반복 작업",
        token_pricing: "$0.75 / $4.50",
    },
    ModelOption {
        label: "gpt-5.3-codex",
        value: "gpt-5.3-codex",
        feature_summary: "고난도 소프트웨어 엔지니어링 특화",
        specialty_domain: "대형 코드베이스·복잡한 구현",
        token_pricing: "$1.75 / $14",
    },
    ModelOption {
        label: "gpt-5.3-codex-spark",
        value: "gpt-5.3-codex-spark",
        feature_summary: "텍스트 전용 초저지연 research preview",
        specialty_domain: "실시간 코딩 iteration·짧은 루프",
        token_pricing: "ChatGPT Pro 포함",
    },
    ModelOption {
        label: "gpt-5.2-codex",
        value: "gpt-5.2-codex",
        feature_summary: "실전 엔지니어링용 고급 코딩형",
        specialty_domain: "장기 호라이즌 코딩 작업",
        token_pricing: "$1.75 / $14",
    },
    ModelOption {
        label: "gpt-5.1-codex",
        value: "gpt-5.1-codex",
        feature_summary: "장기 실행형 agentic coding 최적화",
        specialty_domain: "오래 걸리는 코딩 작업",
        token_pricing: "$1.25 / $10",
    },
    ModelOption {
        label: "gpt-5-codex",
        value: "gpt-5-codex",
        feature_summary: "GPT-5 기반 agentic coding 튜닝형",
        specialty_domain: "구세대 Codex 호환 작업",
        token_pricing: "$1.25 / $10",
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
        label: "gemini-3.1-pro-preview",
        value: "gemini-3.1-pro-preview",
        feature_summary: "3.1 Pro 계열 최상위 추론형",
        specialty_domain: "복잡한 설계·깊은 추론·에이전트 코딩",
        token_pricing: "$2 / $12 (<=200k), $4 / $18 (>200k)",
    },
    ModelOption {
        label: "gemini-3-flash-preview",
        value: "gemini-3-flash-preview",
        feature_summary: "3 시리즈 Flash 고성능 저지연형",
        specialty_domain: "고속 에이전트 작업·범용 멀티모달",
        token_pricing: "$0.50 / $3",
    },
    ModelOption {
        label: "gemini-2.5-pro",
        value: "gemini-2.5-pro",
        feature_summary: "pro 계열 기본 고성능 축",
        specialty_domain: "안정적 고난도 코딩·복합추론",
        token_pricing: "$1.25 / $10 (<=200k), $2.50 / $15 (>200k)",
    },
    ModelOption {
        label: "gemini-2.5-flash",
        value: "gemini-2.5-flash",
        feature_summary: "flash 계열 균형형, 1M 컨텍스트·thinking budgets 지원",
        specialty_domain: "저지연 고볼륨·긴 문맥 작업",
        token_pricing: "$0.30 / $2.50",
    },
    ModelOption {
        label: "gemini-2.5-flash-lite",
        value: "gemini-2.5-flash-lite",
        feature_summary: "flash-lite 계열 최저가/최고속",
        specialty_domain: "번역·간단 데이터 처리·대량 작업",
        token_pricing: "$0.10 / $0.40",
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
        assert!(has_value(gemini, "gemini-3.1-pro-preview"));
        assert!(has_value(gemini, "gemini-3-flash-preview"));
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
        assert!(details.contains("플래그십 코딩+추론+도구사용 통합형"));
        assert!(details.contains("$2.50 / $15"));
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
