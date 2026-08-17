//! Public non-secret provider catalog projection.

use serde::Serialize;

use super::{ProviderRegistryEntry, provider_registry};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProviderCatalogEntry {
    pub id: &'static str,
    pub display_name: &'static str,
    pub channel_suffix: Option<&'static str>,
    pub binary_name: &'static str,
    pub execution_surface: &'static str,
    pub supports_resume: bool,
    pub supports_structured_output: bool,
    pub supports_tool_stream: bool,
    pub supports_restricted_tool_policy: bool,
    pub supports_tui_hosting: bool,
    pub system_prompt_transport: &'static str,
    pub context_window: &'static str,
}

impl ProviderCatalogEntry {
    fn from_registry(entry: &ProviderRegistryEntry) -> Self {
        Self {
            id: entry.id,
            display_name: entry.display_name,
            channel_suffix: entry.channel_suffix,
            binary_name: entry.capabilities.binary_name,
            execution_surface: entry.execution_adapter.execution_surface(),
            supports_resume: entry.capabilities.supports_resume,
            supports_structured_output: entry.capabilities.supports_structured_output,
            supports_tool_stream: entry.capabilities.supports_tool_stream,
            supports_restricted_tool_policy: entry.supports_restricted_tool_policy(),
            supports_tui_hosting: entry.supports_tui_hosting(),
            system_prompt_transport: entry.system_prompt_transport(),
            context_window: if entry.context_window_known {
                "known"
            } else {
                "unknown"
            },
        }
    }
}

pub fn public_provider_catalog() -> Vec<ProviderCatalogEntry> {
    provider_registry()
        .iter()
        .map(ProviderCatalogEntry::from_registry)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_no_secret_fields_and_includes_stream_json_providers() {
        let catalog = public_provider_catalog();
        let encoded = serde_json::to_string(&catalog).unwrap();
        assert!(!encoded.contains("XAI_API_KEY"));
        assert!(!encoded.contains("auth.json"));
        assert!(!encoded.contains("~/.grok"));
        let ids: Vec<_> = catalog.iter().map(|entry| entry.id).collect();
        assert!(ids.contains(&"grok"));
        assert!(ids.contains(&"antigravity"));
        assert!(!ids.contains(&"agy"));
        let agy = catalog
            .iter()
            .find(|entry| entry.id == "antigravity")
            .unwrap();
        assert!(!agy.supports_restricted_tool_policy);
        assert_eq!(agy.system_prompt_transport, "envelope");
        assert_eq!(agy.context_window, "unknown");
        let grok = catalog.iter().find(|entry| entry.id == "grok").unwrap();
        assert!(grok.supports_restricted_tool_policy);
        assert!(!grok.supports_tui_hosting);
    }
}
