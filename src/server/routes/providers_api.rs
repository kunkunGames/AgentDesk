//! Authenticated provider catalog for dashboard selectors.

use axum::{Json, extract::State, http::StatusCode};
use serde_json::Value;

use super::AppState;
use crate::error::AppResult;
use crate::services::provider::public_provider_catalog;

/// GET /api/providers
pub async fn get_providers(State(_state): State<AppState>) -> AppResult<(StatusCode, Json<Value>)> {
    let catalog = public_provider_catalog();
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "providers": catalog })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_payload_excludes_agy_alias_and_secrets() {
        let catalog = public_provider_catalog();
        let ids: Vec<_> = catalog.iter().map(|entry| entry.id).collect();
        assert!(ids.contains(&"grok"));
        assert!(ids.contains(&"antigravity"));
        assert!(!ids.contains(&"agy"));
        let antigravity = catalog
            .iter()
            .find(|entry| entry.id == "antigravity")
            .expect("antigravity row");
        assert_eq!(antigravity.binary_name, "agy");
        let encoded = serde_json::json!({ "providers": catalog }).to_string();
        assert!(!encoded.contains("XAI_API_KEY"));
        assert!(!encoded.contains("auth.json"));
    }
}
