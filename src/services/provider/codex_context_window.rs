//! Codex model cache resolution with exact-slug and conservative maximum fallback.

pub(super) fn codex_model_context_window(model: &str) -> Option<u64> {
    let cache_path = dirs::home_dir()?.join(".codex/models_cache.json");
    let data = std::fs::read_to_string(cache_path).ok()?;
    codex_context_window_from_cache(&data, model)
}

/// Prefer an exact model slug, then the cached maximum; unusable data returns None (#3263).
pub(super) fn codex_context_window_from_cache(data: &str, model: &str) -> Option<u64> {
    let json: serde_json::Value = serde_json::from_str(data).ok()?;
    let models = json.get("models")?.as_array()?;

    let mut max_window: Option<u64> = None;
    for entry in models {
        let Some(window) = entry.get("context_window").and_then(|v| v.as_u64()) else {
            continue;
        };
        // Provider-default requests must not exact-match a blank cache slug.
        if !model.is_empty() && entry.get("slug").and_then(|s| s.as_str()) == Some(model) {
            return Some(window);
        }
        max_window = Some(max_window.map_or(window, |m| m.max(window)));
    }
    max_window
}
