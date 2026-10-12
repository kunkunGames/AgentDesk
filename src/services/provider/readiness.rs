//! Provider-specific readiness evidence from terminal captures.
use super::{ProviderKind, ProviderReadinessAdapter};

pub(crate) fn tmux_capture_indicates_ready_for_input(
    capture: &str,
    provider: &ProviderKind,
) -> bool {
    if let ProviderKind::Unsupported(_) = provider {
        return crate::services::tmux_common::tmux_capture_indicates_generic_ready_banner(capture)
            || tmux_capture_contains_wrapper_ready_marker(capture, provider);
    }
    let Some(adapter) = provider.readiness_adapter() else {
        return false;
    };
    match adapter {
        ProviderReadinessAdapter::Claude => {
            crate::services::tmux_common::tmux_capture_indicates_claude_tui_ready_for_input(capture)
        }
        ProviderReadinessAdapter::Codex => {
            crate::services::codex_tui::input::pane_looks_ready_for_codex_prompt(capture)
                || crate::services::tmux_common::tmux_capture_indicates_generic_ready_banner(
                    capture,
                )
                || tmux_capture_contains_wrapper_ready_marker(capture, provider)
        }
        ProviderReadinessAdapter::Qwen => {
            crate::services::tmux_common::tmux_capture_indicates_generic_ready_banner(capture)
                || tmux_capture_contains_wrapper_ready_marker(capture, provider)
        }
        ProviderReadinessAdapter::Gemini => {
            crate::services::tmux_common::tmux_capture_indicates_generic_ready_banner(capture)
                || tmux_capture_contains_wrapper_ready_marker(capture, provider)
        }
        ProviderReadinessAdapter::OpenCode => {
            crate::services::tmux_common::tmux_capture_indicates_generic_ready_banner(capture)
                || tmux_capture_contains_wrapper_ready_marker(capture, provider)
        }
        ProviderReadinessAdapter::GenericBanner(_) => {
            crate::services::tmux_common::tmux_capture_indicates_generic_ready_banner(capture)
                || tmux_capture_contains_wrapper_ready_marker(capture, provider)
        }
    }
}

pub(super) fn tmux_capture_contains_wrapper_ready_marker(
    capture: &str,
    provider: &ProviderKind,
) -> bool {
    capture
        .lines()
        .rev()
        .filter(|line| !line.trim().is_empty())
        .take(12)
        .any(|line| wrapper_ready_marker_matches_provider(line, provider))
}

pub(super) fn wrapper_ready_marker_matches_provider(line: &str, provider: &ProviderKind) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
        return false;
    };
    value.get("type").and_then(|field| field.as_str())
        == Some(crate::services::tmux_common::WRAPPER_READY_FOR_INPUT_EVENT)
        && value.get("provider").and_then(|field| field.as_str()) == Some(provider.as_str())
}
