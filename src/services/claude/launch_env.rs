//! Environment isolation and overlays for Claude tmux launches.

use crate::services::claude_compact_context::append_auto_compact_window_shell_env;
use crate::services::discord::restart_report::{
    RESTART_REPORT_CHANNEL_ENV, RESTART_REPORT_PROVIDER_ENV,
};
use crate::services::provider::ProviderKind;

pub(super) fn build_tmux_launch_env_lines(
    exec_path: Option<&str>,
    report_channel_id: Option<u64>,
    report_provider: Option<ProviderKind>,
    auto_compact_window: Option<u64>,
) -> String {
    let mut env_lines = String::from("unset CLAUDECODE\nunset AGENTDESK_BINDING_CONTEXT\n");
    if let Some(exec_path) = exec_path {
        env_lines.push_str(&format!(
            "export PATH='{}'\n",
            exec_path.replace('\'', "'\\''")
        ));
    }
    if let Ok(root_dir) = std::env::var("AGENTDESK_ROOT_DIR") {
        let trimmed = root_dir.trim();
        if !trimmed.is_empty() {
            env_lines.push_str(&format!(
                "export AGENTDESK_ROOT_DIR='{}'\n",
                trimmed.replace('\'', "'\\''")
            ));
        }
    }
    if let Some(channel_id) = report_channel_id {
        env_lines.push_str(&format!(
            "export {}={}\n",
            RESTART_REPORT_CHANNEL_ENV, channel_id
        ));
    }
    if let Some(provider) = report_provider {
        env_lines.push_str(&format!(
            "export {}={}\n",
            RESTART_REPORT_PROVIDER_ENV,
            provider.as_str()
        ));
    }
    // Compact-window overlay (#4591): fence off any inherited absolute window
    // and export the freshly resolved one when present.
    append_auto_compact_window_shell_env(&mut env_lines, auto_compact_window);

    env_lines
}
