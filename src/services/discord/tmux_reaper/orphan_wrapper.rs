//! Discover and parse orphaned managed tmux wrapper processes.

use crate::services::provider::ProviderKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OrphanTmuxWrapperProcess {
    pub(super) pid: i32,
    pub(super) provider: ProviderKind,
    pub(super) tmux_session_name: Option<String>,
}

pub(super) fn list_orphan_tmux_wrapper_processes() -> Vec<OrphanTmuxWrapperProcess> {
    let output = match std::process::Command::new("ps")
        .args(["-axo", "pid=,ppid=,command="])
        .output()
    {
        Ok(output) if output.status.success() => output,
        _ => return Vec::new(),
    };

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(parse_orphan_tmux_wrapper_process_line)
        .collect()
}

pub(super) fn parse_orphan_tmux_wrapper_process_line(
    line: &str,
) -> Option<OrphanTmuxWrapperProcess> {
    let trimmed = line.trim_start();
    let first_end = trimmed.find(char::is_whitespace)?;
    let pid = trimmed[..first_end].parse::<i32>().ok()?;
    let rest = trimmed[first_end..].trim_start();
    let second_end = rest.find(char::is_whitespace)?;
    let ppid = rest[..second_end].parse::<i32>().ok()?;
    let command = rest[second_end..].trim_start();

    if ppid != 1 {
        return None;
    }

    let argv: Vec<&str> = command.split_whitespace().collect();
    if !command_invokes_agentdesk_binary(&argv) {
        return None;
    }
    let provider = argv
        .iter()
        .find_map(|token| provider_for_managed_tmux_wrapper_subcommand(token))?;

    if extract_arg_value(&argv, "--output-file").is_none() {
        return None;
    }

    let has_input_fifo = extract_arg_value(&argv, "--input-fifo").is_some();
    let has_pipe_input_mode = extract_arg_value(&argv, "--input-mode") == Some("pipe");
    if !has_input_fifo && !has_pipe_input_mode {
        return None;
    }

    Some(OrphanTmuxWrapperProcess {
        pid,
        provider,
        tmux_session_name: extract_tmux_session_name_from_wrapper_command(command),
    })
}

fn command_invokes_agentdesk_binary(argv: &[&str]) -> bool {
    argv.first()
        .and_then(|exe| std::path::Path::new(exe).file_name())
        .and_then(|basename| basename.to_str())
        == Some("agentdesk")
}

pub(super) fn provider_for_managed_tmux_wrapper_subcommand(token: &str) -> Option<ProviderKind> {
    crate::services::provider::provider_registry()
        .iter()
        .filter(|entry| entry.managed_tmux_backend)
        .find(|entry| entry.managed_tmux_wrapper_subcommand == Some(token))
        .and_then(|entry| ProviderKind::from_str(entry.id))
}

fn extract_arg_value<'a>(argv: &'a [&str], flag: &str) -> Option<&'a str> {
    argv.windows(2)
        .find_map(|window| (window[0] == flag).then_some(window[1]))
}

pub(super) fn extract_tmux_session_name_from_wrapper_command(command: &str) -> Option<String> {
    let argv: Vec<&str> = command.split_whitespace().collect();
    let output_path = extract_arg_value(&argv, "--output-file")?;
    let basename = std::path::Path::new(output_path).file_name()?.to_str()?;
    let stem = basename.strip_suffix(".jsonl").unwrap_or(basename);
    let pos = stem.find("AgentDesk-")?;
    Some(stem[pos..].to_string())
}
