//! Direct-process Claude session launch (Windows and tmux-less hosts).

use super::*;

struct PreparedProcessTurn {
    start_offset: u64,
    followup: bool,
    active_turn: crate::services::session_backend::ProcessSessionActiveTurnGuard,
}

/// Execute Claude via ProcessBackend (direct child process, no tmux).
/// Used when tmux is not available or on Windows.
pub(crate) fn execute_streaming_local_process(
    args: &[String],
    prompt: &str,
    working_dir: &str,
    sender: Sender<StreamMessage>,
    cancel_token: Option<std::sync::Arc<CancelToken>>,
    session_name: &str,
    compact_percent: Option<u64>,
    compact_lower_bound_tokens: u64,
) -> Result<(), String> {
    use crate::services::session_backend::{
        ProcessBackend, SessionConfig, with_process_session_launch_authority,
    };
    use crate::services::tmux_diagnostics::should_recreate_session_after_stdin_error;

    debug_log(&format!(
        "=== execute_streaming_local_process START: {} ===",
        session_name
    ));

    let output_path = format!(
        "{}/agentdesk-{}.jsonl",
        std::env::temp_dir().display(),
        session_name
    );
    let prompt_path = format!(
        "{}/agentdesk-{}.prompt",
        std::env::temp_dir().display(),
        session_name
    );

    let mut recreate_after_followup = false;
    loop {
        let prepared = with_process_session_launch_authority(
            session_name,
            || -> Result<PreparedProcessTurn, String> {
                let overlay = crate::services::discord::org_schema::overlay_from_tmux_session(
                    ProviderKind::Claude,
                    session_name,
                )?;
                let profile_matches = crate::services::session_backend::auth_profiles::prepare(
                    session_name,
                    &overlay.profile_id,
                );
                let fresh_args;
                let args = if profile_matches {
                    args
                } else {
                    fresh_args = without_resume_arg(args);
                    &fresh_args
                };

                if recreate_after_followup {
                    // Keep an exited handle registered until removal and the replacement
                    // spawn can occur atomically under the same source authority.
                    if let Some(handle) = remove_process_session(session_name) {
                        terminate_process_handle(handle);
                    }
                } else if process_session_available_for_followup(session_name) {
                    debug_log("Existing process session found — sending follow-up");
                    match prepare_followup_to_process(
                        prompt,
                        &output_path,
                        session_name,
                        cancel_token.as_deref(),
                    ) {
                        Ok((start_offset, active_turn)) => {
                            return Ok(PreparedProcessTurn {
                                start_offset,
                                followup: true,
                                active_turn,
                            });
                        }
                        Err(error) if should_recreate_session_after_stdin_error(&error) => {
                            debug_log(&format!(
                                "Process follow-up failed, recreating session: {}",
                                error
                            ));
                            if let Some(handle) = remove_process_session(session_name) {
                                terminate_process_handle(handle);
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }

                // Clean up stale files
                let _ = std::fs::remove_file(&output_path);
                let _ = std::fs::remove_file(&prompt_path);

                // Write prompt
                std::fs::write(&prompt_path, prompt)
                    .map_err(|e| format!("Failed to write prompt file: {}", e))?;

                // Build wrapper args — no shell_escape here because ProcessBackend uses
                // Command::new().args() (direct argv), not a shell script.
                let (claude_bin, resolution) = resolve_claude_binary()?;
                let mut wrapper_args = Vec::new();
                claude_bin.append_process_backend_wrapper_args(&mut wrapper_args);
                wrapper_args.extend(args.iter().map(|a| a.to_string()));

                let exe = std::env::current_exe()
                    .map_err(|e| format!("Failed to get executable path: {}", e))?;

                let env_vars = crate::services::provider_auth_profile::merge_overlay_env(
                    resolution
                        .exec_path
                        .clone()
                        .map(|path| vec![("PATH".to_string(), path)])
                        .unwrap_or_default(),
                    &overlay,
                );
                let auto_compact_window = launch_auto_compact_window_for_session(
                    session_name,
                    claude_model_from_args(args),
                    compact_percent,
                    compact_lower_bound_tokens,
                );
                let config = SessionConfig {
                    session_name: session_name.to_string(),
                    working_dir: working_dir.to_string(),
                    agentdesk_exe: exe.display().to_string(),
                    output_path: output_path.clone(),
                    prompt_path: prompt_path.clone(),
                    wrapper_subcommand: "tmux-wrapper".to_string(),
                    wrapper_args,
                    env_vars,
                    unset_env: crate::services::provider_auth_profile::overlay_unset_keys(&overlay),
                };

                let backend = ProcessBackend::new();
                let handle = backend.create_session_with_command_env(&config, |command| {
                    // Compact-window overlay (#4591).
                    apply_auto_compact_window_to_command(command, auto_compact_window);
                })?;

                let handle = crate::services::session_backend::auth_profiles::record_launch(
                    session_name,
                    &overlay.profile_id,
                    handle,
                )?;

                // Store child PID in cancel token
                register_child_pid(cancel_token.as_deref(), handle.pid());

                // Store handle for follow-up messages and protect it from tmux-takeover cleanup.
                let active_turn =
                    insert_process_session_and_mark_active_turn(session_name.to_string(), handle);
                Ok(PreparedProcessTurn {
                    start_offset: 0,
                    followup: false,
                    active_turn,
                })
            },
        )?;

        // Never hold the source authority while waiting for provider output.
        let read_result = read_output_file_until_result(
            &output_path,
            prepared.start_offset,
            sender.clone(),
            cancel_token.clone(),
            process_session_probe(session_name),
        )?;
        drop(prepared.active_turn);

        if prepared.followup {
            match classify_followup_result(
                read_result,
                prepared.start_offset,
                "process died during follow-up output reading",
            ) {
                ClaudeFollowupResult::Delivered => {
                    let current_offset = std::fs::metadata(&output_path)
                        .map(|meta| meta.len())
                        .unwrap_or(prepared.start_offset);
                    let _ = sender.send(StreamMessage::ProcessReady {
                        output_path: output_path.clone(),
                        session_name: session_name.to_string(),
                        last_offset: current_offset,
                    });
                    return Ok(());
                }
                ClaudeFollowupResult::RecreateSession { error } => {
                    debug_log(&format!(
                        "Process follow-up failed before new output, recreating session: {}",
                        error
                    ));
                    recreate_after_followup = true;
                    continue;
                }
                ClaudeFollowupResult::FinalizeWithNotice { error, notice } => {
                    debug_log(&format!(
                        "Process follow-up streamed partial output before session death — suppressing replay: {}",
                        error
                    ));
                    crate::services::tmux_common::with_tmux_source_authority(session_name, |_| {
                        if let Some(handle) = remove_process_session(session_name) {
                            terminate_process_handle(handle);
                        }
                    });
                    emit_followup_restart_suppressed_notice(&sender, &notice);
                    return Ok(());
                }
            }
        }

        fold_read_output_result(
            read_result,
            |offset| {
                let _ = sender.send(StreamMessage::ProcessReady {
                    output_path: output_path.clone(),
                    session_name: session_name.to_string(),
                    last_offset: offset,
                });
            },
            |_| {
                let _ = sender.send(StreamMessage::session_ended("Claude"));
                remove_process_session(session_name);
            },
        );

        debug_log("=== execute_streaming_local_process END ===");
        return Ok(());
    }
}

pub(super) fn without_resume_arg(args: &[String]) -> Vec<String> {
    let mut fresh = Vec::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--resume" {
            let _ = iter.next();
        } else {
            fresh.push(arg.clone());
        }
    }
    fresh
}

#[cfg(test)]
mod tests {
    #[test]
    fn account_switch_drops_only_the_resume_argument_and_its_token() {
        let args = [
            "--verbose",
            "--resume",
            "old-account-session",
            "--model",
            "sonnet",
        ]
        .map(String::from);
        assert_eq!(
            super::without_resume_arg(&args),
            ["--verbose", "--model", "sonnet"]
        );
        assert_eq!(
            super::without_resume_arg(&["--resume".into()]),
            Vec::<String>::new()
        );
    }
}
