//! Confirmed session presence stays distinct from transport and probe failures.

use std::process::Output;
use std::time::Duration;

use super::{exact_target, is_blank_session_name, tmux_command, wait_for_tmux_output};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionPresence {
    Present,
    Missing,
    ProbeFailed,
}

/// Check whether a named tmux session definitely exists or definitely does not.
/// Transport, socket, permission, timeout, and unexpected tmux errors remain
/// distinguishable from a confirmed missing session so destructive callers can
/// fail closed.
pub(super) fn classify_has_session_output(output: &Output) -> SessionPresence {
    if output.status.success() {
        return SessionPresence::Present;
    }

    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    if stderr.contains("can't find session")
        || stderr.contains("no such session")
        || stderr.contains("no server running")
    {
        SessionPresence::Missing
    } else {
        SessionPresence::ProbeFailed
    }
}

pub(crate) fn session_presence(session_name: &str) -> SessionPresence {
    if is_blank_session_name(session_name) {
        return SessionPresence::ProbeFailed;
    }

    let Ok(mut command) = tmux_command() else {
        return SessionPresence::ProbeFailed;
    };
    command.args(["has-session", "-t", &exact_target(session_name)]);
    let Ok(output) = wait_for_tmux_output(command, Duration::from_secs(3), "tmux has-session")
    else {
        return SessionPresence::ProbeFailed;
    };
    classify_has_session_output(&output)
}

/// Compatibility boolean for non-destructive callers. Probe failures continue
/// to read as unavailable; destructive recovery must use [`session_presence`].
pub fn has_session(session_name: &str) -> bool {
    session_presence(session_name) == SessionPresence::Present
}
