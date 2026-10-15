//! Routine thread and session identities retain the distinguishing routine prefix.

pub(super) fn routine_thread_title(routine_name: &str, agent_id: &str) -> String {
    let base = format!(
        "routine {} - {}",
        compact_for_title(routine_name),
        compact_for_title(agent_id)
    );
    base.chars().take(90).collect()
}

pub(crate) fn routine_agent_session_name(routine_name: &str, agent_id: &str) -> String {
    // Put the distinguishing routine name FIRST: `build_tmux_session_name`
    // truncates to 44 chars, so leading with `agent_id` made two routines on the
    // same agent collide to one tmux session (reusing provider/transcript state)
    // once the shared `routine <agent_id> - ` prefix consumed the budget (#3463).
    // Routine-name-first keeps per-routine sessions distinct after truncation and
    // matches `routine_thread_title`'s ordering.
    let base = format!(
        "routine {} - {}",
        compact_for_title(routine_name),
        compact_for_title(agent_id)
    );
    base.chars().take(90).collect()
}

fn compact_for_title(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        "unnamed".to_string()
    } else {
        value
            .chars()
            .map(|ch| if ch.is_control() { ' ' } else { ch })
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}
