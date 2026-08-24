//! #5462 process-generation fence regressions.
//!
//! These belong to the `stall_recovery_tests` family but live outside the
//! `inflight.rs` decomposition parent so its frozen `parent_test_residue`
//! ceiling keeps shrinking (#4267/#4269). The test path retains the
//! `stall_recovery_tests` filter and shared fixtures remain available through
//! the parent module.

use super::*;

#[test]
fn destructive_loader_preserves_current_generation_named_row_even_when_stale() {
    let _guard = stale_override_test_mutex()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let _env_lock = crate::config::shared_test_env_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let temp = TempDir::new().unwrap();
    let _root_env = crate::config::TestEnvVarGuard::set_path_after_shared_test_env_lock(
        "AGENTDESK_ROOT_DIR",
        temp.path(),
    );
    let current = 54_620;
    crate::services::discord::runtime_store::set_process_generation_for_tests(Some(current));
    super::super::set_test_tmux_alive_override(Some(&[]));
    let mut state = InflightTurnState::new(
        ProviderKind::Codex,
        81,
        Some("adk-cdx".to_string()),
        7,
        42,
        43,
        "hello".to_string(),
        Some("session-81".to_string()),
        Some("AgentDesk-codex-current-generation-81".to_string()),
        Some("/tmp/out.jsonl".to_string()),
        Some("/tmp/in.fifo".to_string()),
        0,
    );
    state.born_generation = current;
    save_inflight_state_in_root(temp.path(), &state).expect("seed current-generation row");
    let path = inflight_state_path(temp.path(), &ProviderKind::Codex, state.channel_id);
    filetime::set_file_mtime(
        &path,
        filetime::FileTime::from_unix_time(
            chrono::Utc::now().timestamp() - super::super::INFLIGHT_MAX_AGE_SECS as i64 - 2,
            0,
        ),
    )
    .expect("age row past loader threshold");

    let loaded = load_inflight_states_from_root(temp.path(), &ProviderKind::Codex);
    super::super::set_test_tmux_alive_override(None);
    crate::services::discord::runtime_store::set_process_generation_for_tests(None);
    assert_eq!(loaded.len(), 1);
    assert!(
        path.exists(),
        "the current process's named live row must survive"
    );
}

/// §4.4 narrowed the loader's refusal to `row_is_current_generation(state,
/// current) && state.tmux_session_name.is_some()`, so an unnamed
/// current-generation row stays inside the 300s loader reclaim scope. That
/// is §9-2's accepted limit, and it is deliberate: an unnamed row has no
/// other in-process reclaimer, so widening the gate to it would trade a
/// bounded 300s window for residence that lasts as long as the process.
///
/// The generation must be pinned explicitly (both test mutexes + a rooted
/// env guard + `set_process_generation_for_tests`, matching the named-row
/// sibling above). Reading the ambient `process_generation()` instead yields
/// 0 in this test binary, which fails the fence's first term and diverts the
/// row into the §9-8 legacy fail-open — the removal below would then hold
/// for a reason that never evaluates the narrowing term at all.
#[test]
fn destructive_loader_keeps_unnamed_current_generation_rows_in_reclaim_scope() {
    let _guard = stale_override_test_mutex()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let _env_lock = crate::config::shared_test_env_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let temp = TempDir::new().unwrap();
    let _root_env = crate::config::TestEnvVarGuard::set_path_after_shared_test_env_lock(
        "AGENTDESK_ROOT_DIR",
        temp.path(),
    );
    let current = 54_621;
    crate::services::discord::runtime_store::set_process_generation_for_tests(Some(current));
    let mut state = InflightTurnState::new(
        ProviderKind::Codex,
        82,
        Some("adk-cdx".to_string()),
        7,
        42,
        43,
        "hello".to_string(),
        Some("session-82".to_string()),
        None,
        Some("/tmp/out.jsonl".to_string()),
        None,
        0,
    );
    state.born_generation = current;
    save_inflight_state_in_root(temp.path(), &state).expect("seed unnamed row");
    let path = inflight_state_path(temp.path(), &ProviderKind::Codex, state.channel_id);
    filetime::set_file_mtime(
        &path,
        filetime::FileTime::from_unix_time(
            chrono::Utc::now().timestamp() - super::super::INFLIGHT_MAX_AGE_SECS as i64 - 2,
            0,
        ),
    )
    .expect("age row past loader threshold");

    let loaded = load_inflight_states_from_root(temp.path(), &ProviderKind::Codex);
    crate::services::discord::runtime_store::set_process_generation_for_tests(None);
    assert!(
        super::super::row_is_current_generation(&state, current),
        "the seeded row must clear the fence's first term, or this test says nothing about the narrowing term"
    );
    assert!(
        state.tmux_session_name.is_none(),
        "the missing tmux name is the only thing standing between this row and the fence's refusal"
    );
    assert!(loaded.is_empty());
    assert!(
        !path.exists(),
        "the loader remains the bounded reclaimer for unnamed rows"
    );
}
