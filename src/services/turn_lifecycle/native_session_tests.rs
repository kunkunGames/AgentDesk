use super::*;
use crate::services::session_backend::{self, SessionHandle};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn process_target() -> (TurnLifecycleTarget, Arc<AtomicBool>) {
    let name = format!("lifecycle-native-{}", uuid::Uuid::new_v4());
    let alive = Arc::new(AtomicBool::new(true));
    session_backend::insert_process_session(
        &name,
        SessionHandle::TestProcess {
            pid: 424_260,
            alive: alive.clone(),
        },
    );
    (
        TurnLifecycleTarget {
            provider: None,
            channel_id: None,
            tmux_name: name,
        },
        alive,
    )
}

#[tokio::test]
async fn force_kill_terminates_native_wrapper_without_tmux() {
    let (target, alive) = process_target();
    let result =
        force_kill_turn_without_cancel_event(None, &target, "native test", "force_kill").await;
    assert!(
        result.tmux_killed,
        "legacy API field reports termination of either backend"
    );
    assert!(!alive.load(Ordering::Relaxed));
    assert!(session_backend::process_session_pid(&target.tmux_name).is_none());
    assert!(result.queue_preserved);
}

#[tokio::test]
async fn preserving_session_keeps_native_wrapper_alive() {
    let (target, alive) = process_target();
    let result =
        stop_turn_preserving_queue_without_cancel_event(None, &target, "native test").await;
    let remained_alive = alive.load(Ordering::Relaxed);
    session_backend::terminate_process_session(&target.tmux_name);
    assert!(remained_alive);
    assert!(!result.tmux_killed);
    assert!(result.queue_preserved);
}
