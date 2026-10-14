use crate::services::discord::SharedData;
use crate::services::discord::tmux_watcher_registry::lock_tmux_watcher_registry;
use std::sync::atomic::Ordering;

/// An intentional reset owns the exact managed session it is about to destroy.
/// Retire its watcher under the registry lock before removing process handles,
/// killing a pane or clearing capture files. Otherwise the outgoing watcher
/// can mistake the reset for a crash and fail the next dispatch on this thread.
/// Other sessions, including a rebound channel, retain their current owners.
pub(super) fn retire_watcher(shared: &SharedData, session_name: &str) {
    let registry_guard = lock_tmux_watcher_registry();
    if let Some((owner, handle)) = shared
        .tmux_watchers
        .remove_tmux_session_locked(&registry_guard, session_name)
    {
        let watcher_cancel = handle.cancel;
        watcher_cancel.store(true, Ordering::Release);
        tracing::debug!(
            channel_id = owner.get(),
            "retired managed-session watcher before intentional reset"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::discord::tmux_watcher_registry::TmuxWatcherHandle;
    use poise::serenity_prelude::ChannelId;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};

    fn watcher(session_name: &str) -> TmuxWatcherHandle {
        TmuxWatcherHandle {
            tmux_session_name: session_name.into(),
            output_path: format!("/tmp/{session_name}.jsonl"),
            paused: Arc::new(AtomicBool::new(false)),
            resume_offset: Arc::new(std::sync::Mutex::new(None)),
            cancel: Arc::new(AtomicBool::new(false)),
            pause_epoch: Arc::new(AtomicU64::new(0)),
            turn_delivered: Arc::new(AtomicBool::new(false)),
            last_heartbeat_ts_ms: Arc::new(AtomicI64::new(0)),
        }
    }

    #[test]
    fn reset_retires_only_the_exact_managed_session() {
        let shared = crate::services::discord::make_shared_data_for_tests();
        let channel = ChannelId::new(26_101);
        let target = watcher("AgentDesk-codex-reset-target");
        let cancelled = target.cancel.clone();
        shared.tmux_watchers.insert(channel, target);
        let other_channel = ChannelId::new(26_102);
        let other = watcher("AgentDesk-codex-reset-other");
        let other_cancel = other.cancel.clone();
        shared.tmux_watchers.insert(other_channel, other);

        retire_watcher(&shared, "AgentDesk-codex-reset-target");

        assert!(cancelled.load(Ordering::Acquire));
        assert!(!shared.tmux_watchers.contains_key(&channel));
        assert!(shared.tmux_watchers.contains_key(&other_channel));
        assert!(!other_cancel.load(Ordering::Acquire));
    }

    #[test]
    fn reset_of_an_old_name_preserves_a_rebound_channel() {
        let shared = crate::services::discord::make_shared_data_for_tests();
        let channel = ChannelId::new(26_103);
        shared
            .tmux_watchers
            .insert(channel, watcher("AgentDesk-codex-reset-old"));
        let successor = watcher("AgentDesk-codex-reset-successor");
        let successor_cancel = successor.cancel.clone();
        shared.tmux_watchers.insert(channel, successor);

        retire_watcher(&shared, "AgentDesk-codex-reset-old");

        let current = shared.tmux_watchers.get(&channel).unwrap();
        assert!(Arc::ptr_eq(&current.cancel, &successor_cancel));
        assert!(!successor_cancel.load(Ordering::Acquire));
    }

    #[test]
    fn retired_exit_has_no_authority_over_a_same_named_successor() {
        let shared = crate::services::discord::make_shared_data_for_tests();
        let channel = ChannelId::new(26_104);
        let name = "AgentDesk-codex-reset-same-name";
        let outgoing = watcher(name);
        let outgoing_cancel = outgoing.cancel.clone();
        shared.tmux_watchers.insert(channel, outgoing);
        retire_watcher(&shared, name);
        let successor = watcher(name);
        let successor_cancel = successor.cancel.clone();
        shared.tmux_watchers.insert(channel, successor);

        assert!(outgoing_cancel.load(Ordering::Acquire));
        assert!(
            shared
                .tmux_watchers
                .remove_tmux_session_if_current(name, &outgoing_cancel)
                .is_none()
        );
        assert!(shared.tmux_watchers.contains_key(&channel));
        assert!(!successor_cancel.load(Ordering::Acquire));
    }

    #[test]
    fn managed_process_reset_retires_a_watcher_even_when_the_pane_is_absent() {
        let shared = crate::services::discord::make_shared_data_for_tests();
        let channel = ChannelId::new(26_105);
        let name = format!(
            "AgentDesk-codex-managed-reset-test-{}",
            uuid::Uuid::new_v4()
        );
        let outgoing = watcher(&name);
        let outgoing_cancel = outgoing.cancel.clone();
        shared.tmux_watchers.insert(channel, outgoing);

        assert!(!super::super::reset_managed_process_session(&shared, &name));

        assert!(outgoing_cancel.load(Ordering::Acquire));
        assert!(!shared.tmux_watchers.contains_key(&channel));
    }

    #[test]
    fn hard_reset_retires_a_watcher_even_when_the_pane_is_absent() {
        let shared = crate::services::discord::make_shared_data_for_tests();
        let channel = ChannelId::new(26_106);
        let name = format!("AgentDesk-codex-hard-reset-test-{}", uuid::Uuid::new_v4());
        let outgoing = watcher(&name);
        let outgoing_cancel = outgoing.cancel.clone();
        shared.tmux_watchers.insert(channel, outgoing);

        assert!(!super::super::recreate_tmux_session(
            &shared,
            &name,
            "regression fixture"
        ));

        assert!(outgoing_cancel.load(Ordering::Acquire));
        assert!(!shared.tmux_watchers.contains_key(&channel));
    }
}
