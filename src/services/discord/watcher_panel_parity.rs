//! EPIC #3078 PR-4 — SHADOW parity helper for the tmux watcher's status-panel
//! RECLAIM path.
//!
//! The watcher (`tmux_watcher.rs`) owns the TUI-direct / external-input panel
//! create→edit→complete→reclaim path. PR-4 routes the RECLAIM decision through
//! the [`StatusPanelController`] behind a parity check: the controller computes
//! which panel id it WOULD reclaim and we assert it agrees with the legacy
//! watcher decision, while the legacy path keeps executing the real Discord IO.
//! The executing cutover lands in a later PR.
//!
//! Scope note: CREATE/ADOPT and COMPLETION parity are DEFERRED to that
//! controller execute-cutover PR. A faithful create/complete check must
//! replicate `watcher_should_create_external_input_status_panel` and the
//! SendFallback-aware completion id from RAW inputs (the controller's read-only
//! `orphan_parity_target` collapse would only echo the already-resolved output
//! back — a tautological, non-meaningful comparison). The RECLAIM path is
//! genuinely faithful: it reuses `sweeper_reclaim_parity_id`, the same read-only
//! collapse codex validated for PR-3, against the panel id the legacy reclaim is
//! about to delete.
//!
//! The parity LOGIC lives here (not inline in the grandfathered, LoC-frozen
//! `tmux_watcher.rs`): the watcher reclaim site adds only a single `.await` hook
//! into `assert_watcher_reclaim_parity` below. Same posture as PR-2
//! (`recovery_engine`) and PR-3 (`placeholder_sweeper` + this crate's
//! `shadow_parity_warn`): never `panic!` (so an unseen shape cannot crash a prod
//! turn over the still-executing legacy path) — `debug_assert` (fail loud in
//! test/dev) + a bounded, log-once `warn!` via [`ParityWarnOnce`].
//!
//! v2-gated: when status-panel-v2 is OFF the controller actor is NOT spawned, so
//! the helper short-circuits BEFORE the awaited controller read whose ack would
//! never be answered (mirrors `recovery_engine` / `placeholder_sweeper`).

use serenity::model::id::{ChannelId, MessageId};

use super::SharedData;
use super::shadow_parity_warn::ParityWarnOnce;
use super::turn_finalizer::TurnKey;
use crate::services::provider::ProviderKind;
use std::sync::Arc;

/// Watcher panel parity-mismatch shape: `(channel, site, controller_id, legacy_id)`.
/// `site` is retained for forward-compat with the deferred create/complete sites
/// so each diverging shape logs once.
type WatcherShape = (u64, &'static str, Option<u64>, Option<u64>);

/// One-shot bound for the PR-4 watcher parity-mismatch `warn!`: a hot watcher
/// loop iterating over a persistently-diverging turn must not log-flood, so each
/// distinct mismatch shape logs at most once.
static WATCHER_PARITY_WARNED: ParityWarnOnce<WatcherShape> = ParityWarnOnce::new();

/// Build the `TurnKey` the watcher parity gate keys on, reusing the
/// process-wide generation the controller ledger collapse expects. A TUI-direct
/// turn carries `user_msg_id == 0`, which the controller's `resolve_channel_only`
/// collapses onto the channel's single live entry (the #3003 turn-aware path).
fn watcher_turn_key(channel_id: ChannelId, user_msg_id: u64) -> TurnKey {
    TurnKey::new(
        channel_id,
        user_msg_id,
        super::runtime_store::load_generation(),
    )
}

/// Core parity assertion: `debug_assert` + bounded log-once `warn!`. Never
/// panics in release. Pure (no IO / no await) so the gating + warn-once shape is
/// unit-testable.
fn assert_parity(
    site: &'static str,
    controller_id: Option<MessageId>,
    legacy_id: Option<MessageId>,
    channel_id: u64,
) {
    if controller_id == legacy_id {
        return;
    }
    debug_assert_eq!(
        controller_id, legacy_id,
        "#3078 PR-4 watcher status-panel {site} parity mismatch (channel {channel_id}): controller chose {controller_id:?}, legacy chose {legacy_id:?}"
    );
    if !WATCHER_PARITY_WARNED.should_warn((
        channel_id,
        site,
        controller_id.map(MessageId::get),
        legacy_id.map(MessageId::get),
    )) {
        return;
    }
    tracing::warn!(
        channel = channel_id,
        site = site,
        controller_id = ?controller_id,
        legacy_id = ?legacy_id,
        "#3078 PR-4 watcher status-panel parity mismatch — StatusPanelController chose a different panel id than the legacy watcher; legacy path executed (no behaviour change), divergence logged once for the later controller-executes cutover"
    );
}

/// PR-4 reclaim site: assert the controller's chosen reclaim target equals the
/// `panel_msg_id` the legacy `cleanup_orphan_external_input_status_panel` deletes
/// + clears. v2-off → inert (no controller read). The watcher reclaim path is
/// keyed channel-only (TUI-direct `user_msg_id == 0`), like the sweeper, and
/// reuses the same read-only `sweeper_reclaim_parity_id` collapse (faithful,
/// codex-validated in PR-3).
pub(in crate::services::discord) async fn assert_watcher_reclaim_parity(
    shared: &Arc<SharedData>,
    channel_id: ChannelId,
    provider: &ProviderKind,
    panel_msg_id: MessageId,
) {
    if !shared.status_panel_v2_enabled {
        return;
    }
    let controller_id = shared
        .status_panel_controller
        .sweeper_reclaim_parity_id(
            watcher_turn_key(channel_id, 0),
            provider.clone(),
            Some(panel_msg_id),
        )
        .await;
    assert_parity(
        "reclaim",
        controller_id,
        Some(panel_msg_id),
        channel_id.get(),
    );
}

#[cfg(test)]
mod tests {
    //! EPIC #3078 PR-4: confirm the controller's chosen RECLAIM id equals the
    //! legacy watcher decision (orphan reclaim, #3003 channel-only turn-aware),
    //! that the parity assert never fires for matching ids, that the warn is
    //! bounded, and that the SharedData-gated wrapper short-circuits when v2 is
    //! off (default test SharedData: v2 off, controller actor NOT spawned — so
    //! the awaited read must never be reached). CREATE/ADOPT and COMPLETION
    //! parity are deferred to the controller execute-cutover PR (see module doc).
    use super::*;
    use crate::services::discord::status_panel_controller::StatusPanelController;

    /// Orphan reclaim: the controller's reclaim target IS the persisted panel id
    /// (channel-only key, #3003 turn-aware), agreeing with the legacy watcher
    /// cleanup target.
    #[tokio::test(flavor = "current_thread")]
    async fn controller_reclaim_target_matches_legacy() {
        let ctl = StatusPanelController::spawn(true);
        let key = TurnKey::new(ChannelId::new(4004), 0, 0);
        let legacy = MessageId::new(7004);
        let target = ctl
            .sweeper_reclaim_parity_id(key, ProviderKind::Claude, Some(legacy))
            .await;
        assert_eq!(target, Some(legacy), "reclaim target must match legacy");
        assert_parity("reclaim", target, Some(legacy), 4004);
    }

    /// v2-off: the SharedData-gated wrapper must return BEFORE the awaited
    /// controller read. The default test SharedData has v2 off and an UNSPAWNED
    /// controller, so reaching the read would hang on an ack that is never
    /// answered — completing without hang/panic proves the short-circuit.
    #[tokio::test(flavor = "current_thread")]
    async fn v2_off_short_circuits_without_panic() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        assert!(!shared.status_panel_v2_enabled);
        let channel = ChannelId::new(4005);
        assert_watcher_reclaim_parity(
            &shared,
            channel,
            &ProviderKind::Claude,
            MessageId::new(8003),
        )
        .await;
    }

    /// The bounded guard logs a given `(channel, site, controller, legacy)` shape
    /// at most once (the parity warn cannot flood a hot watcher loop).
    #[test]
    fn warn_is_bounded_once_per_shape() {
        let shape: WatcherShape = (4999, "reclaim", Some(1), Some(2));
        assert!(super::WATCHER_PARITY_WARNED.should_warn(shape));
        assert!(!super::WATCHER_PARITY_WARNED.should_warn(shape));
    }
}
