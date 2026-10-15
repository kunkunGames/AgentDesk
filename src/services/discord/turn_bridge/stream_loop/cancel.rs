//! Guarded cancellation persistence and child settlement for the stream loop.

use crate::services::discord::SharedData;
use crate::services::discord::inflight::{InflightTurnIdentity, InflightTurnState};
use crate::services::provider::CancelToken;

use super::super::{close_all_tracked_background_children, sync_inflight_restart_mode_from_cancel};
use super::expected_identity::refresh_stream_tick_expected_identity_after_handoff;

pub(super) async fn finalize_cancelled_stream_turn(
    shared: &SharedData,
    token: &CancelToken,
    inflight: &mut InflightTurnState,
    persisted_baseline: &mut InflightTurnState,
    expected_identity: &mut InflightTurnIdentity,
    cancelled: &mut bool,
    active_children: &mut Vec<i64>,
) {
    let previous_restart_mode = inflight.restart_mode;
    let previous_restart_generation = inflight.restart_generation;
    if sync_inflight_restart_mode_from_cancel(token, inflight) {
        let outcome = crate::services::discord::inflight::patch_restart_mode_if_matches_identity(
            inflight,
            expected_identity,
            previous_restart_mode,
            previous_restart_generation,
            "turn_bridge::stream_loop::cancel_restart_mode",
        );
        refresh_stream_tick_expected_identity_after_handoff(
            expected_identity,
            persisted_baseline,
            inflight,
            Some(outcome),
        );
    }
    *cancelled = true;
    close_all_tracked_background_children(
        shared.pg_pool.as_ref(),
        active_children,
        "aborted",
        "turn cancel",
    )
    .await;
}
