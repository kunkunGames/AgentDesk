//! Real source and elapsed-time witnesses for watchdog integration fixtures.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use poise::serenity_prelude::ChannelId;

use crate::services::discord::SharedData;
use crate::services::discord::health::session_enrichment::{
    RelayBacklogEvidence, SessionEnrichment,
};
use crate::services::discord::inflight::InflightTurnState;
use crate::services::provider::ProviderKind;

use super::session_enrichment::WATCHER_STATE_DESYNC_STALE_MS;
use super::{HealthRegistry, WatcherStateSnapshot};

pub(in crate::services::discord) fn seed_source_generation(
    shared: &Arc<SharedData>,
    channel: ChannelId,
    state: &mut InflightTurnState,
) {
    let tmux = state
        .tmux_session_name
        .as_deref()
        .expect("fixture tmux binding");
    let marker = crate::services::tmux_common::session_temp_path(tmux, "generation");
    std::fs::create_dir_all(
        std::path::Path::new(&marker)
            .parent()
            .expect("generation parent"),
    )
    .expect("create generation directory");
    std::fs::write(&marker, "1").expect("write owned generation marker");
    let generation = crate::services::discord::tmux::read_generation_file_mtime_ns(tmux);
    assert_ne!(
        generation, 0,
        "the fixture must observe a real generation marker"
    );
    state.turn_start_offset = Some(0);
    let coord = shared.tmux_relay_coord(channel);
    coord
        .confirmed_end_generation_mtime_ns
        .store(generation, Ordering::Release);
}

pub(in crate::services::discord) async fn wait_for_overdue_backlog(
    registry: &HealthRegistry,
    provider: &ProviderKind,
    shared: &Arc<SharedData>,
    channel: ChannelId,
) -> WatcherStateSnapshot {
    let first = SessionEnrichment::load(shared, Some(provider), channel).await;
    assert!(
        matches!(first.relay_backlog,
        RelayBacklogEvidence::Pending { pending_age_ms, .. }
            if pending_age_ms < WATCHER_STATE_DESYNC_STALE_MS as u64),
        "a new source obligation must start with measured, non-overdue age: {:?}",
        first.relay_backlog
    );
    assert!(
        !first.desynced(true, true),
        "turn age alone cannot prove the backlog overdue"
    );
    tokio::time::sleep(Duration::from_millis(
        WATCHER_STATE_DESYNC_STALE_MS as u64 + 50,
    ))
    .await;
    let observed = SessionEnrichment::load(shared, Some(provider), channel).await;
    assert!(
        matches!(observed.relay_backlog,
        RelayBacklogEvidence::Pending { pending_age_ms, .. }
            if pending_age_ms >= WATCHER_STATE_DESYNC_STALE_MS as u64),
        "the same parsed source obligation must remain overdue: {:?}",
        observed.relay_backlog
    );
    registry
        .snapshot_watcher_state_for_shared(provider, shared.clone(), channel.get())
        .await
        .expect("elapsed backlog snapshot")
}
