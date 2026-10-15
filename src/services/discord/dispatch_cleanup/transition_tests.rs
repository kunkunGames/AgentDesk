use super::*;
use crate::services::discord::host_teardown_gate::test_support::{
    Stored, busy_turn, channel_key, runtime, seed, turn_kept,
};
use poise::serenity_prelude::ChannelId;
use std::time::Duration;

thread_local! {
    static OBSERVED: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(super) async fn assert_successor_waits(shared: &super::super::SharedData, channel: ChannelId) {
    if OBSERVED.get().is_none() {
        return;
    }
    let admission = shared.acquire_session_transition(channel);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), admission)
            .await
            .is_err(),
        "a successor must wait through both host judgement and final cleanup"
    );
    OBSERVED.set(OBSERVED.get().map(|count| count + 1));
}

fn set_dispatch(channel: ChannelId, dispatch: &str, nonce: &str) {
    let root = inflight::inflight_runtime_root().unwrap();
    let path = inflight::inflight_state_path(&root, &ProviderKind::Claude, channel.get());
    let mut row: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    row["dispatch_id"] = dispatch.into();
    row["turn_nonce"] = nonce.into();
    std::fs::write(path, serde_json::to_vec(&row).unwrap()).unwrap();
}

#[tokio::test]
async fn successor_waits_through_host_judgement_and_final_cleanup_pg() {
    let _root = crate::config::TestRuntimeRootGuard::new();
    let db = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = db.connect_and_migrate().await;
    let (shared, registry) = runtime(&pool).await;
    let channel = ChannelId::new(1_479_671_301_388_059_800);
    let name = "AgentDesk-claude-dispatch-transition-test";
    seed(
        &pool,
        &channel_key(&shared, name),
        name,
        channel.get(),
        Stored::Legacy,
    )
    .await;
    let original = busy_turn(&shared, channel, name).await;
    set_dispatch(channel, "original-dispatch", original.turn_nonce().unwrap());
    OBSERVED.set(Some(0));
    stop_owned_turn(
        Some(&registry),
        &ProviderKind::Claude,
        channel.get(),
        "original-dispatch",
    )
    .await
    .unwrap();
    assert_eq!(OBSERVED.replace(None), Some(2));
    let transition = shared.acquire_session_transition(channel).await.unwrap();
    let successor = busy_turn(&shared, channel, name).await;
    set_dispatch(
        channel,
        "successor-dispatch",
        successor.turn_nonce().unwrap(),
    );
    drop(transition);
    stop_owned_turn(
        Some(&registry),
        &ProviderKind::Claude,
        channel.get(),
        "original-dispatch",
    )
    .await
    .unwrap();
    assert!(turn_kept(&shared, channel, &successor).await);
    pool.close().await;
    db.drop().await;
}
