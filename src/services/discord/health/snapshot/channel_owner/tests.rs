use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use poise::serenity_prelude::{MessageId, UserId};

use super::*;
use crate::services::discord::SharedData;
use crate::services::discord::inflight::InflightTurnState;
use crate::services::provider::CancelToken;

static NEXT_CHANNEL: AtomicU64 = AtomicU64::new(9_407_700_000_000);

fn fixture_test<F, Fut>(test: F)
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    let _lock = crate::services::turn_orchestrator::test_support::lock_test_env();
    let root = tempfile::tempdir().expect("isolated diagnostic runtime root");
    let _root = crate::config::TestEnvVarGuard::set_path_after_shared_test_env_lock(
        "AGENTDESK_ROOT_DIR",
        root.path(),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime")
        .block_on(test(root.path().to_path_buf()));
}

fn watcher(tmux: &str, output: &str) -> discord::TmuxWatcherHandle {
    discord::TmuxWatcherHandle {
        tmux_session_name: tmux.to_string(),
        output_path: output.to_string(),
        paused: Arc::new(AtomicBool::new(false)),
        resume_offset: Arc::new(std::sync::Mutex::new(None)),
        cancel: Arc::new(AtomicBool::new(false)),
        pause_epoch: Arc::new(AtomicU64::new(0)),
        turn_delivered: Arc::new(AtomicBool::new(false)),
        last_heartbeat_ts_ms: Arc::new(AtomicI64::new(discord::tmux_watcher_now_ms())),
    }
}

fn persist_episode(
    provider: ProviderKind,
    channel: ChannelId,
    token: &CancelToken,
    root: &Path,
) -> InflightTurnState {
    let tmux = token.tmux_session_name().expect("fixture executor binding");
    let output = root.join(format!("{}-{}.jsonl", provider.as_str(), channel.get()));
    std::fs::write(&output, b"").expect("empty fixture transcript");
    let mut row = InflightTurnState::new(
        provider,
        channel.get(),
        None,
        27,
        channel.get() + 100,
        channel.get() + 200,
        "private fixture prompt must not enter owner metadata".to_string(),
        Some("private-provider-session-must-not-enter-owner-metadata".to_string()),
        Some(tmux),
        Some(output.to_str().expect("utf8 fixture path").to_string()),
        None,
        0,
    );
    row.turn_nonce = token.turn_nonce().map(str::to_owned);
    discord::inflight::save_inflight_state(&row).expect("save isolated episode");
    row
}

async fn active_runtime(
    provider: ProviderKind,
    channel: ChannelId,
    root: &Path,
    label: &str,
) -> Arc<SharedData> {
    let shared = discord::make_shared_data_for_tests();
    let token = Arc::new(CancelToken::new());
    token.bind_unmanaged_session_name(&format!("owner-fixture-{label}-{}", channel.get()));
    let row = persist_episode(provider, channel, &token, root);
    assert!(
        discord::mailbox_try_start_turn(
            &shared,
            channel,
            token,
            UserId::new(27),
            MessageId::new(row.user_msg_id),
        )
        .await,
        "start a real fixture mailbox owner"
    );
    shared.tmux_watchers.insert(
        channel,
        watcher(
            row.tmux_session_name.as_deref().expect("fixture tmux"),
            row.output_path.as_deref().expect("fixture transcript"),
        ),
    );
    shared
}

fn episode_observation(active: bool) -> Observation {
    Observation {
        provider: "codex".to_string(),
        active_mailbox: active,
        user_message_id: active.then_some(51),
        turn_nonce: active.then(|| "private-episode-nonce".to_string()),
        token_tmux: active.then(|| "current-executor".to_string()),
        episode: Some(Episode {
            user_message_id: 51,
            mailbox_anchor_message_id: 51,
            current_message_id: 52,
            turn_nonce: Some("private-episode-nonce".to_string()),
            tmux_session: Some("current-executor".to_string()),
        }),
        episode_unobserved: false,
        watcher_tmux: None,
        watcher_owner: None,
        pending_queue: false,
        thread_binding: false,
        relay_coord: false,
    }
}

#[test]
fn current_owner_selection_is_registration_order_independent() {
    let current = episode_observation(true);
    let shared_file_only = episode_observation(false);
    assert_eq!(
        choose(42, &[shared_file_only.clone(), current.clone()]),
        Decision::Selected(1, "current_mailbox_episode")
    );
    assert_eq!(
        choose(42, &[current, shared_file_only]),
        Decision::Selected(0, "current_mailbox_episode")
    );
}

#[test]
fn multiple_current_owners_are_ambiguous_even_when_one_episode_matches() {
    let current = episode_observation(true);
    let mut older_active = current.clone();
    older_active.turn_nonce = Some("different-private-nonce".to_string());
    assert_eq!(choose(42, &[current, older_active]), Decision::Ambiguous);
    let mut watched = episode_observation(false);
    watched.watcher_tmux = Some("current-executor".to_string());
    watched.watcher_owner = Some(42);
    assert_eq!(choose(42, &[watched.clone(), watched]), Decision::Ambiguous);
}

#[test]
fn current_mailbox_proof_rejects_message_nonce_and_executor_mismatches() {
    for axis in ["message", "nonce", "executor"] {
        let mut mismatched = episode_observation(true);
        match axis {
            "message" => mismatched.user_message_id = Some(91),
            "nonce" => mismatched.turn_nonce = Some("other-private-nonce".to_string()),
            "executor" => mismatched.token_tmux = Some("old-executor".to_string()),
            _ => unreachable!(),
        }
        assert!(!mismatched.current_mailbox_episode(), "{axis}");
        assert_eq!(
            choose(42, &[mismatched]),
            Decision::ResolveProvider,
            "{axis}"
        );
    }
    let mut legacy = episode_observation(true);
    legacy.episode.as_mut().expect("episode").turn_nonce = None;
    assert!(
        legacy.current_mailbox_episode(),
        "legacy message/executor proof remains valid"
    );
    let mut injected = episode_observation(true);
    injected.episode.as_mut().expect("episode").user_message_id = 0;
    assert!(
        injected.current_mailbox_episode(),
        "restart adoption uses the durable effective finalizer anchor for injected turns"
    );
}

#[test]
fn owner_selection_metadata_is_bounded_and_keeps_episode_nonce_private() {
    let mut observation = episode_observation(true);
    observation.provider = "p".repeat(300);
    observation.token_tmux = Some("x".repeat(400));
    observation
        .episode
        .as_mut()
        .expect("episode")
        .current_message_id = 0;
    let selection = report(
        42,
        &vec![observation; 20],
        "ambiguous",
        None,
        "multiple_current_runtime_owners",
    );
    let json = serde_json::to_value(selection).expect("selection JSON");
    assert_eq!(json["candidate_count"], 20);
    assert_eq!(json["candidates_truncated"], true);
    assert_eq!(json["candidates"].as_array().expect("candidates").len(), 16);
    assert_eq!(
        json["candidates"][0]["provider"]
            .as_str()
            .expect("provider")
            .len(),
        128
    );
    assert_eq!(
        json["candidates"][0]["tmux_session"]
            .as_str()
            .expect("tmux")
            .len(),
        256
    );
    assert!(json["candidates"][0]["inflight_current_message_id"].is_null());
    let rendered = json.to_string();
    assert!(!rendered.contains("private-episode-nonce"));
    assert!(!rendered.contains("turn_nonce"));
    assert!(!rendered.contains("user_text"));
    assert!(!rendered.contains("session_id"));
}

#[test]
fn current_codex_generation_wins_over_shared_file_in_both_registration_orders() {
    fixture_test(|root| async move {
        for old_first in [true, false] {
            let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
            let old = discord::make_shared_data_for_tests();
            let current = active_runtime(ProviderKind::Codex, channel, &root, "current").await;
            let registry = HealthRegistry::new();
            let order = if old_first {
                [&old, &current]
            } else {
                [&current, &old]
            };
            for runtime in order {
                registry
                    .register("codex".to_string(), runtime.clone())
                    .await;
            }
            let stale = registry
                .snapshot_watcher_state_for_shared(&ProviderKind::Codex, old.clone(), channel.get())
                .await
                .expect("shared file also appears on the wrong runtime");
            assert!(!stale.attached);
            assert!(stale.mailbox_active_user_msg_id.is_none());
            let WatcherStateLookup::Selected {
                snapshot,
                selection,
            } = registry.snapshot_watcher_state_lookup(channel.get()).await
            else {
                panic!("unique current owner must be selected regardless of registration order")
            };
            assert!(snapshot.attached);
            assert_eq!(
                snapshot.mailbox_active_user_msg_id,
                Some(channel.get() + 100)
            );
            assert_eq!(snapshot.inflight_current_msg_id, Some(channel.get() + 200));
            assert_eq!(snapshot.watcher_owner_channel_id, Some(channel.get()));
            assert_eq!(selection.source, Some("current_mailbox_episode"));
            assert_eq!(selection.candidate_count, 2);
            let scoped = registry
                .snapshot_watcher_state_for_provider(&ProviderKind::Codex, channel.get())
                .await
                .expect("provider-scoped lookup selects the same current owner");
            assert_eq!(
                scoped.mailbox_active_user_msg_id,
                snapshot.mailbox_active_user_msg_id
            );
            assert_eq!(scoped.relay_stall_state, snapshot.relay_stall_state);
        }
    });
}

#[test]
fn health_detail_and_channel_lookup_classify_the_same_current_runtime() {
    fixture_test(|root| async move {
        use crate::services::discord::health::session_enrichment::{
            HealthSnapshotOptions, TmuxObservationBudget,
        };
        use crate::services::platform::tmux::SessionPresence;

        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let registry = HealthRegistry::new();
        registry
            .register("codex".to_string(), discord::make_shared_data_for_tests())
            .await;
        let current = active_runtime(ProviderKind::Codex, channel, &root, "health").await;
        registry.register("codex".to_string(), current).await;
        let row_before =
            discord::inflight::load_inflight_state_read_only(&ProviderKind::Codex, channel.get())
                .expect("row");
        let detail = super::super::build_health_snapshot_with_options(
            &registry,
            HealthSnapshotOptions {
                include_mailbox_details: true,
                tmux: TmuxObservationBudget::answering(SessionPresence::Missing),
            },
        )
        .await;
        let json = serde_json::to_value(detail).expect("detail JSON");
        let owner = json["mailboxes"]
            .as_array()
            .expect("mailboxes")
            .iter()
            .find(|entry| entry["channel_id"] == channel.get() && entry["has_cancel_token"] == true)
            .expect("current runtime mailbox");
        let WatcherStateLookup::Selected { snapshot, .. } =
            registry.snapshot_watcher_state_lookup(channel.get()).await
        else {
            panic!("current runtime owner")
        };
        assert!(snapshot.attached);
        assert_eq!(owner["watcher_attached"], snapshot.attached);
        assert_eq!(
            owner["active_user_message_id"],
            snapshot.mailbox_active_user_msg_id.expect("message")
        );
        assert_eq!(
            owner["relay_stall_state"],
            snapshot.relay_stall_state.as_str()
        );
        assert_eq!(
            owner["relay_health"]["desynced"],
            snapshot.relay_health.desynced
        );
        assert_eq!(
            snapshot.relay_stall_state.as_str(),
            "active_foreground_stream"
        );
        let row_after =
            discord::inflight::load_inflight_state_read_only(&ProviderKind::Codex, channel.get())
                .expect("row after");
        assert_eq!(
            serde_json::to_value(row_before).expect("before"),
            serde_json::to_value(row_after).expect("after"),
            "diagnostic ownership lookup must not refresh or mutate persisted inflight state"
        );
    });
}

#[test]
fn persisted_file_without_unique_runtime_authority_is_unobserved() {
    fixture_test(|root| async move {
        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let token = CancelToken::new();
        token.bind_unmanaged_session_name("persisted-only-fixture");
        persist_episode(ProviderKind::Codex, channel, &token, &root);
        let registry = HealthRegistry::new();
        for _ in 0..2 {
            registry
                .register("codex".to_string(), discord::make_shared_data_for_tests())
                .await;
        }
        assert!(matches!(
            registry.snapshot_watcher_state_lookup(channel.get()).await,
            WatcherStateLookup::Unobserved(_)
        ));
        assert!(
            registry
                .snapshot_watcher_state(channel.get())
                .await
                .is_none()
        );
        assert!(
            registry
                .snapshot_watcher_state_for_provider(&ProviderKind::Codex, channel.get())
                .await
                .is_none()
        );
    });
}

#[test]
fn configured_runtime_keeps_its_real_detached_snapshot_and_classifier() {
    fixture_test(|root| async move {
        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let token = CancelToken::new();
        token.bind_unmanaged_session_name("detached-owner-fixture");
        persist_episode(ProviderKind::Codex, channel, &token, &root);
        let registry = HealthRegistry::new();
        let unrelated = discord::make_shared_data_for_tests();
        let owner = discord::make_shared_data_for_tests();
        owner.settings.write().await.allowed_channel_ids = vec![channel.get()];
        registry.register("codex".to_string(), unrelated).await;
        registry.register("codex".to_string(), owner.clone()).await;
        let expected = registry
            .snapshot_watcher_state_for_shared(&ProviderKind::Codex, owner, channel.get())
            .await
            .expect("detached snapshot");
        let WatcherStateLookup::Selected {
            snapshot,
            selection,
        } = registry.snapshot_watcher_state_lookup(channel.get()).await
        else {
            panic!("explicit configured runtime is authoritative")
        };
        assert_eq!(selection.source, Some("provider_channel_binding"));
        assert!(
            !snapshot.attached,
            "selection must not manufacture attachment or health"
        );
        assert_eq!(snapshot.desynced, expected.desynced);
        assert_eq!(snapshot.relay_stall_state, expected.relay_stall_state);
        assert_eq!(
            snapshot.relay_health.watcher_attached,
            expected.relay_health.watcher_attached
        );
    });
}

#[test]
fn conflicting_active_episode_cannot_be_overridden_by_configured_other_runtime() {
    fixture_test(|root| async move {
        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let registry = HealthRegistry::new();
        let active = discord::make_shared_data_for_tests();
        let persisted_token = CancelToken::new();
        persisted_token.bind_unmanaged_session_name("conflicting-episode-fixture");
        let row = persist_episode(ProviderKind::Codex, channel, &persisted_token, &root);
        let active_token = Arc::new(CancelToken::new());
        active_token.bind_unmanaged_session_name("conflicting-episode-fixture");
        assert_ne!(active_token.turn_nonce(), persisted_token.turn_nonce());
        assert!(
            discord::mailbox_try_start_turn(
                &active,
                channel,
                active_token,
                UserId::new(27),
                MessageId::new(row.user_msg_id)
            )
            .await
        );
        let configured_other = discord::make_shared_data_for_tests();
        configured_other.settings.write().await.allowed_channel_ids = vec![channel.get()];
        registry.register("codex".to_string(), active).await;
        registry
            .register("codex".to_string(), configured_other)
            .await;
        let WatcherStateLookup::Unobserved(selection) =
            registry.snapshot_watcher_state_lookup(channel.get()).await
        else {
            panic!("conflicting active episode must prevent another runtime from shadowing it")
        };
        assert_eq!(
            selection.reason,
            "active_mailbox_episode_conflicts_with_runtime_resolution"
        );
    });
}

#[test]
fn two_actual_provider_mailboxes_are_ambiguous_without_health_ranking() {
    fixture_test(|root| async move {
        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let registry = HealthRegistry::new();
        for provider in [ProviderKind::Codex, ProviderKind::Gemini] {
            let owner = active_runtime(provider.clone(), channel, &root, provider.as_str()).await;
            registry
                .register(provider.as_str().to_string(), owner)
                .await;
        }
        assert!(matches!(
            registry.snapshot_watcher_state_lookup(channel.get()).await,
            WatcherStateLookup::Ambiguous(_)
        ));
        assert!(
            registry
                .snapshot_watcher_state(channel.get())
                .await
                .is_none()
        );
        assert!(
            registry
                .snapshot_watcher_state_for_provider(&ProviderKind::Codex, channel.get())
                .await
                .is_some(),
            "provider-scoped observation can still uniquely identify its own current runtime"
        );
    });
}

#[test]
fn already_registered_runtime_acquiring_an_owner_invalidates_pending_selection() {
    fixture_test(|root| async move {
        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let registry = HealthRegistry::new();
        let codex = active_runtime(ProviderKind::Codex, channel, &root, "before-race").await;
        let gemini = discord::make_shared_data_for_tests();
        registry.register("codex".to_string(), codex).await;
        registry
            .register("gemini".to_string(), gemini.clone())
            .await;
        let generation = registry.registration_generation();
        let plan = resolve(&registry, channel, None)
            .await
            .unwrap_or_else(|_| panic!("initial unique owner"));

        let token = Arc::new(CancelToken::new());
        token.bind_unmanaged_session_name("new-owner-between-selection-and-publication");
        let row = persist_episode(ProviderKind::Gemini, channel, &token, &root);
        assert!(
            discord::mailbox_try_start_turn(
                &gemini,
                channel,
                token,
                UserId::new(27),
                MessageId::new(row.user_msg_id)
            )
            .await
        );
        assert_eq!(
            registry.registration_generation(),
            generation,
            "the competing owner was already registered"
        );
        assert!(
            matches!(
                snapshot_selected(&registry, channel, None, generation, plan).await,
                WatcherStateLookup::Ambiguous(_)
            ),
            "revalidating only the originally selected runtime would publish a false unique-owner verdict"
        );
    });
}

#[test]
fn unreadable_persisted_episode_is_unobserved_instead_of_absent_or_prelaunch() {
    fixture_test(|root| async move {
        let channel = ChannelId::new(NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed));
        let registry = HealthRegistry::new();
        let current = active_runtime(ProviderKind::Codex, channel, &root, "unobserved-file").await;
        registry.register("codex".to_string(), current).await;
        let inflight_root =
            discord::inflight::inflight_runtime_root().expect("isolated inflight root");
        let path = discord::inflight::inflight_state_path(
            &inflight_root,
            &ProviderKind::Codex,
            channel.get(),
        );
        std::fs::write(path, b"invalid fixture JSON").expect("unreadable episode fixture");
        let WatcherStateLookup::Unobserved(selection) =
            registry.snapshot_watcher_state_lookup(channel.get()).await
        else {
            panic!("failed inflight observation cannot become absence or prelaunch proof")
        };
        assert_eq!(selection.reason, "persisted_inflight_observation_failed");
        let json = serde_json::to_value(selection).expect("metadata");
        assert_eq!(json["candidates"][0]["persisted_inflight_observed"], false);
        assert!(
            !json.to_string().contains("invalid fixture JSON"),
            "raw parser errors and file content are not diagnostic metadata"
        );
    });
}

/// Shared test fixture for the actual HTTP handler. It exposes no runtime
/// construction or persistence helper in non-test builds.
impl HealthRegistry {
    pub(crate) async fn watcher_state_http_fixture(
        channel_id: u64,
        owners: usize,
        persisted: bool,
        root: &Path,
    ) -> Self {
        let channel = ChannelId::new(channel_id);
        let registry = Self::new();
        registry
            .register("codex".to_string(), discord::make_shared_data_for_tests())
            .await;
        if owners == 0 && persisted {
            let token = CancelToken::new();
            token.bind_unmanaged_session_name("http-persisted-owner-fixture");
            persist_episode(ProviderKind::Codex, channel, &token, root);
            registry
                .register("codex".to_string(), discord::make_shared_data_for_tests())
                .await;
        }
        for index in 0..owners {
            let owner =
                active_runtime(ProviderKind::Codex, channel, root, &format!("http-{index}")).await;
            registry.register("codex".to_string(), owner).await;
        }
        registry
    }
}
