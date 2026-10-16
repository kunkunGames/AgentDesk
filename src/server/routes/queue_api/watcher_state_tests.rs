use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
    routing::get,
};
use serde_json::Value;
use tower::ServiceExt;

use super::*;
use crate::services::discord::health::HealthRegistry;

static NEXT_CHANNEL: AtomicU64 = AtomicU64::new(9_407_800_000_000);

fn fixture_test<F, Fut>(test: F)
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    let _lock = crate::services::turn_orchestrator::test_support::lock_test_env();
    let root = tempfile::tempdir().expect("isolated HTTP diagnostic fixture root");
    let _root = crate::config::TestEnvVarGuard::set_path_after_shared_test_env_lock(
        "AGENTDESK_ROOT_DIR",
        root.path(),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("HTTP fixture runtime")
        .block_on(test(root.path().to_path_buf()));
}

async fn get_fixture(owners: usize, persisted: bool, root: &Path) -> (StatusCode, Value, u64) {
    let channel = NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed);
    let registry =
        HealthRegistry::watcher_state_http_fixture(channel, owners, persisted, root).await;
    let config = crate::config::Config::default();
    let engine = crate::engine::PolicyEngine::new(&config).expect("HTTP fixture engine");
    let broadcast_tx = crate::eventbus::new_broadcast();
    let batch_buffer = crate::eventbus::spawn_batch_flusher(broadcast_tx.clone());
    let state = AppState {
        pg_pool: None,
        engine,
        config: Arc::new(config),
        broadcast_tx,
        batch_buffer,
        health_registry: Some(Arc::new(registry)),
        cluster_instance_id: None,
    };
    let app = Router::new()
        .route("/channels/{id}/watcher-state", get(get_watcher_state))
        .with_state(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/channels/{channel}/watcher-state"))
                .body(Body::empty())
                .expect("watcher diagnostic request"),
        )
        .await
        .expect("watcher diagnostic response");
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("bounded response body");
    (
        status,
        serde_json::from_slice(&body).expect("response JSON"),
        channel,
    )
}

fn assert_no_relay_verdict(body: &Value) {
    assert_eq!(body["observed"], false);
    for field in [
        "attached",
        "desynced",
        "relay_stall_state",
        "relay_health",
        "tmux_session_alive",
    ] {
        assert!(
            body.get(field).is_none(),
            "unresolved ownership must not fabricate {field}"
        );
    }
}

#[test]
fn watcher_http_selects_current_generation_and_preserves_existing_snapshot_fields() {
    fixture_test(|root| async move {
        let (status, body, channel) = get_fixture(1, true, &root).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["provider"], "codex");
        assert_eq!(body["attached"], true);
        assert_eq!(body["mailbox_active_user_msg_id"], channel + 100);
        assert_eq!(body["inflight_current_msg_id"], channel + 200);
        assert_eq!(body["relay_stall_state"], "active_foreground_stream");
        assert_eq!(body["owner_selection"]["status"], "selected");
        assert_eq!(body["owner_selection"]["source"], "current_mailbox_episode");
        let selection = body["owner_selection"].to_string();
        assert!(!selection.contains("private fixture prompt"));
        assert!(!selection.contains("private-provider-session"));
        assert!(!selection.contains("turn_nonce"));
    });
}

#[test]
fn watcher_http_reports_real_current_owner_ambiguity_as_conflict_without_stall() {
    fixture_test(|root| async move {
        let (status, body, _) = get_fixture(2, true, &root).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["diagnostic_status"], "ambiguous");
        assert_eq!(
            body["owner_selection"]["reason"],
            "multiple_current_runtime_owners"
        );
        assert_no_relay_verdict(&body);
    });
}

#[test]
fn watcher_http_reports_unobserved_ownership_without_declaring_server_unhealthy() {
    fixture_test(|root| async move {
        let (status, body, _) = get_fixture(0, true, &root).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["diagnostic_status"], "unobserved");
        assert_eq!(
            body["owner_selection"]["reason"],
            "current_runtime_owner_not_established"
        );
        assert!(
            body["error"]
                .as_str()
                .expect("diagnostic reason")
                .contains("does not determine server or provider health")
        );
        assert_no_relay_verdict(&body);
    });
}

#[test]
fn watcher_http_absent_channel_retains_not_found_contract() {
    fixture_test(|root| async move {
        let (status, body, channel) = get_fixture(0, false, &root).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["channel_id"], channel.to_string());
        assert_eq!(
            body["error"],
            "no watcher, relay-coord, or inflight state for this channel"
        );
        assert_eq!(body["diagnostic_status"], "absent");
        assert_eq!(body["owner_selection"]["candidate_count"], 0);
        assert_no_relay_verdict(&body);
    });
}
