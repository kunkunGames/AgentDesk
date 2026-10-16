//! Exercise the shipped idle-recap JS, real bridge sockets and real API auth.
use super::*;
use axum::{
    Json, Router,
    http::HeaderMap,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

const FIXTURE_TOKEN: &str = "policy-http-synthetic-fixture-token";

async fn protected_api() -> (u16, Arc<Mutex<Vec<bool>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let empty_policies = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config::default();
    config.server.host = "0.0.0.0".to_string();
    config.server.port = port;
    config.server.auth_token = Some(FIXTURE_TOKEN.to_string());
    config.policies.dir = empty_policies.path().to_path_buf();
    config.policies.hot_reload = false;
    let broadcast_tx = crate::eventbus::new_broadcast();
    let batch_buffer = crate::eventbus::spawn_batch_flusher(broadcast_tx.clone());
    let state = crate::server::routes::AppState {
        pg_pool: None,
        engine: crate::engine::PolicyEngine::new(&config).unwrap(),
        config: Arc::new(config),
        broadcast_tx,
        batch_buffer,
        health_registry: None,
        cluster_instance_id: None,
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let post_requests = requests.clone();
    let get_requests = requests.clone();
    let router = Router::new()
        .route(
            "/api/sessions/{key}/idle-recap",
            post(move |headers: HeaderMap, Json(body): Json<Value>| {
                let requests = post_requests.clone();
                async move {
                    assert_eq!(body, json!({"retry": false}));
                    requests
                        .lock()
                        .unwrap()
                        .push(headers.get("authorization").is_some_and(|value| {
                            value.as_bytes() == format!("Bearer {FIXTURE_TOKEN}").as_bytes()
                        }));
                    Json(json!({"posted": true}))
                }
            }),
        )
        .route(
            "/api/cluster/nodes",
            get(move |headers: HeaderMap| {
                let requests = get_requests.clone();
                async move {
                    requests
                        .lock()
                        .unwrap()
                        .push(headers.get("authorization").is_some_and(|value| {
                            value.as_bytes() == format!("Bearer {FIXTURE_TOKEN}").as_bytes()
                        }));
                    Json(json!({"nodes": []}))
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            state,
            crate::server::routes::auth::auth_middleware,
        ));
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (port, requests, server)
}

fn run_idle_recap(port: u16, server_port: u16, token: Option<&str>, tick_engine: bool) -> Value {
    let policies = tempfile::tempdir().unwrap();
    // Use the actual policy module. Only its DB candidate and scheduling fixture
    // are supplied here; HTTP registration and transport remain the real ones.
    let idle_recap = include_str!("../../../../policies/timeouts/idle-recap.js");
    let script = format!(
        r#"
        var fixture = {{ name: "policy-http-auth-fixture", priority: 1 }};
        var observedResult = null;
        var continuedAfterRecap = false;
        var nativePost = agentdesk.http.post;
        agentdesk.http.post = function(url, body) {{
            observedResult = nativePost(url, body);
            return observedResult;
        }};
        agentdesk.db.query = function(sql) {{
            return [{{ session_key: "codex/auth-fixture", provider: "codex", thread_channel_id: null }}];
        }};
        agentdesk.config.get = function(key) {{ return key === "server_port" ? {port} : null; }};
        agentdesk.log.error = function() {{}};
        agentdesk.log.info = function() {{}};
        (function() {{
            var module = {{ exports: {{}} }};
            {idle_recap}
            module.exports(fixture, {{ parseSessionThreadId: function() {{ return null; }} }});
        }})();
        fixture.onTick5min = function() {{
            fixture._section_R_idle_recap();
            continuedAfterRecap = true;
        }};
        agentdesk.registerPolicy(fixture);
    "#
    );
    std::fs::write(policies.path().join("fixture.js"), script).unwrap();
    let mut config = crate::config::Config::default();
    config.server.port = server_port;
    config.server.auth_token = token.map(str::to_owned);
    config.policies.dir = policies.path().to_path_buf();
    config.policies.hot_reload = false;
    let engine = if tick_engine {
        crate::engine::PolicyEngine::new_for_tick(&config, None).unwrap()
    } else {
        crate::engine::PolicyEngine::new(&config).unwrap()
    };
    engine
        .fire_hook_by_name_blocking("onTick5min", json!({}))
        .unwrap();
    let inner = engine.inner.lock().unwrap();
    inner.context.with(|ctx| {
        assert!(ctx.eval::<bool, _>("continuedAfterRecap").unwrap());
        let public_bridge: String = ctx.eval("JSON.stringify(agentdesk)").unwrap();
        assert!(!public_bridge.contains(FIXTURE_TOKEN));
        let result: String = ctx.eval("JSON.stringify(observedResult)").unwrap();
        serde_json::from_str(&result).unwrap()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_recap_policy_carries_boot_auth_through_main_and_tick_engines() {
    let (port, requests, server) = protected_api().await;
    for tick_engine in [false, true] {
        assert_eq!(
            run_idle_recap(port, port, Some(FIXTURE_TOKEN), tick_engine),
            json!({"posted": true})
        );
    }
    assert_eq!(*requests.lock().unwrap(), vec![true, true]);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_recap_policy_missing_auth_preserves_unauthorized_and_hook_continuation() {
    let (port, requests, server) = protected_api().await;
    let result = run_idle_recap(port, port, None, true);
    assert_eq!(
        result,
        json!({"error": "unauthorized", "message": "Bearer token required"})
    );
    assert!(requests.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_recap_policy_other_local_port_never_receives_server_auth() {
    let (port, requests, server) = protected_api().await;
    let other_port = if port == u16::MAX { port - 1 } else { port + 1 };
    let result = run_idle_recap(port, other_port, Some(FIXTURE_TOKEN), true);
    assert_eq!(result["error"], "unauthorized");
    assert!(requests.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_http_get_uses_same_origin_auth_without_exposing_it_to_js() {
    let (port, requests, server) = protected_api().await;
    let runtime = rquickjs::Runtime::new().unwrap();
    let context = rquickjs::Context::full(&runtime).unwrap();
    let mut config = crate::config::ServerConfig::default();
    config.port = port;
    config.auth_token = Some(FIXTURE_TOKEN.to_string());
    context.with(|ctx| {
        ctx.eval::<(), _>("var agentdesk = {};").unwrap();
        register_http_ops(&ctx, &config).unwrap();
        let result: String = ctx
            .eval(format!(
                "JSON.stringify(agentdesk.http.get('http://127.0.0.1:{port}/api/cluster/nodes'))"
            ))
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&result).unwrap(),
            json!({"nodes": []})
        );
        let public_bridge: String = ctx.eval("JSON.stringify(agentdesk)").unwrap();
        assert!(!public_bridge.contains(FIXTURE_TOKEN));
    });
    assert_eq!(*requests.lock().unwrap(), vec![true]);
    server.abort();
}

#[test]
fn policy_http_auth_is_bound_to_plain_http_api_origin_and_rejects_header_injection() {
    let mut server = crate::config::ServerConfig::default();
    server.auth_token = Some(FIXTURE_TOKEN.to_string());
    for url in [
        "http://127.0.0.1:8792/api/sessions/x/idle-recap",
        "http://127.0.0.1:8791/unrelated",
        "https://127.0.0.1:8791/api/sessions/x/idle-recap",
        "http://example.com:8791/api/sessions/x/idle-recap",
        "http://user@127.0.0.1:8791/api/sessions/x/idle-recap",
    ] {
        assert!(
            policy_http_token(Some(&server), &url::Url::parse(url).unwrap())
                .unwrap()
                .is_none()
        );
    }
    server.auth_token = Some("synthetic\r\nInjected: invalid".to_string());
    let url = url::Url::parse("http://127.0.0.1:8791/api/sessions/x/idle-recap").unwrap();
    assert_eq!(
        policy_http_token(Some(&server), &url),
        Err("invalid policy HTTP authorization")
    );
}
