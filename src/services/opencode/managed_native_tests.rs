//! Real pinned native CLI + loopback model + PostgreSQL. Opt in explicitly;
//! no user config, credentials, real inference or production service is used.
use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use crate::services::cluster::execution_capacity::{ExecutionLimits, managed_tests};
use crate::services::provider_auth_profile::{ProviderAuthProfileDef, resolve_at};
use std::io::Write;
use std::process::{Child, ChildStdout};
use std::sync::mpsc;

struct NativeFixture {
    model: Child,
    output: BufReader<ChildStdout>,
    workspace: String,
    server: Option<OpenCodeWarmServerLease>,
}

impl NativeFixture {
    fn start(binary: &str, case: &str) -> Self {
        let mut model = Command::new("python3")
            .args([
                "scripts/probe_opencode_managed.py",
                "--serve-model-fixture",
                case,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start owned loopback model fixture");
        let mut output = BufReader::new(model.stdout.take().unwrap());
        let mut ready = String::new();
        output.read_line(&mut ready).unwrap();
        let metadata: Value = serde_json::from_str(&ready).expect("fixture ready metadata");
        let mut fixture = Self {
            model,
            output,
            workspace: metadata["workspace"].as_str().unwrap().into(),
            server: None,
        };
        let overlay = resolve_at(
            std::path::Path::new(metadata["profile_root"].as_str().unwrap()),
            ProviderKind::OpenCode,
            Some("native-contract-fixture"),
            None,
            &std::collections::HashMap::from([(
                "native-contract-fixture".into(),
                ProviderAuthProfileDef {
                    provider: "opencode".into(),
                    home: Some(metadata["profile_home"].as_str().unwrap().into()),
                    env: std::collections::BTreeMap::from([(
                        "OPENAI_API_KEY".into(),
                        "synthetic-native-provider-token".into(),
                    )]),
                },
            )]),
        )
        .expect("resolve fixture through the production profile catalog contract");
        let resolution = crate::services::platform::BinaryResolution {
            requested_binary: "opencode".into(),
            resolved_path: Some(binary.into()),
            canonical_path: Some(binary.into()),
            source: Some("pinned-native-test".into()),
            attempts: Vec::new(),
            failure_kind: None,
            exec_path: None,
        };
        fixture.server = Some(
            acquire_warm_server(binary, &resolution, &fixture.workspace, &overlay, true)
                .expect("start isolated managed OpenCode through production launcher"),
        );
        fixture
    }

    fn stop(&mut self) -> Value {
        if let Some(server) = self.server.take() {
            let shared = server.shared_server();
            evict_warm_server_from_pool(&shared);
            shared.shutdown();
        }
        if let Some(mut input) = self.model.stdin.take() {
            let _ = input.write_all(b"stop\n");
        }
        let mut receipt = String::new();
        self.output.read_line(&mut receipt).unwrap();
        assert!(self.model.wait().unwrap().success());
        serde_json::from_str(&receipt).unwrap()
    }
}

impl Drop for NativeFixture {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            let shared = server.shared_server();
            evict_warm_server_from_pool(&shared);
            shared.shutdown();
        }
        self.model.stdin.take();
        for _ in 0..50 {
            if self.model.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        crate::services::process::kill_child_tree(&mut self.model);
        let _ = self.model.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Linux root launcher, AGENTDESK_TEST_OPENCODE_BIN=1.14.41 and PostgreSQL; run --include-ignored"]
async fn managed_native_adapter_enforces_db_budget_and_silent_deadline_pg() {
    let binary =
        std::env::var("AGENTDESK_TEST_OPENCODE_BIN").expect("pinned native test binary required");
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    for case in ["read_edit", "exhausted", "quiet", "aliases"] {
        let pool = pool.clone();
        let binary = binary.clone();
        tokio::task::spawn_blocking(move || {
            let mut native = NativeFixture::start(&binary, case);
            let (cancel, guard) = managed_tests::guarded(
                pool.clone(),
                "opencode",
                case,
                &native.workspace,
                ExecutionLimits {
                    max_elapsed_secs: if case == "quiet" { 3 } else { 30 },
                    max_tool_calls: if matches!(case, "exhausted" | "aliases") {
                        3
                    } else {
                        2
                    },
                },
            );
            let server = native.server.as_ref().unwrap();
            let (tx, rx) = mpsc::channel();
            let started = Instant::now();
            let result = run_session(
                "Execute the fixture sequence and finish.",
                None,
                Some(&["read".into(), "edit".into()]),
                None,
                server.base_url(),
                server.auth(),
                &tx,
                Some(cancel.as_ref()),
                Some(server.startup_output()),
                Some(server.shared_server()),
            );
            let elapsed = started.elapsed();
            let events: Vec<_> = rx.try_iter().collect();
            let done = events
                .iter()
                .any(|event| matches!(event, StreamMessage::Done { .. }));
            let used: i32 = tokio::runtime::Handle::current().block_on(sqlx::query_scalar(
                "SELECT cardinality(permission_ids) FROM node_execution_leases WHERE channel_id=$1"
            ).bind(case).fetch_one(&pool)).unwrap();
            if case == "read_edit" {
                assert!(result.is_ok(), "{result:?}");
                assert!(done);
                assert_eq!(used, 2);
                assert_eq!(
                    std::fs::read_to_string(
                        std::path::Path::new(&native.workspace).join("allowed.txt")
                    )
                    .unwrap(),
                    "AFTER_NATIVE_EDIT\n"
                );
            } else if case == "aliases" {
                assert!(
                    result.is_ok(),
                    "tool denials should still have a normal native terminal: {result:?}"
                );
                assert!(done);
                assert!(used <= 3);
                let history: Value = session_request_agent()
                    .get(&format!("{}/session", server.base_url()))
                    .set("Authorization", server.auth())
                    .call()
                    .unwrap()
                    .into_json()
                    .unwrap();
                let session = history.as_array().unwrap().first().unwrap()["id"]
                    .as_str()
                    .unwrap();
                let messages: Value = session_request_agent()
                    .get(&format!("{}/session/{session}/message", server.base_url()))
                    .set("Authorization", server.auth())
                    .call()
                    .unwrap()
                    .into_json()
                    .unwrap();
                let tools: Vec<_> = messages
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|message| message["parts"].as_array().unwrap())
                    .filter(|part| part["type"] == "tool")
                    .collect();
                assert_eq!(
                    tools.len(),
                    3,
                    "every requested alias probe must actually run"
                );
                assert!(
                    tools.iter().all(|part| part["state"]["status"] == "error"),
                    "{tools:?}"
                );
            } else {
                assert!(result.is_err(), "{case} must not authorize Done");
                assert!(!done);
                assert_eq!(used, if case == "exhausted" { 3 } else { 0 });
                if case == "quiet" {
                    assert!(
                        elapsed < Duration::from_secs(10),
                        "silent cancellation took {elapsed:?}"
                    );
                    assert_eq!(
                        cancel.cancel_source().as_deref(),
                        Some("managed_attempt_watchdog_timeout")
                    );
                }
            }
            drop(guard);
            let state: Option<String> = tokio::runtime::Handle::current()
                .block_on(
                    sqlx::query_scalar(
                        "SELECT execution_state FROM node_execution_leases WHERE channel_id=$1",
                    )
                    .bind(case)
                    .fetch_optional(&pool),
                )
                .unwrap();
            assert_eq!(state.as_deref(), if done { None } else { Some("unknown") });
            let receipt = native.stop();
            let observation = if done { None } else {
                tokio::runtime::Handle::current().block_on(async {
                    crate::services::cluster::execution_capacity::recovery::observe_retained(&pool, "native-test").await.unwrap();
                    let retained: (String, i32, Value) = sqlx::query_as("SELECT execution_state,cardinality(permission_ids),stop_observation FROM node_execution_leases WHERE channel_id=$1")
                        .bind(case).fetch_one(&pool).await.unwrap();
                    assert_eq!(retained.0, "unknown", "local process exit must not settle model effects");
                    assert_eq!(retained.1, used, "observation must not refund or reset the tool budget");
                    assert_eq!(retained.2["local_writer_status"], "not_observed");
                    assert_eq!(retained.2["inference_status"], "unknown");
                    Some(retained.2)
                })
            };
            assert_eq!(receipt["outside_unchanged"], true);
            assert_eq!(receipt["outside_marker_leaked"], false);
            assert_eq!(receipt["credential_marker_leaked"], false);
            println!(
                "MANAGED_NATIVE_EVIDENCE {}",
                serde_json::json!({
                    "case":case,"permission_reservations":used,"done":done,"retained_state":state,
                    "elapsed_ms":elapsed.as_millis(),"model_fixture":receipt,"stop_observation":observation,"real_model_used":false
                })
            );
        })
        .await
        .unwrap();
    }
    pool.close().await;
    fixture.drop().await;
}
