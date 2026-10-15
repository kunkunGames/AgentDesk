use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;

async fn setup(kind: RuntimeHandoffKind) -> (TestPostgresDb, PgPool, SessionSnapshot) {
    let db = TestPostgresDb::create().await;
    let pool = db.connect_and_migrate_with_max_connections(4).await;
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("AgentDesk-codex-stale-{tag}");
    let session_key = format!("provider:{name}");
    let dispatch_id = format!("stale-dispatch-{tag}");
    sqlx::query("INSERT INTO agents(id,name,provider) VALUES('stale-agent','Stale Agent','codex')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO kanban_cards(id,title,status,assigned_agent_id) VALUES('stale-card','Stale Card','in_progress','stale-agent')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_dispatches(id,kanban_card_id,to_agent_id,dispatch_type,status,context) VALUES($1,'stale-card','stale-agent','implementation','dispatched','{}')")
        .bind(&dispatch_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_runs(id,repo,agent_id,status) VALUES('stale-run','repo','stale-agent','active')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO auto_queue_entries(id,run_id,kanban_card_id,agent_id,status,dispatch_id,retry_count,slot_index) VALUES('stale-entry','stale-run','stale-card','stale-agent','dispatched',$1,0,0)")
        .bind(&dispatch_id).execute(&pool).await.unwrap();
    let heartbeat = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456Z")
        .unwrap()
        .with_timezone(&Utc);
    sqlx::query("INSERT INTO sessions(session_key,provider,channel_id,instance_id,status,active_dispatch_id,last_heartbeat,idle_recap_turn_generation)
        VALUES($1,'codex','42','hub-old','turn_active',$2,$3,7)")
        .bind(&session_key).bind(&dispatch_id).bind(heartbeat).execute(&pool).await.unwrap();
    crate::services::tmux_common::write_tmux_runtime_kind_marker(&name, kind).unwrap();
    (
        db,
        pool,
        SessionSnapshot {
            session_key,
            status: "turn_active".into(),
            provider: Some("codex".into()),
            channel_id: Some("42".into()),
            instance_id: Some("hub-old".into()),
            turn_generation: 7,
            last_heartbeat: heartbeat,
            active_dispatch_id: Some(dispatch_id),
            active_dispatch_status: Some("dispatched".into()),
            active_turn_nonce: None,
            runtime_evidence: RuntimeEvidence {
                runtime_kind: Some(kind),
            },
        },
    )
}

async fn finish(db: TestPostgresDb, pool: PgPool, snapshot: &SessionSnapshot) {
    let name = crate::services::discord::session_identity::tmux_name_from_session_key(
        &snapshot.session_key,
    )
    .unwrap();
    if let Some(path) = crate::services::tmux_common::resolve_session_temp_path(
        &name,
        crate::services::tmux_common::TMUX_RUNTIME_KIND_TEMP_EXT,
    ) {
        std::fs::remove_file(path).unwrap();
    }
    pool.close().await;
    db.drop().await;
}

fn request(snapshot: &SessionSnapshot) -> RecoveryRequest {
    RecoveryRequest {
        snapshot: snapshot.clone(),
        clear_active_dispatch_id: true,
    }
}

/// An actual responding server proves the unique absent target is missing.
/// A missing socket or executable deliberately remains unknown in production.
struct OwnedTmuxProbeKeeper {
    name: String,
}

impl OwnedTmuxProbeKeeper {
    fn start() -> Self {
        let name = format!(
            "AgentDesk-stale-probe-keeper-{}",
            uuid::Uuid::new_v4().simple()
        );
        let output = crate::services::platform::tmux::create_session(&name, None, "sleep 120")
            .expect("real tmux is required for the proven-dead TUI fixture");
        assert!(output.status.success(), "owned tmux fixture must start");
        Self { name }
    }
}

impl Drop for OwnedTmuxProbeKeeper {
    fn drop(&mut self) {
        let cleaned = crate::services::platform::tmux::kill_session_output_timeout(
            &self.name,
            "stale_recovery_test_owned_keeper_cleanup",
            std::time::Duration::from_secs(2),
        )
        .is_ok_and(|output| output.status.success());
        if !cleaned {
            // Never expand a refused exact-session cleanup into kill-server.
            // Fail this test when possible; during unwinding retain the owned
            // target as unknown rather than reporting a fabricated cleanup.
            if std::thread::panicking() {
                eprintln!("owned stale recovery tmux fixture cleanup remains unknown");
            } else {
                panic!("owned stale recovery tmux fixture cleanup remains unknown");
            }
        }
    }
}

async fn state(
    pool: &PgPool,
    snapshot: &SessionSnapshot,
) -> (String, String, String, Option<String>) {
    let dispatch = sqlx::query_scalar("SELECT status FROM task_dispatches WHERE id=$1")
        .bind(snapshot.active_dispatch_id.as_deref().unwrap())
        .fetch_one(pool)
        .await
        .unwrap();
    let entry = sqlx::query_scalar("SELECT status FROM auto_queue_entries WHERE id='stale-entry'")
        .fetch_one(pool)
        .await
        .unwrap();
    let session: (String, Option<String>) =
        sqlx::query_as("SELECT status,active_dispatch_id FROM sessions WHERE session_key=$1")
            .bind(&snapshot.session_key)
            .fetch_one(pool)
            .await
            .unwrap();
    (dispatch, entry, session.0, session.1)
}

#[tokio::test]
async fn stale_snapshot_cannot_idle_a_replacement_turn_or_terminalize_its_dispatch_pg() {
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::CodexTui).await;
    sqlx::query("UPDATE sessions SET idle_recap_turn_generation=8,instance_id='hub-new',last_heartbeat=NOW() WHERE session_key=$1")
        .bind(&snapshot.session_key).execute(&pool).await.unwrap();
    let outcome = recover_on_pg(&pool, request(&snapshot)).await.unwrap();
    assert_eq!(outcome["applied"], false);
    assert_eq!(outcome["reason"], "session_identity_changed");
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    finish(db, pool, &snapshot).await;
}

#[tokio::test]
async fn proven_dead_tui_settlement_uses_canonical_queue_cleanup_once_pg() {
    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    let _keeper = OwnedTmuxProbeKeeper::start();
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    let outcome = recover_on_pg(&pool, request(&snapshot)).await.unwrap();
    assert_eq!(outcome["applied"], true);
    assert_eq!(
        state(&pool, &snapshot).await,
        ("failed".into(), "failed".into(), "idle".into(), None)
    );
    let outcome = recover_on_pg(&pool, request(&snapshot)).await.unwrap();
    assert_eq!(outcome["applied"], false);
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dispatch_events WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(snapshot.active_dispatch_id.as_deref().unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    finish(db, pool, &snapshot).await;
}

#[tokio::test]
async fn session_write_failure_rolls_back_canonical_dispatch_and_queue_settlement_pg() {
    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    let _keeper = OwnedTmuxProbeKeeper::start();
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::CodexTui).await;
    sqlx::query("CREATE FUNCTION reject_stale_session_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rollback'; END $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_stale_update BEFORE UPDATE ON sessions FOR EACH ROW EXECUTE FUNCTION reject_stale_session_update()")
        .execute(&pool).await.unwrap();
    assert!(recover_on_pg(&pool, request(&snapshot)).await.is_err());
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    finish(db, pool, &snapshot).await;
}

#[tokio::test]
async fn native_runtime_replacement_cannot_enter_tmux_settlement_pg() {
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::ProcessBackend).await;
    let outcome = recover_on_pg(&pool, request(&snapshot)).await.unwrap();
    assert_eq!(outcome["applied"], false);
    assert_eq!(outcome["reason"], "runtime_identity_changed");
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    finish(db, pool, &snapshot).await;
}

#[tokio::test]
async fn fresh_or_future_heartbeat_cannot_enter_stale_settlement_pg() {
    let (db, pool, mut snapshot) = setup(RuntimeHandoffKind::CodexTui).await;
    for interval in ["0 minutes", "30 minutes"] {
        snapshot.last_heartbeat = sqlx::query_scalar("UPDATE sessions SET last_heartbeat=NOW()+CAST($2::TEXT AS INTERVAL) WHERE session_key=$1 RETURNING last_heartbeat")
            .bind(&snapshot.session_key).bind(interval).fetch_one(&pool).await.unwrap();
        let outcome = recover_on_pg(&pool, request(&snapshot)).await.unwrap();
        assert_eq!(outcome["applied"], false);
        assert_eq!(outcome["reason"], "session_heartbeat_fresh");
        assert_eq!(
            state(&pool, &snapshot).await,
            (
                "dispatched".into(),
                "dispatched".into(),
                "turn_active".into(),
                snapshot.active_dispatch_id.clone()
            )
        );
    }
    finish(db, pool, &snapshot).await;
}

struct OwnedNativeChild {
    name: String,
    child: std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
}

impl OwnedNativeChild {
    fn register(snapshot: &SessionSnapshot) -> Self {
        use crate::services::session_backend::{SessionHandle, insert_process_session};
        use std::sync::{Arc, Mutex};
        let name = crate::services::discord::session_identity::tmux_name_from_session_key(
            &snapshot.session_key,
        )
        .unwrap();
        let child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let pid = child.id();
        let child = Arc::new(Mutex::new(Some(child)));
        insert_process_session(
            &name,
            SessionHandle::Process {
                child_stdin: Arc::new(Mutex::new(None)),
                child: child.clone(),
                pid,
                output: Arc::new(Mutex::new(tempfile::tempfile().unwrap())),
            },
        );
        Self { name, child }
    }
}

impl Drop for OwnedNativeChild {
    fn drop(&mut self) {
        crate::services::session_backend::remove_process_session(&self.name);
        if let Some(child) = self.child.lock().unwrap().as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn raw_recovery(pool: &PgPool, snapshot: &SessionSnapshot) -> Value {
    let payload = json!({"snapshot":snapshot,"clear_active_dispatch_id":true}).to_string();
    serde_json::from_str(&recover_stale_session_raw(Some(pool), &payload)).unwrap()
}

#[tokio::test]
async fn native_child_overrides_legacy_marker_for_live_busy_exited_and_unavailable_handles_pg() {
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    let native = OwnedNativeChild::register(&snapshot);
    let evidence = runtime_evidence(&snapshot.session_key);
    assert_eq!(evidence["runtime_kind"], "process_backend");
    assert_eq!(evidence["liveness"], "live");
    assert_eq!(
        raw_recovery(&pool, &snapshot)["reason"],
        "native_or_unknown_runtime_preserved"
    );
    {
        let mut child = native.child.lock().unwrap();
        let evidence = runtime_evidence(&snapshot.session_key);
        assert_eq!(evidence["runtime_kind"], "process_backend");
        assert_eq!(evidence["reason"], "child_handle_busy");
        assert_eq!(raw_recovery(&pool, &snapshot)["applied"], false);
        let child = child.as_mut().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }
    let evidence = runtime_evidence(&snapshot.session_key);
    assert_eq!(evidence["runtime_kind"], "process_backend");
    assert_eq!(evidence["liveness"], "exited");
    assert_eq!(raw_recovery(&pool, &snapshot)["applied"], false);
    native.child.lock().unwrap().take();
    let evidence = runtime_evidence(&snapshot.session_key);
    assert_eq!(evidence["runtime_kind"], "process_backend");
    assert_eq!(evidence["reason"], "child_handle_unavailable");
    assert_eq!(raw_recovery(&pool, &snapshot)["applied"], false);
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    drop(native);
    finish(db, pool, &snapshot).await;
}

#[tokio::test]
async fn busy_native_registry_preserves_unknown_without_legacy_marker_fallback_pg() {
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    crate::services::session_backend::with_process_registry_busy_for_test(|| {
        let evidence = runtime_evidence(&snapshot.session_key);
        assert_eq!(evidence["runtime_kind"], Value::Null);
        assert_eq!(evidence["liveness"], "unknown");
        assert_eq!(evidence["reason"], "registry_busy");
        let outcome = raw_recovery(&pool, &snapshot);
        assert_eq!(outcome["applied"], false);
        assert_eq!(outcome["reason"], "native_or_unknown_runtime_preserved");
    });
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    finish(db, pool, &snapshot).await;
}

/// Own the real ProcessBackend wrapper even before registry publication.
/// The private executable ignores wrapper CLI arguments and execs a real child.
struct OwnedNativeBackend {
    _directory: tempfile::TempDir,
    name: String,
    output_path: std::path::PathBuf,
    child: std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
    unpublished: Option<crate::services::session_backend::SessionHandle>,
}

impl OwnedNativeBackend {
    fn spawn(name: &str) -> Self {
        use crate::services::session_backend::{
            ProcessBackend, SessionBackend, SessionConfig, SessionHandle,
        };
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("owned-native-wrapper");
        std::fs::write(&executable, "#!/bin/sh\nexec sleep 30\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let output_path = directory.path().join("native.jsonl");
        let prompt_path = directory.path().join("native.prompt");
        std::fs::write(&prompt_path, "owned test prompt").unwrap();
        let handle = ProcessBackend::new()
            .create_session(&SessionConfig {
                session_name: name.into(),
                working_dir: directory.path().to_string_lossy().into_owned(),
                agentdesk_exe: executable.to_string_lossy().into_owned(),
                output_path: output_path.to_string_lossy().into_owned(),
                prompt_path: prompt_path.to_string_lossy().into_owned(),
                wrapper_subcommand: "codex-tmux-wrapper".into(),
                wrapper_args: Vec::new(),
                env_vars: Vec::new(),
                unset_env: Vec::new(),
            })
            .unwrap();
        let child = match &handle {
            SessionHandle::Process { child, .. } => child.clone(),
            SessionHandle::TestProcess { .. } => panic!("fixture must spawn a real child"),
        };
        Self {
            _directory: directory,
            name: name.into(),
            output_path,
            child,
            unpublished: Some(handle),
        }
    }

    fn publish(&mut self) {
        crate::services::session_backend::insert_process_session(
            &self.name,
            self.unpublished.take().expect("publish exactly once"),
        );
    }
}

impl Drop for OwnedNativeBackend {
    fn drop(&mut self) {
        crate::services::session_backend::remove_process_session(&self.name);
        if let Some(child) = self.child.lock().unwrap().as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
async fn native_spawn_before_registry_publication_serializes_actual_stale_recovery_pg() {
    use crate::services::session_backend::with_process_session_launch_authority;
    use std::sync::mpsc;
    use std::time::Duration;

    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    let _keeper = OwnedTmuxProbeKeeper::start();
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    let name = crate::services::discord::session_identity::tmux_name_from_session_key(
        &snapshot.session_key,
    )
    .unwrap();
    let (spawned_tx, spawned_rx) = mpsc::channel();
    let (publish_tx, publish_rx) = mpsc::channel();
    let launch_name = name.clone();
    let launcher = std::thread::spawn(move || {
        with_process_session_launch_authority(&launch_name, || {
            let mut native = OwnedNativeBackend::spawn(&launch_name);
            spawned_tx.send(()).unwrap();
            // A timeout still drops the unpublished owned child; no leaked work.
            publish_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            native.publish();
            Ok(native)
        })
    });
    spawned_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    // This deterministic probe proves recovery reaches the identical authority
    // before pane probing or PG writes, rather than relying on thread timing.
    let contention =
        crate::services::tmux_common::source_authority_contention_key_for_tests(|| {
            let _ = raw_recovery(&pool, &snapshot);
        });
    let (started_tx, started_rx) = mpsc::channel();
    let (outcome_tx, outcome_rx) = mpsc::channel();
    let recovery_pool = pool.clone();
    let recovery_snapshot = snapshot.clone();
    let recovery = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        outcome_tx
            .send(raw_recovery(&recovery_pool, &recovery_snapshot))
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let early = outcome_rx.recv_timeout(Duration::from_millis(100));
    let before_publication = state(&pool, &snapshot).await;
    // Release and join before assertions, including a failed regression, so the
    // real child and both worker threads are always owned and cleaned up.
    publish_tx.send(()).unwrap();
    let native = launcher.join().unwrap().unwrap();
    recovery.join().unwrap();
    let waited_for_publication = matches!(early, Err(mpsc::RecvTimeoutError::Timeout));
    let outcome = match early {
        Ok(outcome) => outcome,
        Err(_) => outcome_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
    };
    let after_publication = state(&pool, &snapshot).await;
    let failures: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dispatch_events WHERE dispatch_id=$1 AND to_status='failed'",
    )
    .bind(snapshot.active_dispatch_id.as_deref().unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    drop(native);
    finish(db, pool, &snapshot).await;

    assert_eq!(
        contention,
        Some(
            std::path::Path::new(&crate::services::tmux_common::session_temp_path(
                &name,
                crate::services::tmux_common::TMUX_RUNTIME_KIND_TEMP_EXT,
            ))
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .into_owned()
        )
    );
    assert!(
        waited_for_publication,
        "recovery must wait for native publication"
    );
    assert_eq!(outcome["applied"], false);
    assert_eq!(outcome["reason"], "native_or_unknown_runtime_preserved");
    let preserved = (
        "dispatched".into(),
        "dispatched".into(),
        "turn_active".into(),
        snapshot.active_dispatch_id.clone(),
    );
    assert_eq!(before_publication, preserved);
    assert_eq!(after_publication, preserved);
    assert_eq!(failures, 0);
}

#[tokio::test]
async fn native_output_wait_releases_source_authority_and_preserves_recovery_pg() {
    use crate::services::agent_protocol::StreamMessage;
    use crate::services::provider::{CancelToken, ReadOutputResult};
    use crate::services::session_backend::{
        process_session_probe, read_output_file_until_result, with_process_session_launch_authority,
    };
    use std::io::Write;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    let name = crate::services::discord::session_identity::tmux_name_from_session_key(
        &snapshot.session_key,
    )
    .unwrap();
    let native = with_process_session_launch_authority(&name, || {
        let mut native = OwnedNativeBackend::spawn(&name);
        native.publish();
        Ok(native)
    })
    .unwrap();
    let assistant = "{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"owned fixture output\"}]}}\n";
    let terminal = "{\"type\":\"result\",\"subtype\":\"success\",\"result\":\"fixture terminal\",\"session_id\":\"fixture-session\"}\n";
    let mut output = std::fs::OpenOptions::new()
        .append(true)
        .open(&native.output_path)
        .unwrap();
    output.write_all(assistant.as_bytes()).unwrap();
    output.flush().unwrap();
    let (sender, receiver) = mpsc::channel();
    let cancel = Arc::new(CancelToken::new());
    let reader_cancel = cancel.clone();
    let output_path = native.output_path.to_string_lossy().into_owned();
    let reader_name = name.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        result_tx
            .send(read_output_file_until_result(
                &output_path,
                0,
                sender,
                Some(reader_cancel),
                process_session_probe(&reader_name),
            ))
            .unwrap();
    });
    let opened = receiver.recv_timeout(Duration::from_secs(2));
    let pending = matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    let free = crate::services::tmux_common::try_with_tmux_source_authority(&name, |_| ());
    let outcome = raw_recovery(&pool, &snapshot);
    output.write_all(terminal.as_bytes()).unwrap();
    output.flush().unwrap();
    let completed = result_rx.recv_timeout(Duration::from_secs(2));
    cancel
        .cancelled
        .store(true, std::sync::atomic::Ordering::Relaxed);
    reader.join().unwrap();
    let after = state(&pool, &snapshot).await;
    drop(native);
    finish(db, pool, &snapshot).await;

    assert!(
        matches!(opened, Ok(StreamMessage::OutputOffset { offset }) if offset == assistant.len() as u64)
    );
    assert!(
        pending,
        "actual opened reader must still await the terminal fixture"
    );
    assert_eq!(free, Some(()), "output wait must not hold source authority");
    assert_eq!(outcome["applied"], false);
    assert_eq!(outcome["reason"], "native_or_unknown_runtime_preserved");
    assert_eq!(
        completed.unwrap().unwrap(),
        ReadOutputResult::Completed {
            offset: (assistant.len() + terminal.len()) as u64
        }
    );
    assert_eq!(
        after,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
}

#[tokio::test]
async fn durable_native_hosting_intent_survives_registry_loss_without_adoption_pg() {
    use crate::services::session_backend::{
        NativeRegistryPresence, process_session_evidence, remove_process_session,
        with_process_session_launch_authority,
    };
    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    let name = crate::services::discord::session_identity::tmux_name_from_session_key(
        &snapshot.session_key,
    )
    .unwrap();
    let native = with_process_session_launch_authority(&name, || {
        let mut native = OwnedNativeBackend::spawn(&name);
        native.publish();
        Ok(native)
    })
    .unwrap();
    assert_eq!(
        crate::services::tmux_common::resolve_tmux_runtime_kind_marker(&name),
        Some(RuntimeHandoffKind::ProcessBackend)
    );
    // Losing memory-owned registration across a Hub restart does not kill the
    // actual wrapper, prove its descendants stopped, or authorize re-adoption.
    let detached = remove_process_session(&name).unwrap();
    assert!(
        native
            .child
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        process_session_evidence(&name).registration,
        NativeRegistryPresence::Absent
    );
    let evidence = runtime_evidence(&snapshot.session_key);
    assert_eq!(evidence["runtime_kind"], "process_backend");
    assert_eq!(evidence["liveness"], "unknown");
    let outcome = raw_recovery(&pool, &snapshot);
    assert_eq!(outcome["applied"], false);
    assert_eq!(outcome["reason"], "native_or_unknown_runtime_preserved");
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    drop(detached);
    drop(native);
    finish(db, pool, &snapshot).await;
}

#[tokio::test]
async fn hosting_intent_file_replace_or_parent_sync_failure_refuses_native_effects_pg() {
    use crate::services::session_backend::with_process_session_launch_authority;
    use crate::services::tmux_common::{
        HostingIntentWriteStage, with_hosting_intent_write_failure_for_test,
    };
    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    for stage in [
        HostingIntentWriteStage::FileSync,
        HostingIntentWriteStage::Replace,
        HostingIntentWriteStage::ParentSync,
    ] {
        let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
        let name = crate::services::discord::session_identity::tmux_name_from_session_key(
            &snapshot.session_key,
        )
        .unwrap();
        let mut launched = None;
        let outcome = with_hosting_intent_write_failure_for_test(stage, || {
            with_process_session_launch_authority(&name, || {
                // This is the actual ProcessBackend effect path; a failed
                // durability admission must never enter it, not merely omit
                // registry publication after a provider has already spawned.
                launched = Some(OwnedNativeBackend::spawn(&name));
                Ok(())
            })
        });
        let admitted = launched.is_some();
        drop(launched);
        assert!(
            !admitted,
            "hosting intent {stage:?} failure must precede spawn/input"
        );
        assert!(outcome.is_err());
        let kind = crate::services::tmux_common::resolve_tmux_runtime_kind_marker(&name);
        if stage == HostingIntentWriteStage::ParentSync {
            // The replacement succeeded before the rejected directory sync;
            // keep that conservative native intent, never restore legacy mode.
            assert_eq!(kind, Some(RuntimeHandoffKind::ProcessBackend));
            assert_eq!(raw_recovery(&pool, &snapshot)["applied"], false);
        } else {
            assert_eq!(kind, Some(RuntimeHandoffKind::LegacyTmuxWrapper));
        }
        assert_eq!(
            state(&pool, &snapshot).await,
            (
                "dispatched".into(),
                "dispatched".into(),
                "turn_active".into(),
                snapshot.active_dispatch_id.clone()
            )
        );
        finish(db, pool, &snapshot).await;
    }
}

#[tokio::test]
async fn generic_tmux_cleanup_and_legacy_stamp_preserve_registered_or_unknown_native_pg() {
    let _test_env = crate::config::test_env_lock::acquire_shared_test_env_lock();
    let (db, pool, snapshot) = setup(RuntimeHandoffKind::LegacyTmuxWrapper).await;
    let native = OwnedNativeChild::register(&snapshot);
    let output_path = crate::services::tmux_common::session_temp_path(&native.name, "jsonl");
    std::fs::write(&output_path, "owned fixture source").unwrap();
    crate::services::tmux_common::cleanup_session_temp_files(&native.name);
    assert_eq!(
        std::fs::read_to_string(&output_path).unwrap(),
        "owned fixture source"
    );
    assert!(
        crate::services::tmux_common::write_tmux_runtime_kind_marker(
            &native.name,
            RuntimeHandoffKind::CodexTui
        )
        .is_err()
    );
    crate::services::session_backend::with_process_registry_busy_for_test(|| {
        crate::services::tmux_common::cleanup_session_temp_files(&native.name);
        assert!(
            crate::services::tmux_common::require_tmux_hosting_admission(&native.name).is_err()
        );
        assert!(
            crate::services::tmux_common::write_tmux_runtime_kind_marker(
                &native.name,
                RuntimeHandoffKind::LegacyTmuxWrapper
            )
            .is_err()
        );
    });
    assert_eq!(
        std::fs::read_to_string(&output_path).unwrap(),
        "owned fixture source"
    );
    assert_eq!(
        state(&pool, &snapshot).await,
        (
            "dispatched".into(),
            "dispatched".into(),
            "turn_active".into(),
            snapshot.active_dispatch_id.clone()
        )
    );
    drop(native);
    std::fs::remove_file(output_path).unwrap();
    finish(db, pool, &snapshot).await;
}
