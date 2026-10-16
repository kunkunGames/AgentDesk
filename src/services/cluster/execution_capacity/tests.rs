use super::*;
use crate::db::auto_queue::test_support::TestPostgresDb;
use crate::db::intake_outbox::{InsertPendingPayload, insert_pending};

#[test]
fn resource_admission_preserves_remote_and_unresolved_legacy_workspaces() {
    let workspace = tempfile::tempdir().unwrap();
    let local = workspace.path().to_str().unwrap();
    let missing = workspace.path().join("not-created");
    let missing = missing.to_str().unwrap();
    let canonical = std::fs::canonicalize(local)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(
        admission::resource(local, false, true).unwrap(),
        Some(canonical)
    );
    // An identical local path must not be mistaken for the remote resource.
    assert!(admission::resource(local, true, false).unwrap().is_none());
    assert!(admission::resource(missing, true, false).unwrap().is_none());
    assert!(
        admission::resource(missing, false, false)
            .unwrap()
            .is_none()
    );
    assert!(admission::resource(local, true, true).is_err());
    assert!(admission::resource(missing, false, true).is_err());
}

fn payload(node: &str, channel: &str) -> InsertPendingPayload {
    InsertPendingPayload {
        execution_requirements: json!({}),
        attachment_refs: json!([]),
        target_instance_id: node.into(),
        forwarded_by_instance_id: "leader".into(),
        provider: "claude".into(),
        required_labels: json!([]),
        channel_id: channel.into(),
        user_msg_id: format!("message-{channel}"),
        replay_source_message_ids: None,
        request_owner_id: "user".into(),
        request_owner_name: None,
        user_text: "test".into(),
        reply_context: None,
        has_reply_boundary: false,
        dm_hint: None,
        turn_kind: "foreground".into(),
        merge_consecutive: false,
        reply_to_user_message: false,
        defer_watcher_resume: false,
        wait_for_completion: false,
        preserve_on_cancel: false,
        agent_id: "agent".into(),
        home_epoch: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execution_capacity_atomic_reservations_execution_and_fenced_expiry_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate().await;
    for node in ["mac", "windows"] {
        sqlx::query("INSERT INTO worker_nodes(instance_id,last_heartbeat_at,capabilities) VALUES($1,NOW(),$2)")
            .bind(node).bind(json!({"execution_capacity":{"version":1,"slots":2}})).execute(&pool).await.unwrap();
    }
    let mut jobs = tokio::task::JoinSet::new();
    for n in 0..12 {
        let pool = pool.clone();
        jobs.spawn(async move {
            let channel = format!("{n}");
            let result = insert_pending(&pool, &payload("mac", &channel), 1, None).await;
            (channel, result)
        });
    }
    let mut admitted = Vec::new();
    while let Some(result) = jobs.join_next().await {
        let (channel, result) = result.unwrap();
        match result {
            Ok(_) => admitted.push(channel),
            Err(error) => assert!(is_exhausted(&error), "{error}"),
        }
    }
    assert_eq!(
        admitted.len(),
        2,
        "concurrent producers must share the same budget"
    );
    let channel = &admitted[0];
    let first = uuid::Uuid::new_v4();
    store::acquire(&pool, "mac", "claude", channel, first)
        .await
        .unwrap();
    let nodes = super::super::node_registry::list_worker_nodes(&pool, 30)
        .await
        .unwrap();
    let mac = nodes.iter().find(|n| n["instance_id"] == "mac").unwrap();
    assert_eq!(
        mac["execution_occupied"],
        json!(2),
        "delivery and execution must not double count"
    );
    assert_eq!(mac["execution_active"], json!(1));
    assert!(is_exhausted(
        &store::acquire(&pool, "mac", "claude", "overflow", uuid::Uuid::new_v4())
            .await
            .unwrap_err()
    ));
    let other = uuid::Uuid::new_v4();
    store::acquire(&pool, "windows", "claude", "win-turn", other)
        .await
        .unwrap();
    sqlx::query("DELETE FROM intake_outbox WHERE target_instance_id='mac'")
        .execute(&pool)
        .await
        .unwrap();
    // Delivery finished, but the provider is still running and occupies a slot.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM node_execution_occupancy('mac')")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(
        store::renew(&pool, "mac", "claude", channel, first)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE node_execution_leases SET expires_at=NOW()-INTERVAL '1 second' WHERE instance_id='mac'").execute(&pool).await.unwrap();
    assert!(
        !store::renew(&pool, "mac", "claude", channel, first)
            .await
            .unwrap()
    );
    let successor = uuid::Uuid::new_v4();
    store::acquire(&pool, "mac", "claude", channel, successor)
        .await
        .unwrap();
    store::release(&pool, "mac", "claude", channel, first)
        .await
        .unwrap();
    assert!(
        store::renew(&pool, "mac", "claude", channel, successor)
            .await
            .unwrap(),
        "old cleanup cannot release its successor"
    );
    store::release(&pool, "mac", "claude", channel, successor)
        .await
        .unwrap();
    assert!(
        store::renew(&pool, "windows", "claude", "win-turn", other)
            .await
            .unwrap(),
        "another node's lease is independent"
    );
    let semaphore = Arc::new(Semaphore::new(1));
    let cancel = Arc::new(CancelToken::new());
    let guard = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let semaphore = semaphore.clone();
        let cancel = cancel.clone();
        move || {
            acquire_guard(
                pool,
                "windows".into(),
                "claude".into(),
                "guard".into(),
                cancel,
                semaphore.try_acquire_owned().unwrap(),
                None,
                LEASE_IO_TIMEOUT,
            )
        }
    })
    .await
    .unwrap()
    .unwrap();
    sqlx::query("UPDATE node_execution_leases SET expires_at=NOW()-INTERVAL '1 second' WHERE channel_id='guard'")
        .execute(&pool).await.unwrap();
    tokio::time::timeout(Duration::from_secs(17), async {
        while !cancel.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("lease loss must cancel provider");
    assert_eq!(
        semaphore.available_permits(),
        0,
        "lease expiry cannot free an executing local slot"
    );
    tokio::task::spawn_blocking(move || drop(guard))
        .await
        .unwrap();
    assert_eq!(semaphore.available_permits(), 1);
    // Normal release completes before another turn acquires the same identity.
    for _ in 0..2 {
        let pool = pool.clone();
        let semaphore = semaphore.clone();
        tokio::task::spawn_blocking(move || {
            let guard = acquire_guard(
                pool,
                "windows".into(),
                "claude".into(),
                "guard".into(),
                Arc::new(CancelToken::new()),
                semaphore.try_acquire_owned().unwrap(),
                None,
                LEASE_IO_TIMEOUT,
            )
            .unwrap();
            drop(guard);
        })
        .await
        .unwrap();
    }
    pool.close().await;
    fixture.drop().await;
}

#[test]
fn execution_capacity_ranking_uses_ratio_fairness_and_preserves_legacy_selector() {
    let node = |id: &str, used: u64, slots: u64, last: Option<&str>| {
        json!({
            "instance_id":id,"status":"online","labels":["worker"],
            "execution_occupied":used,"last_execution_assignment_at":last,
            "capabilities":{"execution_capacity":{"version":1,"slots":slots}}
        })
    };
    let mut nodes = vec![
        node("a", 1, 2, Some("2026-09-20")),
        node("z", 1, 4, None),
        node("full", 2, 2, None),
        json!({"instance_id":"legacy"}),
    ];
    rank(&mut nodes);
    assert_eq!(nodes.len(), 2);
    let candidates = super::super::intake_routing::candidates_from_worker_nodes_json(&nodes);
    let selection =
        super::super::intake_routing::pick_intake_target(&candidates, &["worker".into()], "leader");
    assert_eq!(
        selection,
        super::super::intake_routing::IntakeRouteTarget::Worker {
            instance_id: "z".into()
        }
    );
    let mut tied = vec![node("a", 0, 2, Some("2026-09-20")), node("z", 0, 2, None)];
    rank(&mut tied);
    assert_eq!(
        tied[0]["instance_id"], "z",
        "never assigned wins a utilization tie"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execution_lease_survives_pool_timeout_but_cancels_on_lost_ownership_pg() {
    let fixture = TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate_with_max_connections(1).await;
    sqlx::query("INSERT INTO worker_nodes(instance_id,last_heartbeat_at,capabilities) VALUES('renewal-node',NOW(),$1)")
        .bind(json!({"execution_capacity":{"version":1,"slots":1}}))
        .execute(&pool).await.unwrap();
    let cancel = Arc::new(CancelToken::new());
    let (ready_tx, ready_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let worker_pool = pool.clone();
    let worker_cancel = cancel.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let guard = acquire_guard(
            worker_pool,
            "renewal-node".into(),
            "qwen".into(),
            "pool-pressure".into(),
            worker_cancel,
            permit,
            None,
            LEASE_IO_TIMEOUT,
        )
        .unwrap();
        ready_tx.send(()).unwrap();
        // Dropping the sender on an assertion failure also releases the guard.
        let _ = stop_rx.recv();
        drop(guard);
    });
    ready_rx.await.unwrap();
    let original: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT expires_at FROM node_execution_leases WHERE instance_id='renewal-node'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let held_connection = pool.acquire().await.unwrap();
    // Renewal starts at 10 s; holding the only connection beyond its 5 s I/O
    // timeout reproduces transient pool pressure without expiring the 30 s lease.
    tokio::time::sleep(LEASE_RENEW_INTERVAL + LEASE_IO_TIMEOUT + Duration::from_secs(2)).await;
    assert!(
        !cancel.cancelled.load(std::sync::atomic::Ordering::Acquire),
        "one transport timeout must not cancel a still-valid lease"
    );
    drop(held_connection);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let renewed: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
                "SELECT expires_at FROM node_execution_leases WHERE instance_id='renewal-node'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if renewed > original {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("lease must renew after the connection becomes available");
    assert!(!cancel.cancelled.load(std::sync::atomic::Ordering::Acquire));
    sqlx::query("DELETE FROM node_execution_leases WHERE instance_id='renewal-node'")
        .execute(&pool)
        .await
        .unwrap();
    tokio::time::timeout(LEASE_RENEW_INTERVAL + Duration::from_secs(5), async {
        while !cancel.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("definitive ownership loss must still cancel immediately on renewal");
    assert_eq!(
        cancel.cancel_source().as_deref(),
        Some("execution_lease_lost")
    );
    drop(stop_tx);
    worker.await.unwrap();
    pool.close().await;
    fixture.drop().await;
}
