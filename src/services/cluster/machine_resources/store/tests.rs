use super::*;

#[tokio::test]
async fn recorder_queue_is_bounded_without_waiting_for_the_database() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost:1/unused")
        .unwrap();
    let sender = spawn_recorder(pool, "offline-db".into(), Arc::new(AtomicBool::new(false)));
    // This current-thread test has not yielded, so the recorder cannot consume
    // the queue. Saturation must return immediately, without a database await.
    for _ in 0..PENDING_SAMPLES {
        sender.try_send(json!({})).unwrap();
    }
    assert!(matches!(
        sender.try_send(json!({})),
        Err(mpsc::error::TrySendError::Full(_))
    ));
}

#[tokio::test]
async fn postgres_machine_history_is_durable_ordered_deduplicated_and_retained() {
    let db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "machine_history",
        "machine resource history",
    )
    .await;
    let pool = db.connect_and_migrate().await;
    let now = chrono::Utc::now().timestamp_millis();
    let sample = |at| {
        json!({"observed_at_ms": at, "expires_at_ms": at + 30_000,
        "cpu": {"usage_percent": null}, "network": null})
    };
    for at in [now, now - 10_000, now - 20_000, now] {
        record(&pool, "runner-a", &sample(at)).await.unwrap();
    }
    record(&pool, "runner-b", &sample(now)).await.unwrap();
    record(&pool, "runner-a", &json!(null)).await.unwrap();
    let rows = history(&pool, "runner-a", now - 30_000, now, 2)
        .await
        .unwrap();
    assert_eq!(rows, vec![sample(now - 10_000), sample(now)]);
    assert_eq!(
        history(&pool, "runner-a", now - 30_000, now, 10)
            .await
            .unwrap()
            .len(),
        3
    );
    let old = now - (HISTORY_RETENTION_DAYS + 1) * 86_400_000;
    record(&pool, "runner-a", &sample(old)).await.unwrap();
    assert_eq!(purge_expired(&pool).await.unwrap(), 1);
    assert_eq!(
        history(&pool, "runner-b", now - 1, now, 10).await.unwrap(),
        vec![sample(now)]
    );
    assert!(
        history(&pool, "runner-a", old, old, 10)
            .await
            .unwrap()
            .is_empty()
    );
    pool.close().await;
    db.drop().await;
}
