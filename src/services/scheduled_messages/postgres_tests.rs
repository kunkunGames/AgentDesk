use super::*;
use chrono::Duration;
use sqlx::Row;

async fn create_test_pool() -> (
    crate::dispatch::test_support::DispatchPostgresTestDb,
    PgPool,
) {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_retry_exhaustion",
        "scheduled message retry exhaustion regression",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;
    (pg_db, pool)
}

async fn insert_recurring_agent_message(
    pool: &PgPool,
    agent_id: &str,
    on_agent_failure: &str,
) -> ScheduledMessageRow {
    sqlx::query(
        "INSERT INTO agents (id, name, discord_channel_id)
         VALUES ($1, $2, '123456789')",
    )
    .bind(agent_id)
    .bind(format!("Scheduled Test {agent_id}"))
    .execute(pool)
    .await
    .expect("seed scheduled-message agent");

    db::insert_scheduled_message_pg(
        pool,
        &db::NewScheduledMessage {
            content: format!("retry exhaustion payload for {agent_id}"),
            title: None,
            target_channel_id: Some("123456789".to_string()),
            bot: "announce".to_string(),
            delivery_kind: db::KIND_AGENT.to_string(),
            agent_id: Some(agent_id.to_string()),
            agent_instruction: None,
            on_agent_failure: on_agent_failure.to_string(),
            scheduled_at: Utc::now() - Duration::minutes(1),
            schedule: Some("@every 10m".to_string()),
            timezone: "UTC".to_string(),
            expires_at: None,
            source: "postgres_test".to_string(),
            created_by: Some("postgres_test".to_string()),
            dedupe_key: None,
        },
    )
    .await
    .expect("insert recurring scheduled message")
}

async fn claim_one(pool: &PgPool, owner: &str) -> ClaimedFire {
    let mut claimed = db::claim_due_fires_pg(pool, owner, 10, LEASE_SECS, Utc::now())
        .await
        .expect("claim due scheduled message");
    assert_eq!(claimed.len(), 1, "exactly one definition should be due");
    claimed.pop().expect("claimed scheduled-message fire")
}

async fn claim_after_exhausting_rearms(pool: &PgPool) -> ClaimedFire {
    let mut fire = claim_one(pool, "retry-worker-0").await;
    assert_eq!(fire.retry_count, 0);

    for expected_retry_count in 1..=(MAX_FIRE_RETRIES + 1) {
        assert!(
            db::interrupt_delivery_and_rewind_pg(
                pool,
                &fire.delivery_id,
                &fire.claim_token,
                &fire.message.id,
                fire.fire_scheduled_at,
                "test retry before exhaustion",
            )
            .await
            .expect("interrupt retryable fire"),
            "the current claim should rewind its definition"
        );
        fire = claim_one(pool, &format!("retry-worker-{expected_retry_count}")).await;
        assert_eq!(fire.retry_count, expected_retry_count);
    }

    assert!(fire.retry_count > MAX_FIRE_RETRIES);
    fire
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_default_notify_reaches_push_outbox() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_notify_default",
        "scheduled message default notify bot regression",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;

    let stored_bot: String = sqlx::query_scalar(
        "INSERT INTO scheduled_messages
             (id, content, target_channel_id, scheduled_at, timezone)
         VALUES
             ('smsg-notify-default', 'info-only scheduled push', '123456789',
              NOW() - INTERVAL '1 second', 'UTC')
         RETURNING bot",
    )
    .fetch_one(&pool)
    .await
    .expect("insert scheduled message through the database default");
    assert_eq!(stored_bot, "notify");

    let mut claims =
        db::claim_due_fires_pg(&pool, "notify-default-worker", 1, LEASE_SECS, Utc::now())
            .await
            .expect("claim default-notify scheduled push");
    assert_eq!(claims.len(), 1);
    fire_claimed(
        &pool,
        None,
        claims.pop().expect("claimed default-notify fire"),
        Utc::now(),
    )
    .await;

    let deliveries = db::list_deliveries_pg(&pool, "smsg-notify-default", 10, None)
        .await
        .expect("load default-notify delivery");
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, db::DELIVERY_SENT);
    let outbox_id = deliveries[0]
        .outbox_id
        .expect("push should record its outbox handoff");
    let outbox_bot: String = sqlx::query_scalar("SELECT bot FROM message_outbox WHERE id = $1")
        .bind(outbox_id)
        .fetch_one(&pool)
        .await
        .expect("load default-notify outbox row");
    assert_eq!(outbox_bot, "notify");

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_retry_exhaustion_terminalizes_recurring_definitions() {
    let (pg_db, pool) = create_test_pool().await;

    let failed_message =
        insert_recurring_agent_message(&pool, "scheduled-retry-fail-agent", "fail").await;
    let failed_fire = claim_after_exhausting_rearms(&pool).await;
    fire_claimed(&pool, None, failed_fire.clone(), Utc::now()).await;

    let failed_parent = db::get_scheduled_message_pg(&pool, &failed_message.id)
        .await
        .expect("read failed parent")
        .expect("failed parent exists");
    assert_eq!(failed_parent.status, db::STATUS_FAILED);
    assert_eq!(failed_parent.scheduled_at, failed_fire.fire_scheduled_at);
    assert_eq!(failed_parent.in_flight_delivery_id, None);
    assert_eq!(failed_parent.fire_count, 0);
    assert!(
        failed_parent
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("fire retry budget exhausted"))
    );
    let failed_deliveries = db::list_deliveries_pg(&pool, &failed_message.id, 10, None)
        .await
        .expect("list failed deliveries");
    assert_eq!(failed_deliveries.len(), 1);
    assert_eq!(failed_deliveries[0].status, db::DELIVERY_FAILED);
    assert_eq!(failed_deliveries[0].retry_count, MAX_FIRE_RETRIES + 1);
    assert_eq!(failed_deliveries[0].fallback_outbox_id, None);

    let fallback_message =
        insert_recurring_agent_message(&pool, "scheduled-retry-fallback-agent", "push_raw").await;
    let fallback_fire = claim_after_exhausting_rearms(&pool).await;
    fire_claimed(&pool, None, fallback_fire.clone(), Utc::now()).await;

    let fallback_parent = db::get_scheduled_message_pg(&pool, &fallback_message.id)
        .await
        .expect("read fallback parent")
        .expect("fallback parent exists");
    assert_eq!(fallback_parent.status, db::STATUS_FAILED);
    assert_eq!(
        fallback_parent.scheduled_at,
        fallback_fire.fire_scheduled_at
    );
    assert_eq!(fallback_parent.in_flight_delivery_id, None);
    assert_eq!(fallback_parent.fire_count, 1);
    assert!(fallback_parent.last_fired_at.is_some());
    assert!(
        fallback_parent
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("fell back to raw push"))
    );

    let fallback_deliveries = db::list_deliveries_pg(&pool, &fallback_message.id, 10, None)
        .await
        .expect("list fallback deliveries");
    assert_eq!(fallback_deliveries.len(), 1);
    let fallback_delivery = &fallback_deliveries[0];
    assert_eq!(fallback_delivery.status, db::DELIVERY_SENT);
    assert_eq!(fallback_delivery.retry_count, MAX_FIRE_RETRIES + 1);
    let fallback_outbox_id = fallback_delivery
        .fallback_outbox_id
        .expect("push_raw should record its durable outbox handoff");
    let outbox = sqlx::query(
        "SELECT target, content, source, status, reason_code
         FROM message_outbox
         WHERE id = $1",
    )
    .bind(fallback_outbox_id)
    .fetch_one(&pool)
    .await
    .expect("read push_raw outbox row");
    assert_eq!(
        outbox.try_get::<String, _>("target").unwrap(),
        "channel:123456789"
    );
    assert_eq!(
        outbox.try_get::<String, _>("content").unwrap(),
        fallback_message.content
    );
    assert_eq!(
        outbox.try_get::<String, _>("source").unwrap(),
        OUTBOX_SOURCE
    );
    assert_eq!(outbox.try_get::<String, _>("status").unwrap(), "pending");
    assert!(
        outbox
            .try_get::<Option<String>, _>("reason_code")
            .unwrap()
            .as_deref()
            .is_some_and(|reason| reason.contains(":fallback:"))
    );

    assert!(
        db::claim_due_fires_pg(&pool, "post-terminal-worker", 10, LEASE_SECS, Utc::now())
            .await
            .expect("scan after terminal exhaustion")
            .is_empty(),
        "recurring definitions must not advance to another slot after retry exhaustion"
    );

    pool.close().await;
    pg_db.drop().await;
}
