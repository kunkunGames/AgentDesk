use super::*;
use chrono::Duration;
use sqlx::Row;

async fn create_test_pool(
    prefix: &str,
    label: &str,
) -> (
    crate::dispatch::test_support::DispatchPostgresTestDb,
    PgPool,
) {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(prefix, label).await;
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
async fn postgres_scheduled_message_retry_exhaustion_terminalizes_recurring_definitions() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_retry_exhaustion",
        "scheduled message retry exhaustion regression",
    )
    .await;

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

    let expired_message =
        insert_recurring_agent_message(&pool, "scheduled-retry-expired-agent", "push_raw").await;
    let expires_at = Utc::now() + Duration::minutes(5);
    sqlx::query("UPDATE scheduled_messages SET expires_at = $2 WHERE id = $1")
        .bind(&expired_message.id)
        .bind(expires_at)
        .execute(&pool)
        .await
        .expect("set scheduled-message expiry");
    let expired_fire = claim_after_exhausting_rearms(&pool).await;
    let outbox_count_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM message_outbox")
        .fetch_one(&pool)
        .await
        .expect("count outbox rows before expired fire");
    fire_claimed(
        &pool,
        None,
        expired_fire.clone(),
        expires_at + Duration::seconds(1),
    )
    .await;

    let expired_parent = db::get_scheduled_message_pg(&pool, &expired_message.id)
        .await
        .expect("read expired parent")
        .expect("expired parent exists");
    assert_eq!(expired_parent.status, db::STATUS_EXPIRED);
    assert_eq!(expired_parent.fire_count, 0);
    assert_eq!(expired_parent.last_fired_at, None);
    assert_eq!(expired_parent.in_flight_delivery_id, None);
    let expired_deliveries = db::list_deliveries_pg(&pool, &expired_message.id, 10, None)
        .await
        .expect("list expired deliveries");
    assert_eq!(expired_deliveries.len(), 1);
    assert_eq!(expired_deliveries[0].status, db::DELIVERY_INTERRUPTED);
    assert_eq!(expired_deliveries[0].fallback_outbox_id, None);
    let outbox_count_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM message_outbox")
        .fetch_one(&pool)
        .await
        .expect("count outbox rows after expired fire");
    assert_eq!(
        outbox_count_after, outbox_count_before,
        "an expired push_raw definition must not enqueue a fallback"
    );

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_trigger_now_retry_preserves_recurring_anchor() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_trigger_retry",
        "scheduled message trigger-now retry anchor regression",
    )
    .await;
    let original_scheduled_at = Utc::now() + Duration::hours(2);
    let message = db::insert_scheduled_message_pg(
        &pool,
        &db::NewScheduledMessage {
            content: "trigger-now cadence payload".to_string(),
            title: None,
            target_channel_id: Some("123456789".to_string()),
            bot: "announce".to_string(),
            delivery_kind: db::KIND_PUSH.to_string(),
            agent_id: None,
            agent_instruction: None,
            on_agent_failure: "fail".to_string(),
            scheduled_at: original_scheduled_at,
            schedule: Some("@every 1h".to_string()),
            timezone: "UTC".to_string(),
            expires_at: None,
            source: "postgres_test".to_string(),
            created_by: Some("postgres_test".to_string()),
            dedupe_key: None,
        },
    )
    .await
    .expect("insert trigger-now recurring definition");

    let manual = db::trigger_now_pg(&pool, &message.id, "manual-worker", LEASE_SECS)
        .await
        .expect("trigger recurring definition")
        .expect("scheduled definition should trigger");
    assert_eq!(manual.message.scheduled_at, original_scheduled_at);
    assert!(manual.fire_scheduled_at < original_scheduled_at);
    assert!(
        db::interrupt_delivery_and_rewind_pg(
            &pool,
            &manual.delivery_id,
            &manual.claim_token,
            &message.id,
            manual.fire_scheduled_at,
            "retry manual fire",
        )
        .await
        .expect("interrupt manual fire")
    );

    let mut retries = db::claim_due_fires_pg(&pool, "retry-worker", 10, LEASE_SECS, Utc::now())
        .await
        .expect("reclaim manual fire");
    assert_eq!(retries.len(), 1);
    let retry = retries.pop().expect("manual retry exists");
    assert_eq!(retry.delivery_id, manual.delivery_id);
    assert_eq!(retry.fire_scheduled_at, manual.fire_scheduled_at);
    assert_eq!(
        retry.message.scheduled_at, original_scheduled_at,
        "a manual retry must retain the definition's regular cadence anchor"
    );

    fire_claimed(&pool, None, retry, Utc::now()).await;
    let resumed = db::get_scheduled_message_pg(&pool, &message.id)
        .await
        .expect("read resumed recurring definition")
        .expect("resumed recurring definition exists");
    assert_eq!(resumed.status, db::STATUS_SCHEDULED);
    assert_eq!(resumed.scheduled_at, original_scheduled_at);
    assert_eq!(resumed.fire_count, 1);
    assert_eq!(resumed.in_flight_delivery_id, None);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_agent_trigger_now_retry_preserves_recurring_anchor_through_poller() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_agent_trigger_retry",
        "scheduled message agent trigger-now retry anchor regression",
    )
    .await;
    let agent_id = "scheduled-agent-trigger-retry";
    sqlx::query(
        "INSERT INTO agents (id, name, discord_channel_id)
         VALUES ($1, 'Scheduled Agent Trigger Retry', '123456789')",
    )
    .bind(agent_id)
    .execute(&pool)
    .await
    .expect("seed trigger-now retry agent");
    let original_scheduled_at = Utc::now() + Duration::hours(2);
    let message = db::insert_scheduled_message_pg(
        &pool,
        &db::NewScheduledMessage {
            content: "agent trigger-now cadence payload".to_string(),
            title: None,
            target_channel_id: Some("123456789".to_string()),
            bot: "announce".to_string(),
            delivery_kind: db::KIND_AGENT.to_string(),
            agent_id: Some(agent_id.to_string()),
            agent_instruction: None,
            on_agent_failure: "fail".to_string(),
            scheduled_at: original_scheduled_at,
            schedule: Some("@every 1h".to_string()),
            timezone: "UTC".to_string(),
            expires_at: None,
            source: "postgres_test".to_string(),
            created_by: Some("postgres_test".to_string()),
            dedupe_key: None,
        },
    )
    .await
    .expect("insert agent trigger-now recurring definition");

    let manual = db::trigger_now_pg(&pool, &message.id, "manual-agent-worker", LEASE_SECS)
        .await
        .expect("trigger recurring agent definition")
        .expect("scheduled agent definition should trigger");
    assert!(
        db::interrupt_delivery_and_rewind_pg(
            &pool,
            &manual.delivery_id,
            &manual.claim_token,
            &message.id,
            manual.fire_scheduled_at,
            "retry manual agent fire",
        )
        .await
        .expect("interrupt manual agent fire")
    );
    let mut retries =
        db::claim_due_fires_pg(&pool, "retry-agent-worker", 10, LEASE_SECS, Utc::now())
            .await
            .expect("reclaim manual agent fire");
    assert_eq!(retries.len(), 1);
    let retry = retries.pop().expect("manual agent retry exists");
    assert!(
        db::mark_delivery_agent_turn_started_pg(
            &pool,
            &retry.delivery_id,
            &retry.claim_token,
            "agent-trigger-retry-turn",
            LEASE_SECS,
        )
        .await
        .expect("record agent trigger-now retry turn")
    );

    let running = db::list_running_agent_deliveries_pg(&pool, "retry-agent-worker", LEASE_SECS, 10)
        .await
        .expect("poll agent trigger-now retry");
    assert_eq!(running.len(), 1);
    assert_eq!(
        running[0].scheduled_at, original_scheduled_at,
        "the poller must receive the persisted regular cadence anchor"
    );
    sqlx::query(
        "INSERT INTO session_transcripts (turn_id, assistant_message)
         VALUES ('agent-trigger-retry-turn', '예약 메시지 전달 완료')",
    )
    .execute(&pool)
    .await
    .expect("seed delivered trigger-now agent transcript");
    assert!(resolve_agent_delivery(&pool, &running[0], false).await);

    let resumed = db::get_scheduled_message_pg(&pool, &message.id)
        .await
        .expect("read resumed recurring agent definition")
        .expect("resumed recurring agent definition exists");
    assert_eq!(resumed.status, db::STATUS_SCHEDULED);
    assert_eq!(resumed.scheduled_at, original_scheduled_at);
    assert_eq!(resumed.fire_count, 1);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_agent_timeout_with_push_raw_fails_closed_without_outbox() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_agent_timeout_fence",
        "scheduled message timeout fallback relay fence regression",
    )
    .await;
    let message =
        insert_recurring_agent_message(&pool, "scheduled-timeout-agent", "push_raw").await;
    let fire = claim_one(&pool, "timeout-worker").await;
    assert!(
        db::mark_delivery_agent_turn_started_pg(
            &pool,
            &fire.delivery_id,
            &fire.claim_token,
            "scheduled-timeout-turn",
            LEASE_SECS,
        )
        .await
        .expect("record timed-out agent turn")
    );
    sqlx::query(
        "UPDATE scheduled_message_deliveries
         SET started_at = NOW() - INTERVAL '31 minutes'
         WHERE id = $1",
    )
    .bind(&fire.delivery_id)
    .execute(&pool)
    .await
    .expect("age agent delivery beyond completion timeout");
    let mut running = db::list_running_agent_deliveries_pg(&pool, "timeout-worker", LEASE_SECS, 10)
        .await
        .expect("poll timed-out agent turn");
    let delivery = running.pop().expect("timed-out delivery should be polled");

    assert!(resolve_agent_delivery(&pool, &delivery, true).await);

    let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM message_outbox")
        .fetch_one(&pool)
        .await
        .expect("count timeout fallback outbox rows");
    assert_eq!(
        outbox_count, 0,
        "an unconfirmed timeout must not race a late agent relay with raw fallback"
    );
    let deliveries = db::list_deliveries_pg(&pool, &message.id, 10, None)
        .await
        .expect("read timed-out delivery");
    assert_eq!(deliveries[0].status, db::DELIVERY_FAILED);
    assert_eq!(deliveries[0].fallback_outbox_id, None);
    assert!(
        deliveries[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("raw fallback suppressed"))
    );
    let parent = db::get_scheduled_message_pg(&pool, &message.id)
        .await
        .expect("read timeout parent")
        .expect("timeout parent exists");
    assert_eq!(parent.status, db::STATUS_SCHEDULED);
    assert_eq!(parent.fire_count, 0);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_definitive_agent_failure_atomically_enqueues_one_fallback() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_agent_fallback_atomic",
        "scheduled message atomic agent fallback regression",
    )
    .await;
    let message =
        insert_recurring_agent_message(&pool, "scheduled-no-reply-agent", "push_raw").await;
    let fire = claim_one(&pool, "fallback-worker").await;
    let turn_id = "scheduled-no-reply-turn";
    assert!(
        db::mark_delivery_agent_turn_started_pg(
            &pool,
            &fire.delivery_id,
            &fire.claim_token,
            turn_id,
            LEASE_SECS,
        )
        .await
        .expect("record no-reply agent turn")
    );
    let mut running =
        db::list_running_agent_deliveries_pg(&pool, "fallback-worker", LEASE_SECS, 10)
            .await
            .expect("poll no-reply agent turn");
    let delivery = running.pop().expect("no-reply delivery should be polled");
    sqlx::query(
        "INSERT INTO session_transcripts (turn_id, assistant_message)
         VALUES ($1, 'NO_REPLY')",
    )
    .bind(turn_id)
    .execute(&pool)
    .await
    .expect("seed definitive no-reply evidence");

    let first_delivery = delivery.clone();
    let second_delivery = delivery.clone();
    let (first, second) = tokio::join!(
        resolve_agent_delivery(&pool, &first_delivery, false),
        resolve_agent_delivery(&pool, &second_delivery, false),
    );
    assert_eq!(
        usize::from(first) + usize::from(second),
        1,
        "only one competing poller may commit the terminal handoff"
    );

    let outbox_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM message_outbox WHERE source = $1")
            .bind(OUTBOX_SOURCE)
            .fetch_one(&pool)
            .await
            .expect("count definitive fallback rows");
    assert_eq!(outbox_count, 1);
    let deliveries = db::list_deliveries_pg(&pool, &message.id, 10, None)
        .await
        .expect("read definitive fallback delivery");
    assert_eq!(deliveries[0].status, db::DELIVERY_SENT);
    assert!(deliveries[0].fallback_outbox_id.is_some());
    let parent = db::get_scheduled_message_pg(&pool, &message.id)
        .await
        .expect("read definitive fallback parent")
        .expect("definitive fallback parent exists");
    assert_eq!(parent.status, db::STATUS_SCHEDULED);
    assert_eq!(parent.fire_count, 1);

    pool.close().await;
    pg_db.drop().await;
}
