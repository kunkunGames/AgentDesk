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

async fn insert_due_message(pool: &PgPool, delivery_kind: &str) -> ScheduledMessageRow {
    let agent_id = if delivery_kind == KIND_AGENT {
        sqlx::query(
            "INSERT INTO agents (id, name, discord_channel_id)
             VALUES ('scheduled-test-agent', 'Scheduled Test Agent', '123456789')",
        )
        .execute(pool)
        .await
        .expect("seed scheduled-message agent");
        Some("scheduled-test-agent".to_string())
    } else {
        None
    };

    insert_scheduled_message_pg(
        pool,
        &NewScheduledMessage {
            content: "scheduled test message".to_string(),
            title: None,
            target_channel_id: (delivery_kind == KIND_PUSH).then(|| "123456789".to_string()),
            bot: "announce".to_string(),
            delivery_kind: delivery_kind.to_string(),
            agent_id,
            agent_instruction: None,
            on_agent_failure: "fail".to_string(),
            scheduled_at: Utc::now() - Duration::seconds(1),
            schedule: None,
            timezone: "UTC".to_string(),
            expires_at: None,
            source: "postgres_test".to_string(),
            created_by: Some("postgres_test".to_string()),
            dedupe_key: None,
        },
    )
    .await
    .expect("insert due scheduled message")
}

async fn claim_one(pool: &PgPool, owner: &str, lease_secs: i64) -> ClaimedFire {
    let mut claims = claim_due_fires_pg(pool, owner, 10, lease_secs, Utc::now())
        .await
        .expect("claim due scheduled message");
    assert_eq!(claims.len(), 1, "exactly one definition should be due");
    claims.pop().expect("claimed fire")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_rearm_rotates_token_and_clears_attempt_fields() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_rearm",
        "scheduled message rearm fencing regression",
    )
    .await;
    let message = insert_due_message(&pool, KIND_PUSH).await;
    let first = claim_one(&pool, "worker-a", 30).await;

    sqlx::query(
        "UPDATE scheduled_message_deliveries
         SET outbox_id = 101,
             turn_id = 'stale-turn',
             fallback_outbox_id = 202,
             next_attempt_at = NOW() + INTERVAL '5 minutes',
             error = 'stale attempt error'
         WHERE id = $1",
    )
    .bind(&first.delivery_id)
    .execute(&pool)
    .await
    .expect("seed stale attempt fields");

    assert!(
        interrupt_delivery_and_rewind_pg(
            &pool,
            &first.delivery_id,
            &first.claim_token,
            &message.id,
            first.fire_scheduled_at,
            "retry this slot",
        )
        .await
        .expect("interrupt first attempt"),
        "the current claim should rewind its parent"
    );

    let second = claim_one(&pool, "worker-b", 45).await;
    assert_eq!(second.delivery_id, first.delivery_id, "slot row is reused");
    assert_ne!(
        second.claim_token, first.claim_token,
        "every rearm needs a fresh fencing token"
    );
    assert_eq!(second.retry_count, 1);

    let row = sqlx::query(
        "SELECT status, claim_owner, claim_token, retry_count,
                outbox_id, turn_id, fallback_outbox_id, next_attempt_at,
                error, finished_at, lease_expires_at
         FROM scheduled_message_deliveries
         WHERE id = $1",
    )
    .bind(&second.delivery_id)
    .fetch_one(&pool)
    .await
    .expect("read rearmed delivery");
    assert_eq!(row.try_get::<String, _>("status").unwrap(), "running");
    assert_eq!(
        row.try_get::<Option<String>, _>("claim_owner").unwrap(),
        Some("worker-b".to_string())
    );
    assert_eq!(
        row.try_get::<String, _>("claim_token").unwrap(),
        second.claim_token
    );
    assert_eq!(row.try_get::<i32, _>("retry_count").unwrap(), 1);
    assert_eq!(row.try_get::<Option<i64>, _>("outbox_id").unwrap(), None);
    assert_eq!(row.try_get::<Option<String>, _>("turn_id").unwrap(), None);
    assert_eq!(
        row.try_get::<Option<i64>, _>("fallback_outbox_id").unwrap(),
        None
    );
    assert_eq!(
        row.try_get::<Option<DateTime<Utc>>, _>("next_attempt_at")
            .unwrap(),
        None
    );
    assert_eq!(row.try_get::<Option<String>, _>("error").unwrap(), None);
    assert_eq!(
        row.try_get::<Option<DateTime<Utc>>, _>("finished_at")
            .unwrap(),
        None
    );
    assert!(
        row.try_get::<Option<DateTime<Utc>>, _>("lease_expires_at")
            .unwrap()
            .is_some(),
        "the replacement attempt must hold a fresh lease"
    );

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_stale_claim_is_fenced_and_current_claim_renews_lease() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_claim_fence",
        "scheduled message stale claim and lease renewal regression",
    )
    .await;
    let message = insert_due_message(&pool, KIND_AGENT).await;
    let first = claim_one(&pool, "worker-a", 20).await;
    assert!(
        interrupt_delivery_and_rewind_pg(
            &pool,
            &first.delivery_id,
            &first.claim_token,
            &message.id,
            first.fire_scheduled_at,
            "replace worker-a",
        )
        .await
        .expect("interrupt first agent attempt")
    );
    let second = claim_one(&pool, "worker-b", 20).await;

    assert!(
        !mark_delivery_agent_turn_started_pg(
            &pool,
            &second.delivery_id,
            &first.claim_token,
            "stale-turn",
            600,
        )
        .await
        .expect("stale turn start must be a guarded no-op")
    );
    assert!(
        !finish_delivery_and_finalize_parent_pg(
            &pool,
            &second.delivery_id,
            &first.claim_token,
            DELIVERY_SENT,
            None,
            None,
            None,
            &message.id,
            true,
            STATUS_SENT,
            None,
        )
        .await
        .expect("stale finish must be a guarded no-op")
    );
    assert!(
        !interrupt_delivery_and_rewind_pg(
            &pool,
            &second.delivery_id,
            &first.claim_token,
            &message.id,
            second.fire_scheduled_at,
            "stale rewind",
        )
        .await
        .expect("stale rewind must be a guarded no-op")
    );

    let parent_status: String =
        sqlx::query_scalar("SELECT status FROM scheduled_messages WHERE id = $1")
            .bind(&message.id)
            .fetch_one(&pool)
            .await
            .expect("read parent after stale writes");
    assert_eq!(parent_status, STATUS_FIRING);
    let (delivery_status, turn_id, claim_token, lease_before): (
        String,
        Option<String>,
        String,
        Option<DateTime<Utc>>,
    ) = sqlx::query_as(
        "SELECT status, turn_id, claim_token, lease_expires_at
         FROM scheduled_message_deliveries
         WHERE id = $1",
    )
    .bind(&second.delivery_id)
    .fetch_one(&pool)
    .await
    .expect("read delivery after stale writes");
    assert_eq!(delivery_status, "running");
    assert_eq!(turn_id, None);
    assert_eq!(claim_token, second.claim_token);
    let lease_before = lease_before.expect("claimed attempt has a lease");

    assert!(
        mark_delivery_agent_turn_started_pg(
            &pool,
            &second.delivery_id,
            &second.claim_token,
            "current-turn",
            600,
        )
        .await
        .expect("current turn start should update delivery")
    );
    let (turn_id, lease_after): (Option<String>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT turn_id, lease_expires_at
         FROM scheduled_message_deliveries
         WHERE id = $1",
    )
    .bind(&second.delivery_id)
    .fetch_one(&pool)
    .await
    .expect("read renewed agent lease");
    assert_eq!(turn_id.as_deref(), Some("current-turn"));
    assert!(
        lease_after.expect("renewed lease") > lease_before,
        "recording the current turn must extend the claim lease"
    );

    assert!(
        finish_delivery_and_finalize_parent_pg(
            &pool,
            &second.delivery_id,
            &second.claim_token,
            DELIVERY_SENT,
            None,
            None,
            None,
            &message.id,
            true,
            STATUS_SENT,
            None,
        )
        .await
        .expect("current claim should finish delivery")
    );
    let final_parent_status: String =
        sqlx::query_scalar("SELECT status FROM scheduled_messages WHERE id = $1")
            .bind(&message.id)
            .fetch_one(&pool)
            .await
            .expect("read finalized parent");
    let final_delivery_status: String =
        sqlx::query_scalar("SELECT status FROM scheduled_message_deliveries WHERE id = $1")
            .bind(&second.delivery_id)
            .fetch_one(&pool)
            .await
            .expect("read finalized delivery");
    assert_eq!(final_parent_status, STATUS_SENT);
    assert_eq!(final_delivery_status, DELIVERY_SENT);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_running_agent_poll_rotates_before_renewed_rows() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_poll_rotation",
        "scheduled message agent poll lease rotation regression",
    )
    .await;
    let agent_id = "scheduled-poll-agent";
    sqlx::query(
        "INSERT INTO agents (id, name, discord_channel_id)
         VALUES ($1, 'Scheduled Poll Agent', '123456789')",
    )
    .bind(agent_id)
    .execute(&pool)
    .await
    .expect("seed scheduled-message poll agent");

    for label in ["older", "newer"] {
        insert_scheduled_message_pg(
            &pool,
            &NewScheduledMessage {
                content: format!("scheduled poll {label}"),
                title: None,
                target_channel_id: Some("123456789".to_string()),
                bot: "announce".to_string(),
                delivery_kind: KIND_AGENT.to_string(),
                agent_id: Some(agent_id.to_string()),
                agent_instruction: None,
                on_agent_failure: "fail".to_string(),
                scheduled_at: Utc::now() - Duration::seconds(1),
                schedule: None,
                timezone: "UTC".to_string(),
                expires_at: None,
                source: "postgres_test".to_string(),
                created_by: Some("postgres_test".to_string()),
                dedupe_key: None,
            },
        )
        .await
        .expect("insert scheduled-message poll definition");
    }

    let claims = claim_due_fires_pg(&pool, "poll-worker", 10, 60, Utc::now())
        .await
        .expect("claim scheduled-message poll definitions");
    assert_eq!(claims.len(), 2);
    for (index, claim) in claims.iter().enumerate() {
        assert!(
            mark_delivery_agent_turn_started_pg(
                &pool,
                &claim.delivery_id,
                &claim.claim_token,
                &format!("poll-turn-{index}"),
                60,
            )
            .await
            .expect("record scheduled-message poll turn")
        );
    }

    sqlx::query(
        "UPDATE scheduled_message_deliveries
         SET lease_expires_at = NOW() + INTERVAL '60 seconds',
             created_at = CASE WHEN id = $1
                 THEN NOW() - INTERVAL '2 minutes'
                 ELSE NOW() - INTERVAL '1 minute'
             END
         WHERE id = $1 OR id = $2",
    )
    .bind(&claims[0].delivery_id)
    .bind(&claims[1].delivery_id)
    .execute(&pool)
    .await
    .expect("seed deterministic poll ordering");

    let first = list_running_agent_deliveries_pg(&pool, 600, 1)
        .await
        .expect("poll first running agent delivery");
    let second = list_running_agent_deliveries_pg(&pool, 600, 1)
        .await
        .expect("poll second running agent delivery");
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(
        first[0].delivery_id, second[0].delivery_id,
        "renewing one batch must move it behind still-expiring deliveries"
    );
    let mut observed = vec![first[0].delivery_id.clone(), second[0].delivery_id.clone()];
    observed.sort();
    let mut expected = claims
        .iter()
        .map(|claim| claim.delivery_id.clone())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(observed, expected);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_expired_started_turn_is_adopted_instead_of_rearmed() {
    let (pg_db, pool) = create_test_pool(
        "agentdesk_smsg_turn_adoption",
        "scheduled message started turn lease adoption regression",
    )
    .await;
    let message = insert_due_message(&pool, KIND_AGENT).await;
    let fire = claim_one(&pool, "turn-owner", 30).await;
    assert!(
        mark_delivery_agent_turn_started_pg(
            &pool,
            &fire.delivery_id,
            &fire.claim_token,
            "durable-turn",
            -1,
        )
        .await
        .expect("record expired durable turn")
    );

    assert_eq!(
        recover_expired_leases_pg(&pool)
            .await
            .expect("recover expired pre-turn leases"),
        0,
        "a recorded turn must be adopted rather than restarted"
    );
    let parent = get_scheduled_message_pg(&pool, &message.id)
        .await
        .expect("read adopted parent")
        .expect("adopted parent exists");
    assert_eq!(parent.status, STATUS_FIRING);
    assert_eq!(
        parent.in_flight_delivery_id.as_deref(),
        Some(fire.delivery_id.as_str())
    );

    let adopted = list_running_agent_deliveries_pg(&pool, 600, 10)
        .await
        .expect("adopt expired durable turn");
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].delivery_id, fire.delivery_id);
    assert_eq!(adopted[0].claim_token, fire.claim_token);
    assert_eq!(adopted[0].turn_id.as_deref(), Some("durable-turn"));
    let renewed_lease: DateTime<Utc> = sqlx::query_scalar(
        "SELECT lease_expires_at
         FROM scheduled_message_deliveries
         WHERE id = $1",
    )
    .bind(&fire.delivery_id)
    .fetch_one(&pool)
    .await
    .expect("read adopted turn lease");
    assert!(renewed_lease > Utc::now());

    assert!(
        finish_delivery_and_finalize_parent_pg(
            &pool,
            &fire.delivery_id,
            &fire.claim_token,
            DELIVERY_SENT,
            None,
            None,
            None,
            &message.id,
            true,
            STATUS_SENT,
            None,
        )
        .await
        .expect("finish adopted durable turn")
    );

    pool.close().await;
    pg_db.drop().await;
}
