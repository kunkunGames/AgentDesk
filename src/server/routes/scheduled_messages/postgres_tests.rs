use super::*;

#[test]
fn scheduled_message_bot_defaults_to_non_triggering_notify() {
    assert_eq!(scheduled_message_bot_or_default(None), "notify");
    assert_eq!(scheduled_message_bot_or_default(Some("   ")), "notify");
    assert_eq!(scheduled_message_bot_or_default(Some(" notify ")), "notify");
    assert_eq!(
        scheduled_message_bot_or_default(Some("announce")),
        "announce"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_create_persists_trimmed_explicit_bot() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_trimmed_bot",
        "scheduled message explicit bot normalization",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;
    let body = CreateScheduledMessageBody {
        content: "trim explicit bot before persistence".to_string(),
        title: None,
        target_channel_id: Some("123456789".to_string()),
        bot: Some(" notify ".to_string()),
        delivery_kind: Some(db::KIND_PUSH.to_string()),
        agent_id: None,
        agent_instruction: None,
        on_agent_failure: None,
        scheduled_at: (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
        schedule: None,
        timezone: Some("UTC".to_string()),
        expires_at: None,
        source: Some("postgres_test".to_string()),
        created_by: Some("postgres_test".to_string()),
        dedupe_key: None,
    };

    let new = validate_create(&pool, &body)
        .await
        .expect("validate explicit bot create");
    assert_eq!(new.bot, "notify");
    let row = db::insert_scheduled_message_pg(&pool, &new)
        .await
        .expect("persist explicit bot create");
    assert_eq!(row.bot, "notify");

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_explicit_target_still_requires_agent_primary_channel() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_primary_channel",
        "scheduled message agent primary channel validation",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;

    sqlx::query(
        "INSERT INTO agents (id, name, discord_channel_id)
         VALUES ('scheduled-agent-without-primary', 'Scheduled Agent Without Primary', NULL)",
    )
    .execute(&pool)
    .await
    .expect("seed agent without a primary channel");

    let (status, Json(body)) = validate_targeting(
        &pool,
        db::KIND_AGENT,
        Some("987654321"),
        Some("scheduled-agent-without-primary"),
    )
    .await
    .expect_err("an explicit delivery target must not bypass the owner-channel requirement");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("error").and_then(JsonValue::as_str),
        Some("agent 'scheduled-agent-without-primary' has no primary Discord channel")
    );

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_message_rejects_invalid_agent_primary_channel() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_invalid_primary",
        "scheduled message invalid agent primary channel validation",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;

    sqlx::query(
        "INSERT INTO agents (id, name, discord_channel_id)
         VALUES ('scheduled-agent-invalid-primary', 'Scheduled Agent Invalid Primary',
                 'not-a-known-channel-alias')",
    )
    .execute(&pool)
    .await
    .expect("seed agent with an invalid primary channel");

    let (status, Json(body)) = validate_targeting(
        &pool,
        db::KIND_AGENT,
        None,
        Some("scheduled-agent-invalid-primary"),
    )
    .await
    .expect_err("an invalid owner channel must fail before fire time");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("error").and_then(JsonValue::as_str),
        Some("agent 'scheduled-agent-invalid-primary' has an invalid primary Discord channel")
    );

    pool.close().await;
    pg_db.drop().await;
}
