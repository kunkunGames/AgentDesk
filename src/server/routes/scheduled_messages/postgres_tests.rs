use super::*;

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
