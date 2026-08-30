use super::*;

#[test]
fn scheduled_image_attachment_requires_matching_image_bytes() {
    let png = ScheduledMessageImageAttachmentBody {
        filename: "family-thumbnail.png".to_string(),
        content_type: "image/png".to_string(),
        data_base64: "iVBORw0KGgo=".to_string(),
    };
    let attachment = validate_image_attachment(&png).expect("valid PNG header");
    assert_eq!(attachment.filename, "family-thumbnail.png");
    assert_eq!(attachment.content_type, "image/png");
    assert_eq!(attachment.data, b"\x89PNG\r\n\x1a\n");

    let mismatched = ScheduledMessageImageAttachmentBody {
        filename: "family-thumbnail.jpg".to_string(),
        content_type: "image/jpeg".to_string(),
        ..png
    };
    let error = validate_image_attachment(&mismatched).expect_err("MIME/header mismatch");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn scheduled_image_attachment_rejects_paths_and_non_image_mime_types() {
    let path = ScheduledMessageImageAttachmentBody {
        filename: "../thumbnail.png".to_string(),
        content_type: "image/png".to_string(),
        data_base64: "iVBORw0KGgo=".to_string(),
    };
    assert!(validate_image_attachment(&path).is_err());

    let extension_mismatch = ScheduledMessageImageAttachmentBody {
        filename: "thumbnail.jpg".to_string(),
        content_type: "image/png".to_string(),
        data_base64: "iVBORw0KGgo=".to_string(),
    };
    assert!(validate_image_attachment(&extension_mismatch).is_err());

    let mime = ScheduledMessageImageAttachmentBody {
        filename: "thumbnail.txt".to_string(),
        content_type: "text/plain".to_string(),
        data_base64: "aGVsbG8=".to_string(),
    };
    assert!(validate_image_attachment(&mime).is_err());
}

#[test]
fn scheduled_image_attachment_rejects_oversized_discord_content() {
    let content = "x".repeat(crate::services::discord::outbound::DISCORD_HARD_LIMIT_CHARS + 1);
    let error = validate_image_attachment_content_length(&content, true)
        .expect_err("an image-bearing Discord message must fit one message");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error
            .to_json_value()
            .get("error")
            .and_then(JsonValue::as_str),
        Some("content must not exceed 2000 characters when imageAttachment is provided")
    );
    validate_image_attachment_content_length(&content, false)
        .expect("text-only delivery retains existing long-content handling");
}

#[test]
fn discord_only_mentions_require_unique_push_user_ids_and_fit_the_rendered_message() {
    let user_ids = validate_discord_mention_user_ids(
        &[
            "1469509284508340276".to_string(),
            "1469961339920453675".to_string(),
        ],
        db::KIND_PUSH,
    )
    .expect("valid Discord user IDs");
    assert_eq!(user_ids.len(), 2);
    validate_discord_rendered_content_length("Kakao keeps this body", &user_ids)
        .expect("Discord prefix fits separately from the canonical provider body");

    assert!(
        validate_discord_mention_user_ids(&["1".to_string(), "1".to_string()], db::KIND_PUSH,)
            .is_err()
    );
    assert!(validate_discord_mention_user_ids(&["01".to_string()], db::KIND_PUSH).is_err());
    assert!(validate_discord_mention_user_ids(&["1".to_string()], db::KIND_AGENT).is_err());
    assert!(
        validate_discord_rendered_content_length(
            &"x".repeat(crate::services::discord::outbound::DISCORD_HARD_LIMIT_CHARS),
            &["1".to_string()],
        )
        .is_err()
    );
}

#[test]
fn scheduled_message_accepts_null_discord_mentions_for_push_delivery() {
    let body: CreateScheduledMessageBody = serde_json::from_value(serde_json::json!({
        "content": "Discord receives this canonical body",
        "discordMentionUserIds": null,
        "targetChannelId": "123456789",
        "deliveryKind": "push",
        "scheduledAt": "2026-08-29T09:00:00Z"
    }))
    .expect("explicit null Discord mentions are accepted");

    assert_eq!(body.discord_mention_user_ids, None);
    assert_eq!(body.target_channel_id.as_deref(), Some("123456789"));
    assert_eq!(body.delivery_kind.as_deref(), Some(db::KIND_PUSH));
    assert!(
        validate_discord_mention_user_ids(
            body.discord_mention_user_ids.as_deref().unwrap_or_default(),
            body.delivery_kind.as_deref().expect("delivery kind"),
        )
        .expect("null normalizes to an empty mention list")
        .is_empty()
    );
}

#[test]
fn configured_scheduled_push_mentions_override_missing_or_requested_values() {
    let configured = vec![
        "111111111111111111".to_string(),
        "222222222222222222".to_string(),
    ];

    assert_eq!(
        effective_scheduled_discord_mention_user_ids(&[], db::KIND_PUSH, &configured)
            .expect("configured pair applies when the request omits the field"),
        configured
    );
    assert_eq!(
        effective_scheduled_discord_mention_user_ids(
            &["1".to_string()],
            db::KIND_PUSH,
            &configured,
        )
        .expect("configured pair overrides a caller-provided value"),
        configured
    );
    assert!(
        effective_scheduled_discord_mention_user_ids(
            &["1".to_string()],
            db::KIND_AGENT,
            &configured,
        )
        .is_err()
    );
}

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

#[test]
fn scheduled_push_rejects_agent_only_fields_but_allows_explicit_clears() {
    for (agent_id, instruction, explicit_failure, expected_error) in [
        (
            Some("unused-agent"),
            None,
            false,
            "agentId is only valid for agent delivery",
        ),
        (
            None,
            Some("unused instruction"),
            false,
            "agentInstruction is only valid for agent delivery",
        ),
        (
            None,
            None,
            true,
            "onAgentFailure is only valid for agent delivery",
        ),
    ] {
        let err =
            validate_agent_only_fields(db::KIND_PUSH, agent_id, instruction, explicit_failure)
                .expect_err("push must reject agent-only values");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.to_json_value().get("error").and_then(JsonValue::as_str),
            Some(expected_error)
        );
    }

    validate_agent_only_fields(db::KIND_PUSH, None, None, false)
        .expect("an ordinary push or explicit null clears have no agent-only value");
    validate_agent_only_fields(
        db::KIND_AGENT,
        Some("scheduled-agent"),
        Some("delivery instruction"),
        true,
    )
    .expect("agent delivery accepts its dedicated fields");
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
        discord_mention_user_ids: Some(vec!["1469509284508340276".to_string()]),
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
        image_attachment: None,
        provider_targets: None,
        context_strategy: None,
        on_context_failure: None,
    };

    let new = validate_create(
        &pool,
        &body,
        false,
        &crate::config::KakaoFriendShareConfig::default(),
        &[],
    )
    .await
    .expect("validate explicit bot create");
    assert_eq!(new.bot, "notify");
    assert_eq!(new.discord_mention_user_ids, ["1469509284508340276"]);
    let row = db::insert_scheduled_message_pg(&pool, &new)
        .await
        .expect("persist explicit bot create");
    assert_eq!(row.bot, "notify");
    assert_eq!(row.discord_mention_user_ids, ["1469509284508340276"]);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_push_rejects_agent_id_before_foreign_key_insert() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_push_agent_id",
        "scheduled push rejects agent-only foreign key input",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;
    let body = CreateScheduledMessageBody {
        content: "push must not persist an unused agent association".to_string(),
        discord_mention_user_ids: None,
        title: None,
        target_channel_id: Some("123456789".to_string()),
        bot: None,
        delivery_kind: Some(db::KIND_PUSH.to_string()),
        agent_id: Some("typo-missing-agent".to_string()),
        agent_instruction: None,
        on_agent_failure: None,
        scheduled_at: (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
        schedule: None,
        timezone: Some("UTC".to_string()),
        expires_at: None,
        source: Some("postgres_test".to_string()),
        created_by: Some("postgres_test".to_string()),
        dedupe_key: None,
        image_attachment: None,
        provider_targets: None,
        context_strategy: None,
        on_context_failure: None,
    };

    let err = validate_create(
        &pool,
        &body,
        false,
        &crate::config::KakaoFriendShareConfig::default(),
        &[],
    )
    .await
    .expect_err("push agentId must fail as a request error before INSERT");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        err.to_json_value().get("error").and_then(JsonValue::as_str),
        Some("agentId is only valid for agent delivery")
    );
    let stored_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scheduled_messages")
        .fetch_one(&pool)
        .await
        .expect("count scheduled definitions after rejected create");
    assert_eq!(stored_count, 0);

    pool.close().await;
    pg_db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_scheduled_push_patch_distinguishes_values_from_null_clears() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_smsg_push_patch_fields",
        "scheduled push patch agent-only field policy",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;
    let existing = db::insert_scheduled_message_pg(
        &pool,
        &db::NewScheduledMessage {
            content: "ordinary push patch definition".to_string(),
            discord_mention_user_ids: Vec::new(),
            title: None,
            target_channel_id: Some("123456789".to_string()),
            bot: "notify".to_string(),
            delivery_kind: db::KIND_PUSH.to_string(),
            agent_id: None,
            agent_instruction: None,
            on_agent_failure: "fail".to_string(),
            scheduled_at: Utc::now() + chrono::Duration::minutes(5),
            schedule: None,
            timezone: "UTC".to_string(),
            expires_at: None,
            source: "postgres_test".to_string(),
            created_by: Some("postgres_test".to_string()),
            dedupe_key: None,
            image_attachment: None,
            external_delivery_plan: None,
            context_strategy: "fresh".to_string(),
            context_snapshot_id: None,
            on_context_failure: "fail".to_string(),
        },
    )
    .await
    .expect("insert ordinary push definition");

    let metadata_body = json!({"title": "metadata-only update"});
    let metadata_patch = build_patch(
        &pool,
        metadata_body.as_object().expect("metadata patch object"),
        &existing,
        false,
        &crate::config::KakaoFriendShareConfig::default(),
        &[],
    )
    .await
    .expect("metadata-only PATCH must not treat stored default fail as explicit input");
    assert_eq!(
        metadata_patch.title,
        Some(Some("metadata-only update".to_string()))
    );

    let clear_body = json!({"agentId": null, "agentInstruction": null});
    let clear_patch = build_patch(
        &pool,
        clear_body.as_object().expect("agent clear patch object"),
        &existing,
        false,
        &crate::config::KakaoFriendShareConfig::default(),
        &[],
    )
    .await
    .expect("explicit null clears leave no effective agent-only value");
    assert_eq!(clear_patch.agent_id, Some(None));
    assert_eq!(clear_patch.agent_instruction, Some(None));

    for (body, expected_error) in [
        (
            json!({"agentId": "typo-missing-agent"}),
            "agentId is only valid for agent delivery",
        ),
        (
            json!({"agentInstruction": "unused instruction"}),
            "agentInstruction is only valid for agent delivery",
        ),
        (
            json!({"onAgentFailure": "push_raw"}),
            "onAgentFailure is only valid for agent delivery",
        ),
    ] {
        let err = build_patch(
            &pool,
            body.as_object().expect("invalid push patch object"),
            &existing,
            false,
            &crate::config::KakaoFriendShareConfig::default(),
            &[],
        )
        .await
        .expect_err("push PATCH must reject effective agent-only values");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.to_json_value().get("error").and_then(JsonValue::as_str),
            Some(expected_error)
        );
    }

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

    let err = validate_targeting(
        &pool,
        db::KIND_AGENT,
        Some("987654321"),
        Some("scheduled-agent-without-primary"),
    )
    .await
    .expect_err("an explicit delivery target must not bypass the owner-channel requirement");

    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        err.to_json_value().get("error").and_then(JsonValue::as_str),
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

    let err = validate_targeting(
        &pool,
        db::KIND_AGENT,
        None,
        Some("scheduled-agent-invalid-primary"),
    )
    .await
    .expect_err("an invalid owner channel must fail before fire time");

    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        err.to_json_value().get("error").and_then(JsonValue::as_str),
        Some("agent 'scheduled-agent-invalid-primary' has an invalid primary Discord channel")
    );

    pool.close().await;
    pg_db.drop().await;
}
