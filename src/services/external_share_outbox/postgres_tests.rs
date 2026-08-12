use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_external_share_outbox_reclaims_stale_leases_and_scrubs_terminal_payload() {
    let pg_db = crate::dispatch::test_support::DispatchPostgresTestDb::create(
        "agentdesk_external_share_outbox",
        "external share outbox lease and payload lifecycle",
    )
    .await;
    let pool = pg_db.connect_and_migrate_with_max_connections(4).await;
    sqlx::query(
        "INSERT INTO oauth_connection_accounts
            (provider, account_key, token_ciphertext, token_nonce, scopes, status)
         VALUES ('kakao', 'primary', $1, $2, ARRAY['friends', 'talk_message'], 'active')",
    )
    .bind(b"encrypted-test-token".as_slice())
    .bind(vec![7_u8; 24])
    .execute(&pool)
    .await
    .expect("seed Kakao account reference");
    sqlx::query(
        "INSERT INTO scheduled_messages
            (id, content, target_channel_id, scheduled_at, timezone)
         VALUES ('smsg-external-outbox-test', 'safe payload', '123456789', NOW(), 'UTC')",
    )
    .execute(&pool)
    .await
    .expect("seed scheduled definition");
    sqlx::query(
        "INSERT INTO scheduled_message_deliveries
            (id, scheduled_message_id, fire_scheduled_at, resume_scheduled_at,
             delivery_kind, status, claim_token, finished_at)
         VALUES ('smdel-external-outbox-test', 'smsg-external-outbox-test',
                 NOW(), NOW(), 'push', 'sent', 'seed-claim', NOW())",
    )
    .execute(&pool)
    .await
    .expect("seed scheduled delivery");
    let outbox_id = Uuid::new_v4();
    let new = NewExternalShareOutbox {
        id: outbox_id,
        provider: crate::services::oauth_connection::KAKAO_PROVIDER.to_string(),
        channel_id: crate::services::scheduled_messages::external_delivery::KAKAO_CHANNEL_ID
            .to_string(),
        account_key: crate::services::oauth_connection::PRIMARY_ACCOUNT_KEY.to_string(),
        source: "scheduled_message".to_string(),
        source_key: "scheduled_message:v1:test:1".to_string(),
        scheduled_delivery_id: "smdel-external-outbox-test".to_string(),
        requested_count: 2,
        encrypted_payload: EncryptedValue {
            ciphertext: b"encrypted-outbox-payload".to_vec(),
            nonce: vec![5_u8; 24],
            key_version: 1,
        },
        deliver_before: None,
    };
    let mut tx = pool.begin().await.expect("begin outbox enqueue");
    enqueue_external_share_outbox_tx(&mut tx, &new)
        .await
        .expect("enqueue external share outbox");
    tx.commit().await.expect("commit outbox enqueue");

    let pending_api = provider_deliveries_for_scheduled_deliveries_pg(
        &pool,
        &["smdel-external-outbox-test".to_string()],
    )
    .await
    .expect("load pending provider delivery API projection");
    let pending_api = &pending_api["smdel-external-outbox-test"][0];
    assert_eq!(pending_api["status"], "pending");
    assert_eq!(pending_api["requestedCount"], 2);
    assert_eq!(pending_api["successfulCount"], JsonValue::Null);

    let first = claim_next_pg(&pool, "worker-a", Utc::now())
        .await
        .expect("claim external share row")
        .expect("external share row is due");
    sqlx::query(
        "UPDATE external_share_outbox
         SET claimed_at = NOW() - INTERVAL '3 minutes'
         WHERE id = $1",
    )
    .bind(outbox_id)
    .execute(&pool)
    .await
    .expect("expire first worker lease");
    let second = claim_next_pg(&pool, "worker-b", Utc::now())
        .await
        .expect("reclaim stale external share row")
        .expect("stale external share row is reclaimable");
    assert_ne!(first.claim_token, second.claim_token);

    finish_pre_dispatch_failure(&pool, &second, "payload_invalid").await;
    let stored: (
        String,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<i16>,
        JsonValue,
    ) = sqlx::query_as(
        "SELECT status, payload_ciphertext, payload_nonce, payload_key_version,
                    safe_summary
             FROM external_share_outbox WHERE id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await
    .expect("load terminal external share row");
    assert_eq!(stored.0, "failed");
    assert_eq!(stored.1, None);
    assert_eq!(stored.2, None);
    assert_eq!(stored.3, None);
    assert_eq!(stored.4["requested_count"], 2);
    assert_eq!(stored.4["failed_count"], 2);

    let expiring_outbox_id = Uuid::new_v4();
    let expiring = NewExternalShareOutbox {
        id: expiring_outbox_id,
        source_key: "scheduled_message:v1:test:expiring".to_string(),
        deliver_before: Some(Utc::now() + Duration::hours(1)),
        ..new
    };
    let mut tx = pool.begin().await.expect("begin expiring outbox enqueue");
    enqueue_external_share_outbox_tx(&mut tx, &expiring)
        .await
        .expect("enqueue expiring external share outbox");
    tx.commit().await.expect("commit expiring outbox enqueue");
    claim_next_pg(&pool, "worker-expiring", Utc::now())
        .await
        .expect("claim expiring external share row")
        .expect("expiring external share row is due");
    sqlx::query(
        "UPDATE external_share_outbox
         SET deliver_before = NOW() - INTERVAL '1 minute'
         WHERE id = $1",
    )
    .bind(expiring_outbox_id)
    .execute(&pool)
    .await
    .expect("expire active external share claim");

    terminalize_expired_pg(&pool, Utc::now())
        .await
        .expect("sweep active expired external share claim");
    let active_status: String =
        sqlx::query_scalar("SELECT status FROM external_share_outbox WHERE id = $1")
            .bind(expiring_outbox_id)
            .fetch_one(&pool)
            .await
            .expect("load active expired external share claim");
    assert_eq!(active_status, "processing");

    sqlx::query(
        "UPDATE external_share_outbox
         SET claimed_at = NOW() - INTERVAL '3 minutes'
         WHERE id = $1",
    )
    .bind(expiring_outbox_id)
    .execute(&pool)
    .await
    .expect("make expired external share claim stale");
    terminalize_expired_pg(&pool, Utc::now())
        .await
        .expect("sweep stale expired external share claim");
    let expired: (String, Option<Vec<u8>>, JsonValue, Option<String>) = sqlx::query_as(
        "SELECT status, payload_ciphertext, safe_summary, error_code
         FROM external_share_outbox WHERE id = $1",
    )
    .bind(expiring_outbox_id)
    .fetch_one(&pool)
    .await
    .expect("load terminal expired external share claim");
    assert_eq!(expired.0, "unknown");
    assert_eq!(expired.1, None);
    assert_eq!(expired.2["requested_count"], 2);
    assert_eq!(expired.2["failed_count"], 0);
    assert_eq!(expired.3.as_deref(), Some("delivery_expired_after_claim"));

    pool.close().await;
    pg_db.drop().await;
}
