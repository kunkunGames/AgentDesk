use sqlx::PgPool;

/// Delete `message_outbox` rows whose status is terminal and beyond retention.
/// Permanent dedupe sentinels deliberately survive so immutable scheduled fire
/// slots cannot be enqueued a second time after recovery.
/// Returns `(failed_pruned, sent_pruned)` for logging.
pub(super) async fn gc_stale_message_outbox_rows(pool: &PgPool) -> Result<(u64, u64), sqlx::Error> {
    let failed = sqlx::query(
        "DELETE FROM message_outbox
          WHERE status = 'failed'
            AND created_at < NOW() - INTERVAL '7 days'",
    )
    .execute(pool)
    .await?
    .rows_affected();
    let sent = sqlx::query(
        "DELETE FROM message_outbox
          WHERE status = 'sent'
            AND created_at < NOW() - INTERVAL '30 days'
            -- NULL expiry + a live dedupe key is an intentional permanent
            -- sentinel (scheduled-message fire slots use this contract).
            AND NOT (dedupe_key IS NOT NULL AND dedupe_expires_at IS NULL)",
    )
    .execute(pool)
    .await?
    .rows_affected();
    Ok((failed, sent))
}

#[cfg(test)]
mod tests {
    use super::gc_stale_message_outbox_rows;
    use crate::services::message_outbox::{
        OutboxMessage, enqueue_outbox_pg_returning_id_with_persistent_dedupe,
        enqueue_outbox_pg_returning_id_with_ttl,
    };

    #[tokio::test]
    async fn gc_preserves_persistent_dedupe_sentinels_pg() {
        let Some(pg_db) = crate::dispatch::test_support::DispatchPostgresTestDb::try_create(
            "agentdesk_message_outbox_gc_persistent_dedupe",
            "message_outbox persistent dedupe GC contract",
        )
        .await
        else {
            return;
        };
        let pool = pg_db.connect_and_migrate().await;

        let persistent_id = enqueue_outbox_pg_returning_id_with_persistent_dedupe(
            &pool,
            OutboxMessage {
                target: "channel:1",
                content: "persistent",
                bot: "notify",
                source: "scheduled_message",
                reason_code: Some("scheduled_message:v1:gc-test:slot"),
                session_key: None,
            },
        )
        .await
        .expect("enqueue persistent sentinel");
        let ordinary_id = enqueue_outbox_pg_returning_id_with_ttl(
            &pool,
            OutboxMessage {
                target: "channel:1",
                content: "ordinary",
                bot: "notify",
                source: "system",
                reason_code: None,
                session_key: None,
            },
            0,
        )
        .await
        .expect("enqueue ordinary row")
        .expect("ordinary row inserted");
        sqlx::query(
            "UPDATE message_outbox
             SET status = 'sent', created_at = NOW() - INTERVAL '31 days',
                 sent_at = NOW() - INTERVAL '31 days'
             WHERE id = ANY($1)",
        )
        .bind(vec![persistent_id, ordinary_id])
        .execute(&pool)
        .await
        .expect("age sent outbox rows");

        let (failed_pruned, sent_pruned) = gc_stale_message_outbox_rows(&pool)
            .await
            .expect("run message_outbox GC");
        assert_eq!(failed_pruned, 0);
        assert_eq!(sent_pruned, 1);

        let remaining: Vec<String> =
            sqlx::query_scalar("SELECT content FROM message_outbox ORDER BY content")
                .fetch_all(&pool)
                .await
                .expect("read GC survivors");
        assert_eq!(remaining, vec!["persistent"]);
    }
}
