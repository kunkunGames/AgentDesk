use super::{PersistTurnOwned, TurnTokenUsage, upsert_turn_owned_db};
use chrono::{DateTime, Utc};

#[tokio::test]
async fn turn_timestamp_instants_are_independent_of_postgres_timezone_pg() {
    let fixture = crate::db::auto_queue::test_support::TestPostgresDb::create().await;
    let pool = fixture.connect_and_migrate_with_max_connections(1).await;
    let started_at = DateTime::parse_from_rfc3339("2026-10-03T04:55:40.123456+09:00")
        .unwrap()
        .with_timezone(&Utc);
    let finished_at = DateTime::parse_from_rfc3339("2026-10-02T20:00:47.169212Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut entry = PersistTurnOwned {
        turn_id: "turn-timezone".into(),
        session_key: None,
        thread_id: None,
        thread_title: None,
        channel_id: "turn-timezone-channel".into(),
        agent_id: None,
        provider: Some("codex".into()),
        session_id: None,
        dispatch_id: None,
        started_at: Some(started_at),
        finished_at: Some(finished_at),
        duration_ms: Some(306_632),
        token_usage: TurnTokenUsage::default(),
    };
    // One connection makes each actual INSERT/UPSERT use the chosen session
    // timezone; it cannot accidentally run on a different pooled connection.
    for timezone in ["UTC", "Asia/Seoul"] {
        sqlx::query("SELECT set_config('TimeZone', $1, false)")
            .bind(timezone)
            .execute(&pool)
            .await
            .unwrap();
        entry.turn_id = format!("turn-timezone-{timezone}");
        upsert_turn_owned_db(Some(&pool), &entry).await.unwrap();
        upsert_turn_owned_db(Some(&pool), &entry).await.unwrap();
    }
    let mut transaction = pool.begin().await.unwrap();
    for setting in [
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL TIME ZONE 'Asia/Seoul'",
    ] {
        sqlx::query(setting)
            .execute(&mut *transaction)
            .await
            .unwrap();
        let rows = sqlx::query_as::<_, (DateTime<Utc>, DateTime<Utc>, i64)>(
            "SELECT started_at, finished_at, duration_ms FROM turns
             WHERE turn_id IN ('turn-timezone-UTC', 'turn-timezone-Asia/Seoul')
             ORDER BY turn_id",
        )
        .fetch_all(&mut *transaction)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert_eq!(row, (started_at, finished_at, 306_632));
        }
    }
    transaction.rollback().await.unwrap();
    pool.close().await;
    fixture.drop().await;
}
