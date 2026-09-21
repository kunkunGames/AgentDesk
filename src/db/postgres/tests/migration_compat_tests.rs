use super::{TestDatabase, close_test_pool, connect_test_pool_with_max_connections};
use crate::db::postgres::{POSTGRES_MIGRATOR, migrate};
use sqlx::migrate::Migrate;
use sqlx::{PgConnection, PgPool};

async fn fixture() -> (TestDatabase, PgPool) {
    let db = TestDatabase::create().await;
    let pool =
        connect_test_pool_with_max_connections(&db.database_url, "migration relocation fixture", 4)
            .await
            .expect("connect isolated fixture");
    (db, pool)
}

async fn history(pool: &PgPool) -> serde_json::Value {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(m) ORDER BY version), '[]'::jsonb) FROM _sqlx_migrations m",
    )
    .fetch_one(pool)
    .await
    .expect("read exact history")
}

async fn seed_legacy_history(conn: &mut PgConnection) {
    conn.ensure_migrations_table().await.expect("history table");
    for version in [122, 123] {
        let m = POSTGRES_MIGRATOR
            .iter()
            .find(|m| m.version == version)
            .unwrap();
        sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, $2, true, $3, 12345)")
            .bind(version - 2)
            .bind(m.description.as_ref())
            .bind(m.checksum.as_ref())
            .execute(&mut *conn)
            .await
            .expect("legacy history");
    }
}

#[tokio::test]
async fn postgres_migration_relocation_preserves_data_and_serializes_upgrade() {
    let (db, pool) = fixture().await;
    let mut conn = pool.acquire().await.unwrap();
    conn.ensure_migrations_table().await.unwrap();
    // Reconstruct the schema shipped before the two versions were renumbered.
    for source in POSTGRES_MIGRATOR
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .filter(|m| m.version < 120 || [122, 123].contains(&m.version))
    {
        let mut old = source.clone();
        if old.version >= 122 {
            old.version -= 2;
        }
        conn.apply(&old)
            .await
            .expect("apply released legacy schema");
    }
    sqlx::query("INSERT INTO kakao_calendar_events (id, revision, content) VALUES ('00000000-0000-0000-0000-000000000001', 7, '{\"title\":\"preserve me\"}')")
        .execute(&mut *conn).await.unwrap();
    drop(conn);
    let before = history(&pool).await;
    let (a, b) = tokio::join!(migrate(&pool), migrate(&pool));
    a.expect("leader upgrade");
    b.expect("concurrent worker upgrade");
    let after = history(&pool).await;
    for mut original in before.as_array().unwrap().clone() {
        let old_version = original["version"].as_i64().unwrap();
        let new_version = if [120, 121].contains(&old_version) {
            old_version + 2
        } else {
            old_version
        };
        original["version"] = new_version.into();
        assert_eq!(
            after
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["version"] == new_version)
                .unwrap(),
            &original,
            "only the known version may change, never checksum/timestamp/execution time"
        );
    }
    assert_eq!(
        after.as_array().unwrap().len(),
        POSTGRES_MIGRATOR
            .iter()
            .filter(|m| !m.migration_type.is_down_migration())
            .count()
    );
    let content: serde_json::Value =
        sqlx::query_scalar("SELECT content FROM kakao_calendar_events WHERE revision = 7")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(content["title"], "preserve me");
    // SQLx's normal validation still accepts the fully upgraded database.
    POSTGRES_MIGRATOR.run(&pool).await.unwrap();
    migrate(&pool).await.expect("idempotent repeated upgrade");
    assert_eq!(history(&pool).await, after);
    close_test_pool(pool, "migration relocation").await.unwrap();
    db.drop().await;
}

#[tokio::test]
async fn postgres_migration_relocation_rejects_drift_without_partial_repair() {
    let (db, pool) = fixture().await;
    let mutations = [
        (
            "UPDATE _sqlx_migrations SET checksum = decode('00','hex') WHERE version = 121",
            "checksum mismatch",
        ),
        (
            "UPDATE _sqlx_migrations SET description = 'not the released migration' WHERE version = 121",
            "checksum mismatch",
        ),
        (
            "UPDATE _sqlx_migrations SET success = false WHERE version = 121",
            "dirty",
        ),
        (
            "INSERT INTO _sqlx_migrations SELECT 123, description, installed_on, success, checksum, execution_time FROM _sqlx_migrations WHERE version = 121",
            "conflicts",
        ),
        (
            "INSERT INTO _sqlx_migrations SELECT 99999, description, installed_on, success, checksum, execution_time FROM _sqlx_migrations WHERE version = 121",
            "unknown postgres migration",
        ),
        (
            "INSERT INTO _sqlx_migrations SELECT 1, description, installed_on, success, checksum, execution_time FROM _sqlx_migrations WHERE version = 121",
            "checksum mismatch",
        ),
    ];
    for (mutation, expected_error) in mutations {
        let mut conn = pool.acquire().await.unwrap();
        conn.ensure_migrations_table().await.unwrap();
        sqlx::query("TRUNCATE _sqlx_migrations")
            .execute(&mut *conn)
            .await
            .unwrap();
        seed_legacy_history(&mut conn).await;
        sqlx::query(mutation).execute(&mut *conn).await.unwrap();
        drop(conn);
        let before = history(&pool).await;
        let error = migrate(&pool).await.expect_err("reject ambiguous upgrade");
        assert!(error.contains(expected_error), "{error}");
        assert_eq!(history(&pool).await, before, "no partial history rewrite");
        let mut probe = pool.acquire().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), probe.lock())
            .await
            .expect("migration error released its advisory lock")
            .unwrap();
        probe.unlock().await.unwrap();
    }
    close_test_pool(pool, "migration rejection").await.unwrap();
    db.drop().await;
}

#[tokio::test]
async fn postgres_migration_relocation_cancellation_does_not_leak_lock() {
    let (db, pool) = fixture().await;
    let mut blocker = pool.acquire().await.unwrap();
    blocker.lock().await.unwrap();
    let migrating_pool = pool.clone();
    let task = tokio::spawn(async move { migrate(&migrating_pool).await });
    // Verify the second connection is actually waiting on the migration lock.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))")
                .fetch_one(&pool).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("migration reached the lock");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    blocker.unlock().await.unwrap();
    drop(blocker);
    tokio::time::timeout(std::time::Duration::from_secs(30), migrate(&pool))
        .await
        .expect("cancelled migration released its connection")
        .unwrap();
    close_test_pool(pool, "migration cancellation")
        .await
        .unwrap();
    db.drop().await;
}
