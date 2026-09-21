//! Narrow, checksum-preserving upgrades for migrations renumbered after release.
//!
//! Never infer a repair from a similar description or rewrite a checksum. The
//! two Kakao migrations shipped in d06d1d234 at 120/121 before upstream added
//! different migrations at those versions; their SQL is unchanged at 122/123.

use sqlx::migrate::Migrate;
use sqlx::{Connection, PgConnection, PgPool};

use super::{POSTGRES_MIGRATOR, checksum_hex};

struct Relocation {
    from: i64,
    to: i64,
    description: &'static str,
    checksum: &'static str,
}

const RELOCATIONS: &[Relocation] = &[
    Relocation {
        from: 120,
        to: 122,
        description: "kakao calendar sync",
        checksum: "b2771aa8febc19090e271cddbbc77cbec85cb9ff82f6ec1f19d58a04821262bb20d48dcb3b099b62bdcbc1a635b8b25f",
    },
    Relocation {
        from: 121,
        to: 123,
        description: "kakao calendar lease expiry",
        checksum: "ee806ae6958adc0a58e5a8c5a7a206de6cf3d961a44296f21c2f014efeb4602e10020aaa962578cbcac03179ff57ed09",
    },
];

pub(super) async fn migrate(pool: &PgPool) -> Result<(), String> {
    // A detached connection is closed on cancellation too: a session advisory
    // lock must never escape back into the pool after an error or timeout.
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("acquire postgres migration connection: {e}"))?
        .detach();
    let result = async {
        // Use SQLx's actual migration lock, including against older binaries.
        // run_direct acquires this reentrant PostgreSQL lock again and releases
        // its own acquisition. Closing the connection releases ours on all paths.
        tokio::time::timeout(super::STARTUP_INITIALIZATION_LOCK_WAIT_TIMEOUT, conn.lock())
            .await
            .map_err(|_| "postgres migration lock timed out".to_string())?
            .map_err(|e| format!("lock postgres migrations: {e}"))?;
        conn.ensure_migrations_table()
            .await
            .map_err(|e| format!("prepare postgres migration history: {e}"))?;
        relocate_known_versions(&mut conn).await?;
        POSTGRES_MIGRATOR
            .run_direct(&mut conn)
            .await
            .map_err(|e| format!("run postgres migrations: {e}"))
    }
    .await;
    let closed = conn
        .close()
        .await
        .map_err(|e| format!("close postgres migration connection: {e}"));
    result.and(closed)
}

async fn relocate_known_versions(conn: &mut PgConnection) -> Result<(), String> {
    let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
    sqlx::query("LOCK TABLE _sqlx_migrations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("lock postgres migration history: {e}"))?;
    let applied: Vec<(i64, String, bool, Vec<u8>)> = sqlx::query_as(
        "SELECT version, description, success, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("read postgres migration history: {e}"))?;
    let mut moves = Vec::new();
    // Validate the entire history before changing even one row. Unrelated drift,
    // dirty migrations, unknown versions and destination collisions fail closed.
    for (version, description, success, checksum) in &applied {
        if !success {
            return Err(format!(
                "postgres migration {version} is dirty; no history changed"
            ));
        }
        let target = if let Some(relocation) = RELOCATIONS
            .iter()
            .find(|r| r.from == *version && r.description == description)
        {
            if checksum_hex(checksum) != relocation.checksum {
                return Err(format!(
                    "legacy postgres migration {version} checksum mismatch; no history changed"
                ));
            }
            if applied.iter().any(|(v, _, _, _)| *v == relocation.to) {
                return Err(format!(
                    "postgres migration relocation {version} -> {} conflicts with existing history",
                    relocation.to
                ));
            }
            moves.push(relocation);
            relocation.to
        } else {
            *version
        };
        let expected = POSTGRES_MIGRATOR
            .iter()
            .find(|m| m.version == target && !m.migration_type.is_down_migration())
            .ok_or_else(|| format!("unknown postgres migration {version}; no history changed"))?;
        if expected.checksum.as_ref() != checksum {
            return Err(format!(
                "postgres migration {version} checksum mismatch; no history changed"
            ));
        }
    }
    for relocation in &moves {
        sqlx::query("UPDATE _sqlx_migrations SET version = $1 WHERE version = $2")
            .bind(relocation.to)
            .bind(relocation.from)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("relocate postgres migration history: {e}"))?;
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    for relocation in moves {
        tracing::info!(
            from = relocation.from,
            to = relocation.to,
            description = relocation.description,
            "relocated verified postgres migration; SQL and checksum unchanged"
        );
    }
    Ok(())
}
