//! Read-only migration history and checksum diagnostics for the selected lineage.

use super::migration_catalog;
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMigrationInfo {
    pub version: i64,
    pub description: String,
    pub success: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationStatus {
    pub applied: Vec<AppliedMigrationInfo>,
    pub resolved_versions: Vec<i64>,
    pub missing_from_resolved: Vec<i64>,
    pub pending_versions: Vec<i64>,
    pub history_issues: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationChecksumMismatch {
    pub version: i64,
    pub applied_checksum: String,
    pub resolved_checksum: String,
}

pub async fn query_applied_migrations(pool: &PgPool) -> Result<Vec<AppliedMigrationInfo>, String> {
    let rows = sqlx::query(
        "SELECT version, description, success
         FROM _sqlx_migrations
         ORDER BY version",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("query _sqlx_migrations: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|row| AppliedMigrationInfo {
            version: row.get::<i64, _>("version"),
            description: row.get::<String, _>("description"),
            success: row.get::<bool, _>("success"),
        })
        .collect())
}

pub async fn migration_status(pool: &PgPool) -> Result<MigrationStatus, String> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("migration status connection: {e}"))?;
    let rows = migration_catalog::receipts(&mut conn).await?;
    let lineage = migration_catalog::diagnostic_lineage(&rows);
    let history_issues = migration_catalog::history_issues(&mut conn, &rows, lineage).await;
    let catalog = lineage.migrator();
    let applied = rows
        .iter()
        .map(|row| AppliedMigrationInfo {
            version: row.version,
            description: row.description.clone(),
            success: row.success,
        })
        .collect::<Vec<_>>();
    let resolved_versions = catalog
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    let resolved_set = resolved_versions.iter().copied().collect::<BTreeSet<_>>();
    let applied_set = applied
        .iter()
        .filter(|migration| migration.success)
        .map(|migration| migration.version)
        .collect::<BTreeSet<_>>();
    let missing_from_resolved = applied_set
        .difference(&resolved_set)
        .copied()
        .collect::<Vec<_>>();
    let pending_versions = resolved_set
        .difference(&applied_set)
        .copied()
        .collect::<Vec<_>>();
    Ok(MigrationStatus {
        applied,
        resolved_versions,
        missing_from_resolved,
        pending_versions,
        history_issues,
    })
}

pub async fn applied_migration_checksum_mismatch_details(
    pool: &PgPool,
) -> Result<Vec<MigrationChecksumMismatch>, String> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("migration checksum connection: {e}"))?;
    let rows = migration_catalog::receipts(&mut conn).await?;
    let catalog = migration_catalog::diagnostic_lineage(&rows).migrator();

    // sqlx Migrator yields both ReversibleUp and ReversibleDown entries for the
    // same version. The applied checksum stored in `_sqlx_migrations` is the Up
    // checksum (sqlx::run_direct skips Down migrations when applying), so the
    // expected map must mirror that — collecting all entries collapses Up/Down
    // for the same version and a Down checksum can shadow the Up, producing a
    // false-positive drift signal. Mirror sqlx's own `is_down_migration()`
    // filter from `Migrator::run_direct`.
    let resolved_checksums = catalog
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| (migration.version, migration.checksum.as_ref()))
        .collect::<BTreeMap<_, _>>();

    let mut mismatches = Vec::new();
    for row in rows {
        let version = row.version;
        let success = row.success;
        if !success {
            continue;
        }
        let Some(expected_checksum) = resolved_checksums.get(&version) else {
            continue;
        };
        let applied_checksum = row.checksum;
        if applied_checksum.as_slice() != *expected_checksum {
            mismatches.push(MigrationChecksumMismatch {
                version,
                applied_checksum: checksum_hex(&applied_checksum),
                resolved_checksum: checksum_hex(*expected_checksum),
            });
        }
    }

    Ok(mismatches)
}

pub(super) fn checksum_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
