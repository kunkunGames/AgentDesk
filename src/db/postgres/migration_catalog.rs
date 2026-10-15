//! Immutable PostgreSQL histories, selected from receipts before any schema mutation.
//!
//! The campaign deployment and upstream independently assigned versions 131 onward.
//! Their receipts cannot be renumbered or repaired. Version 146 converges capabilities;
//! later migrations are shared. Diagnostics remain available even for rejected histories.

use std::borrow::Cow;

use sqlx::migrate::{Migrate, Migration, Migrator};
use sqlx::{Connection, PgConnection, PgPool, Row};

static CAMPAIGN_V5: Migrator = sqlx::migrate!("./migrations/postgres_campaign_v5");
static SHARED: Migrator = sqlx::migrate!("./migrations/postgres_shared");
const LINEAGE_ENV: &str = "AGENTDESK_PG_MIGRATION_LINEAGE";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lineage {
    Upstream,
    CampaignV5,
}

impl Lineage {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Upstream => "upstream",
            Self::CampaignV5 => "campaign_v5",
        }
    }

    pub(super) fn migrator(self) -> Migrator {
        let mut migrations: Vec<Migration> = match self {
            Self::Upstream => super::POSTGRES_MIGRATOR.iter().cloned().collect(),
            Self::CampaignV5 => super::POSTGRES_MIGRATOR
                .iter()
                .filter(|m| m.version <= 130)
                .chain(CAMPAIGN_V5.iter())
                .cloned()
                .collect(),
        };
        migrations.extend(SHARED.iter().cloned());
        Migrator {
            migrations: Cow::Owned(migrations),
            // The dedicated connection is already locked before selection.
            locking: false,
            ..Migrator::DEFAULT
        }
    }
}

#[derive(Debug)]
pub(super) struct Receipt {
    pub(super) version: i64,
    pub(super) description: String,
    pub(super) success: bool,
    pub(super) checksum: Vec<u8>,
}

pub(super) async fn receipts(conn: &mut PgConnection) -> Result<Vec<Receipt>, String> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| format!("inspect migration ledger: {e}"))?;
    if !exists {
        return Ok(Vec::new());
    }
    sqlx::query(
        "SELECT version, description, success, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(conn)
    .await
    .map_err(|e| format!("read migration receipts: {e}"))?
    .into_iter()
    .map(|row| {
        Ok(Receipt {
            version: row
                .try_get("version")
                .map_err(|e| format!("migration version: {e}"))?,
            description: row
                .try_get("description")
                .map_err(|e| format!("migration description: {e}"))?,
            success: row
                .try_get("success")
                .map_err(|e| format!("migration status: {e}"))?,
            checksum: row
                .try_get("checksum")
                .map_err(|e| format!("migration checksum: {e}"))?,
        })
    })
    .collect()
}

fn requested_lineage() -> Result<Option<Lineage>, String> {
    match std::env::var(LINEAGE_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Ok(value) if value == "upstream" => Ok(Some(Lineage::Upstream)),
        Ok(value) if value == "campaign_v5" => Ok(Some(Lineage::CampaignV5)),
        _ => Err(format!("{LINEAGE_ENV} must be upstream or campaign_v5")),
    }
}

/// Infer a catalog for diagnostics without hiding checksum, failed or future rows.
pub(super) fn inferred_lineage(rows: &[Receipt]) -> Option<Lineage> {
    let mut inferred = None;
    for row in rows.iter().filter(|row| (131..=146).contains(&row.version)) {
        for lineage in [Lineage::Upstream, Lineage::CampaignV5] {
            if lineage.migrator().iter().any(|m| {
                !m.migration_type.is_down_migration()
                    && m.version == row.version
                    && m.checksum.as_ref() == row.checksum.as_slice()
            }) {
                if inferred.is_some_and(|previous| previous != lineage) {
                    return None;
                }
                inferred = Some(lineage);
            }
        }
    }
    inferred
}

pub(super) fn diagnostic_lineage(rows: &[Receipt]) -> Lineage {
    inferred_lineage(rows)
        .or_else(|| requested_lineage().ok().flatten())
        .unwrap_or(Lineage::Upstream)
}

fn validate_prefix(rows: &[Receipt], lineage: Lineage) -> Result<(), String> {
    let catalog = lineage.migrator();
    let expected: Vec<_> = catalog
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .collect();
    for (index, row) in rows.iter().enumerate() {
        let Some(migration) = expected.get(index) else {
            return Err(format!(
                "{} history contains unknown migration {}",
                lineage.label(),
                row.version
            ));
        };
        if !row.success
            || row.version != migration.version
            || row.checksum.as_slice() != migration.checksum.as_ref()
        {
            return Err(format!(
                "{} migration history is not an exact successful prefix: receipt {}, expected {}, success {}",
                lineage.label(),
                row.version,
                migration.version,
                row.success,
            ));
        }
    }
    Ok(())
}

async fn choose_for_execution(
    conn: &mut PgConnection,
    rows: &[Receipt],
) -> Result<Lineage, String> {
    let requested = requested_lineage()?;
    let inferred = inferred_lineage(rows);
    if inferred.is_some() && requested.is_some() && inferred != requested {
        return Err("requested migration lineage conflicts with existing receipts".into());
    }
    if rows.is_empty() {
        // A missing ledger is not evidence of an empty application database.
        let occupied: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM (
                    SELECT c.oid, c.tableoid AS catalog, c.relnamespace AS namespace
                      FROM pg_class c
                     WHERE c.relkind IN ('r','p','v','m','S','f','c') AND c.relname <> '_sqlx_migrations'
                    UNION ALL
                    SELECT p.oid, p.tableoid, p.pronamespace FROM pg_proc p
                    UNION ALL
                    SELECT t.oid, t.tableoid, t.typnamespace FROM pg_type t
                     WHERE t.typrelid=0 AND t.typelem=0 AND t.typtype <> 'p'
                ) object JOIN pg_namespace n ON n.oid=object.namespace
                WHERE n.nspname=ANY(current_schemas(false))
                  AND NOT starts_with(n.nspname,'pg_') AND n.nspname <> 'information_schema'
                  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                      WHERE d.classid=object.catalog AND d.objid=object.oid AND d.deptype='e'))",
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| format!("inspect empty database: {e}"))?;
        if occupied {
            return Err(
                "application objects exist without migration receipts; restore the verified ledger"
                    .into(),
            );
        }
    }
    let lineage = if let Some(lineage) = inferred.or(requested) {
        lineage
    } else if rows.is_empty() {
        Lineage::Upstream
    } else {
        return Err(format!(
            "migration lineage is ambiguous; set {LINEAGE_ENV} explicitly after verifying the database history"
        ));
    };
    validate_prefix(rows, lineage)?;
    Ok(lineage)
}

/// Diagnose known-history corruption separately from a clean future-version suffix.
/// A normal pending suffix is admissible; a missing receipt below an applied row is not.
pub(super) async fn history_issues(
    conn: &mut PgConnection,
    rows: &[Receipt],
    lineage: Lineage,
) -> Vec<String> {
    let result = async {
        let requested = requested_lineage()?;
        let inferred = inferred_lineage(rows);
        if inferred.is_some() && requested.is_some() && inferred != requested {
            return Err("requested migration lineage conflicts with existing receipts".into());
        }
        if rows.is_empty() {
            return choose_for_execution(conn, rows).await.map(|_| ());
        }
        if inferred.is_none() && requested.is_none() {
            return Err(format!(
                "migration lineage is ambiguous; set {LINEAGE_ENV} after verification"
            ));
        }
        let catalog = lineage.migrator();
        let expected: Vec<_> = catalog
            .iter()
            .filter(|m| !m.migration_type.is_down_migration())
            .collect();
        let latest = expected.last().expect("nonempty migration catalog").version;
        let known_len = rows.partition_point(|row| row.version <= latest);
        validate_prefix(&rows[..known_len], lineage)?;
        if known_len < rows.len() && known_len != expected.len() {
            return Err("future migration receipts follow an incomplete known history".into());
        }
        if rows[known_len..].iter().any(|row| !row.success) {
            return Err("future migration history contains an unsuccessful receipt".into());
        }
        Ok(())
    }
    .await;
    result.err().into_iter().collect()
}

pub(super) async fn migrate(pool: &PgPool) -> Result<(), String> {
    // Detach before taking a session lock: cancellation drops and closes this connection
    // instead of returning a locked connection to the pool.
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("acquire migration connection: {e}"))?
        .detach();
    let result = async {
        conn.lock()
            .await
            .map_err(|e| format!("lock migrations: {e}"))?;
        let result = async {
            let rows = receipts(&mut conn).await?;
            let lineage = choose_for_execution(&mut conn, &rows).await?;
            tracing::info!(
                lineage = lineage.label(),
                applied = rows.len(),
                "verified immutable migration history"
            );
            lineage
                .migrator()
                .run_direct(&mut conn)
                .await
                .map_err(|e| format!("run {} migrations: {e}", lineage.label()))
        }
        .await;
        let unlock = conn
            .unlock()
            .await
            .map_err(|e| format!("unlock migrations: {e}"));
        match (result, unlock) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
    .await;
    let close = conn
        .close()
        .await
        .map_err(|e| format!("close migration connection: {e}"));
    result.and(close)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) async fn install_campaign_fixture_through(pool: &PgPool, version: i64) {
    tests::install_fixture(pool, Lineage::CampaignV5, version).await;
}
