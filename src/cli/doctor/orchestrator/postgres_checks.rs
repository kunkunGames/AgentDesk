//! PostgreSQL connectivity, migration history and checksum diagnostics.

use super::{Check, CheckGroup, FixSafety, SecurityExposure, config, dcserver_log_hint};
use serde_json::{Value, json};

pub(super) fn check_postgres_connection(cfg: &config::Config) -> Check {
    let summary = crate::db::postgres::database_summary(cfg);
    if !crate::db::postgres::database_enabled(cfg) {
        return Check::ok(
            "postgres_connection",
            CheckGroup::Core,
            "PostgreSQL",
            "disabled",
        )
        .with_expected_actual("postgres bootstrap configured", "disabled");
    }

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return Check::fail(
                "postgres_connection",
                CheckGroup::Core,
                "PostgreSQL",
                format!("{summary} — runtime init failed"),
                "Failed to create async runtime for postgres connection check.",
            )
            .with_expected_actual(
                "postgres check runtime initializes",
                format!("runtime build failed: {error}"),
            );
        }
    };

    match runtime.block_on(crate::db::postgres::connect(cfg)) {
        Ok(Some(pool)) => {
            let migration_status = runtime.block_on(crate::db::postgres::migration_status(&pool));
            let checksum_mismatches = runtime
                .block_on(crate::db::postgres::applied_migration_checksum_mismatch_details(&pool));
            drop(pool);
            match (migration_status, checksum_mismatches) {
                (Ok(status), Ok(checksum_mismatches))
                    if postgres_migration_status_is_healthy(&status, &checksum_mismatches) =>
                {
                    let pending = status.pending_versions.len();
                    Check::ok(
                        "postgres_connection",
                        CheckGroup::Core,
                        "PostgreSQL",
                        format!(
                            "{summary} — ok; applied={} resolved={} pending={pending}",
                            status.applied.len(),
                            status.resolved_versions.len(),
                        ),
                    )
                    .with_subsystem("postgres")
                    .with_evidence(json!({
                        "applied_count": status.applied.len(),
                        "resolved_count": status.resolved_versions.len(),
                        "pending_versions": status.pending_versions,
                        "checksum_mismatches": checksum_mismatch_evidence(&checksum_mismatches),
                    }))
                    .with_expected_actual("postgres connection and migration metadata readable", "ok")
                }
                (Ok(status), Ok(checksum_mismatches)) => Check::fail(
                    "postgres_connection",
                    CheckGroup::Core,
                    "PostgreSQL",
                    format!(
                        "{summary} — migration drift: missing_from_resolved={:?} unsuccessful={:?} checksum_mismatches={} history_issues={:?}",
                        status.missing_from_resolved,
                        unsuccessful_migration_versions(&status),
                        format_checksum_mismatches(&checksum_mismatches),
                        status.history_issues
                    ),
                    "Postgres _sqlx_migrations contains drift or unsuccessful migration records.",
                )
                .with_subsystem("postgres")
                .with_fix_safety(FixSafety::NotFixable)
                .with_security_exposure(SecurityExposure::OperationalMetadata)
                .with_evidence(json!({
                    "applied_count": status.applied.len(),
                    "resolved_count": status.resolved_versions.len(),
                    "missing_from_resolved": status.missing_from_resolved,
                    "history_issues": status.history_issues,
                    "unsuccessful_versions": unsuccessful_migration_versions(&status),
                    "pending_versions": status.pending_versions,
                    "checksum_mismatches": checksum_mismatch_evidence(&checksum_mismatches),
                }))
                .with_expected_actual(
                    "applied migrations all exist in resolved migrations, succeeded, and checksum-matched",
                    "migration drift, checksum mismatch, or unsuccessful migration",
                ),
                (Err(error), _) => Check::fail(
                    "postgres_connection",
                    CheckGroup::Core,
                    "PostgreSQL",
                    format!("{summary} — migration metadata failed"),
                    "Postgres connection succeeded but read-only migration metadata could not be queried.",
                )
                .with_subsystem("postgres")
                .with_fix_safety(FixSafety::NotFixable)
                .with_security_exposure(SecurityExposure::OperationalMetadata)
                .with_expected_actual("read-only _sqlx_migrations query succeeds", error),
                (_, Err(error)) => Check::fail(
                    "postgres_connection",
                    CheckGroup::Core,
                    "PostgreSQL",
                    format!("{summary} — migration checksum verification failed"),
                    "Postgres connection succeeded but read-only migration checksum metadata could not be queried.",
                )
                .with_subsystem("postgres")
                .with_fix_safety(FixSafety::NotFixable)
                .with_security_exposure(SecurityExposure::OperationalMetadata)
                .with_expected_actual("read-only _sqlx_migrations checksum query succeeds", error),
            }
        }
        Ok(None) => Check::ok(
            "postgres_connection",
            CheckGroup::Core,
            "PostgreSQL",
            "disabled",
        )
        .with_expected_actual("postgres bootstrap configured", "disabled"),
        Err(error) => Check::fail(
            "postgres_connection",
            CheckGroup::Core,
            "PostgreSQL",
            format!("{summary} — failed"),
            "check the DATABASE_URL or database configuration values (host/port/dbname/user/password).",
        )
        .with_expected_actual("postgres connection succeeds", error)
        .with_next_steps(vec![
            "agentdesk doctor --json".to_string(),
            format!("tail -n 200 {}", dcserver_log_hint()),
        ]),
    }
}

fn unsuccessful_migration_versions(status: &crate::db::postgres::MigrationStatus) -> Vec<i64> {
    status
        .applied
        .iter()
        .filter(|migration| !migration.success)
        .map(|migration| migration.version)
        .collect()
}

pub(super) fn checksum_mismatch_evidence(
    mismatches: &[crate::db::postgres::MigrationChecksumMismatch],
) -> Vec<Value> {
    mismatches
        .iter()
        .map(|mismatch| {
            json!({
                "version": mismatch.version,
                "applied_checksum": mismatch.applied_checksum,
                "resolved_checksum": mismatch.resolved_checksum,
            })
        })
        .collect()
}

pub(super) fn format_checksum_mismatches(
    mismatches: &[crate::db::postgres::MigrationChecksumMismatch],
) -> String {
    if mismatches.is_empty() {
        return "[]".to_string();
    }
    let parts = mismatches
        .iter()
        .map(|mismatch| {
            format!(
                "version={} applied_checksum={} resolved_checksum={}",
                mismatch.version, mismatch.applied_checksum, mismatch.resolved_checksum
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", parts.join(", "))
}

pub(super) fn postgres_migration_status_is_healthy(
    status: &crate::db::postgres::MigrationStatus,
    checksum_mismatches: &[crate::db::postgres::MigrationChecksumMismatch],
) -> bool {
    status.missing_from_resolved.is_empty()
        && status.history_issues.is_empty()
        && unsuccessful_migration_versions(status).is_empty()
        && checksum_mismatches.is_empty()
}
