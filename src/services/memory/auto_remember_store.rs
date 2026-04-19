use std::fs;
use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension, params};

pub(crate) const AUTO_REMEMBER_MAX_RETRIES: u32 = 3;
// P0 keeps durable retry coupled to the next eligible turn instead of running a
// dedicated worker. The sidecar queue is still durable across process restarts
// as long as the same runtime root is reused.
pub(crate) const AUTO_REMEMBER_RETRY_DRAIN_LIMIT: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AutoRememberMemoryStatus {
    Remembered,
    VerifiedPromoted,
    DuplicateSkip,
    ValidationSkipped,
    RememberFailed,
    AbandonedAfterRetries,
}

impl AutoRememberMemoryStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Remembered => "remembered",
            Self::VerifiedPromoted => "verified_promoted",
            Self::DuplicateSkip => "duplicate_skip",
            Self::ValidationSkipped => "validation_skipped",
            Self::RememberFailed => "remember_failed",
            Self::AbandonedAfterRetries => "abandoned_after_retries",
        }
    }

    pub(crate) fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "remembered" => Some(Self::Remembered),
            "verified_promoted" => Some(Self::VerifiedPromoted),
            "duplicate_skip" => Some(Self::DuplicateSkip),
            "validation_skipped" => Some(Self::ValidationSkipped),
            "remember_failed" => Some(Self::RememberFailed),
            "abandoned_after_retries" => Some(Self::AbandonedAfterRetries),
            _ => None,
        }
    }

    pub(crate) fn suppresses_repeat(self) -> bool {
        matches!(
            self,
            Self::Remembered
                | Self::VerifiedPromoted
                | Self::DuplicateSkip
                | Self::ValidationSkipped
                | Self::AbandonedAfterRetries
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AutoRememberStage {
    Validate,
    Remember,
    Verify,
    Dedupe,
}

impl AutoRememberStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Validate => "validate",
            Self::Remember => "remember",
            Self::Verify => "verify",
            Self::Dedupe => "dedupe",
        }
    }

    pub(crate) fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "validate" => Some(Self::Validate),
            "remember" => Some(Self::Remember),
            "verify" => Some(Self::Verify),
            "dedupe" => Some(Self::Dedupe),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRememberAuditEntry<'a> {
    pub(crate) turn_id: &'a str,
    pub(crate) candidate_hash: &'a str,
    pub(crate) signal_kind: &'a str,
    pub(crate) workspace: &'a str,
    pub(crate) stage: AutoRememberStage,
    pub(crate) status: AutoRememberMemoryStatus,
    pub(crate) retry_count: u32,
    pub(crate) error: Option<&'a str>,
    pub(crate) raw_content: Option<&'a str>,
    pub(crate) entity_key: Option<&'a str>,
    pub(crate) supporting_evidence: Option<&'a [String]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRememberAuditRecord {
    pub(crate) status: AutoRememberMemoryStatus,
    pub(crate) retry_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRememberAuditDetail {
    pub(crate) turn_id: String,
    pub(crate) workspace: String,
    pub(crate) candidate_hash: String,
    pub(crate) signal_kind: String,
    pub(crate) stage: AutoRememberStage,
    pub(crate) status: AutoRememberMemoryStatus,
    pub(crate) retry_count: u32,
    pub(crate) error: Option<String>,
    pub(crate) raw_content: Option<String>,
    pub(crate) entity_key: Option<String>,
    pub(crate) supporting_evidence: Vec<String>,
    pub(crate) created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRememberRetryRecord {
    pub(crate) turn_id: String,
    pub(crate) workspace: String,
    pub(crate) candidate_hash: String,
    pub(crate) signal_kind: String,
    pub(crate) raw_content: String,
    pub(crate) entity_key: Option<String>,
    pub(crate) supporting_evidence: Vec<String>,
    pub(crate) retry_count: u32,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct AutoRememberStore {
    path: PathBuf,
}

impl AutoRememberStore {
    pub(crate) fn open() -> Result<Self, String> {
        let path = sidecar_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create auto-remember sidecar dir: {err}"))?;
        }

        let store = Self { path };
        store.ensure_schema()?;
        Ok(store)
    }

    pub(crate) fn open_existing() -> Result<Option<Self>, String> {
        let path = sidecar_path()?;
        if !path.exists() {
            return Ok(None);
        }
        let store = Self { path };
        store.ensure_schema()?;
        Ok(Some(store))
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn lookup_record(
        &self,
        workspace: &str,
        candidate_hash: &str,
    ) -> Result<Option<AutoRememberAuditRecord>, String> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT memory_status, retry_count
                 FROM auto_remember_audit
                 WHERE workspace = ?1 AND candidate_hash = ?2",
                params![workspace, candidate_hash],
                |row| {
                    let status = row.get::<_, String>(0)?;
                    let retry_count = row.get::<_, u32>(1)?;
                    Ok((status, retry_count))
                },
            )
            .optional()
            .map(|record| {
                record.and_then(|(status, retry_count)| {
                    AutoRememberMemoryStatus::from_str(&status).map(|status| {
                        AutoRememberAuditRecord {
                            status,
                            retry_count,
                        }
                    })
                })
            })
        })
    }

    pub(crate) fn upsert_audit(&self, entry: AutoRememberAuditEntry<'_>) -> Result<(), String> {
        let error = entry
            .error
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let raw_content = entry
            .raw_content
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let entity_key = entry
            .entity_key
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let supporting_evidence_json = serde_json::to_string(
            &entry
                .supporting_evidence
                .unwrap_or(&[])
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>(),
        )
        .map_err(|err| format!("failed to encode auto-remember audit evidence: {err}"))?;
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO auto_remember_audit (
                    workspace,
                    candidate_hash,
                    turn_id,
                    signal_kind,
                    stage,
                    memory_status,
                    retry_count,
                    error,
                    raw_content,
                    entity_key,
                    supporting_evidence_json,
                    created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(workspace, candidate_hash) DO UPDATE SET
                    turn_id = excluded.turn_id,
                    signal_kind = excluded.signal_kind,
                    stage = excluded.stage,
                    memory_status = excluded.memory_status,
                    retry_count = excluded.retry_count,
                    error = excluded.error,
                    raw_content = COALESCE(excluded.raw_content, auto_remember_audit.raw_content),
                    entity_key = COALESCE(excluded.entity_key, auto_remember_audit.entity_key),
                    supporting_evidence_json = CASE
                        WHEN excluded.supporting_evidence_json = '[]'
                            THEN auto_remember_audit.supporting_evidence_json
                        ELSE excluded.supporting_evidence_json
                    END,
                    created_at = excluded.created_at",
                params![
                    entry.workspace,
                    entry.candidate_hash,
                    entry.turn_id,
                    entry.signal_kind,
                    entry.stage.as_str(),
                    entry.status.as_str(),
                    entry.retry_count,
                    error,
                    raw_content,
                    entity_key,
                    supporting_evidence_json,
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                ],
            )?;
            Ok(())
        })
    }

    pub(crate) fn next_retry_count(
        &self,
        workspace: &str,
        candidate_hash: &str,
    ) -> Result<u32, String> {
        Ok(self
            .lookup_record(workspace, candidate_hash)?
            .map(|record| record.retry_count.saturating_add(1))
            .unwrap_or(1))
    }

    pub(crate) fn next_failure_status(
        &self,
        workspace: &str,
        candidate_hash: &str,
    ) -> Result<(AutoRememberMemoryStatus, u32), String> {
        let retry_count = self.next_retry_count(workspace, candidate_hash)?;
        let status = if retry_count >= AUTO_REMEMBER_MAX_RETRIES {
            AutoRememberMemoryStatus::AbandonedAfterRetries
        } else {
            AutoRememberMemoryStatus::RememberFailed
        };
        Ok((status, retry_count))
    }

    pub(crate) fn upsert_retry(&self, entry: &AutoRememberRetryRecord) -> Result<(), String> {
        let evidence_json = serde_json::to_string(&entry.supporting_evidence)
            .map_err(|err| format!("failed to encode auto-remember retry evidence: {err}"))?;
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO auto_remember_retry_queue (
                    workspace,
                    candidate_hash,
                    turn_id,
                    signal_kind,
                    raw_content,
                    entity_key,
                    supporting_evidence_json,
                    retry_count,
                    error,
                    updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(workspace, candidate_hash) DO UPDATE SET
                    turn_id = excluded.turn_id,
                    signal_kind = excluded.signal_kind,
                    raw_content = excluded.raw_content,
                    entity_key = excluded.entity_key,
                    supporting_evidence_json = excluded.supporting_evidence_json,
                    retry_count = excluded.retry_count,
                    error = excluded.error,
                    updated_at_ms = excluded.updated_at_ms",
                params![
                    entry.workspace,
                    entry.candidate_hash,
                    entry.turn_id,
                    entry.signal_kind,
                    entry.raw_content,
                    entry.entity_key,
                    evidence_json,
                    entry.retry_count,
                    entry.error,
                    chrono::Utc::now().timestamp_millis(),
                ],
            )?;
            Ok(())
        })
    }

    pub(crate) fn load_retry_batch(&self) -> Result<Vec<AutoRememberRetryRecord>, String> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT
                    turn_id,
                    workspace,
                    candidate_hash,
                    signal_kind,
                    raw_content,
                    entity_key,
                    supporting_evidence_json,
                    retry_count,
                    error
                 FROM auto_remember_retry_queue
                 ORDER BY updated_at_ms ASC
                 LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![AUTO_REMEMBER_RETRY_DRAIN_LIMIT as i64], |row| {
                let evidence_json = row.get::<_, String>(6)?;
                let supporting_evidence =
                    serde_json::from_str::<Vec<String>>(&evidence_json).unwrap_or_default();
                Ok(AutoRememberRetryRecord {
                    turn_id: row.get(0)?,
                    workspace: row.get(1)?,
                    candidate_hash: row.get(2)?,
                    signal_kind: row.get(3)?,
                    raw_content: row.get(4)?,
                    entity_key: row.get(5)?,
                    supporting_evidence,
                    retry_count: row.get(7)?,
                    error: row.get(8)?,
                })
            })?;

            let mut records = Vec::new();
            for row in rows {
                records.push(row?);
            }
            Ok(records)
        })
    }

    pub(crate) fn delete_retry(&self, workspace: &str, candidate_hash: &str) -> Result<(), String> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM auto_remember_retry_queue
                 WHERE workspace = ?1 AND candidate_hash = ?2",
                params![workspace, candidate_hash],
            )?;
            Ok(())
        })
    }

    pub(crate) fn list_audit(
        &self,
        workspace: Option<&str>,
        status: Option<AutoRememberMemoryStatus>,
        limit: usize,
    ) -> Result<Vec<AutoRememberAuditDetail>, String> {
        let limit = limit.max(1) as i64;
        self.with_conn(|conn| {
            let mut records = Vec::new();
            match (
                workspace.map(str::trim).filter(|value| !value.is_empty()),
                status,
            ) {
                (Some(workspace), Some(status)) => {
                    let mut stmt = conn.prepare(
                        "SELECT
                            turn_id,
                            workspace,
                            candidate_hash,
                            signal_kind,
                            stage,
                            memory_status,
                            retry_count,
                            error,
                            raw_content,
                            entity_key,
                            supporting_evidence_json,
                            created_at
                         FROM auto_remember_audit
                         WHERE workspace = ?1 AND memory_status = ?2
                         ORDER BY created_at DESC
                         LIMIT ?3",
                    )?;
                    let rows = stmt
                        .query_map(params![workspace, status.as_str(), limit], |row| {
                            decode_audit_detail_row(row)
                        })?;
                    for row in rows {
                        records.push(row?);
                    }
                }
                (Some(workspace), None) => {
                    let mut stmt = conn.prepare(
                        "SELECT
                            turn_id,
                            workspace,
                            candidate_hash,
                            signal_kind,
                            stage,
                            memory_status,
                            retry_count,
                            error,
                            raw_content,
                            entity_key,
                            supporting_evidence_json,
                            created_at
                         FROM auto_remember_audit
                         WHERE workspace = ?1
                         ORDER BY created_at DESC
                         LIMIT ?2",
                    )?;
                    let rows = stmt.query_map(params![workspace, limit], |row| {
                        decode_audit_detail_row(row)
                    })?;
                    for row in rows {
                        records.push(row?);
                    }
                }
                (None, Some(status)) => {
                    let mut stmt = conn.prepare(
                        "SELECT
                            turn_id,
                            workspace,
                            candidate_hash,
                            signal_kind,
                            stage,
                            memory_status,
                            retry_count,
                            error,
                            raw_content,
                            entity_key,
                            supporting_evidence_json,
                            created_at
                         FROM auto_remember_audit
                         WHERE memory_status = ?1
                         ORDER BY created_at DESC
                         LIMIT ?2",
                    )?;
                    let rows = stmt.query_map(params![status.as_str(), limit], |row| {
                        decode_audit_detail_row(row)
                    })?;
                    for row in rows {
                        records.push(row?);
                    }
                }
                (None, None) => {
                    let mut stmt = conn.prepare(
                        "SELECT
                            turn_id,
                            workspace,
                            candidate_hash,
                            signal_kind,
                            stage,
                            memory_status,
                            retry_count,
                            error,
                            raw_content,
                            entity_key,
                            supporting_evidence_json,
                            created_at
                         FROM auto_remember_audit
                         ORDER BY created_at DESC
                         LIMIT ?1",
                    )?;
                    let rows =
                        stmt.query_map(params![limit], |row| decode_audit_detail_row(row))?;
                    for row in rows {
                        records.push(row?);
                    }
                }
            }
            Ok(records)
        })
    }

    pub(crate) fn count_by_status(
        &self,
        workspace: Option<&str>,
    ) -> Result<Vec<(String, u64)>, String> {
        grouped_count_query(self, workspace, "memory_status", None)
    }

    pub(crate) fn count_validation_skip_reasons(
        &self,
        workspace: Option<&str>,
    ) -> Result<Vec<(String, u64)>, String> {
        grouped_count_query(
            self,
            workspace,
            "error",
            Some(AutoRememberMemoryStatus::ValidationSkipped),
        )
    }

    pub(crate) fn load_resubmittable_candidate(
        &self,
        workspace: &str,
        candidate_hash: &str,
    ) -> Result<Option<AutoRememberRetryRecord>, String> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT
                    signal_kind,
                    raw_content,
                    entity_key,
                    supporting_evidence_json,
                    retry_count,
                    error,
                    memory_status,
                    turn_id
                 FROM auto_remember_audit
                 WHERE workspace = ?1 AND candidate_hash = ?2",
                params![workspace, candidate_hash],
                |row| {
                    let evidence_json = row
                        .get::<_, Option<String>>(3)?
                        .unwrap_or_else(|| "[]".to_string());
                    let supporting_evidence =
                        serde_json::from_str::<Vec<String>>(&evidence_json).unwrap_or_default();
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        supporting_evidence,
                        row.get::<_, u32>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .map(|record| {
                record.and_then(
                    |(
                        signal_kind,
                        raw_content,
                        entity_key,
                        supporting_evidence,
                        retry_count,
                        error,
                        status_raw,
                        turn_id,
                    )| {
                        let status = AutoRememberMemoryStatus::from_str(&status_raw)?;
                        if !matches!(
                            status,
                            AutoRememberMemoryStatus::RememberFailed
                                | AutoRememberMemoryStatus::AbandonedAfterRetries
                        ) {
                            return None;
                        }
                        raw_content.map(|raw_content| AutoRememberRetryRecord {
                            turn_id,
                            workspace: workspace.to_string(),
                            candidate_hash: candidate_hash.to_string(),
                            signal_kind,
                            raw_content,
                            entity_key,
                            supporting_evidence,
                            retry_count,
                            error,
                        })
                    },
                )
            })
        })
    }

    pub(crate) fn reset_retry_state(
        &self,
        workspace: &str,
        candidate_hash: &str,
    ) -> Result<(), String> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE auto_remember_audit
                 SET stage = ?3,
                     memory_status = ?4,
                     retry_count = 0,
                     error = ?5,
                     created_at = ?6
                 WHERE workspace = ?1 AND candidate_hash = ?2",
                params![
                    workspace,
                    candidate_hash,
                    AutoRememberStage::Remember.as_str(),
                    AutoRememberMemoryStatus::RememberFailed.as_str(),
                    "manual resubmit requested",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                ],
            )?;
            conn.execute(
                "DELETE FROM auto_remember_retry_queue
                 WHERE workspace = ?1 AND candidate_hash = ?2",
                params![workspace, candidate_hash],
            )?;
            Ok(())
        })
    }

    fn ensure_schema(&self) -> Result<(), String> {
        self.with_conn(|conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS auto_remember_audit (
                    workspace TEXT NOT NULL,
                    candidate_hash TEXT NOT NULL,
                    turn_id TEXT NOT NULL,
                    signal_kind TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    memory_status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    raw_content TEXT,
                    entity_key TEXT,
                    supporting_evidence_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (workspace, candidate_hash)
                );
                CREATE TABLE IF NOT EXISTS auto_remember_retry_queue (
                    workspace TEXT NOT NULL,
                    candidate_hash TEXT NOT NULL,
                    turn_id TEXT NOT NULL,
                    signal_kind TEXT NOT NULL,
                    raw_content TEXT NOT NULL,
                    entity_key TEXT,
                    supporting_evidence_json TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (workspace, candidate_hash)
                );",
            )?;
            ensure_column(conn, "auto_remember_audit", "raw_content", "TEXT")?;
            ensure_column(conn, "auto_remember_audit", "entity_key", "TEXT")?;
            ensure_column(
                conn,
                "auto_remember_audit",
                "supporting_evidence_json",
                "TEXT NOT NULL DEFAULT '[]'",
            )
        })
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, String> {
        let conn = Connection::open(&self.path)
            .map_err(|err| format!("failed to open auto-remember sidecar: {err}"))?;
        f(&conn).map_err(|err| format!("auto-remember sidecar query failed: {err}"))
    }
}

fn sidecar_path() -> Result<PathBuf, String> {
    let root = crate::config::runtime_root()
        .ok_or_else(|| "AgentDesk runtime root is unavailable".to_string())?;
    Ok(root.join("data").join("memory-auto-remember.sqlite"))
}

fn decode_audit_detail_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AutoRememberAuditDetail> {
    let stage_raw = row.get::<_, String>(4)?;
    let status_raw = row.get::<_, String>(5)?;
    let supporting_evidence_json = row
        .get::<_, Option<String>>(10)?
        .unwrap_or_else(|| "[]".to_string());
    let supporting_evidence =
        serde_json::from_str::<Vec<String>>(&supporting_evidence_json).unwrap_or_default();
    Ok(AutoRememberAuditDetail {
        turn_id: row.get(0)?,
        workspace: row.get(1)?,
        candidate_hash: row.get(2)?,
        signal_kind: row.get(3)?,
        stage: AutoRememberStage::from_str(&stage_raw).unwrap_or(AutoRememberStage::Remember),
        status: AutoRememberMemoryStatus::from_str(&status_raw)
            .unwrap_or(AutoRememberMemoryStatus::RememberFailed),
        retry_count: row.get(6)?,
        error: row.get(7)?,
        raw_content: row.get(8)?,
        entity_key: row.get(9)?,
        supporting_evidence,
        created_at: row.get(11)?,
    })
}

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(Result::ok)
        .any(|name| name == column);
    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
    }
    Ok(())
}

fn grouped_count_query(
    store: &AutoRememberStore,
    workspace: Option<&str>,
    group_column: &str,
    status_filter: Option<AutoRememberMemoryStatus>,
) -> Result<Vec<(String, u64)>, String> {
    store.with_conn(|conn| {
        let mut rows_out = Vec::new();
        match (
            workspace.map(str::trim).filter(|value| !value.is_empty()),
            status_filter,
        ) {
            (Some(workspace), Some(status)) => {
                let mut stmt = conn.prepare(&format!(
                    "SELECT COALESCE({group_column}, '') AS key, COUNT(*)
                     FROM auto_remember_audit
                     WHERE workspace = ?1 AND memory_status = ?2
                     GROUP BY key
                     ORDER BY COUNT(*) DESC, key ASC"
                ))?;
                let rows = stmt.query_map(params![workspace, status.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })?;
                for row in rows {
                    rows_out.push(row?);
                }
            }
            (Some(workspace), None) => {
                let mut stmt = conn.prepare(&format!(
                    "SELECT COALESCE({group_column}, '') AS key, COUNT(*)
                     FROM auto_remember_audit
                     WHERE workspace = ?1
                     GROUP BY key
                     ORDER BY COUNT(*) DESC, key ASC"
                ))?;
                let rows = stmt.query_map(params![workspace], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })?;
                for row in rows {
                    rows_out.push(row?);
                }
            }
            (None, Some(status)) => {
                let mut stmt = conn.prepare(&format!(
                    "SELECT COALESCE({group_column}, '') AS key, COUNT(*)
                     FROM auto_remember_audit
                     WHERE memory_status = ?1
                     GROUP BY key
                     ORDER BY COUNT(*) DESC, key ASC"
                ))?;
                let rows = stmt.query_map(params![status.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })?;
                for row in rows {
                    rows_out.push(row?);
                }
            }
            (None, None) => {
                let mut stmt = conn.prepare(&format!(
                    "SELECT COALESCE({group_column}, '') AS key, COUNT(*)
                     FROM auto_remember_audit
                     GROUP BY key
                     ORDER BY COUNT(*) DESC, key ASC"
                ))?;
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })?;
                for row in rows {
                    rows_out.push(row?);
                }
            }
        }
        Ok(rows_out)
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::services::discord::runtime_store::lock_test_env;

    fn with_temp_root<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let _guard = lock_test_env();
        let temp = TempDir::new().unwrap();
        let previous = std::env::var_os("AGENTDESK_ROOT_DIR");
        unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", temp.path()) };
        f(&temp);
        match previous {
            Some(value) => unsafe { std::env::set_var("AGENTDESK_ROOT_DIR", value) },
            None => unsafe { std::env::remove_var("AGENTDESK_ROOT_DIR") },
        }
    }

    #[test]
    fn sidecar_store_tracks_retry_count_and_status() {
        with_temp_root(|temp| {
            let store = AutoRememberStore::open().unwrap();
            let evidence = vec!["SQLite sidecar is the audit store.".to_string()];
            assert_eq!(
                store.path(),
                &temp.path().join("data").join("memory-auto-remember.sqlite")
            );

            store
                .upsert_audit(AutoRememberAuditEntry {
                    turn_id: "turn-1",
                    candidate_hash: "hash-1",
                    signal_kind: "technical_decision",
                    workspace: "agentdesk-default",
                    stage: AutoRememberStage::Remember,
                    status: AutoRememberMemoryStatus::RememberFailed,
                    retry_count: 1,
                    error: Some("network error"),
                    raw_content: Some("SQLite sidecar is the audit store."),
                    entity_key: None,
                    supporting_evidence: Some(evidence.as_slice()),
                })
                .unwrap();

            let record = store
                .lookup_record("agentdesk-default", "hash-1")
                .unwrap()
                .unwrap();
            assert_eq!(record.status, AutoRememberMemoryStatus::RememberFailed);
            assert_eq!(record.retry_count, 1);
            assert_eq!(
                store
                    .next_failure_status("agentdesk-default", "hash-1")
                    .unwrap(),
                (AutoRememberMemoryStatus::RememberFailed, 2)
            );
        });
    }

    #[test]
    fn remember_failed_status_abandons_after_max_retries() {
        with_temp_root(|_| {
            let store = AutoRememberStore::open().unwrap();
            let evidence = vec!["memory.backend changed from file to memento.".to_string()];
            store
                .upsert_audit(AutoRememberAuditEntry {
                    turn_id: "turn-1",
                    candidate_hash: "hash-2",
                    signal_kind: "config_change",
                    workspace: "agentdesk-default",
                    stage: AutoRememberStage::Remember,
                    status: AutoRememberMemoryStatus::RememberFailed,
                    retry_count: AUTO_REMEMBER_MAX_RETRIES - 1,
                    error: Some("network error"),
                    raw_content: Some("memory.backend changed from file to memento."),
                    entity_key: Some("memory.backend"),
                    supporting_evidence: Some(evidence.as_slice()),
                })
                .unwrap();

            let (status, retry_count) = store
                .next_failure_status("agentdesk-default", "hash-2")
                .unwrap();
            assert_eq!(status, AutoRememberMemoryStatus::AbandonedAfterRetries);
            assert_eq!(retry_count, AUTO_REMEMBER_MAX_RETRIES);
            assert!(status.suppresses_repeat());
        });
    }
}
