use std::fs;
use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension, params};

pub(crate) const AUTO_REMEMBER_MAX_RETRIES: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AutoRememberMemoryStatus {
    Remembered,
    DuplicateSkip,
    ValidationSkipped,
    RememberFailed,
    AbandonedAfterRetries,
}

impl AutoRememberMemoryStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Remembered => "remembered",
            Self::DuplicateSkip => "duplicate_skip",
            Self::ValidationSkipped => "validation_skipped",
            Self::RememberFailed => "remember_failed",
            Self::AbandonedAfterRetries => "abandoned_after_retries",
        }
    }

    fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "remembered" => Some(Self::Remembered),
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
                | Self::DuplicateSkip
                | Self::ValidationSkipped
                | Self::AbandonedAfterRetries
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AutoRememberStage {
    Extract,
    Validate,
    RecallLookup,
    Remember,
    Dedupe,
}

impl AutoRememberStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Extract => "extract",
            Self::Validate => "validate",
            Self::RecallLookup => "recall_lookup",
            Self::Remember => "remember",
            Self::Dedupe => "dedupe",
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRememberAuditRecord {
    pub(crate) status: AutoRememberMemoryStatus,
    pub(crate) retry_count: u32,
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
        store.with_conn(|conn| {
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
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (workspace, candidate_hash)
                );",
            )
        })?;
        Ok(store)
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
                    created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(workspace, candidate_hash) DO UPDATE SET
                    turn_id = excluded.turn_id,
                    signal_kind = excluded.signal_kind,
                    stage = excluded.stage,
                    memory_status = excluded.memory_status,
                    retry_count = excluded.retry_count,
                    error = excluded.error,
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
