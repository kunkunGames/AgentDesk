use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::utils::redact::{contains_registered_secret, redact_known_secrets};

pub const DEFAULT_READ_EVENT_LIMIT: usize = 8;
pub const READ_BYTE_CAP: usize = 24 * 1024;
pub const LAST_USER_MESSAGE_MAX: usize = 4000;
pub const DEFAULT_MAX_CHECKPOINT_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointEventKind {
    OwnerProgress,
    Stall,
    FallbackProgress,
    Complete,
    Restore,
}

impl CheckpointEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OwnerProgress => "owner_progress",
            Self::Stall => "stall",
            Self::FallbackProgress => "fallback_progress",
            Self::Complete => "complete",
            Self::Restore => "restore",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "owner_progress" => Some(Self::OwnerProgress),
            "stall" => Some(Self::Stall),
            "fallback_progress" => Some(Self::FallbackProgress),
            "complete" => Some(Self::Complete),
            "restore" => Some(Self::Restore),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRecoveryStatus {
    Owner,
    FallbackRunning,
    FallbackDone,
    Restored,
    Aborted,
}

impl ChannelRecoveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::FallbackRunning => "fallback_running",
            Self::FallbackDone => "fallback_done",
            Self::Restored => "restored",
            Self::Aborted => "aborted",
        }
    }

    pub fn lock_held(self) -> bool {
        matches!(self, Self::FallbackRunning | Self::FallbackDone)
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "fallback_running" => Some(Self::FallbackRunning),
            "fallback_done" => Some(Self::FallbackDone),
            "restored" => Some(Self::Restored),
            "aborted" => Some(Self::Aborted),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointPayload {
    pub schema_version: u32,
    pub identity_label: String,
    pub goal: String,
    pub progress: String,
    pub decisions: String,
    pub files: Vec<String>,
    pub next: String,
    pub last_user_message: String,
}

impl CheckpointPayload {
    pub fn compact(
        identity_label: impl Into<String>,
        goal: impl Into<String>,
        progress: impl Into<String>,
        decisions: impl Into<String>,
        files: Vec<String>,
        next: impl Into<String>,
        last_user_message: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: 1,
            identity_label: identity_label.into(),
            goal: goal.into(),
            progress: progress.into(),
            decisions: decisions.into(),
            files,
            next: next.into(),
            last_user_message: truncate_chars(&last_user_message.into(), LAST_USER_MESSAGE_MAX),
        }
    }

    fn redact(&self) -> Self {
        Self {
            schema_version: self.schema_version,
            identity_label: redact_known_secrets(&self.identity_label),
            goal: redact_known_secrets(&self.goal),
            progress: redact_known_secrets(&self.progress),
            decisions: redact_known_secrets(&self.decisions),
            files: self
                .files
                .iter()
                .map(|path| redact_known_secrets(path))
                .collect(),
            next: redact_known_secrets(&self.next),
            last_user_message: redact_known_secrets(&self.last_user_message),
        }
    }

    pub fn five_section_text(&self) -> String {
        let files = if self.files.is_empty() {
            String::new()
        } else {
            self.files.join(", ")
        };
        format!(
            "Goal: {}\nProgress: {}\nDecisions: {}\nFiles: {}\nNext: {}",
            self.goal, self.progress, self.decisions, files, self.next
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointEvent {
    pub id: String,
    pub channel_id: String,
    pub seq: i64,
    pub at: DateTime<Utc>,
    pub writer_agent_id: String,
    pub kind: CheckpointEventKind,
    pub payload: CheckpointPayload,
    pub payload_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelState {
    pub channel_id: String,
    pub status: ChannelRecoveryStatus,
    pub owner_agent_id: String,
    pub fallback_agent_id: String,
    pub active_writer_agent_id: String,
    pub workspace: String,
    pub primary_turn_id: Option<String>,
    pub next_seq: i64,
}

impl ChannelState {
    pub fn lock_held(&self) -> bool {
        self.status.lock_held()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointError {
    RedactFailed,
    TooLarge { bytes: usize, max_bytes: usize },
    Serialize(String),
}

impl CheckpointError {
    pub fn message(&self) -> String {
        match self {
            Self::RedactFailed => "recovery checkpoint redact failed; write refused".to_string(),
            Self::TooLarge { bytes, max_bytes } => {
                format!(
                    "recovery checkpoint is too large: {bytes} bytes exceeds max_checkpoint_bytes {max_bytes}"
                )
            }
            Self::Serialize(error) => format!("recovery checkpoint serialize failed: {error}"),
        }
    }
}

pub fn prepare_event(
    channel_id: &str,
    seq: i64,
    writer_agent_id: &str,
    kind: CheckpointEventKind,
    payload: CheckpointPayload,
    max_checkpoint_bytes: usize,
) -> Result<CheckpointEvent, CheckpointError> {
    let redacted = payload.redact();
    let serialized = serde_json::to_vec(&redacted)
        .map_err(|error| CheckpointError::Serialize(error.to_string()))?;
    if serialized.len() > max_checkpoint_bytes.max(1) {
        return Err(CheckpointError::TooLarge {
            bytes: serialized.len(),
            max_bytes: max_checkpoint_bytes.max(1),
        });
    }
    let joined = String::from_utf8_lossy(&serialized);
    // Field-level redact already ran ASSIGNMENT_RE. Re-running it on JSON
    // treats `token=***"` as a new assignment and mutates quotes, so fail
    // closed only when a registered secret or PEM still survives.
    if contains_registered_secret(&joined) || contains_unredacted_secret_pattern(&joined) {
        return Err(CheckpointError::RedactFailed);
    }
    Ok(CheckpointEvent {
        id: format!("arc_{}", uuid::Uuid::new_v4().simple()),
        channel_id: channel_id.to_string(),
        seq,
        at: Utc::now(),
        writer_agent_id: writer_agent_id.to_string(),
        kind,
        payload: redacted,
        payload_bytes: serialized.len(),
    })
}

pub fn last_n_events(
    events: &[CheckpointEvent],
    limit: usize,
    byte_cap: usize,
) -> Vec<CheckpointEvent> {
    let mut selected = Vec::new();
    let mut bytes = 0usize;
    for event in events.iter().rev() {
        if selected.len() >= limit.max(1) {
            break;
        }
        let next_bytes = bytes.saturating_add(event.payload_bytes);
        if !selected.is_empty() && next_bytes > byte_cap {
            break;
        }
        bytes = next_bytes;
        selected.push(event.clone());
    }
    selected.reverse();
    selected
}

pub fn payload_from_json(value: &Value) -> Result<CheckpointPayload, CheckpointError> {
    serde_json::from_value(value.clone())
        .map_err(|error| CheckpointError::Serialize(error.to_string()))
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value.chars().take(max_chars).collect()
}

fn contains_unredacted_secret_pattern(text: &str) -> bool {
    let upper = text.to_ascii_uppercase();
    upper.contains("BEGIN PRIVATE KEY") || upper.contains("BEGIN RSA PRIVATE KEY")
}

pub async fn persist_channel_state(
    pool: &sqlx::PgPool,
    state: &ChannelState,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO agent_recovery_channel_state (
             channel_id, status, owner_agent_id, fallback_agent_id,
             active_writer_agent_id, workspace, primary_turn_id, next_seq, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (channel_id) DO UPDATE SET
             status = EXCLUDED.status,
             owner_agent_id = EXCLUDED.owner_agent_id,
             fallback_agent_id = EXCLUDED.fallback_agent_id,
             active_writer_agent_id = EXCLUDED.active_writer_agent_id,
             workspace = EXCLUDED.workspace,
             primary_turn_id = EXCLUDED.primary_turn_id,
             next_seq = EXCLUDED.next_seq,
             updated_at = NOW()",
    )
    .bind(&state.channel_id)
    .bind(state.status.as_str())
    .bind(&state.owner_agent_id)
    .bind(&state.fallback_agent_id)
    .bind(&state.active_writer_agent_id)
    .bind(&state.workspace)
    .bind(state.primary_turn_id.as_deref())
    .bind(state.next_seq)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn persist_checkpoint_event(
    pool: &sqlx::PgPool,
    event: &CheckpointEvent,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::to_value(&event.payload).unwrap_or(Value::Null);
    sqlx::query(
        "INSERT INTO agent_recovery_checkpoint_events (
             id, channel_id, seq, at, writer_agent_id, kind, payload, payload_bytes
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&event.id)
    .bind(&event.channel_id)
    .bind(event.seq)
    .bind(event.at)
    .bind(&event.writer_agent_id)
    .bind(event.kind.as_str())
    .bind(payload)
    .bind(event.payload_bytes as i32)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn load_channel_state(
    pool: &sqlx::PgPool,
    channel_id: &str,
) -> Result<Option<ChannelState>, sqlx::Error> {
    let row = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            i64,
        ),
    >(
        "SELECT channel_id, status, owner_agent_id, fallback_agent_id,
                active_writer_agent_id, workspace, primary_turn_id, next_seq
           FROM agent_recovery_channel_state
          WHERE channel_id = $1",
    )
    .bind(channel_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(
        |(
            channel_id,
            status,
            owner_agent_id,
            fallback_agent_id,
            active_writer_agent_id,
            workspace,
            primary_turn_id,
            next_seq,
        )| {
            Some(ChannelState {
                channel_id,
                status: ChannelRecoveryStatus::parse(&status)?,
                owner_agent_id,
                fallback_agent_id,
                active_writer_agent_id,
                workspace,
                primary_turn_id,
                next_seq,
            })
        },
    ))
}

pub async fn load_locked_channel_states(
    pool: &sqlx::PgPool,
) -> Result<Vec<ChannelState>, sqlx::Error> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            i64,
        ),
    >(
        "SELECT channel_id, status, owner_agent_id, fallback_agent_id,
                active_writer_agent_id, workspace, primary_turn_id, next_seq
           FROM agent_recovery_channel_state
          WHERE status IN ('fallback_running', 'fallback_done')",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(
                channel_id,
                status,
                owner_agent_id,
                fallback_agent_id,
                active_writer_agent_id,
                workspace,
                primary_turn_id,
                next_seq,
            )| {
                Some(ChannelState {
                    channel_id,
                    status: ChannelRecoveryStatus::parse(&status)?,
                    owner_agent_id,
                    fallback_agent_id,
                    active_writer_agent_id,
                    workspace,
                    primary_turn_id,
                    next_seq,
                })
            },
        )
        .collect())
}

pub async fn load_checkpoint_events(
    pool: &sqlx::PgPool,
    channel_id: &str,
    limit: i64,
) -> Result<Vec<CheckpointEvent>, sqlx::Error> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            i64,
            DateTime<Utc>,
            String,
            String,
            Value,
            i32,
        ),
    >(
        "SELECT id, channel_id, seq, at, writer_agent_id, kind, payload, payload_bytes
           FROM agent_recovery_checkpoint_events
          WHERE channel_id = $1
          ORDER BY seq DESC
          LIMIT $2",
    )
    .bind(channel_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut events = Vec::new();
    for (id, channel_id, seq, at, writer_agent_id, kind, payload, payload_bytes) in rows {
        let Some(kind) = CheckpointEventKind::parse(&kind) else {
            continue;
        };
        let Ok(payload) = payload_from_json(&payload) else {
            continue;
        };
        events.push(CheckpointEvent {
            id,
            channel_id,
            seq,
            at,
            writer_agent_id,
            kind,
            payload,
            payload_bytes: payload_bytes.max(0) as usize,
        });
    }
    events.reverse();
    Ok(events)
}
