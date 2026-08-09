use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

const DISPATCH_DEADLINE_SECONDS: f64 = 20.0;

#[derive(Debug, Clone, Copy)]
pub struct ExternalShareScope<'a> {
    pub provider: &'a str,
    pub channel_id: &'a str,
    pub account_key: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareOperationState {
    Dispatching,
    Success,
    PartialSuccess,
    Failed,
    Unknown,
}

impl ShareOperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dispatching => "dispatching",
            Self::Success => "success",
            Self::PartialSuccess => "partial_success",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }

    fn parse(raw: &str) -> Result<Self, ExternalShareError> {
        match raw {
            "dispatching" => Ok(Self::Dispatching),
            "success" => Ok(Self::Success),
            "partial_success" => Ok(Self::PartialSuccess),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            _ => Err(ExternalShareError::CorruptOperation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SafeShareSummary {
    pub requested_count: usize,
    pub successful_count: usize,
    pub failed_count: usize,
}

impl SafeShareSummary {
    pub fn unknown(requested_count: usize) -> Self {
        Self {
            requested_count,
            successful_count: 0,
            failed_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ShareOperationResult {
    #[serde(rename = "request_id")]
    pub operation_id: Uuid,
    pub status: ShareOperationState,
    #[serde(flatten)]
    pub summary: SafeShareSummary,
    pub replayed: bool,
    pub delivery_may_have_occurred: bool,
    pub automatic_retry_allowed: bool,
}

#[derive(Debug)]
pub enum BeginShareOperation {
    Dispatch { operation_id: Uuid },
    Replay(ShareOperationResult),
    InProgress,
}

#[derive(Debug, Error)]
pub enum ExternalShareError {
    #[error("external share storage is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("Idempotency-Key was already used for a different request")]
    IdempotencyConflict,
    #[error("external share rate limit reached")]
    RateLimited,
    #[error("stored external share operation is invalid")]
    CorruptOperation,
}

impl From<sqlx::Error> for ExternalShareError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

struct ExistingOperation {
    operation_id: Uuid,
    request_fingerprint: Vec<u8>,
    state: ShareOperationState,
    safe_summary: Option<Value>,
    dispatch_deadline: DateTime<Utc>,
}

pub async fn begin_operation(
    pool: &PgPool,
    scope: ExternalShareScope<'_>,
    idempotency_key: &str,
    request_fingerprint: &[u8],
    requested_count: usize,
    hourly_limit: u32,
) -> Result<BeginShareOperation, ExternalShareError> {
    let idempotency_hash = Sha256::digest(idempotency_key.as_bytes()).to_vec();
    let mut tx = begin_serialized_transaction(pool, scope).await?;

    let existing = sqlx::query(
        r#"
        SELECT operation_id, request_fingerprint, state, safe_summary, dispatch_deadline
        FROM external_share_operations
        WHERE provider = $1
          AND channel_id = $2
          AND account_key = $3
          AND idempotency_key_hash = $4
        FOR UPDATE
        "#,
    )
    .bind(scope.provider)
    .bind(scope.channel_id)
    .bind(scope.account_key)
    .bind(&idempotency_hash)
    .fetch_optional(&mut *tx)
    .await?
    .map(|row: sqlx::postgres::PgRow| {
        Ok::<_, ExternalShareError>(ExistingOperation {
            operation_id: row.try_get("operation_id")?,
            request_fingerprint: row.try_get("request_fingerprint")?,
            state: ShareOperationState::parse(row.try_get::<String, _>("state")?.as_str())?,
            safe_summary: row.try_get("safe_summary")?,
            dispatch_deadline: row.try_get("dispatch_deadline")?,
        })
    })
    .transpose()?;

    if let Some(existing) = existing {
        if existing.request_fingerprint != request_fingerprint {
            return Err(ExternalShareError::IdempotencyConflict);
        }
        if existing.state == ShareOperationState::Dispatching {
            if existing.dispatch_deadline > Utc::now() {
                tx.commit().await?;
                return Ok(BeginShareOperation::InProgress);
            }
            let summary = SafeShareSummary::unknown(requested_count);
            sqlx::query(
                r#"
                UPDATE external_share_operations
                SET state = 'unknown', safe_summary = $2, updated_at = NOW()
                WHERE operation_id = $1 AND state = 'dispatching'
                "#,
            )
            .bind(existing.operation_id)
            .bind(json!(summary))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(BeginShareOperation::Replay(ShareOperationResult {
                operation_id: existing.operation_id,
                status: ShareOperationState::Unknown,
                summary,
                replayed: true,
                delivery_may_have_occurred: true,
                automatic_retry_allowed: false,
            }));
        }
        let result = stored_result(existing)?;
        tx.commit().await?;
        return Ok(BeginShareOperation::Replay(result));
    }

    let recent_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM external_share_operations
        WHERE provider = $1
          AND channel_id = $2
          AND account_key = $3
          AND created_at > NOW() - INTERVAL '1 hour'
        "#,
    )
    .bind(scope.provider)
    .bind(scope.channel_id)
    .bind(scope.account_key)
    .fetch_one(&mut *tx)
    .await?;
    if recent_count >= i64::from(hourly_limit) {
        return Err(ExternalShareError::RateLimited);
    }

    let operation_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO external_share_operations (
            operation_id,
            provider,
            channel_id,
            account_key,
            idempotency_key_hash,
            request_fingerprint,
            state,
            dispatch_deadline
        ) VALUES (
            $1, $2, $3, $4, $5, $6, 'dispatching',
            NOW() + make_interval(secs => $7)
        )
        "#,
    )
    .bind(operation_id)
    .bind(scope.provider)
    .bind(scope.channel_id)
    .bind(scope.account_key)
    .bind(idempotency_hash)
    .bind(request_fingerprint)
    .bind(DISPATCH_DEADLINE_SECONDS)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(BeginShareOperation::Dispatch { operation_id })
}

async fn begin_serialized_transaction<'a>(
    pool: &'a PgPool,
    scope: ExternalShareScope<'_>,
) -> Result<Transaction<'a, Postgres>, ExternalShareError> {
    let mut tx = pool.begin().await?;
    let lock_key = format!(
        "external-share:{}:{}:{}",
        scope.provider, scope.channel_id, scope.account_key
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::BIGINT)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

pub async fn finish_operation(
    pool: &PgPool,
    operation_id: Uuid,
    state: ShareOperationState,
    summary: SafeShareSummary,
) -> Result<ShareOperationResult, ExternalShareError> {
    if state == ShareOperationState::Dispatching {
        return Err(ExternalShareError::CorruptOperation);
    }
    let updated = sqlx::query(
        r#"
        UPDATE external_share_operations
        SET state = $2, safe_summary = $3, updated_at = NOW()
        WHERE operation_id = $1 AND state = 'dispatching'
        "#,
    )
    .bind(operation_id)
    .bind(state.as_str())
    .bind(json!(summary))
    .execute(pool)
    .await?;
    if updated.rows_affected() == 1 {
        return Ok(ShareOperationResult {
            operation_id,
            status: state,
            summary,
            replayed: false,
            delivery_may_have_occurred: state != ShareOperationState::Failed,
            automatic_retry_allowed: false,
        });
    }
    load_result(pool, operation_id).await
}

pub async fn mark_unknown(
    pool: &PgPool,
    operation_id: Uuid,
    requested_count: usize,
) -> ShareOperationResult {
    let summary = SafeShareSummary::unknown(requested_count);
    match finish_operation(
        pool,
        operation_id,
        ShareOperationState::Unknown,
        summary.clone(),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!(
                operation_id = %operation_id,
                "failed to persist terminal external share outcome; retaining unknown"
            );
            ShareOperationResult {
                operation_id,
                status: ShareOperationState::Unknown,
                summary,
                replayed: false,
                delivery_may_have_occurred: true,
                automatic_retry_allowed: false,
            }
        }
    }
}

async fn load_result(
    pool: &PgPool,
    operation_id: Uuid,
) -> Result<ShareOperationResult, ExternalShareError> {
    let row = sqlx::query(
        r#"
        SELECT operation_id, request_fingerprint, state, safe_summary, dispatch_deadline
        FROM external_share_operations
        WHERE operation_id = $1
        "#,
    )
    .bind(operation_id)
    .fetch_one(pool)
    .await?;
    stored_result(ExistingOperation {
        operation_id: row.try_get("operation_id")?,
        request_fingerprint: row.try_get("request_fingerprint")?,
        state: ShareOperationState::parse(row.try_get::<String, _>("state")?.as_str())?,
        safe_summary: row.try_get("safe_summary")?,
        dispatch_deadline: row.try_get("dispatch_deadline")?,
    })
}

fn stored_result(existing: ExistingOperation) -> Result<ShareOperationResult, ExternalShareError> {
    if existing.state == ShareOperationState::Dispatching {
        return Err(ExternalShareError::CorruptOperation);
    }
    let summary = existing
        .safe_summary
        .ok_or(ExternalShareError::CorruptOperation)
        .and_then(|value| {
            serde_json::from_value(value).map_err(|_| ExternalShareError::CorruptOperation)
        })?;
    Ok(ShareOperationResult {
        operation_id: existing.operation_id,
        status: existing.state,
        summary,
        replayed: true,
        delivery_may_have_occurred: existing.state != ShareOperationState::Failed,
        automatic_retry_allowed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_states_use_stable_wire_names() {
        assert_eq!(ShareOperationState::Success.as_str(), "success");
        assert_eq!(
            ShareOperationState::PartialSuccess.as_str(),
            "partial_success"
        );
        assert_eq!(ShareOperationState::Failed.as_str(), "failed");
        assert_eq!(ShareOperationState::Unknown.as_str(), "unknown");
    }

    #[test]
    fn safe_summary_contains_counts_only() {
        let encoded = serde_json::to_value(SafeShareSummary {
            requested_count: 3,
            successful_count: 2,
            failed_count: 1,
        })
        .unwrap();
        assert_eq!(
            encoded,
            json!({
                "requested_count": 3,
                "successful_count": 2,
                "failed_count": 1
            })
        );
    }
}
