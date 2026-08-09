//! Durable external-provider delivery queue.
//!
//! The scheduled-message fire transaction enqueues encrypted payload snapshots
//! here beside the Discord outbox row. This worker mirrors the Discord outbox's
//! lease/CAS ownership model, while provider-specific dispatch remains behind
//! the Kakao service and its non-reclaiming POST fence.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use serde_json::{Value as JsonValue, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::config::KakaoFriendShareConfig;
use crate::services::external_share::{
    ExternalShareError, SafeShareSummary, ShareOperationResult, ShareOperationState,
};
use crate::services::kakao::{KakaoError, KakaoFriendShareCommand, KakaoFriendShareService};
use crate::services::oauth_connection::{EncryptedValue, OAuthConnectionError, TokenVault};
use crate::services::scheduled_messages::external_delivery::outbox_aad;

const CLAIM_LEASE_SECONDS: i64 = 120;
const MAX_RETRIES: i16 = 5;
const CLAIM_BATCH_IDLE_MILLIS: u64 = 500;
const CLAIM_MAX_IDLE_SECONDS: u64 = 5;

#[derive(Debug, Clone)]
pub struct NewExternalShareOutbox {
    pub id: Uuid,
    pub provider: String,
    pub channel_id: String,
    pub account_key: String,
    pub source: String,
    pub source_key: String,
    pub scheduled_delivery_id: String,
    pub requested_count: i16,
    pub encrypted_payload: EncryptedValue,
    pub deliver_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ClaimedExternalShare {
    id: Uuid,
    provider: String,
    channel_id: String,
    account_key: String,
    requested_count: i16,
    payload_ciphertext: Vec<u8>,
    payload_nonce: Vec<u8>,
    payload_key_version: i16,
    claim_token: Uuid,
    retry_count: i16,
    deliver_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ExternalShareDeliveryRow {
    id: Uuid,
    scheduled_delivery_id: String,
    provider: String,
    channel_id: String,
    status: String,
    requested_count: i16,
    safe_summary: Option<JsonValue>,
    error_code: Option<String>,
    operation_id: Option<Uuid>,
    retry_count: i16,
    created_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

impl ExternalShareDeliveryRow {
    fn to_api_json(&self) -> JsonValue {
        let summary = self
            .safe_summary
            .clone()
            .and_then(|value| serde_json::from_value::<SafeShareSummary>(value).ok());
        json!({
            "id": self.id,
            "provider": self.provider,
            "channelId": self.channel_id,
            "status": self.status,
            "requestedCount": summary
                .as_ref()
                .map(|value| value.requested_count)
                .unwrap_or(self.requested_count as usize),
            "successfulCount": summary.as_ref().map(|value| value.successful_count),
            "failedCount": summary.as_ref().map(|value| value.failed_count),
            "errorCode": self.error_code,
            "operationId": self.operation_id,
            "retryCount": self.retry_count,
            "createdAt": self.created_at.to_rfc3339(),
            "finishedAt": self.finished_at.map(|value| value.to_rfc3339()),
        })
    }
}

pub(crate) async fn enqueue_external_share_outbox_tx(
    tx: &mut Transaction<'_, Postgres>,
    new: &NewExternalShareOutbox,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        INSERT INTO external_share_outbox (
            id, provider, channel_id, account_key, source, source_key,
            scheduled_delivery_id, requested_count, payload_ciphertext,
            payload_nonce, payload_key_version, deliver_before
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (provider, channel_id, account_key, source, source_key)
        DO UPDATE SET updated_at = external_share_outbox.updated_at
        RETURNING id
        "#,
    )
    .bind(new.id)
    .bind(&new.provider)
    .bind(&new.channel_id)
    .bind(&new.account_key)
    .bind(&new.source)
    .bind(&new.source_key)
    .bind(&new.scheduled_delivery_id)
    .bind(new.requested_count)
    .bind(&new.encrypted_payload.ciphertext)
    .bind(&new.encrypted_payload.nonce)
    .bind(new.encrypted_payload.key_version)
    .bind(new.deliver_before)
    .fetch_one(&mut **tx)
    .await
}

pub async fn provider_deliveries_for_scheduled_deliveries_pg(
    pool: &PgPool,
    delivery_ids: &[String],
) -> Result<BTreeMap<String, Vec<JsonValue>>, sqlx::Error> {
    if delivery_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query_as::<_, ExternalShareDeliveryRow>(
        r#"
        SELECT id, scheduled_delivery_id, provider, channel_id, status,
               requested_count, safe_summary, error_code, operation_id, retry_count,
               created_at, finished_at
        FROM external_share_outbox
        WHERE scheduled_delivery_id = ANY($1)
        ORDER BY created_at, id
        "#,
    )
    .bind(delivery_ids)
    .fetch_all(pool)
    .await?;
    let mut by_delivery = BTreeMap::<String, Vec<JsonValue>>::new();
    for row in rows {
        by_delivery
            .entry(row.scheduled_delivery_id.clone())
            .or_default()
            .push(row.to_api_json());
    }
    Ok(by_delivery)
}

pub async fn external_share_outbox_loop(pool: Arc<PgPool>, kakao_config: KakaoFriendShareConfig) {
    tokio::time::sleep(StdDuration::from_secs(3)).await;
    let claim_owner = format!(
        "external-share-outbox:{}:{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string()),
        std::process::id(),
        Uuid::new_v4()
    );
    tracing::info!("external share outbox worker started");
    let mut poll_interval = StdDuration::from_millis(CLAIM_BATCH_IDLE_MILLIS);
    let max_interval = StdDuration::from_secs(CLAIM_MAX_IDLE_SECONDS);
    loop {
        tokio::time::sleep(poll_interval).await;
        match claim_next_pg(&pool, &claim_owner, Utc::now()).await {
            Ok(Some(claim)) => {
                poll_interval = StdDuration::from_millis(CLAIM_BATCH_IDLE_MILLIS);
                process_claim(&pool, &kakao_config, claim).await;
            }
            Ok(None) => {
                poll_interval = (poll_interval.mul_f64(1.5)).min(max_interval);
            }
            Err(error) => {
                tracing::warn!("external share outbox claim failed: {error}");
                poll_interval = (poll_interval.mul_f64(1.5)).min(max_interval);
            }
        }
    }
}

async fn claim_next_pg(
    pool: &PgPool,
    claim_owner: &str,
    now: DateTime<Utc>,
) -> Result<Option<ClaimedExternalShare>, sqlx::Error> {
    terminalize_expired_pg(pool, now).await?;
    let claim_token = Uuid::new_v4();
    sqlx::query_as::<_, ClaimedExternalShare>(
        r#"
        WITH candidate AS (
            SELECT id
            FROM external_share_outbox
            WHERE (
                (
                    status = 'pending'
                    AND next_attempt_at <= $1
                ) OR (
                    status = 'processing'
                    AND claimed_at <= $1 - make_interval(secs => $2)
                )
              )
              AND (deliver_before IS NULL OR deliver_before > $1)
            ORDER BY next_attempt_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE external_share_outbox AS outbox
        SET status = 'processing', claim_owner = $3, claim_token = $4,
            claimed_at = $1, updated_at = $1
        FROM candidate
        WHERE outbox.id = candidate.id
        RETURNING outbox.id, outbox.provider, outbox.channel_id,
                  outbox.account_key, outbox.requested_count,
                  outbox.payload_ciphertext, outbox.payload_nonce,
                  outbox.payload_key_version, outbox.claim_token,
                  outbox.retry_count, outbox.deliver_before
        "#,
    )
    .bind(now)
    .bind(CLAIM_LEASE_SECONDS as i32)
    .bind(claim_owner)
    .bind(claim_token)
    .fetch_optional(pool)
    .await
}

async fn terminalize_expired_pg(pool: &PgPool, now: DateTime<Utc>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE external_share_outbox
        SET status = 'failed', claim_owner = NULL, claim_token = NULL,
            claimed_at = NULL, payload_ciphertext = NULL, payload_nonce = NULL,
            payload_key_version = NULL,
            safe_summary = jsonb_build_object(
                'requested_count', requested_count,
                'successful_count', 0,
                'failed_count', requested_count
            ),
            error_code = 'delivery_expired', finished_at = $1, updated_at = $1
        WHERE status = 'pending'
          AND deliver_before IS NOT NULL
          AND deliver_before <= $1
        "#,
    )
    .bind(now)
    .execute(pool)
    .await?;
    // A processing row may already have crossed the provider POST boundary.
    // Keep an active lease alive so it can persist a known result. Once that
    // lease is stale, fail closed as unknown and never dispatch after expiry.
    sqlx::query(
        r#"
        UPDATE external_share_outbox
        SET status = 'unknown', claim_owner = NULL, claim_token = NULL,
            claimed_at = NULL, payload_ciphertext = NULL, payload_nonce = NULL,
            payload_key_version = NULL,
            safe_summary = jsonb_build_object(
                'requested_count', requested_count,
                'successful_count', 0,
                'failed_count', 0
            ),
            error_code = 'delivery_expired_after_claim',
            finished_at = $1, updated_at = $1
        WHERE status = 'processing'
          AND deliver_before IS NOT NULL
          AND deliver_before <= $1
          AND claimed_at <= $1 - make_interval(secs => $2)
        "#,
    )
    .bind(now)
    .bind(CLAIM_LEASE_SECONDS as i32)
    .execute(pool)
    .await?;
    Ok(())
}

async fn process_claim(
    pool: &PgPool,
    kakao_config: &KakaoFriendShareConfig,
    claim: ClaimedExternalShare,
) {
    if claim.provider != crate::services::oauth_connection::KAKAO_PROVIDER
        || claim.channel_id
            != crate::services::scheduled_messages::external_delivery::KAKAO_CHANNEL_ID
        || claim.account_key != crate::services::oauth_connection::PRIMARY_ACCOUNT_KEY
    {
        finish_pre_dispatch_failure(pool, &claim, "unsupported_provider").await;
        return;
    }

    let command = decrypt_claim_command(&claim);
    let command = match command {
        Ok(command) => command,
        Err((code, retryable)) => {
            handle_retryable_failure(pool, &claim, code, retryable).await;
            return;
        }
    };
    let service = match KakaoFriendShareService::new(Some(pool), kakao_config) {
        Ok(service) => service,
        Err(error) => {
            handle_kakao_error(pool, &claim, &error).await;
            return;
        }
    };
    let idempotency_key = format!("scheduled-kakao:{}", claim.id);
    match service.send_friend_message(&idempotency_key, command).await {
        Ok(result) => finish_result_pg(pool, &claim, result).await,
        Err(error) => handle_kakao_error(pool, &claim, &error).await,
    }
}

fn decrypt_claim_command(
    claim: &ClaimedExternalShare,
) -> Result<KakaoFriendShareCommand, (&'static str, bool)> {
    let vault = TokenVault::from_env().map_err(|error| match error {
        OAuthConnectionError::MissingTokenKey | OAuthConnectionError::InvalidTokenKey => {
            ("token_key_unavailable", true)
        }
        _ => ("token_key_unavailable", false),
    })?;
    let plaintext = vault
        .open(
            &EncryptedValue {
                ciphertext: claim.payload_ciphertext.clone(),
                nonce: claim.payload_nonce.clone(),
                key_version: claim.payload_key_version,
            },
            outbox_aad(claim.id).as_bytes(),
        )
        .map_err(|_| ("payload_decrypt_failed", false))?;
    serde_json::from_slice(&plaintext).map_err(|_| ("payload_invalid", false))
}

async fn finish_result_pg(
    pool: &PgPool,
    claim: &ClaimedExternalShare,
    result: ShareOperationResult,
) {
    let status = result.status.as_str();
    if result.status == ShareOperationState::Dispatching {
        finish_pre_dispatch_failure(pool, claim, "invalid_provider_result").await;
        return;
    }
    let summary = json!(result.summary);
    if let Err(error) = finish_terminal_pg(
        pool,
        claim,
        status,
        Some(result.operation_id),
        summary,
        None,
    )
    .await
    {
        // Reclaim is safe: the next worker uses the same idempotency key and
        // Kakao's external_share_operations fence replays this exact outcome.
        tracing::warn!(outbox_id = %claim.id, "external share result persistence failed: {error}");
    }
}

async fn handle_kakao_error(pool: &PgPool, claim: &ClaimedExternalShare, error: &KakaoError) {
    let (code, retryable, delivery_may_have_occurred) = classify_kakao_error(error);
    if delivery_may_have_occurred {
        let summary = json!(SafeShareSummary::unknown(claim.requested_count as usize));
        if let Err(db_error) =
            finish_terminal_pg(pool, claim, "unknown", None, summary, Some(code)).await
        {
            tracing::warn!(outbox_id = %claim.id, "external share unknown persistence failed: {db_error}");
        }
    } else {
        handle_retryable_failure(pool, claim, code, retryable).await;
    }
}

fn classify_kakao_error(error: &KakaoError) -> (&'static str, bool, bool) {
    match error {
        KakaoError::AmbiguousProviderResult => ("provider_result_ambiguous", false, true),
        KakaoError::Validation(_) => ("payload_invalid", false, false),
        KakaoError::ExternalShare(ExternalShareError::IdempotencyConflict) => {
            ("idempotency_conflict", false, false)
        }
        KakaoError::ExternalShare(ExternalShareError::CorruptOperation) => {
            ("operation_corrupt", false, false)
        }
        KakaoError::ExternalShare(ExternalShareError::RateLimited) => ("rate_limited", true, false),
        KakaoError::ExternalShare(ExternalShareError::Database(_)) => {
            ("operation_storage_unavailable", true, false)
        }
        KakaoError::Connection(OAuthConnectionError::Decrypt)
        | KakaoError::Connection(OAuthConnectionError::InvalidTokenPayload) => {
            ("oauth_token_invalid", false, false)
        }
        KakaoError::Connection(OAuthConnectionError::Database(_)) => {
            ("oauth_storage_unavailable", true, false)
        }
        KakaoError::Connection(OAuthConnectionError::RefreshInProgress) => {
            ("oauth_refresh_in_progress", true, false)
        }
        KakaoError::Connection(OAuthConnectionError::MissingTokenKey)
        | KakaoError::Connection(OAuthConnectionError::InvalidTokenKey) => {
            ("token_key_unavailable", true, false)
        }
        KakaoError::Connection(OAuthConnectionError::Encrypt) => {
            ("oauth_token_encrypt_failed", true, false)
        }
        KakaoError::Disabled => ("connector_disabled", true, false),
        KakaoError::MissingConfig => ("connector_config_incomplete", true, false),
        KakaoError::MissingDatabase => ("storage_unavailable", true, false),
        KakaoError::ConsentIncomplete => ("consent_incomplete", true, false),
        KakaoError::ReauthorizationRequired => ("reauthorization_required", true, false),
        KakaoError::NotConnected => ("not_connected", true, false),
        KakaoError::OAuthExchange => ("oauth_exchange_failed", true, false),
        KakaoError::Provider => ("provider_unavailable", true, false),
        KakaoError::OperationInProgress => ("operation_in_progress", true, false),
        KakaoError::InvalidOAuthRequest | KakaoError::InvalidOAuthState => {
            ("oauth_request_invalid", false, false)
        }
    }
}

async fn handle_retryable_failure(
    pool: &PgPool,
    claim: &ClaimedExternalShare,
    error_code: &'static str,
    retryable: bool,
) {
    let next_retry_count = claim.retry_count.saturating_add(1);
    let delay = retry_delay(next_retry_count, error_code);
    let next_attempt_at = Utc::now() + delay;
    let inside_delivery_window = claim
        .deliver_before
        .is_none_or(|deliver_before| next_attempt_at < deliver_before);
    if retryable && next_retry_count <= MAX_RETRIES && inside_delivery_window {
        match sqlx::query(
            r#"
            UPDATE external_share_outbox
            SET status = 'pending', claim_owner = NULL, claim_token = NULL,
                claimed_at = NULL, retry_count = $3, next_attempt_at = $4,
                error_code = $5, updated_at = NOW()
            WHERE id = $1 AND claim_token = $2 AND status = 'processing'
            "#,
        )
        .bind(claim.id)
        .bind(claim.claim_token)
        .bind(next_retry_count)
        .bind(next_attempt_at)
        .bind(error_code)
        .execute(pool)
        .await
        {
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(outbox_id = %claim.id, "external share retry persistence failed: {error}")
            }
        }
        return;
    }
    finish_pre_dispatch_failure(pool, claim, error_code).await;
}

fn retry_delay(retry_count: i16, error_code: &str) -> Duration {
    if error_code == "rate_limited" {
        return Duration::hours(1);
    }
    match retry_count {
        0 | 1 => Duration::seconds(15),
        2 => Duration::minutes(1),
        3 => Duration::minutes(5),
        4 => Duration::minutes(15),
        _ => Duration::hours(1),
    }
}

async fn finish_pre_dispatch_failure(
    pool: &PgPool,
    claim: &ClaimedExternalShare,
    error_code: &'static str,
) {
    let summary = json!({
        "requested_count": claim.requested_count,
        "successful_count": 0,
        "failed_count": claim.requested_count,
    });
    if let Err(error) =
        finish_terminal_pg(pool, claim, "failed", None, summary, Some(error_code)).await
    {
        tracing::warn!(outbox_id = %claim.id, "external share failure persistence failed: {error}");
    }
}

async fn finish_terminal_pg(
    pool: &PgPool,
    claim: &ClaimedExternalShare,
    status: &str,
    operation_id: Option<Uuid>,
    safe_summary: JsonValue,
    error_code: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(
        r#"
        UPDATE external_share_outbox
        SET status = $3, operation_id = COALESCE($4, operation_id),
            safe_summary = $5, error_code = $6,
            payload_ciphertext = NULL, payload_nonce = NULL,
            payload_key_version = NULL, claim_owner = NULL, claim_token = NULL,
            claimed_at = NULL, finished_at = NOW(), updated_at = NOW()
        WHERE id = $1 AND claim_token = $2 AND status = 'processing'
        "#,
    )
    .bind(claim.id)
    .bind(claim.claim_token)
    .bind(status)
    .bind(operation_id)
    .bind(safe_summary)
    .bind(error_code)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() == 1)
}

#[cfg(test)]
mod postgres_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_delivery_api_exposes_only_the_safe_requested_count() {
        let row = ExternalShareDeliveryRow {
            id: Uuid::new_v4(),
            scheduled_delivery_id: "smdel-safe-summary".to_string(),
            provider: "kakao".to_string(),
            channel_id: "kakao_friend_share".to_string(),
            status: "pending".to_string(),
            requested_count: 3,
            safe_summary: None,
            error_code: None,
            operation_id: None,
            retry_count: 0,
            created_at: Utc::now(),
            finished_at: None,
        };

        let api = row.to_api_json();
        assert_eq!(api["requestedCount"], 3);
        assert_eq!(api["successfulCount"], JsonValue::Null);
        assert_eq!(api["failedCount"], JsonValue::Null);
        assert!(api.get("receiverUuids").is_none());
    }

    #[test]
    fn retry_backoff_is_bounded_and_rate_limit_uses_hour_window() {
        assert_eq!(retry_delay(1, "not_connected"), Duration::seconds(15));
        assert_eq!(retry_delay(3, "not_connected"), Duration::minutes(5));
        assert_eq!(retry_delay(5, "not_connected"), Duration::hours(1));
        assert_eq!(retry_delay(1, "rate_limited"), Duration::hours(1));
    }

    #[test]
    fn ambiguous_provider_results_are_never_automatically_retried() {
        assert_eq!(
            classify_kakao_error(&KakaoError::AmbiguousProviderResult),
            ("provider_result_ambiguous", false, true)
        );
    }
}
