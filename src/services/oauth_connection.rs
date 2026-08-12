use base64::{Engine as _, engine::general_purpose::STANDARD};
use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, Payload},
};
use chrono::{DateTime, Duration, Utc};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

pub const KAKAO_PROVIDER: &str = "kakao";
pub const PRIMARY_ACCOUNT_KEY: &str = "primary";
pub const TOKEN_KEY_ENV: &str = "AGENTDESK_OAUTH_TOKEN_KEY_V1";
const KEY_VERSION: i16 = 1;
const SESSION_TTL_MINUTES: i64 = 10;
const REFRESH_LEASE_SECONDS: i64 = 30;

#[derive(Debug, Error)]
pub enum OAuthConnectionError {
    #[error("OAuth token encryption key is not configured")]
    MissingTokenKey,
    #[error("OAuth token encryption key must be standard base64 for exactly 32 bytes")]
    InvalidTokenKey,
    #[error("OAuth token could not be encrypted")]
    Encrypt,
    #[error("OAuth token could not be decrypted")]
    Decrypt,
    #[error("OAuth connection storage is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("OAuth token payload is invalid")]
    InvalidTokenPayload,
    #[error("OAuth token refresh is already in progress")]
    RefreshInProgress,
}

impl From<sqlx::Error> for OAuthConnectionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

pub struct TokenVault {
    key: Zeroizing<[u8; 32]>,
}

impl TokenVault {
    pub fn from_env() -> Result<Self, OAuthConnectionError> {
        let encoded = Zeroizing::new(
            std::env::var(TOKEN_KEY_ENV)
                .ok()
                .filter(|value| !value.is_empty())
                .ok_or(OAuthConnectionError::MissingTokenKey)?,
        );
        crate::utils::redact::register_known_secret(&encoded);
        let decoded = Zeroizing::new(
            STANDARD
                .decode(encoded.as_bytes())
                .map_err(|_| OAuthConnectionError::InvalidTokenKey)?,
        );
        let key: [u8; 32] = decoded
            .as_slice()
            .try_into()
            .map_err(|_| OAuthConnectionError::InvalidTokenKey)?;
        Ok(Self {
            key: Zeroizing::new(key),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(key: [u8; 32]) -> Self {
        Self {
            key: Zeroizing::new(key),
        }
    }

    pub fn seal(
        &self,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<EncryptedValue, OAuthConnectionError> {
        let cipher = XChaCha20Poly1305::new_from_slice(self.key.as_ref())
            .map_err(|_| OAuthConnectionError::InvalidTokenKey)?;
        let mut nonce = [0_u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| OAuthConnectionError::Encrypt)?;
        Ok(EncryptedValue {
            ciphertext,
            nonce: nonce.to_vec(),
            key_version: KEY_VERSION,
        })
    }

    pub fn open(
        &self,
        encrypted: &EncryptedValue,
        aad: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, OAuthConnectionError> {
        if encrypted.key_version != KEY_VERSION || encrypted.nonce.len() != 24 {
            return Err(OAuthConnectionError::Decrypt);
        }
        let cipher = XChaCha20Poly1305::new_from_slice(self.key.as_ref())
            .map_err(|_| OAuthConnectionError::InvalidTokenKey)?;
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&encrypted.nonce),
                Payload {
                    msg: &encrypted.ciphertext,
                    aad,
                },
            )
            .map_err(|_| OAuthConnectionError::Decrypt)?;
        Ok(Zeroizing::new(plaintext))
    }
}

#[derive(Debug, Clone)]
pub struct EncryptedValue {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub key_version: i16,
}

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct StoredOAuthTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

impl StoredOAuthTokens {
    fn encrypt(
        &self,
        vault: &TokenVault,
        account_key: &str,
    ) -> Result<EncryptedValue, OAuthConnectionError> {
        let mut serialized = Zeroizing::new(
            serde_json::to_vec(self).map_err(|_| OAuthConnectionError::InvalidTokenPayload)?,
        );
        let encrypted = vault.seal(&serialized, account_aad(account_key).as_bytes());
        serialized.zeroize();
        encrypted
    }

    fn decrypt(
        encrypted: &EncryptedValue,
        vault: &TokenVault,
        account_key: &str,
    ) -> Result<Self, OAuthConnectionError> {
        let plaintext = vault.open(encrypted, account_aad(account_key).as_bytes())?;
        serde_json::from_slice(&plaintext).map_err(|_| OAuthConnectionError::InvalidTokenPayload)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OAuthAccountRecord {
    pub account_key: String,
    pub subject_hash: Option<Vec<u8>>,
    pub token_ciphertext: Vec<u8>,
    pub token_nonce: Vec<u8>,
    pub key_version: i16,
    pub scopes: Vec<String>,
    pub access_expires_at: Option<DateTime<Utc>>,
    pub refresh_expires_at: Option<DateTime<Utc>>,
    pub status: String,
}

impl OAuthAccountRecord {
    pub fn decrypt_tokens(
        &self,
        vault: &TokenVault,
    ) -> Result<StoredOAuthTokens, OAuthConnectionError> {
        StoredOAuthTokens::decrypt(
            &EncryptedValue {
                ciphertext: self.token_ciphertext.clone(),
                nonce: self.token_nonce.clone(),
                key_version: self.key_version,
            },
            vault,
            &self.account_key,
        )
    }
}

pub fn sha256_bytes(value: &[u8]) -> Vec<u8> {
    Sha256::digest(value).to_vec()
}

pub async fn create_session(pool: &PgPool) -> Result<String, OAuthConnectionError> {
    let mut raw = [0_u8; 32];
    OsRng.fill_bytes(&mut raw);
    let state = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    let state_hash = sha256_bytes(state.as_bytes());
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        DELETE FROM oauth_connection_sessions
        WHERE provider = $1 AND (expires_at <= NOW() OR consumed_at IS NOT NULL)
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO oauth_connection_sessions (id, provider, state_hash, expires_at)
        VALUES ($1, $2, $3, NOW() + make_interval(mins => $4))
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(KAKAO_PROVIDER)
    .bind(state_hash)
    .bind(SESSION_TTL_MINUTES as i32)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(state)
}

pub async fn consume_session(pool: &PgPool, state: &str) -> Result<bool, OAuthConnectionError> {
    if state.len() > 256 {
        return Ok(false);
    }
    let state_hash = sha256_bytes(state.as_bytes());
    let consumed = sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE oauth_connection_sessions
        SET consumed_at = NOW()
        WHERE provider = $1
          AND state_hash = $2
          AND consumed_at IS NULL
          AND expires_at > NOW()
        RETURNING id
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(state_hash)
    .fetch_optional(pool)
    .await?;
    Ok(consumed.is_some())
}

pub async fn load_account(
    pool: &PgPool,
    account_key: &str,
) -> Result<Option<OAuthAccountRecord>, OAuthConnectionError> {
    sqlx::query_as::<_, OAuthAccountRecord>(
        r#"
        SELECT
            account_key,
            subject_hash,
            token_ciphertext,
            token_nonce,
            key_version,
            scopes,
            access_expires_at,
            refresh_expires_at,
            status
        FROM oauth_connection_accounts
        WHERE provider = $1 AND account_key = $2
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .fetch_optional(pool)
    .await
    .map_err(Into::into)
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct OAuthAccountSummary {
    pub account_id: String,
    pub status: String,
    pub scopes: Vec<String>,
    pub access_expires_at: Option<DateTime<Utc>>,
    pub is_legacy: bool,
}

pub async fn list_accounts(
    pool: &PgPool,
) -> Result<Vec<OAuthAccountSummary>, OAuthConnectionError> {
    sqlx::query_as::<_, OAuthAccountSummary>(
        r#"
        SELECT account_key AS account_id, status, scopes, access_expires_at,
               account_key = $2 AS is_legacy
        FROM oauth_connection_accounts
        WHERE provider = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(PRIMARY_ACCOUNT_KEY)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub async fn find_account_by_subject_hash(
    pool: &PgPool,
    subject_hash: &[u8],
) -> Result<Option<OAuthAccountRecord>, OAuthConnectionError> {
    sqlx::query_as::<_, OAuthAccountRecord>(
        r#"
        SELECT account_key, subject_hash, token_ciphertext, token_nonce, key_version,
               scopes, access_expires_at, refresh_expires_at, status
        FROM oauth_connection_accounts
        WHERE provider = $1 AND subject_hash = $2
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(subject_hash)
    .fetch_optional(pool)
    .await
    .map_err(Into::into)
}

pub struct AccountTokenUpdate<'a> {
    pub tokens: &'a StoredOAuthTokens,
    pub scopes: &'a [String],
    pub access_expires_at: Option<DateTime<Utc>>,
    pub refresh_expires_at: Option<DateTime<Utc>>,
}

pub async fn upsert_account(
    pool: &PgPool,
    vault: &TokenVault,
    account_key: &str,
    subject_hash: Option<&[u8]>,
    update: AccountTokenUpdate<'_>,
) -> Result<(), OAuthConnectionError> {
    let encrypted = update.tokens.encrypt(vault, account_key)?;
    sqlx::query(
        r#"
        INSERT INTO oauth_connection_accounts (
            provider,
            account_key,
            subject_hash,
            token_ciphertext,
            token_nonce,
            key_version,
            scopes,
            access_expires_at,
            refresh_expires_at,
            status,
            updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'active', NOW())
        ON CONFLICT (provider, account_key) DO UPDATE SET
            token_ciphertext = EXCLUDED.token_ciphertext,
            subject_hash = COALESCE(EXCLUDED.subject_hash, oauth_connection_accounts.subject_hash),
            token_nonce = EXCLUDED.token_nonce,
            key_version = EXCLUDED.key_version,
            scopes = EXCLUDED.scopes,
            access_expires_at = EXCLUDED.access_expires_at,
            refresh_expires_at = EXCLUDED.refresh_expires_at,
            status = 'active',
            refresh_lease_id = NULL,
            refresh_lease_expires_at = NULL,
            updated_at = NOW()
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .bind(subject_hash)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce)
    .bind(encrypted.key_version)
    .bind(update.scopes)
    .bind(update.access_expires_at)
    .bind(update.refresh_expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_account(
    pool: &PgPool,
    account_key: &str,
) -> Result<bool, OAuthConnectionError> {
    let result = sqlx::query(
        "DELETE FROM oauth_connection_accounts WHERE provider = $1 AND account_key = $2",
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn account_has_active_delivery_references(
    pool: &PgPool,
    account_key: &str,
) -> Result<bool, OAuthConnectionError> {
    let referenced = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM scheduled_messages
            WHERE external_delivery_account_key = $1
              AND status IN ('scheduled', 'firing')
            UNION ALL
            SELECT 1
            FROM external_share_outbox
            WHERE provider = $2
              AND account_key = $1
              AND status IN ('pending', 'processing')
            UNION ALL
            SELECT 1
            FROM external_share_operations
            WHERE provider = $2
              AND account_key = $1
              AND state = 'dispatching'
        )
        "#,
    )
    .bind(account_key)
    .bind(KAKAO_PROVIDER)
    .fetch_one(pool)
    .await?;
    Ok(referenced)
}

pub async fn set_account_status(
    pool: &PgPool,
    account_key: &str,
    status: &str,
) -> Result<(), OAuthConnectionError> {
    sqlx::query(
        r#"
        UPDATE oauth_connection_accounts
        SET status = $3, updated_at = NOW()
        WHERE provider = $1 AND account_key = $2
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn acquire_refresh_lease(
    pool: &PgPool,
    account_key: &str,
) -> Result<Uuid, OAuthConnectionError> {
    let lease_id = Uuid::new_v4();
    // Seal the account before the provider call. If the process or database
    // fails after Kakao may have rotated the refresh token, the old token can
    // never become automatically eligible again when the lease expires.
    let acquired = sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE oauth_connection_accounts
        SET
            status = 'reauth_required',
            refresh_lease_id = $3,
            refresh_lease_expires_at = NOW() + make_interval(secs => $4),
            updated_at = NOW()
        WHERE provider = $1
          AND account_key = $2
          AND status = 'active'
          AND (refresh_lease_id IS NULL OR refresh_lease_expires_at <= NOW())
        RETURNING refresh_lease_id
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .bind(lease_id)
    .bind(REFRESH_LEASE_SECONDS as f64)
    .fetch_optional(pool)
    .await?;
    acquired.ok_or(OAuthConnectionError::RefreshInProgress)
}

pub async fn complete_refresh(
    pool: &PgPool,
    vault: &TokenVault,
    account_key: &str,
    lease_id: Uuid,
    update: AccountTokenUpdate<'_>,
) -> Result<bool, OAuthConnectionError> {
    let encrypted = update.tokens.encrypt(vault, account_key)?;
    let result = sqlx::query(
        r#"
        UPDATE oauth_connection_accounts
        SET
            token_ciphertext = $4,
            token_nonce = $5,
            key_version = $6,
            scopes = $7,
            access_expires_at = $8,
            refresh_expires_at = $9,
            status = 'active',
            refresh_lease_id = NULL,
            refresh_lease_expires_at = NULL,
            updated_at = NOW()
        WHERE provider = $1 AND account_key = $2 AND refresh_lease_id = $3
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .bind(lease_id)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce)
    .bind(encrypted.key_version)
    .bind(update.scopes)
    .bind(update.access_expires_at)
    .bind(update.refresh_expires_at)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn fail_refresh(
    pool: &PgPool,
    account_key: &str,
    lease_id: Uuid,
) -> Result<(), OAuthConnectionError> {
    sqlx::query(
        r#"
        UPDATE oauth_connection_accounts
        SET
            status = 'reauth_required',
            refresh_lease_id = NULL,
            refresh_lease_expires_at = NULL,
            updated_at = NOW()
        WHERE provider = $1 AND account_key = $2 AND refresh_lease_id = $3
        "#,
    )
    .bind(KAKAO_PROVIDER)
    .bind(account_key)
    .bind(lease_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub fn token_expiry_from_now(seconds: Option<std::time::Duration>) -> Option<DateTime<Utc>> {
    seconds
        .and_then(|value| i64::try_from(value.as_secs()).ok())
        .and_then(|seconds| Utc::now().checked_add_signed(Duration::seconds(seconds)))
}

pub fn account_aad(account_key: &str) -> String {
    format!("agentdesk/oauth-account/v1/{KAKAO_PROVIDER}/{account_key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_vault() -> TokenVault {
        TokenVault {
            key: Zeroizing::new([7_u8; 32]),
        }
    }

    #[test]
    fn token_vault_round_trips_and_binds_aad() {
        let vault = test_vault();
        let encrypted = vault.seal(b"secret", b"account-a").unwrap();
        assert_eq!(
            vault.open(&encrypted, b"account-a").unwrap().as_slice(),
            b"secret"
        );
        assert!(vault.open(&encrypted, b"account-b").is_err());
        assert_ne!(encrypted.ciphertext, b"secret");
    }

    #[test]
    fn token_payload_round_trips_without_plaintext_in_ciphertext() {
        let vault = test_vault();
        let tokens = StoredOAuthTokens {
            access_token: "access-secret".to_string(),
            refresh_token: Some("refresh-secret".to_string()),
        };
        let encrypted = tokens.encrypt(&vault, "account-a").unwrap();
        assert!(!String::from_utf8_lossy(&encrypted.ciphertext).contains("access-secret"));
        let opened = StoredOAuthTokens::decrypt(&encrypted, &vault, "account-a").unwrap();
        assert_eq!(opened.access_token, "access-secret");
        assert_eq!(opened.refresh_token.as_deref(), Some("refresh-secret"));
    }

    #[test]
    fn sha256_helper_is_stable_and_fixed_width() {
        let first = sha256_bytes(b"same");
        assert_eq!(first, sha256_bytes(b"same"));
        assert_ne!(first, sha256_bytes(b"different"));
        assert_eq!(first.len(), 32);
    }
}
