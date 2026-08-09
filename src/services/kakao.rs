use std::collections::BTreeSet;
use std::sync::LazyLock;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use oauth2::basic::{
    BasicErrorResponse, BasicRevocationErrorResponse, BasicTokenIntrospectionResponse,
    BasicTokenType,
};
use oauth2::{
    AuthType, AuthUrl, AuthorizationCode, Client, ClientId, ClientSecret, CsrfToken,
    EndpointNotSet, EndpointSet, ExtraTokenFields, RedirectUrl, RefreshToken, Scope,
    StandardRevocableToken, StandardTokenResponse, TokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::config::KakaoFriendShareConfig;

use super::external_share::{
    BeginShareOperation, ExternalShareError, ExternalShareScope, SafeShareSummary,
    ShareOperationResult, ShareOperationState, begin_operation, finish_operation, mark_unknown,
};
use super::oauth_connection::{
    AccountTokenUpdate, KAKAO_PROVIDER, OAuthAccountRecord, OAuthConnectionError,
    PRIMARY_ACCOUNT_KEY, StoredOAuthTokens, TokenVault, acquire_refresh_lease, complete_refresh,
    consume_session, create_session, delete_connection, fail_refresh, load_account,
    set_account_status, token_expiry_from_now, upsert_account,
};

pub const KAKAO_CONNECTOR_ID: &str = "kakao_friend_share";
pub const KAKAO_REST_API_KEY_ENV: &str = "KAKAO_REST_API_KEY";
pub const KAKAO_CLIENT_SECRET_ENV: &str = "KAKAO_CLIENT_SECRET";
pub const REQUIRED_SCOPES: [&str; 2] = ["friends", "talk_message"];

const AUTHORIZE_URL: &str = "https://kauth.kakao.com/oauth/authorize";
const TOKEN_URL: &str = "https://kauth.kakao.com/oauth/token";
const FRIENDS_URL: &str = "https://kapi.kakao.com/v1/api/talk/friends";
const SEND_URL: &str = "https://kapi.kakao.com/v1/api/talk/friends/message/default/send";
const KAKAO_SCOPE_VALUE: &str = "friends,talk_message";
const HTTP_TIMEOUT_SECONDS: u64 = 10;
const ACCESS_TOKEN_REFRESH_SKEW_SECONDS: i64 = 60;
const FRIENDS_RESPONSE_MAX_BYTES: usize = 256 * 1024;
const SEND_RESPONSE_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct KakaoExtraTokenFields {
    #[serde(default)]
    refresh_token_expires_in: Option<u64>,
}

impl ExtraTokenFields for KakaoExtraTokenFields {}

type KakaoTokenResponse = StandardTokenResponse<KakaoExtraTokenFields, BasicTokenType>;
type KakaoOAuthClient<
    HasAuthUrl = EndpointNotSet,
    HasDeviceAuthUrl = EndpointNotSet,
    HasIntrospectionUrl = EndpointNotSet,
    HasRevocationUrl = EndpointNotSet,
    HasTokenUrl = EndpointNotSet,
> = Client<
    BasicErrorResponse,
    KakaoTokenResponse,
    BasicTokenIntrospectionResponse,
    StandardRevocableToken,
    BasicRevocationErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
>;
type ConfiguredOAuthClient =
    KakaoOAuthClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

static HTTP_CLIENT: LazyLock<Result<reqwest::Client, String>> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .build()
        .map_err(|_| "failed to build Kakao HTTP client".to_string())
});

#[derive(Debug, Error)]
pub enum KakaoError {
    #[error("Kakao friend share is disabled")]
    Disabled,
    #[error("Kakao friend share configuration is incomplete")]
    MissingConfig,
    #[error("Kakao friend share storage is unavailable")]
    MissingDatabase,
    #[error("Kakao OAuth request is invalid")]
    InvalidOAuthRequest,
    #[error("Kakao OAuth state is invalid or expired")]
    InvalidOAuthState,
    #[error("Kakao OAuth exchange failed")]
    OAuthExchange,
    #[error("Kakao connection does not have the required consent")]
    ConsentIncomplete,
    #[error("Kakao connection requires authorization again")]
    ReauthorizationRequired,
    #[error("Kakao connection is not established")]
    NotConnected,
    #[error("Kakao provider request failed")]
    Provider,
    #[error("Kakao provider result is ambiguous")]
    AmbiguousProviderResult,
    #[error("Kakao request validation failed: {0}")]
    Validation(&'static str),
    #[error("Kakao share operation is already in progress")]
    OperationInProgress,
    #[error(transparent)]
    Connection(#[from] OAuthConnectionError),
    #[error(transparent)]
    ExternalShare(#[from] ExternalShareError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KakaoConnectionState {
    Disabled,
    MissingConfig,
    NotConnected,
    Connected,
    ConsentIncomplete,
    ReauthorizationRequired,
    StorageUnavailable,
    InvalidConfig,
}

impl KakaoConnectionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::MissingConfig => "missing_config",
            Self::NotConnected => "not_connected",
            Self::Connected => "connected",
            Self::ConsentIncomplete => "consent_incomplete",
            Self::ReauthorizationRequired => "reauthorization_required",
            Self::StorageUnavailable => "storage_unavailable",
            Self::InvalidConfig => "invalid_config",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KakaoConnectionStatus {
    pub state: KakaoConnectionState,
    pub reason: Option<&'static str>,
    pub scopes: Vec<String>,
    pub access_expires_at: Option<DateTime<Utc>>,
}

impl KakaoConnectionStatus {
    fn simple(state: KakaoConnectionState, reason: &'static str) -> Self {
        Self {
            state,
            reason: Some(reason),
            scopes: Vec::new(),
            access_expires_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OAuthStart {
    pub authorize_url: String,
    pub expires_in_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct KakaoFriend {
    pub uuid: String,
    #[serde(default)]
    pub profile_nickname: String,
    #[serde(default)]
    pub allowed_msg: bool,
}

#[derive(Debug, Serialize)]
pub struct FriendView {
    pub uuid: String,
    pub display_name: String,
}

#[derive(Debug, Deserialize)]
struct KakaoFriendsResponse {
    #[serde(default)]
    elements: Vec<KakaoFriend>,
    #[serde(default)]
    total_count: usize,
    #[serde(default)]
    after_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FriendsPage {
    pub friends: Vec<FriendView>,
    pub total_count: usize,
    pub offset: u32,
    pub limit: u32,
    pub next_offset: Option<u32>,
}

#[derive(Clone, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
pub struct KakaoFriendShareCommand {
    pub receiver_uuids: Vec<String>,
    pub text: String,
    #[serde(default)]
    pub confirmed: bool,
}

#[derive(Debug, Deserialize)]
struct KakaoSendResponse {
    #[serde(default)]
    successful_receiver_uuids: Vec<String>,
    #[serde(default)]
    failure_info: Vec<KakaoFailureInfo>,
}

#[derive(Debug, Deserialize)]
struct KakaoFailureInfo {
    #[allow(dead_code)]
    code: Option<i64>,
    #[serde(default)]
    receiver_uuids: Vec<String>,
}

pub struct KakaoFriendShareService<'a> {
    pool: &'a PgPool,
    config: &'a KakaoFriendShareConfig,
}

impl<'a> KakaoFriendShareService<'a> {
    pub fn new(
        pool: Option<&'a PgPool>,
        config: &'a KakaoFriendShareConfig,
    ) -> Result<Self, KakaoError> {
        if !config.enabled {
            return Err(KakaoError::Disabled);
        }
        let pool = pool.ok_or(KakaoError::MissingDatabase)?;
        Ok(Self { pool, config })
    }

    pub async fn start_oauth(&self) -> Result<OAuthStart, KakaoError> {
        let client = oauth_client(self.config)?;
        let state = create_session(self.pool).await?;
        let (authorize_url, _) = client
            .authorize_url(|| CsrfToken::new(state))
            .add_scope(Scope::new(KAKAO_SCOPE_VALUE.to_string()))
            .url();
        Ok(OAuthStart {
            authorize_url: authorize_url.to_string(),
            expires_in_seconds: 600,
        })
    }

    pub async fn complete_oauth(&self, state: &str, code: &str) -> Result<(), KakaoError> {
        if state.is_empty() || code.is_empty() || state.len() > 256 || code.len() > 2_048 {
            return Err(KakaoError::InvalidOAuthRequest);
        }
        if !consume_session(self.pool, state).await? {
            return Err(KakaoError::InvalidOAuthState);
        }
        let client = oauth_client(self.config)?;
        let token = client
            .exchange_code(AuthorizationCode::new(code.to_string()))
            .request_async(http_client()?)
            .await
            .map_err(|_| KakaoError::OAuthExchange)?;
        let scopes = normalized_scopes(token.scopes());
        let has_required_scopes = required_scopes_present(&scopes);
        let tokens = StoredOAuthTokens {
            access_token: token.access_token().secret().to_string(),
            refresh_token: token
                .refresh_token()
                .map(|value| value.secret().to_string()),
        };
        upsert_account(
            self.pool,
            &TokenVault::from_env()?,
            AccountTokenUpdate {
                tokens: &tokens,
                scopes: &scopes,
                access_expires_at: token_expiry_from_now(token.expires_in()),
                refresh_expires_at: token_expiry_from_now(
                    token
                        .extra_fields()
                        .refresh_token_expires_in
                        .map(std::time::Duration::from_secs),
                ),
            },
        )
        .await?;
        if !has_required_scopes {
            set_account_status(self.pool, "consent_incomplete").await?;
            return Err(KakaoError::ConsentIncomplete);
        }
        Ok(())
    }

    pub async fn consume_denied_oauth(&self, state: &str) -> Result<(), KakaoError> {
        if !consume_session(self.pool, state).await? {
            return Err(KakaoError::InvalidOAuthState);
        }
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), KakaoError> {
        delete_connection(self.pool).await?;
        Ok(())
    }

    pub async fn list_friends(&self, offset: u32, limit: u32) -> Result<FriendsPage, KakaoError> {
        if !(1..=100).contains(&limit) {
            return Err(KakaoError::Validation("limit must be between 1 and 100"));
        }
        let access_token = self.access_token().await?;
        let response = http_client()?
            .get(FRIENDS_URL)
            .bearer_auth(&access_token)
            .query(&[("offset", offset), ("limit", limit)])
            .send()
            .await
            .map_err(|_| KakaoError::Provider)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            set_account_status(self.pool, "reauth_required").await?;
            return Err(KakaoError::ReauthorizationRequired);
        }
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            set_account_status(self.pool, "consent_incomplete").await?;
            return Err(KakaoError::ConsentIncomplete);
        }
        if !response.status().is_success() {
            return Err(KakaoError::Provider);
        }
        let payload =
            read_bounded_json::<KakaoFriendsResponse>(response, FRIENDS_RESPONSE_MAX_BYTES)
                .await
                .map_err(|_| KakaoError::Provider)?;
        let had_after = payload.after_url.is_some();
        let received_count = payload.elements.len();
        let friends = payload
            .elements
            .into_iter()
            .filter(|friend| friend.allowed_msg)
            .map(|friend| FriendView {
                uuid: friend.uuid,
                display_name: friend.profile_nickname,
            })
            .collect::<Vec<_>>();
        let consumed = usize::try_from(offset).unwrap_or(usize::MAX) + received_count;
        let next_offset =
            (had_after || consumed < payload.total_count).then(|| offset.saturating_add(limit));
        Ok(FriendsPage {
            friends,
            total_count: payload.total_count,
            offset,
            limit,
            next_offset,
        })
    }

    pub async fn send_friend_message(
        &self,
        idempotency_key: &str,
        request: KakaoFriendShareCommand,
    ) -> Result<ShareOperationResult, KakaoError> {
        validate_send_request(idempotency_key, &request)?;
        // Complete connection validation and any refresh before creating the
        // non-reclaiming send fence. A disconnected account therefore returns
        // a normal 409 and never consumes an idempotency key as `unknown`.
        let access_token = self.access_token().await?;
        let fingerprint = request_fingerprint(&request, &self.config.landing_url);
        let requested_count = request.receiver_uuids.len();
        match begin_operation(
            self.pool,
            ExternalShareScope {
                provider: KAKAO_PROVIDER,
                channel_id: KAKAO_CONNECTOR_ID,
                account_key: PRIMARY_ACCOUNT_KEY,
            },
            idempotency_key,
            &fingerprint,
            requested_count,
            self.config.send_limit_per_hour,
        )
        .await?
        {
            BeginShareOperation::Replay(result) => return Ok(result),
            BeginShareOperation::InProgress => {
                return Err(KakaoError::OperationInProgress);
            }
            BeginShareOperation::Dispatch { operation_id } => {
                let result = self
                    .dispatch_once(&access_token, &request)
                    .await
                    .and_then(|payload| classify_send_response(&request.receiver_uuids, payload));
                match result {
                    Ok((state, summary)) => {
                        match finish_operation(self.pool, operation_id, state, summary).await {
                            Ok(result) => Ok(result),
                            Err(_) => Ok(ShareOperationResult {
                                operation_id,
                                status: ShareOperationState::Unknown,
                                summary: SafeShareSummary::unknown(requested_count),
                                replayed: false,
                                delivery_may_have_occurred: true,
                                automatic_retry_allowed: false,
                            }),
                        }
                    }
                    Err(_) => Ok(mark_unknown(self.pool, operation_id, requested_count).await),
                }
            }
        }
    }

    async fn dispatch_once(
        &self,
        access_token: &str,
        request: &KakaoFriendShareCommand,
    ) -> Result<KakaoSendResponse, KakaoError> {
        let receiver_uuids = serde_json::to_string(&request.receiver_uuids)
            .map_err(|_| KakaoError::Validation("receiver_uuids are invalid"))?;
        let template = json!({
            "object_type": "text",
            "text": request.text,
            "link": {
                "web_url": self.config.landing_url,
                "mobile_web_url": self.config.landing_url
            }
        })
        .to_string();
        let response = http_client()?
            .post(SEND_URL)
            .bearer_auth(access_token)
            .form(&[
                ("receiver_uuids", receiver_uuids),
                ("template_object", template),
            ])
            .send()
            .await
            .map_err(|_| KakaoError::AmbiguousProviderResult)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            set_account_status(self.pool, "reauth_required").await?;
            return Err(KakaoError::AmbiguousProviderResult);
        }
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            set_account_status(self.pool, "consent_incomplete").await?;
            return Err(KakaoError::AmbiguousProviderResult);
        }
        if response.status() != reqwest::StatusCode::OK {
            return Err(KakaoError::AmbiguousProviderResult);
        }
        read_bounded_json::<KakaoSendResponse>(response, SEND_RESPONSE_MAX_BYTES)
            .await
            .map_err(|_| KakaoError::AmbiguousProviderResult)
    }

    async fn access_token(&self) -> Result<String, KakaoError> {
        let vault = TokenVault::from_env()?;
        let account = load_account(self.pool)
            .await?
            .ok_or(KakaoError::NotConnected)?;
        ensure_account_usable(&account)?;
        let tokens = account.decrypt_tokens(&vault)?;
        let should_refresh = account.access_expires_at.is_some_and(|expires| {
            expires <= Utc::now() + ChronoDuration::seconds(ACCESS_TOKEN_REFRESH_SKEW_SECONDS)
        });
        if !should_refresh {
            return Ok(tokens.access_token.clone());
        }
        let refresh_token = match tokens.refresh_token.as_deref() {
            Some(value) => value.to_string(),
            None => {
                set_account_status(self.pool, "reauth_required").await?;
                return Err(KakaoError::ReauthorizationRequired);
            }
        };
        let lease_id = acquire_refresh_lease(self.pool).await?;
        let refreshed = oauth_client(self.config)?
            .exchange_refresh_token(&RefreshToken::new(refresh_token))
            .request_async(http_client()?)
            .await;
        let refreshed = match refreshed {
            Ok(value) => value,
            Err(_) => {
                fail_refresh(self.pool, lease_id).await?;
                return Err(KakaoError::ReauthorizationRequired);
            }
        };
        let scopes = if refreshed.scopes().is_some() {
            normalized_scopes(refreshed.scopes())
        } else {
            account.scopes.clone()
        };
        let updated_tokens = StoredOAuthTokens {
            access_token: refreshed.access_token().secret().to_string(),
            refresh_token: refreshed
                .refresh_token()
                .map(|value| value.secret().to_string())
                .or(tokens.refresh_token.clone()),
        };
        let completed = match complete_refresh(
            self.pool,
            &vault,
            lease_id,
            AccountTokenUpdate {
                tokens: &updated_tokens,
                scopes: &scopes,
                access_expires_at: token_expiry_from_now(refreshed.expires_in()),
                refresh_expires_at: token_expiry_from_now(
                    refreshed
                        .extra_fields()
                        .refresh_token_expires_in
                        .map(std::time::Duration::from_secs),
                )
                .or(account.refresh_expires_at),
            },
        )
        .await
        {
            Ok(completed) => completed,
            Err(_) => {
                let _ = fail_refresh(self.pool, lease_id).await;
                return Err(KakaoError::ReauthorizationRequired);
            }
        };
        if !completed {
            return Err(KakaoError::ReauthorizationRequired);
        }
        if !required_scopes_present(&scopes) {
            set_account_status(self.pool, "consent_incomplete").await?;
            return Err(KakaoError::ConsentIncomplete);
        }
        Ok(updated_tokens.access_token.clone())
    }
}

pub async fn connection_status(
    pool: Option<&PgPool>,
    config: &KakaoFriendShareConfig,
) -> KakaoConnectionStatus {
    if !config.enabled {
        return KakaoConnectionStatus::simple(KakaoConnectionState::Disabled, "disabled");
    }
    if !env_present(KAKAO_REST_API_KEY_ENV) || !env_present(KAKAO_CLIENT_SECRET_ENV) {
        return KakaoConnectionStatus::simple(
            KakaoConnectionState::MissingConfig,
            "missing_kakao_credentials",
        );
    }
    let vault = match TokenVault::from_env() {
        Ok(value) => value,
        Err(_) => {
            return KakaoConnectionStatus::simple(
                KakaoConnectionState::InvalidConfig,
                "invalid_token_key",
            );
        }
    };
    let Some(pool) = pool else {
        return KakaoConnectionStatus::simple(
            KakaoConnectionState::StorageUnavailable,
            "database_unavailable",
        );
    };
    let account = match load_account(pool).await {
        Ok(Some(account)) => account,
        Ok(None) => {
            return KakaoConnectionStatus::simple(
                KakaoConnectionState::NotConnected,
                "not_connected",
            );
        }
        Err(_) => {
            return KakaoConnectionStatus::simple(
                KakaoConnectionState::StorageUnavailable,
                "database_unavailable",
            );
        }
    };
    if account.decrypt_tokens(&vault).is_err() {
        return KakaoConnectionStatus::simple(
            KakaoConnectionState::InvalidConfig,
            "token_decryption_failed",
        );
    }
    let state = match account.status.as_str() {
        "active" if required_scopes_present(&account.scopes) => KakaoConnectionState::Connected,
        "active" | "consent_incomplete" => KakaoConnectionState::ConsentIncomplete,
        "reauth_required" => KakaoConnectionState::ReauthorizationRequired,
        _ => KakaoConnectionState::InvalidConfig,
    };
    KakaoConnectionStatus {
        state,
        reason: match state {
            KakaoConnectionState::Connected => None,
            KakaoConnectionState::ConsentIncomplete => Some("consent_incomplete"),
            KakaoConnectionState::ReauthorizationRequired => Some("reauth_required"),
            _ => Some("invalid_account_state"),
        },
        scopes: account.scopes,
        access_expires_at: account.access_expires_at,
    }
}

fn oauth_client(config: &KakaoFriendShareConfig) -> Result<ConfiguredOAuthClient, KakaoError> {
    let client_id = required_env(KAKAO_REST_API_KEY_ENV).ok_or(KakaoError::MissingConfig)?;
    let client_secret = required_env(KAKAO_CLIENT_SECRET_ENV).ok_or(KakaoError::MissingConfig)?;
    crate::utils::redact::register_known_secret(&client_id);
    crate::utils::redact::register_known_secret(&client_secret);
    let auth_url =
        AuthUrl::new(AUTHORIZE_URL.to_string()).map_err(|_| KakaoError::MissingConfig)?;
    let token_url = TokenUrl::new(TOKEN_URL.to_string()).map_err(|_| KakaoError::MissingConfig)?;
    let redirect =
        RedirectUrl::new(config.redirect_uri.clone()).map_err(|_| KakaoError::MissingConfig)?;
    Ok(KakaoOAuthClient::new(ClientId::new(client_id))
        .set_client_secret(ClientSecret::new(client_secret))
        .set_auth_uri(auth_url)
        .set_token_uri(token_url)
        .set_redirect_uri(redirect)
        .set_auth_type(AuthType::RequestBody))
}

fn http_client() -> Result<&'static reqwest::Client, KakaoError> {
    HTTP_CLIENT.as_ref().map_err(|_| KakaoError::Provider)
}

async fn read_bounded_json<T: DeserializeOwned>(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<T, ()> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(());
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ())?;
        let next_len = body.len().checked_add(chunk.len()).ok_or(())?;
        if next_len > max_bytes {
            return Err(());
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| ())
}

fn required_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_present(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
}

fn normalized_scopes(scopes: Option<&Vec<Scope>>) -> Vec<String> {
    let mut normalized = BTreeSet::new();
    let Some(scopes) = scopes else {
        return REQUIRED_SCOPES.into_iter().map(str::to_string).collect();
    };
    for scope in scopes {
        for value in scope.as_ref().split([',', ' ']) {
            let value = value.trim();
            if !value.is_empty() {
                normalized.insert(value.to_string());
            }
        }
    }
    normalized.into_iter().collect()
}

fn required_scopes_present(scopes: &[String]) -> bool {
    REQUIRED_SCOPES
        .iter()
        .all(|required| scopes.iter().any(|scope| scope == required))
}

fn ensure_account_usable(account: &OAuthAccountRecord) -> Result<(), KakaoError> {
    match account.status.as_str() {
        "active" if required_scopes_present(&account.scopes) => Ok(()),
        "active" | "consent_incomplete" => Err(KakaoError::ConsentIncomplete),
        "reauth_required" => Err(KakaoError::ReauthorizationRequired),
        _ => Err(KakaoError::NotConnected),
    }
}

fn validate_send_request(
    idempotency_key: &str,
    request: &KakaoFriendShareCommand,
) -> Result<(), KakaoError> {
    if !(8..=128).contains(&idempotency_key.len())
        || !idempotency_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(KakaoError::Validation("Idempotency-Key is invalid"));
    }
    validate_friend_share_payload(request)
}

/// Validate the provider payload independently from HTTP idempotency metadata.
/// Scheduled delivery uses this at reservation time, then derives a stable
/// idempotency key from the durable outbox id when the provider worker runs.
pub fn validate_friend_share_payload(request: &KakaoFriendShareCommand) -> Result<(), KakaoError> {
    if !request.confirmed {
        return Err(KakaoError::Validation("confirmed must be true"));
    }
    validate_friend_share_recipients(&request.receiver_uuids)?;
    validate_friend_share_text(&request.text)
}

pub fn validate_friend_share_recipients(receiver_uuids: &[String]) -> Result<(), KakaoError> {
    if receiver_uuids.is_empty() || receiver_uuids.len() > 5 {
        return Err(KakaoError::Validation(
            "receiver_uuids must contain 1 to 5 entries",
        ));
    }
    let recipients = receiver_uuids.iter().collect::<BTreeSet<_>>();
    if recipients.len() != receiver_uuids.len()
        || receiver_uuids.iter().any(|value| {
            value.is_empty()
                || value.len() > 128
                || !value.bytes().all(|byte| byte.is_ascii_graphic())
        })
    {
        return Err(KakaoError::Validation("receiver_uuids are invalid"));
    }
    Ok(())
}

pub fn validate_friend_share_text(text: &str) -> Result<(), KakaoError> {
    let char_count = text.chars().count();
    if text.trim().is_empty() || char_count > 200 {
        return Err(KakaoError::Validation(
            "text must contain 1 to 200 characters",
        ));
    }
    Ok(())
}

fn request_fingerprint(request: &KakaoFriendShareCommand, landing_url: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"agentdesk/kakao-friend-share/request/v1\0");
    update_length_prefixed(&mut hasher, request.text.as_bytes());
    update_length_prefixed(&mut hasher, landing_url.as_bytes());
    let mut recipients = request.receiver_uuids.iter().collect::<Vec<_>>();
    recipients.sort_unstable();
    for recipient in recipients {
        update_length_prefixed(&mut hasher, recipient.as_bytes());
    }
    hasher.finalize().to_vec()
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn classify_send_response(
    requested: &[String],
    response: KakaoSendResponse,
) -> Result<(ShareOperationState, SafeShareSummary), KakaoError> {
    let requested_set = requested.iter().collect::<BTreeSet<_>>();
    let success_set = response
        .successful_receiver_uuids
        .iter()
        .collect::<BTreeSet<_>>();
    let failed_set = response
        .failure_info
        .iter()
        .flat_map(|failure| failure.receiver_uuids.iter())
        .collect::<BTreeSet<_>>();
    let failure_receiver_count = response
        .failure_info
        .iter()
        .map(|failure| failure.receiver_uuids.len())
        .sum::<usize>();
    if success_set.len() != response.successful_receiver_uuids.len()
        || failed_set.len() != failure_receiver_count
        || !success_set.is_disjoint(&failed_set)
        || !success_set.is_subset(&requested_set)
        || !failed_set.is_subset(&requested_set)
        || success_set.len() + failed_set.len() != requested_set.len()
    {
        return Err(KakaoError::AmbiguousProviderResult);
    }
    let successful_count = success_set.len();
    let failed_count = failed_set.len();
    let state = if successful_count == requested.len() {
        ShareOperationState::Success
    } else if successful_count > 0 {
        ShareOperationState::PartialSuccess
    } else {
        ShareOperationState::Failed
    };
    Ok((
        state,
        SafeShareSummary {
            requested_count: requested.len(),
            successful_count,
            failed_count,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_response(raw_response: Vec<u8>) -> reqwest::Response {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(&raw_response).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        server.await.unwrap();
        response
    }

    fn request(recipients: &[&str], text: &str) -> KakaoFriendShareCommand {
        KakaoFriendShareCommand {
            receiver_uuids: recipients
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            text: text.to_string(),
            confirmed: true,
        }
    }

    #[test]
    fn validates_send_limits_and_idempotency_key() {
        assert!(validate_send_request("send-key-123", &request(&["a"], "hello")).is_ok());
        assert!(validate_send_request("short", &request(&["a"], "hello")).is_err());
        assert!(validate_send_request("send-key-123", &request(&[], "hello")).is_err());
        assert!(validate_send_request("send-key-123", &request(&["a", "a"], "hello")).is_err());
        assert!(validate_send_request("send-key-123", &request(&["a"], "   ")).is_err());
        assert!(
            validate_send_request("send-key-123", &request(&["a"], &"가".repeat(201))).is_err()
        );
    }

    #[test]
    fn request_fingerprint_is_recipient_order_independent_and_payload_bound() {
        let first = request_fingerprint(&request(&["b", "a"], "hello"), "https://example.com");
        let reordered = request_fingerprint(&request(&["a", "b"], "hello"), "https://example.com");
        let changed = request_fingerprint(&request(&["a", "b"], "changed"), "https://example.com");
        assert_eq!(first, reordered);
        assert_ne!(first, changed);
    }

    #[test]
    fn classifies_complete_and_partial_provider_results() {
        let requested = vec!["a".to_string(), "b".to_string()];
        let success = classify_send_response(
            &requested,
            KakaoSendResponse {
                successful_receiver_uuids: requested.clone(),
                failure_info: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(success.0, ShareOperationState::Success);

        let partial = classify_send_response(
            &requested,
            KakaoSendResponse {
                successful_receiver_uuids: vec!["a".to_string()],
                failure_info: vec![KakaoFailureInfo {
                    code: Some(-532),
                    receiver_uuids: vec!["b".to_string()],
                }],
            },
        )
        .unwrap();
        assert_eq!(partial.0, ShareOperationState::PartialSuccess);
        assert_eq!(partial.1.successful_count, 1);
        assert_eq!(partial.1.failed_count, 1);
    }

    #[test]
    fn incomplete_provider_accounting_is_ambiguous() {
        let requested = vec!["a".to_string(), "b".to_string()];
        assert!(
            classify_send_response(
                &requested,
                KakaoSendResponse {
                    successful_receiver_uuids: vec!["a".to_string()],
                    failure_info: Vec::new(),
                }
            )
            .is_err()
        );
    }

    #[test]
    fn comma_scope_is_one_oauth_scope_value() {
        assert_eq!(KAKAO_SCOPE_VALUE, "friends,talk_message");
        assert!(required_scopes_present(&normalized_scopes(Some(&vec![
            Scope::new(KAKAO_SCOPE_VALUE.to_string())
        ]))));
    }

    #[test]
    fn an_explicit_empty_scope_response_does_not_gain_consent() {
        assert!(!required_scopes_present(&normalized_scopes(Some(
            &Vec::new()
        ))));
    }

    #[test]
    fn parses_kakao_refresh_token_expiry_extension() {
        let token: KakaoTokenResponse = serde_json::from_value(json!({
            "access_token": "redacted-access",
            "token_type": "bearer",
            "expires_in": 21_599,
            "refresh_token": "redacted-refresh",
            "refresh_token_expires_in": 5_184_000,
            "scope": "friends,talk_message"
        }))
        .unwrap();

        assert_eq!(
            token.extra_fields().refresh_token_expires_in,
            Some(5_184_000)
        );
        assert!(required_scopes_present(&normalized_scopes(token.scopes())));
    }

    #[tokio::test]
    async fn bounded_json_rejects_declared_and_streamed_oversize() {
        let declared = loopback_response(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789"
                .to_vec(),
        )
        .await;
        assert!(
            read_bounded_json::<serde_json::Value>(declared, 8)
                .await
                .is_err()
        );

        let streamed = loopback_response(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n9\r\n123456789\r\n0\r\n\r\n"
                .to_vec(),
        )
        .await;
        assert!(
            read_bounded_json::<serde_json::Value>(streamed, 8)
                .await
                .is_err()
        );
    }
}
