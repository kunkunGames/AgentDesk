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
    AccountTokenUpdate, DeleteAccountOutcome, KAKAO_PROVIDER, OAuthAccountRecord,
    OAuthAccountSummary, OAuthConnectionError, PRIMARY_ACCOUNT_KEY, StoredOAuthTokens, TokenVault,
    acquire_refresh_lease, complete_refresh, consume_session, create_session, delete_account,
    fail_refresh, find_account_by_subject_hash, list_accounts, load_account, set_account_status,
    token_expiry_from_now, upsert_account,
};
use uuid::Uuid;

pub const KAKAO_CONNECTOR_ID: &str = "kakao_friend_share";
pub const KAKAO_REST_API_KEY_ENV: &str = "KAKAO_REST_API_KEY";
pub const KAKAO_CLIENT_SECRET_ENV: &str = "KAKAO_CLIENT_SECRET";
pub const REQUIRED_SCOPES: [&str; 2] = ["friends", "talk_message"];
pub const KAKAO_MEMO_CHANNEL_ID: &str = "kakao_memo";

const AUTHORIZE_URL: &str = "https://kauth.kakao.com/oauth/authorize";
const TOKEN_URL: &str = "https://kauth.kakao.com/oauth/token";
const USER_ME_URL: &str = "https://kapi.kakao.com/v2/user/me";
const FRIENDS_URL: &str = "https://kapi.kakao.com/v1/api/talk/friends";
const SEND_URL: &str = "https://kapi.kakao.com/v1/api/talk/friends/message/default/send";
const MEMO_SEND_URL: &str = "https://kapi.kakao.com/v2/api/talk/memo/default/send";
const KAKAO_SCOPE_VALUE: &str = "friends,talk_message";
const HTTP_TIMEOUT_SECONDS: u64 = 10;
const ACCESS_TOKEN_REFRESH_SKEW_SECONDS: i64 = 60;
const FRIENDS_RESPONSE_MAX_BYTES: usize = 256 * 1024;
const SEND_RESPONSE_MAX_BYTES: usize = 64 * 1024;
const USER_ME_RESPONSE_MAX_BYTES: usize = 16 * 1024;

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
    #[error("Kakao account selection is invalid")]
    InvalidAccount,
    #[error("Kakao account is referenced by scheduled delivery")]
    AccountInUse,
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
    pub accounts: Vec<OAuthAccountSummary>,
}

impl KakaoConnectionStatus {
    fn simple(state: KakaoConnectionState, reason: &'static str) -> Self {
        Self {
            state,
            reason: Some(reason),
            scopes: Vec::new(),
            access_expires_at: None,
            accounts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OAuthStart {
    pub authorize_url: String,
    pub expires_in_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OAuthCompletion {
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
struct KakaoUserMeResponse {
    id: i64,
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
    pub image_url: Option<String>,
    #[serde(default)]
    pub confirmed: bool,
}

/// A deliberate, operator-confirmed message to the connected Kakao account's
/// own "My Chatroom". It must not depend on the friends-list permission.
#[derive(Clone, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
pub struct KakaoMemoSendCommand {
    pub text: String,
    #[serde(default)]
    pub image_url: Option<String>,
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
struct KakaoMemoSendResponse {
    result_code: i64,
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
            .add_extra_param("prompt", "select_account")
            .url();
        Ok(OAuthStart {
            authorize_url: authorize_url.to_string(),
            expires_in_seconds: 600,
        })
    }

    pub async fn complete_oauth(
        &self,
        state: &str,
        code: &str,
    ) -> Result<OAuthCompletion, KakaoError> {
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
        let subject_hash = self.subject_hash(&tokens.access_token).await?;
        let candidate_account_key =
            match find_account_by_subject_hash(self.pool, &subject_hash).await? {
                Some(account) => account.account_key,
                None => self.legacy_primary_key_for_subject(&subject_hash).await,
            };
        let account_key = upsert_account(
            self.pool,
            &TokenVault::from_env()?,
            &candidate_account_key,
            Some(&subject_hash),
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
            set_account_status(self.pool, &account_key, "consent_incomplete").await?;
            return Err(KakaoError::ConsentIncomplete);
        }
        Ok(OAuthCompletion {
            account_id: account_key,
        })
    }

    pub async fn consume_denied_oauth(&self, state: &str) -> Result<(), KakaoError> {
        if !consume_session(self.pool, state).await? {
            return Err(KakaoError::InvalidOAuthState);
        }
        Ok(())
    }

    pub async fn disconnect(&self, account_key: &str) -> Result<bool, KakaoError> {
        validate_account_key(account_key)?;
        match delete_account(self.pool, account_key).await? {
            DeleteAccountOutcome::Deleted => Ok(true),
            DeleteAccountOutcome::NotFound => Ok(false),
            DeleteAccountOutcome::InUse => Err(KakaoError::AccountInUse),
        }
    }

    pub async fn accounts(&self) -> Result<Vec<OAuthAccountSummary>, KakaoError> {
        Ok(list_accounts(self.pool).await?)
    }

    pub async fn list_friends(
        &self,
        account_key: &str,
        offset: u32,
        limit: u32,
    ) -> Result<FriendsPage, KakaoError> {
        validate_account_key(account_key)?;
        if !(1..=100).contains(&limit) {
            return Err(KakaoError::Validation("limit must be between 1 and 100"));
        }
        let access_token = self.access_token(account_key).await?;
        let response = http_client()?
            .get(FRIENDS_URL)
            .bearer_auth(&access_token)
            .query(&[("offset", offset), ("limit", limit)])
            .send()
            .await
            .map_err(|_| KakaoError::Provider)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            set_account_status(self.pool, account_key, "reauth_required").await?;
            return Err(KakaoError::ReauthorizationRequired);
        }
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            set_account_status(self.pool, account_key, "consent_incomplete").await?;
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
        let friends = message_eligible_friend_views(payload.elements);
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
        account_key: &str,
        idempotency_key: &str,
        request: KakaoFriendShareCommand,
    ) -> Result<ShareOperationResult, KakaoError> {
        validate_send_request(idempotency_key, &request)?;
        // Complete connection validation and any refresh before creating the
        // non-reclaiming send fence. A disconnected account therefore returns
        // a normal 409 and never consumes an idempotency key as `unknown`.
        validate_account_key(account_key)?;
        let access_token = self.access_token(account_key).await?;
        let fingerprint = request_fingerprint(&request, &self.config.landing_url);
        let requested_count = request.receiver_uuids.len();
        match begin_operation(
            self.pool,
            ExternalShareScope {
                provider: KAKAO_PROVIDER,
                channel_id: KAKAO_CONNECTOR_ID,
                account_key,
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
                    .dispatch_once(account_key, &access_token, &request)
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

    pub async fn send_memo_message(
        &self,
        account_key: &str,
        idempotency_key: &str,
        request: KakaoMemoSendCommand,
    ) -> Result<ShareOperationResult, KakaoError> {
        validate_memo_send_request(idempotency_key, &request)?;
        // A memo send only needs `talk_message`. In particular, an unavailable
        // friends-list entitlement must not prevent the operator's own-account
        // smoke test.
        validate_account_key(account_key)?;
        let access_token = self.memo_access_token(account_key).await?;
        let fingerprint = memo_request_fingerprint(&request, &self.config.landing_url);
        match begin_operation(
            self.pool,
            ExternalShareScope {
                provider: KAKAO_PROVIDER,
                channel_id: KAKAO_MEMO_CHANNEL_ID,
                account_key,
            },
            idempotency_key,
            &fingerprint,
            1,
            self.config.send_limit_per_hour,
        )
        .await?
        {
            BeginShareOperation::Replay(result) => Ok(result),
            BeginShareOperation::InProgress => Err(KakaoError::OperationInProgress),
            BeginShareOperation::Dispatch { operation_id } => {
                let result = self
                    .dispatch_memo_once(account_key, &access_token, &request)
                    .await;
                match result {
                    Ok((state, summary)) => {
                        match finish_operation(self.pool, operation_id, state, summary).await {
                            Ok(result) => Ok(result),
                            Err(_) => Ok(ShareOperationResult {
                                operation_id,
                                status: ShareOperationState::Unknown,
                                summary: SafeShareSummary::unknown(1),
                                replayed: false,
                                delivery_may_have_occurred: true,
                                automatic_retry_allowed: false,
                            }),
                        }
                    }
                    Err(_) => Ok(mark_unknown(self.pool, operation_id, 1).await),
                }
            }
        }
    }

    async fn dispatch_once(
        &self,
        account_key: &str,
        access_token: &str,
        request: &KakaoFriendShareCommand,
    ) -> Result<KakaoSendResponse, KakaoError> {
        let receiver_uuids = serde_json::to_string(&request.receiver_uuids)
            .map_err(|_| KakaoError::Validation("receiver_uuids are invalid"))?;
        let template = message_template(
            &request.text,
            request.image_url.as_deref(),
            &self.config.landing_url,
        );
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
            set_account_status(self.pool, account_key, "reauth_required").await?;
            return Err(KakaoError::AmbiguousProviderResult);
        }
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            set_account_status(self.pool, account_key, "consent_incomplete").await?;
            return Err(KakaoError::AmbiguousProviderResult);
        }
        if response.status() != reqwest::StatusCode::OK {
            return Err(KakaoError::AmbiguousProviderResult);
        }
        read_bounded_json::<KakaoSendResponse>(response, SEND_RESPONSE_MAX_BYTES)
            .await
            .map_err(|_| KakaoError::AmbiguousProviderResult)
    }

    async fn dispatch_memo_once(
        &self,
        account_key: &str,
        access_token: &str,
        request: &KakaoMemoSendCommand,
    ) -> Result<(ShareOperationState, SafeShareSummary), KakaoError> {
        let template = message_template(
            &request.text,
            request.image_url.as_deref(),
            &self.config.landing_url,
        );
        let response = http_client()?
            .post(MEMO_SEND_URL)
            .bearer_auth(access_token)
            .form(&[("template_object", template)])
            .send()
            .await
            .map_err(|_| KakaoError::AmbiguousProviderResult)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            set_account_status(self.pool, account_key, "reauth_required").await?;
            return Err(KakaoError::AmbiguousProviderResult);
        }
        if response.status() != reqwest::StatusCode::OK {
            return Err(KakaoError::AmbiguousProviderResult);
        }
        let response =
            read_bounded_json::<KakaoMemoSendResponse>(response, SEND_RESPONSE_MAX_BYTES)
                .await
                .map_err(|_| KakaoError::AmbiguousProviderResult)?;
        if response.result_code == 0 {
            Ok((
                ShareOperationState::Success,
                SafeShareSummary {
                    requested_count: 1,
                    successful_count: 1,
                    failed_count: 0,
                },
            ))
        } else {
            Ok((
                ShareOperationState::Failed,
                SafeShareSummary {
                    requested_count: 1,
                    successful_count: 0,
                    failed_count: 1,
                },
            ))
        }
    }

    async fn access_token(&self, account_key: &str) -> Result<String, KakaoError> {
        self.access_token_for_scopes(account_key, &REQUIRED_SCOPES, false)
            .await
    }

    async fn memo_access_token(&self, account_key: &str) -> Result<String, KakaoError> {
        self.access_token_for_scopes(account_key, &["talk_message"], true)
            .await
    }

    async fn access_token_for_scopes(
        &self,
        account_key: &str,
        required_scopes: &[&str],
        allows_consent_incomplete: bool,
    ) -> Result<String, KakaoError> {
        let vault = TokenVault::from_env()?;
        let account = load_account(self.pool, account_key)
            .await?
            .ok_or(KakaoError::NotConnected)?;
        ensure_account_usable_for_scopes(&account, required_scopes, allows_consent_incomplete)?;
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
                set_account_status(self.pool, account_key, "reauth_required").await?;
                return Err(KakaoError::ReauthorizationRequired);
            }
        };
        let lease_id = acquire_refresh_lease(self.pool, account_key).await?;
        let refreshed = oauth_client(self.config)?
            .exchange_refresh_token(&RefreshToken::new(refresh_token))
            .request_async(http_client()?)
            .await;
        let refreshed = match refreshed {
            Ok(value) => value,
            Err(_) => {
                fail_refresh(self.pool, account_key, lease_id).await?;
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
            account_key,
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
                let _ = fail_refresh(self.pool, account_key, lease_id).await;
                return Err(KakaoError::ReauthorizationRequired);
            }
        };
        if !completed {
            return Err(KakaoError::ReauthorizationRequired);
        }
        if !scopes_present(&scopes, required_scopes) {
            set_account_status(self.pool, account_key, "consent_incomplete").await?;
            return Err(KakaoError::ConsentIncomplete);
        }
        Ok(updated_tokens.access_token.clone())
    }

    async fn subject_hash(&self, access_token: &str) -> Result<Vec<u8>, KakaoError> {
        let response = http_client()?
            .get(USER_ME_URL)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|_| KakaoError::OAuthExchange)?;
        if !response.status().is_success() {
            return Err(KakaoError::OAuthExchange);
        }
        let user = read_bounded_json::<KakaoUserMeResponse>(response, USER_ME_RESPONSE_MAX_BYTES)
            .await
            .map_err(|_| KakaoError::OAuthExchange)?;
        Ok(TokenVault::from_env()?.subject_hash(KAKAO_PROVIDER, user.id.to_string().as_bytes()))
    }

    async fn legacy_primary_key_for_subject(&self, subject_hash: &[u8]) -> String {
        let Ok(Some(primary)) = load_account(self.pool, PRIMARY_ACCOUNT_KEY).await else {
            return Uuid::new_v4().to_string();
        };
        if primary.subject_hash.is_some() {
            return Uuid::new_v4().to_string();
        }
        // A legacy primary row predates subject hashing. Best-effort identity
        // lookup keeps a same-account reconnect on primary without changing its
        // legacy AAD. Failure is non-destructive: the new OAuth account is kept
        // separately rather than risking a different account overwrite.
        let Ok(access_token) = self
            .access_token_for_scopes(PRIMARY_ACCOUNT_KEY, &[], true)
            .await
        else {
            return Uuid::new_v4().to_string();
        };
        match self.subject_hash(&access_token).await {
            Ok(primary_hash) if primary_hash == subject_hash => PRIMARY_ACCOUNT_KEY.to_string(),
            _ => Uuid::new_v4().to_string(),
        }
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
    let accounts = match list_accounts(pool).await {
        Ok(accounts) if accounts.is_empty() => {
            return KakaoConnectionStatus::simple(
                KakaoConnectionState::NotConnected,
                "not_connected",
            );
        }
        Ok(accounts) => accounts,
        Err(_) => {
            return KakaoConnectionStatus::simple(
                KakaoConnectionState::StorageUnavailable,
                "database_unavailable",
            );
        }
    };
    // Multi-account status represents the best usable local connection. A
    // broken legacy `primary` row must not hide a healthy secondary account.
    let mut representative: Option<(usize, KakaoConnectionState, Option<&'static str>)> = None;
    for (index, summary) in accounts.iter().enumerate() {
        let account = match load_account(pool, &summary.account_id).await {
            Ok(Some(account)) => account,
            Ok(None) => continue,
            Err(_) => {
                return KakaoConnectionStatus::simple(
                    KakaoConnectionState::StorageUnavailable,
                    "database_unavailable",
                );
            }
        };
        let (state, reason) = if account.decrypt_tokens(&vault).is_err() {
            (
                KakaoConnectionState::InvalidConfig,
                Some("token_decryption_failed"),
            )
        } else {
            let state = stored_account_connection_state(&account.status, &account.scopes);
            let reason = match state {
                KakaoConnectionState::Connected => None,
                KakaoConnectionState::ConsentIncomplete => Some("consent_incomplete"),
                KakaoConnectionState::ReauthorizationRequired => Some("reauth_required"),
                _ => Some("invalid_account_state"),
            };
            (state, reason)
        };
        if representative.is_none_or(|(_, current, _)| {
            connection_state_priority(state) < connection_state_priority(current)
        }) {
            representative = Some((index, state, reason));
        }
    }
    let Some((representative_index, state, reason)) = representative else {
        return KakaoConnectionStatus::simple(KakaoConnectionState::NotConnected, "not_connected");
    };
    let representative = &accounts[representative_index];
    KakaoConnectionStatus {
        state,
        reason,
        scopes: representative.scopes.clone(),
        access_expires_at: representative.access_expires_at,
        accounts,
    }
}

fn stored_account_connection_state(status: &str, scopes: &[String]) -> KakaoConnectionState {
    match status {
        "active" if required_scopes_present(scopes) => KakaoConnectionState::Connected,
        "active" | "consent_incomplete" => KakaoConnectionState::ConsentIncomplete,
        "reauth_required" => KakaoConnectionState::ReauthorizationRequired,
        _ => KakaoConnectionState::InvalidConfig,
    }
}

fn connection_state_priority(state: KakaoConnectionState) -> u8 {
    match state {
        KakaoConnectionState::Connected => 0,
        KakaoConnectionState::ConsentIncomplete => 1,
        KakaoConnectionState::ReauthorizationRequired => 2,
        KakaoConnectionState::InvalidConfig => 3,
        _ => 4,
    }
}

fn message_eligible_friend_views(friends: Vec<KakaoFriend>) -> Vec<FriendView> {
    friends
        .into_iter()
        .filter(|friend| friend.allowed_msg)
        .map(|friend| FriendView {
            uuid: friend.uuid,
            display_name: friend.profile_nickname,
        })
        .collect()
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
    scopes_present(scopes, &REQUIRED_SCOPES)
}

fn scopes_present(scopes: &[String], required_scopes: &[&str]) -> bool {
    required_scopes
        .iter()
        .all(|required| scopes.iter().any(|scope| scope == required))
}

fn ensure_account_usable_for_scopes(
    account: &OAuthAccountRecord,
    required_scopes: &[&str],
    allows_consent_incomplete: bool,
) -> Result<(), KakaoError> {
    match account.status.as_str() {
        "active" if scopes_present(&account.scopes, required_scopes) => Ok(()),
        "consent_incomplete"
            if allows_consent_incomplete && scopes_present(&account.scopes, required_scopes) =>
        {
            Ok(())
        }
        "active" | "consent_incomplete" => Err(KakaoError::ConsentIncomplete),
        "reauth_required" => Err(KakaoError::ReauthorizationRequired),
        _ => Err(KakaoError::NotConnected),
    }
}

pub fn validate_account_key(account_key: &str) -> Result<(), KakaoError> {
    if account_key == PRIMARY_ACCOUNT_KEY || Uuid::parse_str(account_key).is_ok() {
        Ok(())
    } else {
        Err(KakaoError::InvalidAccount)
    }
}

fn validate_send_request(
    idempotency_key: &str,
    request: &KakaoFriendShareCommand,
) -> Result<(), KakaoError> {
    validate_idempotency_key(idempotency_key)?;
    validate_friend_share_payload(request)
}

fn validate_memo_send_request(
    idempotency_key: &str,
    request: &KakaoMemoSendCommand,
) -> Result<(), KakaoError> {
    validate_idempotency_key(idempotency_key)?;
    validate_memo_send_payload(request)
}

fn validate_idempotency_key(idempotency_key: &str) -> Result<(), KakaoError> {
    if !(8..=128).contains(&idempotency_key.len())
        || !idempotency_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(KakaoError::Validation("Idempotency-Key is invalid"));
    }
    Ok(())
}

/// Validate the provider payload independently from HTTP idempotency metadata.
/// Scheduled delivery uses this at reservation time, then derives a stable
/// idempotency key from the durable outbox id when the provider worker runs.
pub fn validate_friend_share_payload(request: &KakaoFriendShareCommand) -> Result<(), KakaoError> {
    if !request.confirmed {
        return Err(KakaoError::Validation("confirmed must be true"));
    }
    validate_friend_share_recipients(&request.receiver_uuids)?;
    validate_friend_share_text(&request.text)?;
    validate_kakao_image_url(request.image_url.as_deref())
}

/// Validate an operator-confirmed self-chatroom payload independently from its
/// idempotency metadata so scheduled outbox workers can replay it safely.
pub fn validate_memo_send_payload(request: &KakaoMemoSendCommand) -> Result<(), KakaoError> {
    if !request.confirmed {
        return Err(KakaoError::Validation("confirmed must be true"));
    }
    validate_friend_share_text(&request.text)?;
    validate_kakao_image_url(request.image_url.as_deref())
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

/// Kakao fetches feed images itself. Restrict the URL to a bounded public HTTPS
/// location so an operator cannot accidentally hand a private network address
/// or a local scheduled attachment blob to the external provider.
pub fn validate_kakao_image_url(image_url: Option<&str>) -> Result<(), KakaoError> {
    let Some(image_url) = image_url else {
        return Ok(());
    };
    if image_url.len() > 2_048 || image_url.trim() != image_url {
        return Err(KakaoError::Validation(
            "image_url must be a public HTTPS URL",
        ));
    }
    let url = reqwest::Url::parse(image_url)
        .map_err(|_| KakaoError::Validation("image_url must be a public HTTPS URL"))?;
    let host = url.host_str();
    if url.scheme() != "https"
        || host.is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.port().is_some()
        || host.is_some_and(|value| value.eq_ignore_ascii_case("localhost"))
        || host.is_some_and(is_private_ip_literal)
    {
        return Err(KakaoError::Validation(
            "image_url must be a public HTTPS URL",
        ));
    }
    Ok(())
}

fn is_private_ip_literal(host: &str) -> bool {
    let Ok(address) = host.parse::<std::net::IpAddr>() else {
        return false;
    };
    match address {
        std::net::IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || matches!(
                    octets,
                    [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
                )
        }
        std::net::IpAddr::V6(address) => {
            let segments = address.segments();
            address.is_loopback()
                || address.is_unspecified()
                || address.is_unique_local()
                || address.is_unicast_link_local()
                || address.is_multicast()
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

fn request_fingerprint(request: &KakaoFriendShareCommand, landing_url: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"agentdesk/kakao-friend-share/request/v1\0");
    update_length_prefixed(&mut hasher, request.text.as_bytes());
    update_optional_length_prefixed(&mut hasher, request.image_url.as_deref());
    update_length_prefixed(&mut hasher, landing_url.as_bytes());
    let mut recipients = request.receiver_uuids.iter().collect::<Vec<_>>();
    recipients.sort_unstable();
    for recipient in recipients {
        update_length_prefixed(&mut hasher, recipient.as_bytes());
    }
    hasher.finalize().to_vec()
}

fn memo_request_fingerprint(request: &KakaoMemoSendCommand, landing_url: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"agentdesk/kakao-memo/request/v1\0");
    update_length_prefixed(&mut hasher, request.text.as_bytes());
    update_optional_length_prefixed(&mut hasher, request.image_url.as_deref());
    update_length_prefixed(&mut hasher, landing_url.as_bytes());
    hasher.finalize().to_vec()
}

fn message_template(text: &str, image_url: Option<&str>, landing_url: &str) -> String {
    let link = json!({
        "web_url": landing_url,
        "mobile_web_url": landing_url
    });
    match image_url {
        Some(image_url) => json!({
            "object_type": "feed",
            "content": {
                "title": feed_title(text),
                "description": text,
                "image_url": image_url,
                "link": link
            },
            "button_title": "문서 보기"
        })
        .to_string(),
        None => json!({
            "object_type": "text",
            "text": text,
            "link": link
        })
        .to_string(),
    }
}

fn feed_title(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("예약 메시지")
        .chars()
        .take(50)
        .collect()
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn update_optional_length_prefixed(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            update_length_prefixed(hasher, value.as_bytes());
        }
        None => hasher.update([0]),
    }
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
            image_url: None,
            confirmed: true,
        }
    }

    #[test]
    fn friend_projection_excludes_provider_ineligible_recipients() {
        let projected = message_eligible_friend_views(vec![
            KakaoFriend {
                uuid: "eligible".to_string(),
                profile_nickname: "Eligible".to_string(),
                allowed_msg: true,
            },
            KakaoFriend {
                uuid: "blocked".to_string(),
                profile_nickname: "Blocked".to_string(),
                allowed_msg: false,
            },
        ]);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].uuid, "eligible");
    }

    #[test]
    fn multi_account_connection_state_prefers_a_usable_sender() {
        let required = REQUIRED_SCOPES.map(str::to_string).to_vec();
        assert_eq!(
            stored_account_connection_state("active", &required),
            KakaoConnectionState::Connected
        );
        assert!(
            connection_state_priority(KakaoConnectionState::Connected)
                < connection_state_priority(KakaoConnectionState::ReauthorizationRequired)
        );
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
    fn validates_memo_send_without_friend_recipients() {
        let valid = KakaoMemoSendCommand {
            text: "self test".to_string(),
            image_url: None,
            confirmed: true,
        };
        assert!(validate_memo_send_request("memo-send-123", &valid).is_ok());
        assert!(validate_memo_send_request("short", &valid).is_err());
        assert!(
            validate_memo_send_request(
                "memo-send-123",
                &KakaoMemoSendCommand {
                    text: " ".to_string(),
                    image_url: None,
                    confirmed: true,
                },
            )
            .is_err()
        );
        assert!(
            validate_memo_send_payload(&KakaoMemoSendCommand {
                text: "feed memo".to_string(),
                image_url: Some("https://example.com/thumbnail.jpg".to_string()),
                confirmed: true,
            })
            .is_ok()
        );
        assert!(
            validate_memo_send_payload(&KakaoMemoSendCommand {
                text: "unsafe feed memo".to_string(),
                image_url: Some("http://127.0.0.1/image.jpg".to_string()),
                confirmed: true,
            })
            .is_err()
        );
        assert!(
            validate_memo_send_payload(&KakaoMemoSendCommand {
                text: "private feed memo".to_string(),
                image_url: Some("https://192.168.0.2/image.jpg".to_string()),
                confirmed: true,
            })
            .is_err()
        );
        assert!(
            validate_memo_send_request(
                "memo-send-123",
                &KakaoMemoSendCommand {
                    text: "hello".to_string(),
                    image_url: None,
                    confirmed: false,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn memo_fingerprint_is_distinct_from_friend_send() {
        let memo = KakaoMemoSendCommand {
            text: "hello".to_string(),
            image_url: None,
            confirmed: true,
        };
        assert_ne!(
            memo_request_fingerprint(&memo, "https://example.com"),
            request_fingerprint(&request(&["friend-a"], "hello"), "https://example.com"),
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
    fn feed_template_is_used_only_for_a_validated_image_url() {
        let feed: serde_json::Value = serde_json::from_str(&message_template(
            "소복이 D-7 알림",
            Some("https://example.com/thumbnail.jpg"),
            "https://universe.vr11.net/Docs/",
        ))
        .unwrap();
        assert_eq!(feed["object_type"], "feed");
        assert_eq!(
            feed["content"]["image_url"],
            "https://example.com/thumbnail.jpg"
        );
        assert_eq!(
            feed["content"]["link"]["web_url"],
            "https://universe.vr11.net/Docs/"
        );

        let text: serde_json::Value = serde_json::from_str(&message_template(
            "소복이 D-7 알림",
            None,
            "https://universe.vr11.net/Docs/",
        ))
        .unwrap();
        assert_eq!(text["object_type"], "text");
        assert!(text.get("content").is_none());
    }

    #[test]
    fn image_url_validation_rejects_private_and_credentialed_locations() {
        for invalid in [
            "https://127.0.0.1/image.jpg",
            "https://[::1]/image.jpg",
            "https://user@example.com/image.jpg",
            "https://example.com:8443/image.jpg",
        ] {
            assert!(
                validate_kakao_image_url(Some(invalid)).is_err(),
                "{invalid}"
            );
        }
        assert!(validate_kakao_image_url(Some("https://cdn.example.com/image.jpg")).is_ok());
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
