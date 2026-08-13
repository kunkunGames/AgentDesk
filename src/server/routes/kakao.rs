use axum::{
    Json,
    extract::{
        Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Redirect, Response},
};
use serde::{Deserialize, Serialize};

use super::AppState;
use crate::error::{AppError, ErrorCode};
use crate::services::external_share::ExternalShareError;
use crate::services::kakao::{
    FriendsPage, KakaoError, KakaoFriendShareCommand, KakaoFriendShareService,
    KakaoMemoSendCommand, OAuthStart,
};
use crate::services::oauth_connection::{OAuthAccountSummary, OAuthConnectionError};

const CALLBACK_OK: &str = "/settings?connector=kakao_friend_share&oauth=ok";

#[derive(Debug, Deserialize)]
pub struct OAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    #[allow(dead_code)]
    error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FriendsQuery {
    account_id: String,
    #[serde(default)]
    offset: u32,
    #[serde(default = "default_friend_limit")]
    limit: u32,
}

#[derive(Debug, Serialize)]
pub struct DisconnectResponse {
    ok: bool,
    account_id: String,
    remote_unlinked: bool,
}

#[derive(Debug, Serialize)]
pub struct AccountsResponse {
    accounts: Vec<OAuthAccountSummary>,
}

#[derive(Debug, Deserialize)]
pub struct KakaoFriendMessageRequest {
    account_id: String,
    receiver_uuids: Vec<String>,
    text: String,
    #[serde(default)]
    confirmed: bool,
}

#[derive(Debug, Deserialize)]
pub struct KakaoMemoMessageRequest {
    account_id: String,
    text: String,
    #[serde(default)]
    confirmed: bool,
}

pub struct KakaoRouteError {
    error: AppError,
    retry_after_seconds: Option<u64>,
}

impl KakaoRouteError {
    fn validation(message: impl Into<String>, operation: &'static str) -> Self {
        Self {
            error: AppError::bad_request(message).with_operation(operation),
            retry_after_seconds: None,
        }
    }
}

impl IntoResponse for KakaoRouteError {
    fn into_response(self) -> Response {
        let mut response = self.error.into_response();
        if let Some(seconds) = self.retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        response
    }
}

/// GET /api/kakao/oauth/callback
pub async fn oauth_callback(
    State(state): State<AppState>,
    query: Result<Query<OAuthCallbackQuery>, QueryRejection>,
) -> Redirect {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return callback_error("invalid_state"),
    };
    let Some(callback_state) = query.state.as_deref() else {
        return callback_error("invalid_state");
    };
    let service = match KakaoFriendShareService::new(
        state.pg_pool_ref(),
        &state.config.integrations.kakao_friend_share,
    ) {
        Ok(service) => service,
        Err(_) => return callback_error("internal"),
    };

    if query.error.is_some() {
        return match service.consume_denied_oauth(callback_state).await {
            Ok(()) => callback_error("denied"),
            Err(KakaoError::InvalidOAuthState) => callback_error("invalid_state"),
            Err(_) => callback_error("internal"),
        };
    }
    let Some(code) = query.code.as_deref() else {
        return callback_error("invalid_state");
    };
    match service.complete_oauth(callback_state, code).await {
        Ok(completion) => Redirect::to(&format!("{CALLBACK_OK}&account={}", completion.account_id)),
        Err(KakaoError::InvalidOAuthState) => callback_error("invalid_state"),
        Err(KakaoError::ConsentIncomplete) => callback_error("consent"),
        Err(KakaoError::OAuthExchange) => callback_error("token_exchange"),
        Err(_) => callback_error("internal"),
    }
}

/// POST /api/kakao/oauth/start
pub async fn start_oauth(
    State(state): State<AppState>,
) -> Result<Json<OAuthStart>, KakaoRouteError> {
    service(&state, "kakao.oauth.start")?
        .start_oauth()
        .await
        .map(Json)
        .map_err(|error| map_error(error, "kakao.oauth.start"))
}

/// GET /api/kakao/accounts
pub async fn list_accounts(
    State(state): State<AppState>,
) -> Result<Json<AccountsResponse>, KakaoRouteError> {
    service(&state, "kakao.accounts.list")?
        .accounts()
        .await
        .map(|accounts| Json(AccountsResponse { accounts }))
        .map_err(|error| map_error(error, "kakao.accounts.list"))
}

/// DELETE /api/kakao/accounts/{account_id}
pub async fn disconnect(
    State(state): State<AppState>,
    axum::extract::Path(account_id): axum::extract::Path<String>,
) -> Result<Json<DisconnectResponse>, KakaoRouteError> {
    let deleted = service(&state, "kakao.connection.disconnect")?
        .disconnect(&account_id)
        .await
        .map_err(|error| map_error(error, "kakao.connection.disconnect"))?;
    if !deleted {
        return Err(KakaoRouteError {
            error: AppError::not_found("Kakao account is not connected")
                .with_operation("kakao.connection.disconnect"),
            retry_after_seconds: None,
        });
    }
    Ok(Json(DisconnectResponse {
        ok: true,
        account_id,
        remote_unlinked: false,
    }))
}

/// GET /api/kakao/friends
pub async fn list_friends(
    State(state): State<AppState>,
    Query(query): Query<FriendsQuery>,
) -> Result<Json<FriendsPage>, KakaoRouteError> {
    service(&state, "kakao.friends.list")?
        .list_friends(&query.account_id, query.offset, query.limit)
        .await
        .map(Json)
        .map_err(|error| map_error(error, "kakao.friends.list"))
}

/// POST /api/kakao/messages/send
pub async fn send_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<KakaoFriendMessageRequest>, JsonRejection>,
) -> Result<Json<crate::services::external_share::ShareOperationResult>, KakaoRouteError> {
    let operation = "kakao.messages.send";
    let Json(body) =
        body.map_err(|_| KakaoRouteError::validation("invalid JSON body", operation))?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            KakaoRouteError::validation("Idempotency-Key header is required", operation)
        })?;
    let account_id = body.account_id.clone();
    let command = KakaoFriendShareCommand {
        receiver_uuids: body.receiver_uuids,
        text: body.text,
        confirmed: body.confirmed,
    };
    service(&state, operation)?
        .send_friend_message(&account_id, idempotency_key, command)
        .await
        .map(Json)
        .map_err(|error| map_error(error, operation))
}

/// POST /api/kakao/messages/send-to-me
pub async fn send_memo_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<KakaoMemoMessageRequest>, JsonRejection>,
) -> Result<Json<crate::services::external_share::ShareOperationResult>, KakaoRouteError> {
    let operation = "kakao.messages.send_to_me";
    let Json(body) =
        body.map_err(|_| KakaoRouteError::validation("invalid JSON body", operation))?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            KakaoRouteError::validation("Idempotency-Key header is required", operation)
        })?;
    let account_id = body.account_id.clone();
    let command = KakaoMemoSendCommand {
        text: body.text,
        confirmed: body.confirmed,
    };
    service(&state, operation)?
        .send_memo_message(&account_id, idempotency_key, command)
        .await
        .map(Json)
        .map_err(|error| map_error(error, operation))
}

fn service<'a>(
    state: &'a AppState,
    operation: &'static str,
) -> Result<KakaoFriendShareService<'a>, KakaoRouteError> {
    KakaoFriendShareService::new(
        state.pg_pool_ref(),
        &state.config.integrations.kakao_friend_share,
    )
    .map_err(|error| map_error(error, operation))
}

fn callback_error(reason: &'static str) -> Redirect {
    Redirect::to(&format!(
        "/settings?connector=kakao_friend_share&oauth=error&reason={reason}"
    ))
}

fn map_error(error: KakaoError, operation: &'static str) -> KakaoRouteError {
    let (app_error, retry_after_seconds) = match error {
        KakaoError::Validation(message) => (AppError::bad_request(message), None),
        KakaoError::OperationInProgress => (
            AppError::conflict("Kakao operation is already in progress")
                .with_context("retry_after_seconds", 20),
            Some(20),
        ),
        KakaoError::Connection(OAuthConnectionError::RefreshInProgress) => (
            AppError::conflict("Kakao token refresh is already in progress")
                .with_context("retry_after_seconds", 2),
            Some(2),
        ),
        KakaoError::NotConnected => (
            AppError::conflict("Kakao connection is not established"),
            None,
        ),
        KakaoError::InvalidAccount => (
            AppError::bad_request("Kakao account selection is invalid"),
            None,
        ),
        KakaoError::AccountInUse => (
            AppError::conflict("Kakao account is referenced by scheduled delivery"),
            None,
        ),
        KakaoError::ConsentIncomplete => (AppError::conflict("Kakao consent is incomplete"), None),
        KakaoError::ReauthorizationRequired => (
            AppError::conflict("Kakao authorization must be renewed"),
            None,
        ),
        KakaoError::Disabled
        | KakaoError::MissingConfig
        | KakaoError::Connection(OAuthConnectionError::MissingTokenKey)
        | KakaoError::Connection(OAuthConnectionError::InvalidTokenKey)
        | KakaoError::Connection(OAuthConnectionError::Decrypt) => (
            AppError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::Config,
                "Kakao integration configuration is unavailable",
            ),
            None,
        ),
        KakaoError::MissingDatabase => (
            AppError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::Database,
                "Kakao integration storage is unavailable",
            ),
            None,
        ),
        KakaoError::Connection(OAuthConnectionError::Database(_))
        | KakaoError::ExternalShare(ExternalShareError::Database(_)) => (
            AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Database,
                "Kakao integration storage is unavailable",
            ),
            None,
        ),
        KakaoError::ExternalShare(ExternalShareError::IdempotencyConflict) => (
            AppError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorCode::Validation,
                "Idempotency-Key was already used for a different request",
            )
            .with_context("reason", "fingerprint_mismatch"),
            None,
        ),
        KakaoError::ExternalShare(ExternalShareError::RateLimited) => (
            AppError::new(
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::Policy,
                "Kakao send safety limit reached",
            ),
            Some(3_600),
        ),
        KakaoError::Provider | KakaoError::AmbiguousProviderResult => (
            AppError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::Dispatch,
                "Kakao provider request failed",
            ),
            None,
        ),
        KakaoError::InvalidOAuthRequest
        | KakaoError::InvalidOAuthState
        | KakaoError::OAuthExchange => (
            AppError::bad_request("Kakao OAuth request is invalid"),
            None,
        ),
        KakaoError::Connection(OAuthConnectionError::Encrypt)
        | KakaoError::Connection(OAuthConnectionError::InvalidTokenPayload)
        | KakaoError::Connection(OAuthConnectionError::AccountIdentityConflict)
        | KakaoError::ExternalShare(ExternalShareError::CorruptOperation) => (
            AppError::internal("Kakao integration encountered an internal error"),
            None,
        ),
    };
    KakaoRouteError {
        error: app_error.with_operation(operation),
        retry_after_seconds,
    }
}

fn default_friend_limit() -> u32 {
    20
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_error_never_reflects_provider_input() {
        let response = callback_error("invalid_state").into_response();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get("location").unwrap(),
            "/settings?connector=kakao_friend_share&oauth=error&reason=invalid_state"
        );
    }

    #[test]
    fn friends_query_defaults_are_bounded() {
        assert_eq!(default_friend_limit(), 20);
    }
}
