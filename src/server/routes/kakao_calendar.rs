//! Single trusted-operator API. Explicit Bearer proof is required even in local/no-auth mode.
use super::AppState;
use crate::db::calendar_sync as db;
use crate::error::{AppError, ErrorCode};
use crate::services::{calendar_sync as service, kakao::account};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

type ApiResult = Result<(StatusCode, Json<Value>), AppError>;

fn operator<'a>(state: &'a AppState, headers: &HeaderMap) -> Result<&'a sqlx::PgPool, AppError> {
    verify_operator_token(state.config.server.auth_token.as_deref(), headers)?;
    if state.config.cluster.enabled {
        return Err(AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Config,
            "calendar supports a single credential-owning node",
        ));
    }
    state
        .pg_pool_ref()
        .ok_or_else(|| AppError::internal("PostgreSQL unavailable"))
}

fn verify_operator_token(expected: Option<&str>, headers: &HeaderMap) -> Result<(), AppError> {
    let expected = expected.filter(|value| !value.is_empty());
    let presented = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if !expected
        .zip(presented)
        .is_some_and(|(a, b)| crate::utils::auth::constant_time_token_eq(a, b))
    {
        return Err(AppError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Config,
            "calendar requires operator Bearer authentication",
        ));
    }
    Ok(())
}

fn key(headers: &HeaderMap) -> Result<&str, AppError> {
    headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic()))
        .ok_or_else(|| {
            AppError::bad_request(
                "Idempotency-Key with 1 to 128 printable ASCII characters required",
            )
        })
}

fn map_error(error: service::CalendarError) -> AppError {
    match error {
        service::CalendarError::Invalid(message) => AppError::bad_request(message),
        service::CalendarError::Storage(db::CalendarDbError::NotFound) => {
            AppError::not_found("managed event not found")
        }
        service::CalendarError::Storage(
            db::CalendarDbError::Conflict | db::CalendarDbError::Binding,
        ) => AppError::conflict("request, revision or account binding conflict"),
        service::CalendarError::Storage(_) => AppError::internal("calendar storage unavailable"),
        service::CalendarError::Provider(error) => AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Config,
            service::error_code(&error),
        ),
    }
}
fn storage(error: db::CalendarDbError) -> AppError {
    map_error(error.into())
}
fn ok(value: Value) -> ApiResult {
    Ok((StatusCode::OK, Json(value)))
}
fn accepted(receipt: db::Receipt) -> ApiResult {
    Ok((StatusCode::ACCEPTED, Json(json!(receipt))))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn calendar_requires_real_bearer_not_claimed_identity_or_local_origin() {
        let mut headers = HeaderMap::new();
        headers.insert("x-agent-id", "admin".parse().unwrap());
        headers.insert("x-channel-id", "trusted".parse().unwrap());
        headers.insert("origin", "http://localhost:8080".parse().unwrap());
        assert!(verify_operator_token(None, &headers).is_err());
        assert!(verify_operator_token(Some("test-operator"), &headers).is_err());
        headers.insert("authorization", "Bearer wrong".parse().unwrap());
        assert!(verify_operator_token(Some("test-operator"), &headers).is_err());
        headers.insert("authorization", "Bearer test-operator".parse().unwrap());
        assert!(verify_operator_token(Some("test-operator"), &headers).is_ok());
        assert!(verify_operator_token(Some(""), &headers).is_err());
    }
    #[test]
    fn calendar_mutations_require_stable_nonempty_bounded_keys() {
        let mut headers = HeaderMap::new();
        assert!(key(&headers).is_err());
        headers.insert(
            "idempotency-key",
            "turn-123-calendar-create".parse().unwrap(),
        );
        assert_eq!(key(&headers).unwrap(), "turn-123-calendar-create");
        headers.insert("idempotency-key", " ".parse().unwrap());
        assert!(key(&headers).is_err());
    }
}

pub async fn accounts(State(state): State<AppState>, headers: HeaderMap) -> ApiResult {
    let pool = operator(&state, &headers)?;
    let ids = account::calendar_accounts().map_err(|e| map_error(e.into()))?;
    let mut accounts = Vec::new();
    for id in ids {
        let checked: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT checked_at FROM kakao_calendar_bindings WHERE account_id=$1",
        )
        .bind(&id)
        .fetch_optional(pool)
        .await
        .map_err(|_| AppError::internal("calendar storage unavailable"))?;
        accounts.push(json!({"accountId":id,"configured":true,"credentialVerified":false,"lastCheckedAt":checked,"nextAction":"POST account check for current identity and consent verification"}));
    }
    ok(json!({"accounts":accounts,"credentialBoundary":"single-node Unix private token store"}))
}

pub async fn check_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account): Path<String>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    service::check_account(pool, &account)
        .await
        .map_err(map_error)?;
    ok(
        json!({"accountId":account,"credentialVerified":true,"calendarConsent":true,"checkedAt":chrono::Utc::now()}),
    )
}

pub async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<service::CreateEvent>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    accepted(
        service::create(pool, key(&headers)?, body)
            .await
            .map_err(map_error)?,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    before: Option<Uuid>,
    limit: Option<i64>,
}
pub async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<Page>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    let accounts = account::calendar_accounts().map_err(|e| map_error(e.into()))?;
    let limit = page.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(AppError::bad_request("limit must be 1 to 100"));
    }
    let ids = db::list(pool, &accounts, page.before, limit + 1)
        .await
        .map_err(storage)?;
    let mut events = Vec::new();
    for id in ids.iter().take(limit as usize) {
        events.push(db::get(pool, *id).await.map_err(storage)?);
    }
    let next = if ids.len() > limit as usize {
        ids.get(limit as usize - 1)
    } else {
        None
    };
    ok(json!({"events":events,"nextCursor":next}))
}
pub async fn get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event): Path<Uuid>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    service::authorize_event(pool, event)
        .await
        .map_err(map_error)?;
    ok(db::get(pool, event).await.map_err(storage)?)
}
pub async fn patch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event): Path<Uuid>,
    Json(body): Json<Value>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    accepted(
        service::mutate(pool, event, key(&headers)?, body, false)
            .await
            .map_err(map_error)?,
    )
}
pub async fn delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event): Path<Uuid>,
    Json(body): Json<Value>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    accepted(
        service::mutate(pool, event, key(&headers)?, body, true)
            .await
            .map_err(map_error)?,
    )
}
pub async fn operations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event): Path<Uuid>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    service::authorize_event(pool, event)
        .await
        .map_err(map_error)?;
    ok(json!({"operations":db::operations(pool,event).await.map_err(storage)?,"limit":200}))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Recovery {
    resolution: String,
    remote_event_id: Option<String>,
    note: String,
    credential_owner_restarted: bool,
}

pub async fn recover(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((event, operation)): Path<(Uuid, Uuid)>,
    Json(body): Json<Recovery>,
) -> ApiResult {
    let pool = operator(&state, &headers)?;
    service::authorize_event(pool, event)
        .await
        .map_err(map_error)?;
    if body.note.trim().len() < 10 || body.note.len() > 1000 {
        return Err(AppError::bad_request(
            "recovery requires a 10 to 1000 byte non-secret audit note",
        ));
    }
    if !["retry", "adopt", "confirm_not_applied"].contains(&body.resolution.as_str())
        || (body.resolution != "adopt" && body.remote_event_id.is_some())
    {
        return Err(AppError::bad_request(
            "invalid recovery resolution or unexpected remoteEventId",
        ));
    }
    if body.resolution != "retry" && !body.credential_owner_restarted {
        return Err(AppError::bad_request(
            "stop and restart the old credential owner before resolving uncertainty",
        ));
    }
    let claim = db::recovery_target(pool, event, operation)
        .await
        .map_err(storage)?;
    let binding = service::check_account(pool, &claim.account_id)
        .await
        .map_err(map_error)?;
    if binding.app_id != claim.app_id || binding.user_id != claim.user_id {
        return Err(AppError::conflict("account binding changed"));
    }
    if body.resolution == "adopt" {
        let id = body
            .remote_event_id
            .as_deref()
            .filter(|s| !s.is_empty() && s.len() <= 512)
            .ok_or_else(|| AppError::bad_request("remoteEventId required for adoption"))?;
        if claim.remote_id.as_deref().is_some_and(|known| known != id) {
            return Err(AppError::conflict(
                "recovery cannot replace a known remote event",
            ));
        }
        let client = account::shared_client(&claim.account_id, true)
            .await
            .map_err(|e| map_error(e.into()))?;
        let detail = client
            .calendar_detail(id)
            .await
            .map_err(|e| map_error(e.into()))?;
        let expected: service::model::EventContent = serde_json::from_value(claim.snapshot.clone())
            .map_err(|_| AppError::internal("invalid stored intent"))?;
        // Full provider verification is restricted to recover; CRUD never accepts a remote ID.
        expected.validate().map_err(AppError::bad_request)?;
        if !crate::services::kakao::calendar::matches_adoption(
            &detail,
            id,
            &expected.provider_json(),
        ) {
            return Err(AppError::conflict(
                "remote event identity, calendar or content could not be verified",
            ));
        }
    }
    db::recover(
        pool,
        &claim,
        &body.resolution,
        body.remote_event_id.as_deref(),
        &body.note,
    )
    .await
    .map_err(storage)?;
    ok(json!({"operationId":operation,"resolution":body.resolution}))
}
