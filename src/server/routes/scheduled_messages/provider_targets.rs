//! Provider-target request validation and encrypted-plan construction.
//!
//! The scheduled-message route owns HTTP semantics; this module keeps the
//! provider-specific request contract out of the general reservation handler.

use std::fmt;

use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};
use sqlx::PgPool;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::config::KakaoFriendShareConfig;
use crate::db::scheduled_messages as db;
use crate::db::scheduled_messages::{EncryptedExternalDeliveryPlan, ScheduledMessagePatch};
use crate::error::AppError;
use crate::services::kakao::{
    KakaoFriendShareCommand, validate_friend_share_payload, validate_friend_share_text,
};
use crate::services::oauth_connection::TokenVault;
use crate::services::scheduled_messages::external_delivery::encrypt_kakao_provider_target;

use super::app_error;

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduledProviderTargetsBody {
    pub kakao_friend_share: ScheduledKakaoFriendShareTargetBody,
}

impl fmt::Debug for ScheduledProviderTargetsBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScheduledProviderTargetsBody")
            .field("kakao_friend_share_enabled", &true)
            .field(
                "kakao_friend_share_recipient_count",
                &self.kakao_friend_share.receiver_uuids.len(),
            )
            .field(
                "kakao_friend_share_confirmed",
                &self.kakao_friend_share.confirmed,
            )
            .field("kakao_send_to_me", &self.kakao_friend_share.send_to_me)
            .field(
                "kakao_image_feed_enabled",
                &self.kakao_friend_share.image_url.is_some(),
            )
            .finish()
    }
}

#[derive(Deserialize, Zeroize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduledKakaoFriendShareTargetBody {
    pub account_id: String,
    pub receiver_uuids: Vec<String>,
    #[serde(default)]
    pub send_to_me: bool,
    #[serde(default)]
    pub image_url: Option<String>,
    #[serde(default)]
    pub confirmed: bool,
}

pub(super) fn prepare_provider_targets(
    provider_targets: Option<&ScheduledProviderTargetsBody>,
    content: &str,
    delivery_kind: &str,
    kakao_config: &KakaoFriendShareConfig,
) -> Result<Option<EncryptedExternalDeliveryPlan>, AppError> {
    let Some(provider_targets) = provider_targets else {
        return Ok(None);
    };
    if delivery_kind != db::KIND_PUSH {
        return Err(app_error(
            StatusCode::BAD_REQUEST,
            "providerTargets is only valid for push delivery",
        ));
    }
    if !kakao_config.enabled {
        return Err(app_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Kakao friend share must be enabled before scheduling a provider target",
        ));
    }
    let target = &provider_targets.kakao_friend_share;
    let command = KakaoFriendShareCommand {
        receiver_uuids: target.receiver_uuids.clone(),
        text: content.to_string(),
        image_url: target.image_url.clone(),
        confirmed: target.confirmed,
    };
    validate_friend_share_payload(&command).map_err(|error| {
        app_error(
            StatusCode::BAD_REQUEST,
            format!("invalid providerTargets.kakaoFriendShare: {error}"),
        )
    })?;
    let vault = TokenVault::from_env().map_err(|_| {
        app_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "OAuth token encryption key is required for scheduled provider targets",
        )
    })?;
    crate::services::kakao::validate_account_key(&target.account_id).map_err(|_| {
        app_error(
            StatusCode::BAD_REQUEST,
            "providerTargets.kakaoFriendShare.accountId is invalid",
        )
    })?;
    encrypt_kakao_provider_target(
        &vault,
        target.account_id.clone(),
        target.receiver_uuids.clone(),
        target.send_to_me,
        target.image_url.clone(),
    )
    .map(Some)
    .map_err(|_| {
        app_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encrypt scheduled provider targets",
        )
    })
}

pub(super) async fn ensure_provider_target_account_connected(
    pool: &PgPool,
    plan: Option<&EncryptedExternalDeliveryPlan>,
) -> Result<(), AppError> {
    let Some(plan) = plan else {
        return Ok(());
    };
    match crate::services::oauth_connection::load_account(pool, &plan.account_key).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(app_error(
            StatusCode::BAD_REQUEST,
            "providerTargets.kakaoFriendShare.accountId is not connected",
        )),
        Err(error) => Err(app_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("load Kakao provider-target account: {error}"),
        )),
    }
}

pub(super) fn patch_provider_targets(
    body: &Map<String, JsonValue>,
    content: &str,
    delivery_kind: &str,
    kakao_config: &KakaoFriendShareConfig,
) -> Result<Option<Option<EncryptedExternalDeliveryPlan>>, AppError> {
    let Some(value) = body.get("providerTargets") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(Some(None));
    }
    let parsed: ScheduledProviderTargetsBody =
        serde_json::from_value(value.clone()).map_err(|error| {
            app_error(
                StatusCode::BAD_REQUEST,
                format!("providerTargets must be an object or null: {error}"),
            )
        })?;
    Ok(Some(prepare_provider_targets(
        Some(&parsed),
        content,
        delivery_kind,
        kakao_config,
    )?))
}

pub(super) fn validate_kakao_content_if_targeted(content: &str) -> Result<(), AppError> {
    validate_friend_share_text(content).map_err(|_| {
        app_error(
            StatusCode::BAD_REQUEST,
            "content must contain 1 to 200 characters while a Kakao provider target is enabled",
        )
    })
}

pub(super) fn provider_delivery_intent_changed(
    patch: &ScheduledMessagePatch,
    existing_content: &str,
    existing_scheduled_at: chrono::DateTime<chrono::Utc>,
    existing_schedule: Option<&str>,
    existing_timezone: &str,
    existing_expires_at: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    patch
        .content
        .as_ref()
        .is_some_and(|content| content != existing_content)
        || patch
            .scheduled_at
            .is_some_and(|scheduled_at| scheduled_at != existing_scheduled_at)
        || patch
            .schedule
            .as_ref()
            .is_some_and(|schedule| schedule.as_deref() != existing_schedule)
        || patch
            .timezone
            .as_ref()
            .is_some_and(|timezone| timezone != existing_timezone)
        || patch
            .expires_at
            .is_some_and(|expires_at| expires_at != existing_expires_at)
}

pub(super) async fn ensure_external_delivery_rollout_ready(
    pool: &PgPool,
    cluster_enabled: bool,
) -> Result<(), AppError> {
    if !cluster_enabled {
        return Ok(());
    }
    match db::external_delivery_rollout_ready_pg(pool).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(app_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "scheduled provider targets require every online worker to advertise external_delivery_consumer_v1",
        )),
        Err(error) => Err(app_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("check scheduled provider-target rollout readiness: {error}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn targets(confirmed: bool, recipients: &[&str]) -> ScheduledProviderTargetsBody {
        ScheduledProviderTargetsBody {
            kakao_friend_share: ScheduledKakaoFriendShareTargetBody {
                account_id: "primary".to_string(),
                receiver_uuids: recipients
                    .iter()
                    .map(|recipient| (*recipient).to_string())
                    .collect(),
                send_to_me: false,
                image_url: None,
                confirmed,
            },
        }
    }

    #[test]
    fn provider_target_requires_push_confirmation_and_valid_kakao_content() {
        let disabled = KakaoFriendShareConfig::default();
        let non_push = prepare_provider_targets(
            Some(&targets(true, &["recipient-a"])),
            "hello",
            db::KIND_AGENT,
            &disabled,
        )
        .expect_err("provider fan-out is push-only");
        assert_eq!(non_push.status(), StatusCode::BAD_REQUEST);

        let enabled = KakaoFriendShareConfig {
            enabled: true,
            ..KakaoFriendShareConfig::default()
        };
        let unconfirmed = prepare_provider_targets(
            Some(&targets(false, &["recipient-a"])),
            "hello",
            db::KIND_PUSH,
            &enabled,
        )
        .expect_err("scheduled Kakao target needs explicit confirmation");
        assert_eq!(unconfirmed.status(), StatusCode::BAD_REQUEST);

        let oversized = prepare_provider_targets(
            Some(&targets(true, &["recipient-a"])),
            &"가".repeat(201),
            db::KIND_PUSH,
            &enabled,
        )
        .expect_err("scheduled Kakao content uses the connector limit");
        assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);

        let duplicate_recipients = prepare_provider_targets(
            Some(&targets(true, &["recipient-a", "recipient-a"])),
            "hello",
            db::KIND_PUSH,
            &enabled,
        )
        .expect_err("scheduled Kakao recipients must be unique");
        assert_eq!(duplicate_recipients.status(), StatusCode::BAD_REQUEST);

        let blank = prepare_provider_targets(
            Some(&targets(true, &["recipient-a"])),
            "   ",
            db::KIND_PUSH,
            &enabled,
        )
        .expect_err("scheduled Kakao content cannot be blank");
        assert_eq!(blank.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn provider_target_schema_is_closed_and_patch_null_removes_the_plan() {
        let private_recipient = "private-recipient-uuid";
        let debug = format!("{:?}", targets(true, &[private_recipient]));
        assert!(!debug.contains(private_recipient));
        assert!(debug.contains("recipient_count: 1"));

        assert!(
            serde_json::from_value::<ScheduledProviderTargetsBody>(json!({
                "kakaoFriendShare": {
                    "receiverUuids": ["recipient-a"],
                    "confirmed": true,
                    "unexpected": true
                }
            }))
            .is_err()
        );

        let feed_target = serde_json::from_value::<ScheduledProviderTargetsBody>(json!({
            "kakaoFriendShare": {
                "accountId": "primary",
                "receiverUuids": ["recipient-a"],
                "sendToMe": true,
                "imageUrl": "https://cdn.example.com/thumbnail.jpg",
                "confirmed": true
            }
        }))
        .expect("feed and self-send target is accepted");
        assert!(feed_target.kakao_friend_share.send_to_me);
        assert_eq!(
            feed_target.kakao_friend_share.image_url.as_deref(),
            Some("https://cdn.example.com/thumbnail.jpg")
        );

        let body = json!({"providerTargets": null});
        let patch = patch_provider_targets(
            body.as_object().expect("patch object"),
            "hello",
            db::KIND_PUSH,
            &KakaoFriendShareConfig::default(),
        )
        .expect("null provider target patch is valid");
        assert!(matches!(patch, Some(None)));
    }

    #[test]
    fn material_provider_delivery_changes_require_reconfirmation() {
        let scheduled_at = chrono::Utc::now();
        let mut patch = ScheduledMessagePatch::default();
        assert!(!provider_delivery_intent_changed(
            &patch,
            "hello",
            scheduled_at,
            Some("@every 1h"),
            "UTC",
            None,
        ));

        patch.content = Some("changed".to_string());
        assert!(provider_delivery_intent_changed(
            &patch,
            "hello",
            scheduled_at,
            Some("@every 1h"),
            "UTC",
            None,
        ));

        let mut cadence_patch = ScheduledMessagePatch::default();
        cadence_patch.schedule = Some(Some("@every 5m".to_string()));
        assert!(provider_delivery_intent_changed(
            &cadence_patch,
            "hello",
            scheduled_at,
            Some("@every 1h"),
            "UTC",
            None,
        ));
    }
}
