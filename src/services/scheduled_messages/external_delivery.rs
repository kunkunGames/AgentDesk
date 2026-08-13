//! Encrypted provider-target plans for scheduled push fan-out.
//!
//! Recipient identifiers are sensitive connector data. Active reservations
//! persist them only as an AEAD envelope bound to a random plan id; list/API
//! paths retain only a count summary. Each fire snapshots the current message
//! and targets into a second encrypted envelope owned by `external_share_outbox`.

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::db::scheduled_messages::{EncryptedExternalDeliveryPlan, ScheduledMessageRow};
use crate::services::external_share_outbox::NewExternalShareOutbox;
use crate::services::kakao::{
    KakaoFriendShareCommand, validate_friend_share_payload, validate_friend_share_recipients,
};
use crate::services::oauth_connection::{
    EncryptedValue, KAKAO_PROVIDER, OAuthConnectionError, PRIMARY_ACCOUNT_KEY, TokenVault,
};

const PLAN_SCHEMA_VERSION: u8 = 2;
const PLAN_AAD_DOMAIN: &str = "agentdesk/scheduled-message/provider-targets/v1";
const OUTBOX_AAD_DOMAIN: &str = "agentdesk/external-share-outbox/payload/v1";
pub(crate) const KAKAO_CHANNEL_ID: &str = "kakao_friend_share";
pub(crate) const OUTBOX_SOURCE: &str = "scheduled_message";

#[derive(Debug, Error)]
pub enum ExternalDeliveryPlanError {
    #[error("provider target encryption is unavailable")]
    Vault(#[from] OAuthConnectionError),
    #[error("provider target payload is invalid")]
    InvalidPayload,
    #[error("stored provider target plan is invalid")]
    InvalidStoredPlan,
    #[error("Kakao friend share target is invalid")]
    InvalidKakaoTarget,
}

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
struct StoredProviderTargets {
    schema_version: u8,
    kakao_friend_share: StoredKakaoFriendShare,
}

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
struct StoredKakaoFriendShare {
    receiver_uuids: Vec<String>,
    #[serde(default = "legacy_primary_account_key")]
    account_key: String,
}

pub fn encrypt_kakao_provider_target(
    vault: &TokenVault,
    account_key: String,
    receiver_uuids: Vec<String>,
) -> Result<EncryptedExternalDeliveryPlan, ExternalDeliveryPlanError> {
    crate::services::kakao::validate_account_key(&account_key)
        .map_err(|_| ExternalDeliveryPlanError::InvalidKakaoTarget)?;
    validate_friend_share_recipients(&receiver_uuids)
        .map_err(|_| ExternalDeliveryPlanError::InvalidKakaoTarget)?;

    let plan_id = Uuid::new_v4();
    let plan = StoredProviderTargets {
        schema_version: PLAN_SCHEMA_VERSION,
        kakao_friend_share: StoredKakaoFriendShare {
            receiver_uuids,
            account_key,
        },
    };
    let mut plaintext = Zeroizing::new(
        serde_json::to_vec(&plan).map_err(|_| ExternalDeliveryPlanError::InvalidPayload)?,
    );
    let encrypted = vault.seal(&plaintext, plan_aad(plan_id).as_bytes())?;
    plaintext.zeroize();
    let recipient_count = plan.kakao_friend_share.receiver_uuids.len();
    Ok(EncryptedExternalDeliveryPlan {
        id: plan_id,
        ciphertext: encrypted.ciphertext,
        nonce: encrypted.nonce,
        key_version: encrypted.key_version,
        summary: json!({
            "kakaoFriendShare": {
                "enabled": true,
                "recipientCount": recipient_count,
                "contentMode": "text",
                "imageForwarded": false
            }
        }),
        account_key: plan.kakao_friend_share.account_key.clone(),
    })
}

pub(crate) fn prepare_external_share_outbox(
    message: &ScheduledMessageRow,
    delivery_id: &str,
    source_key: &str,
) -> Result<Option<NewExternalShareOutbox>, ExternalDeliveryPlanError> {
    let Some(plan_id) = message.external_delivery_plan_id else {
        return Ok(None);
    };
    let encrypted = EncryptedValue {
        ciphertext: message
            .external_delivery_plan_ciphertext
            .clone()
            .ok_or(ExternalDeliveryPlanError::InvalidStoredPlan)?,
        nonce: message
            .external_delivery_plan_nonce
            .clone()
            .ok_or(ExternalDeliveryPlanError::InvalidStoredPlan)?,
        key_version: message
            .external_delivery_plan_key_version
            .ok_or(ExternalDeliveryPlanError::InvalidStoredPlan)?,
    };
    let vault = TokenVault::from_env()?;
    let plaintext = vault.open(&encrypted, plan_aad(plan_id).as_bytes())?;
    let stored: StoredProviderTargets = serde_json::from_slice(&plaintext)
        .map_err(|_| ExternalDeliveryPlanError::InvalidStoredPlan)?;
    if !(1..=PLAN_SCHEMA_VERSION).contains(&stored.schema_version) {
        return Err(ExternalDeliveryPlanError::InvalidStoredPlan);
    }
    let account_key = stored.kakao_friend_share.account_key.clone();
    if message.external_delivery_account_key.as_deref() != Some(account_key.as_str()) {
        return Err(ExternalDeliveryPlanError::InvalidStoredPlan);
    }

    let command = KakaoFriendShareCommand {
        receiver_uuids: stored.kakao_friend_share.receiver_uuids.clone(),
        text: message.content.clone(),
        // The protected schedule create/PATCH request is the explicit operator
        // confirmation boundary. Workers replay that durable authorization.
        confirmed: true,
    };
    validate_friend_share_payload(&command)
        .map_err(|_| ExternalDeliveryPlanError::InvalidKakaoTarget)?;
    let requested_count = command.receiver_uuids.len();
    let outbox_id = Uuid::new_v4();
    let mut payload = Zeroizing::new(
        serde_json::to_vec(&command).map_err(|_| ExternalDeliveryPlanError::InvalidPayload)?,
    );
    let encrypted_payload = vault.seal(&payload, outbox_aad(outbox_id).as_bytes())?;
    payload.zeroize();

    Ok(Some(NewExternalShareOutbox {
        id: outbox_id,
        provider: KAKAO_PROVIDER.to_string(),
        channel_id: KAKAO_CHANNEL_ID.to_string(),
        account_key,
        source: OUTBOX_SOURCE.to_string(),
        source_key: source_key.to_string(),
        scheduled_delivery_id: delivery_id.to_string(),
        requested_count: i16::try_from(requested_count)
            .map_err(|_| ExternalDeliveryPlanError::InvalidPayload)?,
        encrypted_payload,
        deliver_before: message.expires_at,
    }))
}

pub(crate) fn outbox_aad(id: Uuid) -> String {
    format!("{OUTBOX_AAD_DOMAIN}:{id}")
}

fn plan_aad(id: Uuid) -> String {
    format!("{PLAN_AAD_DOMAIN}:{id}")
}

fn legacy_primary_account_key() -> String {
    PRIMARY_ACCOUNT_KEY.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_plan_exposes_counts_but_not_recipient_identifiers() {
        let vault = TokenVault::for_test([7_u8; 32]);
        let recipient = "recipient-private-uuid".to_string();
        let plan = encrypt_kakao_provider_target(
            &vault,
            PRIMARY_ACCOUNT_KEY.to_string(),
            vec![recipient.clone()],
        )
        .unwrap();

        assert_eq!(plan.summary["kakaoFriendShare"]["recipientCount"], 1);
        assert!(!String::from_utf8_lossy(&plan.ciphertext).contains(&recipient));
        let plaintext = vault
            .open(
                &EncryptedValue {
                    ciphertext: plan.ciphertext,
                    nonce: plan.nonce,
                    key_version: plan.key_version,
                },
                plan_aad(plan.id).as_bytes(),
            )
            .unwrap();
        assert!(String::from_utf8_lossy(&plaintext).contains(&recipient));
    }

    #[test]
    fn plan_ciphertext_is_bound_to_its_plan_id() {
        let vault = TokenVault::for_test([8_u8; 32]);
        let plan = encrypt_kakao_provider_target(
            &vault,
            PRIMARY_ACCOUNT_KEY.to_string(),
            vec!["recipient-a".to_string()],
        )
        .unwrap();
        let encrypted = EncryptedValue {
            ciphertext: plan.ciphertext,
            nonce: plan.nonce,
            key_version: plan.key_version,
        };
        assert!(
            vault
                .open(&encrypted, plan_aad(Uuid::new_v4()).as_bytes())
                .is_err()
        );
    }
}
