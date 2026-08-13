use serde_json::json;

use super::super::{EndpointDoc, body_param, ep, header_param, query_param};

pub(super) fn endpoints() -> Vec<EndpointDoc> {
    vec![
        ep(
            "POST",
            "/api/kakao/oauth/start",
            "kakao",
            "Create a single-use Kakao OAuth state and return the fixed-provider authorization URL. Requires authenticated AgentDesk access.",
        )
        .with_example(
            json!({}),
            json!({
                "authorize_url": "https://kauth.kakao.com/oauth/authorize?response_type=code&client_id=REDACTED&state=REDACTED&scope=friends%2Ctalk_message",
                "expires_in_seconds": 600
            }),
        ),
        ep(
            "GET",
            "/api/kakao/accounts",
            "kakao",
            "List locally connected Kakao accounts using opaque AgentDesk account IDs. Tokens and Kakao user identifiers are never returned.",
        ),
        ep(
            "GET",
            "/api/kakao/oauth/callback",
            "kakao",
            "Public Kakao OAuth callback. Consumes the hashed single-use state and always redirects to a fixed Settings result URL; query values are never reflected.",
        )
        .with_params([
            (
                "state",
                query_param("string", true, "Single-use OAuth CSRF state returned by Kakao"),
            ),
            (
                "code",
                query_param("string", false, "Authorization code on an approved consent flow"),
            ),
            (
                "error",
                query_param("string", false, "Provider denial marker; never reflected in the redirect"),
            ),
        ]),
        ep(
            "DELETE",
            "/api/kakao/accounts/{account_id}",
            "kakao",
            "Delete one locally encrypted Kakao account. This does not revoke the remote Kakao app grant and rejects accounts referenced by active scheduled delivery.",
        )
        .with_params([(
            "account_id",
            query_param("string", true, "Opaque local account ID from GET /api/kakao/accounts"),
        )])
        .with_example(
            json!({}),
            json!({
                "ok": true,
                "account_id": "primary",
                "remote_unlinked": false
            }),
        ),
        ep(
            "GET",
            "/api/kakao/friends",
            "kakao",
            "List the selected sender account's Kakao friends without server-side friend caching.",
        )
        .with_params([
            (
                "account_id",
                query_param("string", true, "Opaque sender account ID"),
            ),
            (
                "offset",
                query_param("integer", false, "Provider page offset").with_default(0),
            ),
            (
                "limit",
                query_param("integer", false, "Page size from 1 through 100").with_default(20),
            ),
        ])
        .with_example(
            json!({"query": {"account_id": "primary", "offset": 0, "limit": 20}}),
            json!({
                "friends": [{"uuid": "provider-opaque-uuid", "display_name": "Friend"}],
                "total_count": 1,
                "offset": 0,
                "limit": 20,
                "next_offset": null
            }),
        ),
        ep(
            "POST",
            "/api/kakao/messages/send",
            "kakao",
            "Send one confirmed text template to 1–5 selected friends behind a durable, non-reclaiming at-most-once fence. Ambiguous delivery is sticky unknown and is never retried automatically.",
        )
        .with_params([
            (
                "Idempotency-Key",
                header_param(
                    "string",
                    true,
                    "8–128 safe-ASCII bytes scoped to the Kakao connector account",
                ),
            ),
            (
                "receiver_uuids",
                body_param("string[]", true, "1–5 unique provider UUIDs"),
            ),
            (
                "account_id",
                body_param("string", true, "Opaque sender account ID"),
            ),
            (
                "text",
                body_param("string", true, "1–200 Unicode scalar values"),
            ),
            (
                "confirmed",
                body_param("boolean", true, "Must be true for every manual send"),
            ),
        ])
        .with_example(
            json!({
                "headers": {"Idempotency-Key": "3c855579-2c78-4cf2-a814-4dfef84e744f"},
                "body": {
                    "account_id": "primary",
                    "receiver_uuids": ["provider-opaque-uuid"],
                    "text": "AgentDesk test message",
                    "confirmed": true
                }
            }),
            json!({
                "request_id": "46a44a24-790e-4f41-aec6-8bf6ac5b2d3d",
                "status": "success",
                "requested_count": 1,
                "successful_count": 1,
                "failed_count": 0,
                "replayed": false,
                "delivery_may_have_occurred": true,
                "automatic_retry_allowed": false
            }),
        ),
        ep(
            "POST",
            "/api/kakao/messages/send-to-me",
            "kakao",
            "Send one confirmed default text template to the connected operator's Kakao My Chatroom. This does not require friends-list access and uses the same durable at-most-once operation fence.",
        )
        .with_params([
            (
                "Idempotency-Key",
                header_param(
                    "string",
                    true,
                    "8–128 safe-ASCII bytes scoped to the Kakao connector account",
                ),
            ),
            (
                "text",
                body_param("string", true, "1–200 Unicode scalar values"),
            ),
            (
                "account_id",
                body_param("string", true, "Opaque sender account ID"),
            ),
            (
                "confirmed",
                body_param("boolean", true, "Must be true for every manual send"),
            ),
        ])
        .with_example(
            json!({
                "headers": {"Idempotency-Key": "self-send-3c855579-2c78-4cf2-a814-4dfef84e744f"},
                "body": {
                    "account_id": "primary",
                    "text": "AgentDesk self-send test message",
                    "confirmed": true
                }
            }),
            json!({
                "request_id": "46a44a24-790e-4f41-aec6-8bf6ac5b2d3d",
                "status": "success",
                "requested_count": 1,
                "successful_count": 1,
                "failed_count": 0,
                "replayed": false,
                "delivery_may_have_occurred": true,
                "automatic_retry_allowed": false
            }),
        ),
    ]
}
