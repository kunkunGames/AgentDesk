//! Bind the shared attempt budget to native permission-before-effect gates.
//!
//! The first supported template uses native read/edit only. Other adapters and
//! broader tool sets must provide their own admission and termination evidence.
use super::*;

pub(super) fn prepare(
    policy: &mut permissions::PreparedPermissions,
    requested_model: Option<&OpenCodeModelRef>,
) -> Result<OpenCodeModelRef, String> {
    let model = validate_runtime_config(&policy.config, requested_model)?;
    // Preserve operator denies; human asks become denies for this headless
    // template. Only existing grants become metered asks. Native rule order
    // and path matching still decide which permission is requested.
    for rule in policy
        .rules
        .as_array_mut()
        .ok_or("invalid managed permission rules")?
    {
        match rule["action"].as_str() {
            Some("deny") => {}
            Some("ask") => rule["action"] = Value::String("deny".into()),
            Some("allow") if matches!(rule["permission"].as_str(), Some("read" | "edit")) => {
                rule["action"] = Value::String("ask".into());
            }
            _ => return Err("managed OpenCode currently supports pre-authorized native read/edit only; other effectful tools require another verified contract".into()),
        }
    }
    Ok(model)
}

pub(super) fn validate_runtime_config(
    config: &Value,
    requested_model: Option<&OpenCodeModelRef>,
) -> Result<OpenCodeModelRef, String> {
    if config.get("lsp") != Some(&Value::Bool(false))
        || config.get("formatter") != Some(&Value::Bool(false))
        || config.pointer("/compaction/auto") != Some(&Value::Bool(false))
        || !config
            .get("plugin")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        || config
            .get("mcp")
            .is_some_and(|mcp| !mcp.as_object().is_some_and(|mcp| mcp.is_empty()))
    {
        return Err(
            "managed OpenCode requires disabled LSP, formatter, auto-compaction, plugins and MCP"
                .into(),
        );
    }
    let model = requested_model
        .cloned()
        .or(parse_model_override(
            config.get("model").and_then(Value::as_str),
        )?)
        .ok_or("managed OpenCode requires an explicit operator-selected model")?;
    let small_model = parse_model_override(config.get("small_model").and_then(Value::as_str))?
        .ok_or("managed OpenCode requires an explicit small model")?;
    let enabled = config.get("enabled_providers").and_then(Value::as_array);
    if small_model.provider_id != model.provider_id
        || !enabled
            .is_some_and(|providers| providers.len() == 1 && providers[0] == model.provider_id)
    {
        return Err(
            "managed OpenCode requires one approved provider for both primary and auxiliary models"
                .into(),
        );
    }
    Ok(model)
}

pub(super) fn handle_permission(
    data: &str,
    session: &str,
    cancel: Option<&CancelToken>,
    origin: &str,
    auth: &str,
) -> Result<(), String> {
    let Some(cancel) = cancel.filter(|token| token.managed_execution.get().is_some()) else {
        return Ok(());
    };
    let Ok(envelope) = serde_json::from_str::<Value>(data) else {
        return Ok(());
    };
    let event = envelope.get("payload").unwrap_or(&envelope);
    if event["type"] != "permission.asked" || event["properties"]["sessionID"] != session {
        return Ok(());
    }
    let props = &event["properties"];
    let id = props["id"]
        .as_str()
        .filter(|id| {
            id.starts_with("per_") && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
        .ok_or("invalid native permission ID")?;
    let valid = matches!(props["permission"].as_str(), Some("read" | "edit"))
        && props
            .pointer("/tool/callID")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty());
    let reservation = if !valid || is_cancelled(Some(cancel)) {
        Err("managed tool request is unsupported or cancelled".to_string())
    } else {
        cancel
            .managed_execution
            .get()
            .expect("managed token checked")
            .admit_permission(session, id)
    };
    let reply = if reservation.is_ok() {
        "once"
    } else {
        "reject"
    };
    if reservation.is_ok() && is_cancelled(Some(cancel)) {
        abort_session(origin, auth, session);
        return Err("managed native grant cancelled after reservation".into());
    }
    let response = session_request_agent()
        .post(&format!("{origin}/permission/{id}/reply"))
        .set("Authorization", auth)
        .send_json(serde_json::json!({"reply":reply}));
    if let Err(error) = reservation {
        cancel.publish_cancel_if_source_absent("managed_tool_budget_or_lease_rejected");
        abort_session(origin, auth, session);
        return Err(error);
    }
    response.map_err(|error| format!("managed native grant delivery is unknown: {error}"))?;
    Ok(())
}

pub(super) fn finish(
    session: &str,
    cancel: Option<&CancelToken>,
    origin: &str,
    auth: &str,
) -> Result<(), String> {
    let Some(token) = cancel else { return Ok(()) };
    let Some(attempt) = token.managed_execution.get() else {
        return Ok(());
    };
    if is_cancelled(Some(token)) {
        return Err("managed completion was cancelled".into());
    }
    let get = |path: &str| -> Result<Value, String> {
        session_request_agent()
            .get(&format!("{origin}{path}"))
            .set("Authorization", auth)
            .call()
            .map_err(|error| format!("managed native termination observation failed: {error}"))?
            .into_json()
            .map_err(|_| "invalid managed native termination observation".into())
    };
    let statuses = get("/session/status")?;
    let statuses = statuses
        .as_object()
        .ok_or("invalid native session statuses")?;
    if statuses
        .get(session)
        .is_some_and(|status| status["type"] != "idle")
    {
        return Err("managed native session is still active".into());
    }
    let messages = get(&format!("/session/{session}/message"))?;
    let messages = messages
        .as_array()
        .filter(|messages| !messages.is_empty())
        .ok_or("managed terminal has no native message evidence")?;
    for message in messages {
        let parts = message
            .get("parts")
            .and_then(Value::as_array)
            .ok_or("invalid native message parts")?;
        for part in parts.iter().filter(|part| part["type"] == "tool") {
            if !matches!(
                part.pointer("/state/status").and_then(Value::as_str),
                Some("completed" | "error")
            ) {
                return Err("managed native tool remains active or unknown".into());
            }
        }
    }
    if is_cancelled(Some(token)) {
        return Err("managed completion cancelled during termination observation".into());
    }
    // This is only the adapter's normal-terminal contract for the restricted
    // native tool set. EOF, abort acknowledgements and errors never get here.
    // Broader tools/OS isolation and crash reconciliation need separate evidence.
    attempt.record_quiesced(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy() -> permissions::PreparedPermissions {
        permissions::PreparedPermissions {
            agent: "build".into(),
            config: json!({"model":"approved/main","small_model":"approved/small",
                "enabled_providers":["approved"],"lsp":false,"formatter":false,
                "compaction":{"auto":false},"plugin":[],"mcp":{}}),
            rules: json!([
                {"permission":"*","pattern":"*","action":"deny"},
                {"permission":"read","pattern":"*","action":"allow"},
                {"permission":"edit","pattern":"*","action":"allow"},
                {"permission":"edit","pattern":"*.secret","action":"deny"}
            ]),
        }
    }

    #[test]
    fn managed_permissions_meter_existing_grants_without_weakening_denies() {
        let mut policy = policy();
        let model = prepare(&mut policy, None).unwrap();
        assert_eq!(model.provider_id, "approved");
        assert_eq!(model.model_id, "main");
        assert_eq!(policy.rules[0]["action"], "deny");
        assert_eq!(policy.rules[1]["action"], "ask");
        assert_eq!(policy.rules[2]["action"], "ask");
        assert_eq!(policy.rules[3]["action"], "deny");
    }

    #[test]
    fn managed_profile_denies_human_asks_and_rejects_effectful_helpers_and_unapproved_fallbacks() {
        let mut human = policy();
        human.rules.as_array_mut().unwrap().push(json!({
            "permission":"read","pattern":"*.env","action":"ask"
        }));
        prepare(&mut human, None).unwrap();
        assert_eq!(human.rules[4]["action"], "deny");
        for rule in [
            json!({"permission":"bash","pattern":"*","action":"allow"}),
            json!({"permission":"task","pattern":"*","action":"allow"}),
        ] {
            let mut policy = policy();
            policy.rules.as_array_mut().unwrap().push(rule);
            assert!(prepare(&mut policy, None).is_err());
        }
        for (field, value) in [
            ("lsp", json!({})),
            ("formatter", json!({})),
            ("compaction", json!({"auto":true})),
            ("plugin", json!(["unverified-plugin"])),
            ("mcp", json!({"helper":{}})),
            ("small_model", json!("other/small")),
            ("enabled_providers", json!(["approved", "other"])),
        ] {
            let mut policy = policy();
            policy.config[field] = value;
            assert!(prepare(&mut policy, None).is_err(), "{field}");
        }
        let unapproved = OpenCodeModelRef {
            provider_id: "other".into(),
            model_id: "model".into(),
        };
        assert!(prepare(&mut policy(), Some(&unapproved)).is_err());
    }
}
