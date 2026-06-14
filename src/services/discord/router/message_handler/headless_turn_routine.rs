//! Routine-headless-turn helpers extracted verbatim from `headless_turn.rs`
//! (#730 split, to keep the parent file under its giant-file ratchet baseline):
//! metadata-derived role binding, role-binding precedence, and routine session
//! identity refresh. Behavior is unchanged from the inline implementation.
use super::*;

/// Build a role binding from trusted routine metadata (`routine_id` + `agent_id`).
/// The HTTP `start_agent_turn` boundary rejects a `metadata.agent_id` that does not
/// match the requested agent, so only the in-process routine executor reaches here
/// with a server-trusted agent id.
pub(super) fn routine_metadata_role_binding(
    metadata: Option<&serde_json::Value>,
    provider: &ProviderKind,
) -> Option<settings::RoleBinding> {
    let metadata = metadata?;
    metadata.get("routine_id")?;
    let agent_id = metadata_agent_id(Some(metadata))?;
    let prompt_file = super::super::super::runtime_store::agentdesk_root()
        .unwrap_or_default()
        .join("config")
        .join("agents")
        .join(agent_id)
        .join("IDENTITY.md")
        .display()
        .to_string();

    Some(settings::RoleBinding {
        role_id: agent_id.to_string(),
        prompt_file,
        provider: Some(provider.clone()),
        model: None,
        reasoning_effort: None,
        peer_agents_enabled: true,
        quality_feedback_injection_enabled: true,
        memory: settings::resolve_memory_settings(None, None),
    })
}

/// Whether `metadata` carries a `routine_id` (i.e. the turn was started by the
/// routine executor).
pub(super) fn metadata_has_routine_id(metadata: Option<&serde_json::Value>) -> bool {
    metadata.and_then(|value| value.get("routine_id")).is_some()
}

fn metadata_agent_id(metadata: Option<&serde_json::Value>) -> Option<&str> {
    metadata?
        .get("agent_id")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Resolve a role binding with routine-metadata precedence: the routine binding
/// wins, then a metadata parent-channel binding, then `fallback` (evaluated lazily
/// only when neither routine source resolves). Mirrors the early and final sites.
pub(super) fn role_binding_with_routine_precedence(
    routine_role_binding: &Option<settings::RoleBinding>,
    metadata: Option<&serde_json::Value>,
    fallback: impl FnOnce() -> Option<settings::RoleBinding>,
) -> Option<settings::RoleBinding> {
    routine_role_binding
        .clone()
        .or_else(|| {
            metadata_parent_channel_id(metadata)
                .and_then(|parent_channel_id| resolve_role_binding(parent_channel_id, None))
        })
        .or_else(fallback)
}

/// When a routine turn resolved to its own metadata agent (agent_id == resolved
/// role) and carries a channel-name hint, rebind the session's channel name so a
/// stale session row for a different agent cannot leak identity. Returns `true`
/// when the session was reset, so the caller can clear session id / memento flags.
pub(super) async fn maybe_refresh_routine_session_channel_name(
    shared: &SharedData,
    channel_id: ChannelId,
    metadata: Option<&serde_json::Value>,
    role_binding: Option<&settings::RoleBinding>,
    channel_name_hint: Option<&String>,
) -> bool {
    if !metadata_has_routine_id(metadata) {
        return false;
    }
    let matches_requested_agent = metadata_agent_id(metadata)
        .zip(role_binding.map(|binding| binding.role_id.as_str()))
        .is_some_and(|(metadata_agent_id, role_id)| metadata_agent_id == role_id);
    if !matches_requested_agent {
        return false;
    }
    let Some(channel_name_hint) = channel_name_hint.filter(|value| !value.trim().is_empty()) else {
        return false;
    };
    let mut data = shared.core.lock().await;
    if let Some(session) = data.sessions.get_mut(&channel_id)
        && session.channel_name.as_deref() != Some(channel_name_hint.as_str())
    {
        session.channel_name = Some(channel_name_hint.clone());
        session.clear_provider_session();
        return true;
    }
    false
}

/// Persist the routine agent identity onto the open session row so analytics and
/// reconnection observe the routine's agent/provider/session key. Best-effort;
/// logs on failure. No-op for non-routine turns or missing prerequisites.
pub(super) async fn refresh_routine_session_identity_row(
    shared: &SharedData,
    channel_id: ChannelId,
    metadata: Option<&serde_json::Value>,
    role_binding: Option<&settings::RoleBinding>,
    provider: &ProviderKind,
    adk_session_key: Option<&str>,
) {
    if !metadata_has_routine_id(metadata) {
        return;
    }
    let (Some(pool), Some(binding), Some(session_key)) =
        (shared.pg_pool.as_ref(), role_binding, adk_session_key)
    else {
        return;
    };
    if let Err(error) = sqlx::query(
        "UPDATE sessions
                SET agent_id = $1,
                    provider = $2,
                    session_key = $3
              WHERE channel_id = $4
                AND COALESCE(status, '') <> 'closed'",
    )
    .bind(&binding.role_id)
    .bind(provider.as_str())
    .bind(session_key)
    .bind(channel_id.get().to_string())
    .execute(pool)
    .await
    {
        tracing::warn!(
            channel_id = channel_id.get(),
            agent_id = %binding.role_id,
            provider = %provider.as_str(),
            error = %error,
            "failed to refresh routine headless session identity"
        );
    }
}
