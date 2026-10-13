//! Resolve a headless turn's role and channel identity before any turn claim.
use super::routine_metadata::routine_metadata_role_binding;
use super::{
    Arc, ChannelId, ProviderKind, ResolvedThreadRoleBinding, SharedData,
    metadata_parent_channel_id, resolve_channel_category, resolve_role_binding,
    resolve_thread_role_binding, serenity,
};

pub(super) struct HeadlessTarget {
    pub(super) early_stale_session_id: Option<String>,
    pub(super) early_thread_parent: Option<(ChannelId, Option<String>)>,
    pub(super) early_resolved_channel_name: Option<String>,
    pub(super) resolved_role_binding: ResolvedThreadRoleBinding,
}

pub(super) async fn resolve_headless_target(
    ctx: &serenity::Context,
    channel_id: ChannelId,
    shared: &Arc<SharedData>,
    metadata: Option<&serde_json::Value>,
    channel_name_hint: Option<&str>,
    settings_provider: &ProviderKind,
) -> HeadlessTarget {
    let routine_role_binding = routine_metadata_role_binding(metadata, settings_provider);
    let (early_stale_session_id, early_channel_name) = {
        let data = shared.core.lock().await;
        data.sessions
            .get(&channel_id)
            .map(|session| (session.session_id.clone(), session.channel_name.clone()))
            .unwrap_or_default()
    };
    let early_thread_parent =
        crate::services::discord::resolve_thread_parent(&ctx.http, channel_id).await;
    let early_resolved_channel_name = if early_channel_name.is_none() && channel_name_hint.is_none()
    {
        let (channel_name, _) = resolve_channel_category(&ctx.http, None, channel_id).await;
        channel_name
    } else {
        None
    };
    let mut resolved_role_binding = if let Some(binding) = routine_role_binding {
        ResolvedThreadRoleBinding::direct(Some(binding))
    } else {
        resolve_thread_role_binding(
            channel_id,
            early_channel_name
                .as_deref()
                .or(channel_name_hint)
                .or(early_resolved_channel_name.as_deref())
                .or(
                    crate::services::discord::adk_session::registered_channel_fallback_name(
                        channel_id,
                        settings_provider,
                    )
                    .as_deref(),
                ),
            early_thread_parent.as_ref(),
        )
    };
    if resolved_role_binding.role_binding.is_none() && early_thread_parent.is_none() {
        resolved_role_binding.role_binding = metadata_parent_channel_id(metadata)
            .and_then(|parent_id| resolve_role_binding(parent_id, None));
    }
    HeadlessTarget {
        early_stale_session_id,
        early_thread_parent,
        early_resolved_channel_name,
        resolved_role_binding,
    }
}
