use std::sync::Arc;

use poise::CreateReply;
use poise::serenity_prelude as serenity;

use super::super::formatting::{canonical_tool_name, risk_badge, send_long_message_ctx, tool_info};
use super::super::settings::{resolve_role_binding, save_bot_settings};
use super::super::{Context, Error, SharedData, check_auth, check_owner};
use crate::services::model_catalog::{
    ModelOption, catalog_for_provider, matches_catalog_query, normalize_model_override,
};
use crate::services::provider::ProviderKind;

const MODEL_CLEAR_KEYWORDS: &[&str] = &["default", "none", "clear"];
const MODEL_INFO_KEYWORDS: &[&str] = &["info"];
const MODEL_LIST_KEYWORDS: &[&str] = &["list"];
pub(in crate::services::discord) const MODEL_PICKER_CUSTOM_ID: &str = "agentdesk:model-picker";
pub(in crate::services::discord) const MODEL_DEFAULT_CUSTOM_ID: &str = "agentdesk:model-default";
pub(in crate::services::discord) const MODEL_CANCEL_CUSTOM_ID: &str = "agentdesk:model-cancel";
pub(in crate::services::discord) const MODEL_SAVE_CUSTOM_ID_PREFIX: &str = "agentdesk:model-save:";
pub(in crate::services::discord) const MODEL_SAVE_DEFAULT_CUSTOM_ID: &str =
    "agentdesk:model-save-default";
const MODEL_PICKER_OPTION_DESCRIPTION_LIMIT: usize = 100;
const MODEL_DEFAULT_PICKER_VALUE: &str = "__default__";
const RUNTIME_OVERRIDE_SOURCE: &str = "runtime override";
const PARENT_RUNTIME_OVERRIDE_SOURCE: &str = "parent channel runtime override";
const DISPATCH_ROLE_OVERRIDE_SOURCE: &str = "dispatch role override";
const PARENT_DISPATCH_ROLE_OVERRIDE_SOURCE: &str = "parent channel dispatch role override";

#[derive(Debug, Clone, Copy)]
pub(in crate::services::discord) struct ModelScopeChannels {
    current_channel_id: serenity::ChannelId,
    parent_channel_id: Option<serenity::ChannelId>,
}

impl ModelScopeChannels {
    fn role_binding_channel_id(self) -> serenity::ChannelId {
        self.parent_channel_id.unwrap_or(self.current_channel_id)
    }

    fn has_parent_fallback(self) -> bool {
        self.parent_channel_id.is_some()
    }
}

pub(in crate::services::discord) async fn resolve_model_scope_channels(
    ctx: &serenity::Context,
    channel_id: serenity::ChannelId,
) -> ModelScopeChannels {
    let parent_channel_id = super::super::resolve_thread_parent(ctx, channel_id)
        .await
        .map(|(parent_id, _)| parent_id);

    ModelScopeChannels {
        current_channel_id: channel_id,
        parent_channel_id,
    }
}

fn truncate_embed_text(input: &str, limit: usize) -> String {
    if input.chars().count() <= limit {
        return input.to_string();
    }
    if limit <= 3 {
        return "...".chars().take(limit).collect();
    }

    let mut out: String = input.chars().take(limit - 3).collect();
    out.push_str("...");
    out
}

pub(in crate::services::discord) fn is_model_picker_interaction_custom_id(custom_id: &str) -> bool {
    custom_id == MODEL_PICKER_CUSTOM_ID
        || custom_id == MODEL_DEFAULT_CUSTOM_ID
        || custom_id == MODEL_CANCEL_CUSTOM_ID
        || custom_id == MODEL_SAVE_DEFAULT_CUSTOM_ID
        || custom_id.starts_with(MODEL_SAVE_CUSTOM_ID_PREFIX)
}

pub(in crate::services::discord) fn parse_model_picker_save_custom_id(
    custom_id: &str,
) -> Option<&str> {
    if custom_id == MODEL_SAVE_DEFAULT_CUSTOM_ID {
        Some("default")
    } else {
        custom_id.strip_prefix(MODEL_SAVE_CUSTOM_ID_PREFIX)
    }
}

fn build_model_picker_save_custom_id(selected_value: &str) -> String {
    if selected_value == MODEL_DEFAULT_PICKER_VALUE {
        return MODEL_SAVE_DEFAULT_CUSTOM_ID.to_string();
    }
    format!("{MODEL_SAVE_CUSTOM_ID_PREFIX}{selected_value}")
}
pub(in crate::services::discord) fn provider_supports_model_override(
    provider: &ProviderKind,
) -> bool {
    matches!(
        provider,
        ProviderKind::Claude | ProviderKind::Codex | ProviderKind::Gemini
    )
}

fn model_hint(provider: &ProviderKind) -> String {
    match provider {
        ProviderKind::Claude => {
            "Use `default` or a curated Claude alias. Raw Claude full model names are also allowed."
                .to_string()
        }
        ProviderKind::Codex => {
            "Use `default` or one of the curated Codex models shown in the picker.".to_string()
        }
        ProviderKind::Gemini => {
            "Use `default` or one of the curated Gemini models shown in the picker.".to_string()
        }
        ProviderKind::Unsupported(_) => "Use a supported model name or `default`.".to_string(),
    }
}

fn normalize_keyword<'a>(raw: &'a str, keywords: &[&str]) -> Option<&'a str> {
    let trimmed = raw.trim();
    if keywords.iter().any(|kw| kw.eq_ignore_ascii_case(trimmed)) {
        Some(trimmed)
    } else {
        None
    }
}

fn picker_option_description(option: &ModelOption) -> String {
    truncate_embed_text(
        &format!("{} | {}", option.feature_summary, option.token_pricing),
        MODEL_PICKER_OPTION_DESCRIPTION_LIMIT,
    )
}

pub(in crate::services::discord) fn validate_model_input(
    provider: &ProviderKind,
    raw: &str,
) -> Result<String, String> {
    match normalize_model_override(provider, raw) {
        Ok(Some(model)) => Ok(model),
        Ok(None) => Err("Use `default` or `clear` to remove the override.".to_string()),
        Err(message) => Err(message),
    }
}

fn doctor_guidance_suffix(provider: &ProviderKind) -> String {
    match provider.probe_runtime() {
        Some(probe) if probe.binary_path.is_some() && probe.version.is_some() => String::new(),
        _ => format!(
            "\nRuntime check: `{}` CLI probe is unavailable. Try `agentdesk doctor` or `agentdesk doctor --json`.",
            provider.as_str()
        ),
    }
}

fn resolve_effective_model(
    override_model: Option<&str>,
    override_source: &'static str,
    dispatch_role_model: Option<&str>,
    dispatch_source: &'static str,
    role_model: Option<&str>,
) -> (String, &'static str) {
    if let Some(model) = override_model {
        (model.to_string(), override_source)
    } else if let Some(model) = dispatch_role_model {
        (model.to_string(), dispatch_source)
    } else if let Some(model) = role_model {
        (model.to_string(), "role-map")
    } else {
        ("default".to_string(), "provider default")
    }
}

fn resolve_fallback_model(
    dispatch_role_model: Option<&str>,
    dispatch_source: &'static str,
    role_model: Option<&str>,
) -> (String, &'static str) {
    if let Some(model) = dispatch_role_model {
        (model.to_string(), dispatch_source)
    } else if let Some(model) = role_model {
        (model.to_string(), "role-map")
    } else {
        ("default".to_string(), "provider default")
    }
}

fn resolve_runtime_source(
    current_runtime_override: Option<&str>,
    parent_runtime_override: Option<&str>,
) -> &'static str {
    if current_runtime_override.is_some() {
        RUNTIME_OVERRIDE_SOURCE
    } else if parent_runtime_override.is_some() {
        PARENT_RUNTIME_OVERRIDE_SOURCE
    } else {
        RUNTIME_OVERRIDE_SOURCE
    }
}

fn resolve_runtime_overrides(
    shared: &Arc<SharedData>,
    scope: ModelScopeChannels,
) -> (Option<String>, Option<String>, &'static str) {
    let current_runtime_override = shared
        .model_overrides
        .get(&scope.current_channel_id)
        .map(|value| value.clone());
    let parent_runtime_override = scope.parent_channel_id.and_then(|parent_id| {
        shared
            .model_overrides
            .get(&parent_id)
            .map(|value| value.clone())
    });
    let runtime_source = resolve_runtime_source(
        current_runtime_override.as_deref(),
        parent_runtime_override.as_deref(),
    );

    (
        current_runtime_override,
        parent_runtime_override,
        runtime_source,
    )
}

fn resolve_dispatch_role_model(
    shared: &Arc<SharedData>,
    scope: ModelScopeChannels,
) -> (Option<String>, &'static str) {
    if let Some(override_channel) = shared
        .dispatch_role_overrides
        .get(&scope.current_channel_id)
        .map(|value| *value)
    {
        return (
            resolve_role_binding(override_channel, None).and_then(|binding| binding.model),
            DISPATCH_ROLE_OVERRIDE_SOURCE,
        );
    }

    if let Some(parent_channel_id) = scope.parent_channel_id {
        if let Some(override_channel) = shared
            .dispatch_role_overrides
            .get(&parent_channel_id)
            .map(|value| *value)
        {
            return (
                resolve_role_binding(override_channel, None).and_then(|binding| binding.model),
                PARENT_DISPATCH_ROLE_OVERRIDE_SOURCE,
            );
        }
    }

    (None, DISPATCH_ROLE_OVERRIDE_SOURCE)
}

async fn resolve_role_model(shared: &Arc<SharedData>, scope: ModelScopeChannels) -> Option<String> {
    let role_binding_channel_id = scope.role_binding_channel_id();
    let channel_name = {
        let data = shared.core.lock().await;
        data.sessions
            .get(&role_binding_channel_id)
            .and_then(|session| session.channel_name.clone())
    };

    resolve_role_binding(role_binding_channel_id, channel_name.as_deref())
        .and_then(|binding| binding.model)
}

async fn mark_inherited_thread_reset_candidates(
    ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    parent_channel_id: serenity::ChannelId,
) -> usize {
    let candidate_channel_ids = {
        let data = shared.core.lock().await;
        data.sessions.keys().copied().collect::<Vec<_>>()
    };

    let mut reset_count = 0usize;
    for candidate_channel_id in candidate_channel_ids {
        if candidate_channel_id == parent_channel_id
            || shared.model_overrides.contains_key(&candidate_channel_id)
        {
            continue;
        }

        let Some((candidate_parent_id, _)) =
            super::super::resolve_thread_parent(ctx, candidate_channel_id).await
        else {
            continue;
        };

        if candidate_parent_id == parent_channel_id {
            shared
                .pending_model_session_resets
                .insert(candidate_channel_id, true);
            reset_count += 1;
        }
    }

    reset_count
}

fn format_scoped_model(
    value: Option<&str>,
    source: &'static str,
    inherited_source: &'static str,
) -> String {
    match value {
        Some(value) if source == inherited_source => format!("{value} (parent channel)"),
        Some(value) => value.to_string(),
        None => "(none)".to_string(),
    }
}

#[derive(Debug, Clone)]
struct EffectiveModelSnapshot {
    has_parent_fallback: bool,
    current_runtime_override: Option<String>,
    parent_runtime_override: Option<String>,
    dispatch_role_model: Option<String>,
    dispatch_role_source: &'static str,
    role_model: Option<String>,
    effective: String,
    source: &'static str,
    fallback_model: String,
    fallback_source: &'static str,
}

fn picker_selected_value<'a>(
    snapshot: &'a EffectiveModelSnapshot,
    pending_selection: Option<&'a str>,
) -> Option<&'a str> {
    match pending_selection {
        Some(selection) if selection == MODEL_DEFAULT_PICKER_VALUE => None,
        Some(selection) => Some(selection),
        None => snapshot
            .current_runtime_override
            .as_deref()
            .or(snapshot.parent_runtime_override.as_deref()),
    }
}

fn has_pending_model_change(
    snapshot: &EffectiveModelSnapshot,
    pending_selection: Option<&str>,
) -> bool {
    match pending_selection {
        Some(selection) if selection == MODEL_DEFAULT_PICKER_VALUE => {
            snapshot.current_runtime_override.is_some()
        }
        Some(selection) => !snapshot
            .current_runtime_override
            .as_deref()
            .is_some_and(|current| current.eq_ignore_ascii_case(selection)),
        None => false,
    }
}

fn picker_status_text(
    snapshot: &EffectiveModelSnapshot,
    pending_selection: Option<&str>,
    reset_pending: bool,
) -> String {
    if let Some(selection) = pending_selection {
        if selection == MODEL_DEFAULT_PICKER_VALUE {
            return if snapshot.current_runtime_override.is_some() {
                "기본값 저장 대기".to_string()
            } else {
                "기본값 유지".to_string()
            };
        }

        return if snapshot
            .current_runtime_override
            .as_deref()
            .is_some_and(|current| current.eq_ignore_ascii_case(selection))
        {
            format!("저장할 변경 없음 ({selection})")
        } else {
            format!("저장 대기 ({selection})")
        };
    }

    if reset_pending {
        "다음 턴부터 반영".to_string()
    } else if snapshot.current_runtime_override.is_some() {
        "오버라이드 사용 중".to_string()
    } else if snapshot.parent_runtime_override.is_some() {
        "부모 설정 상속 중".to_string()
    } else if snapshot.dispatch_role_model.is_some() || snapshot.role_model.is_some() {
        "기본 라우팅 사용 중".to_string()
    } else {
        "프로바이더 기본값 사용 중".to_string()
    }
}

pub(in crate::services::discord) async fn apply_model_override(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    provider: &ProviderKind,
    channel_id: serenity::ChannelId,
    raw: &str,
    bot_token: &str,
) -> Result<(), String> {
    if catalog_for_provider(provider).is_none() {
        return Err(format!(
            "Model override catalog is not available for provider `{}`.",
            provider.as_str()
        ));
    }

    let scope = resolve_model_scope_channels(discord_ctx, channel_id).await;
    let (current_runtime_override, parent_runtime_override, previous_runtime_source) =
        resolve_runtime_overrides(shared, scope);
    let previous_runtime_override = current_runtime_override
        .as_deref()
        .or(parent_runtime_override.as_deref());
    let (dispatch_role_model, dispatch_role_source) = resolve_dispatch_role_model(shared, scope);
    let role_model = resolve_role_model(shared, scope).await;
    let (previous_effective, _) = resolve_effective_model(
        previous_runtime_override,
        previous_runtime_source,
        dispatch_role_model.as_deref(),
        dispatch_role_source,
        role_model.as_deref(),
    );
    let normalized = normalize_model_override(provider, raw)?;

    match normalized.as_deref() {
        Some(model) => {
            shared.model_overrides.insert(channel_id, model.to_string());
        }
        None => {
            shared.model_overrides.remove(&channel_id);
        }
    }

    {
        let mut settings = shared.settings.write().await;
        let channel_key = channel_id.get().to_string();
        match normalized.as_deref() {
            Some(model) => {
                settings
                    .channel_model_overrides
                    .insert(channel_key, model.to_string());
            }
            None => {
                settings.channel_model_overrides.remove(&channel_key);
            }
        }
        save_bot_settings(bot_token, &settings);
    }

    let (effective_next_turn, _) = resolve_effective_model(
        normalized.as_deref().or(parent_runtime_override.as_deref()),
        resolve_runtime_source(normalized.as_deref(), parent_runtime_override.as_deref()),
        dispatch_role_model.as_deref(),
        dispatch_role_source,
        role_model.as_deref(),
    );
    let reset_required = previous_effective != effective_next_turn;
    if reset_required {
        shared.pending_model_session_resets.insert(channel_id, true);
        if !scope.has_parent_fallback() {
            let inherited_thread_resets =
                mark_inherited_thread_reset_candidates(discord_ctx, shared, channel_id).await;
            if inherited_thread_resets > 0 {
                println!(
                    "  [{}] ▶ propagated model reset to {} inherited thread session(s)",
                    chrono::Local::now().format("%H:%M:%S"),
                    inherited_thread_resets
                );
            }
        }
    } else {
        shared.pending_model_session_resets.remove(&channel_id);
    }

    let action_line = match normalized {
        Some(model) => format!("Model override set to **{}** for this channel.", model),
        None => "Model override cleared for this channel.".to_string(),
    };

    let reset_line = if reset_required {
        "Next turn will start a fresh session."
    } else {
        "Effective model did not change, so no session reset is needed."
    };
    println!(
        "  [{}] ▶ {} effective next turn: {} ({})",
        chrono::Local::now().format("%H:%M:%S"),
        action_line,
        effective_next_turn,
        reset_line
    );

    Ok(())
}

fn runtime_probe_detail(provider: &ProviderKind) -> (String, String, String) {
    match provider.probe_runtime() {
        Some(probe) => {
            let binary_path = probe
                .binary_path
                .unwrap_or_else(|| "(not found)".to_string());
            let version = probe
                .version
                .unwrap_or_else(|| "(version unavailable)".to_string());
            let runtime_status = if binary_path == "(not found)" {
                "missing"
            } else if version == "(version unavailable)" {
                "degraded"
            } else {
                "ok"
            };
            (runtime_status.to_string(), binary_path, version)
        }
        None => (
            "unsupported".to_string(),
            "(unsupported)".to_string(),
            "(unsupported)".to_string(),
        ),
    }
}

async fn effective_model_snapshot(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
) -> EffectiveModelSnapshot {
    let scope = resolve_model_scope_channels(discord_ctx, channel_id).await;
    let (current_runtime_override, parent_runtime_override, runtime_source) =
        resolve_runtime_overrides(shared, scope);
    let effective_runtime_override = current_runtime_override
        .as_deref()
        .or(parent_runtime_override.as_deref());
    let (dispatch_role_model, dispatch_role_source) = resolve_dispatch_role_model(shared, scope);
    let role_model = resolve_role_model(shared, scope).await;
    let (effective, source) = resolve_effective_model(
        effective_runtime_override,
        runtime_source,
        dispatch_role_model.as_deref(),
        dispatch_role_source,
        role_model.as_deref(),
    );
    let (fallback_model, fallback_source) = resolve_fallback_model(
        dispatch_role_model.as_deref(),
        dispatch_role_source,
        role_model.as_deref(),
    );

    EffectiveModelSnapshot {
        has_parent_fallback: scope.has_parent_fallback(),
        current_runtime_override,
        parent_runtime_override,
        dispatch_role_model,
        dispatch_role_source,
        role_model,
        effective,
        source,
        fallback_model,
        fallback_source,
    }
}

pub(in crate::services::discord) async fn resolve_model_for_turn(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
) -> Option<String> {
    let snapshot = effective_model_snapshot(discord_ctx, shared, channel_id).await;
    if snapshot.source == "provider default" && snapshot.effective.eq_ignore_ascii_case("default") {
        None
    } else {
        Some(snapshot.effective)
    }
}

pub(in crate::services::discord) async fn build_model_status_message(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
) -> String {
    let snapshot = effective_model_snapshot(discord_ctx, shared, channel_id).await;
    let mut lines = vec![
        format!("Provider: **{}**", provider.display_name()),
        format!("Model: **{}**", snapshot.effective),
        format!("Source: **{}**", snapshot.source),
        format!(
            "Runtime override: `{}`",
            snapshot
                .current_runtime_override
                .as_deref()
                .unwrap_or("(none)")
        ),
    ];
    if snapshot.has_parent_fallback {
        lines.push(format!(
            "Inherited parent override: `{}`",
            snapshot
                .parent_runtime_override
                .as_deref()
                .unwrap_or("(none)")
        ));
    }
    lines.extend([
        format!(
            "Dispatch role default: `{}`",
            format_scoped_model(
                snapshot.dispatch_role_model.as_deref(),
                snapshot.dispatch_role_source,
                PARENT_DISPATCH_ROLE_OVERRIDE_SOURCE,
            )
        ),
        format!(
            "Role default: `{}`",
            snapshot.role_model.as_deref().unwrap_or("(none)")
        ),
        format!(
            "Default if cleared: **{}** (`{}`)",
            snapshot.fallback_model, snapshot.fallback_source
        ),
        "Applies: next turn".to_string(),
        model_hint(provider),
    ]);

    lines.join("\n")
}

pub(in crate::services::discord) async fn build_model_info_message(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
) -> String {
    let snapshot = effective_model_snapshot(discord_ctx, shared, channel_id).await;
    let (runtime_status, binary_path, version) = runtime_probe_detail(provider);
    let mut lines = vec![
        format!("Provider: **{}**", provider.display_name()),
        format!("Model: **{}**", snapshot.effective),
        format!("Source: **{}**", snapshot.source),
        format!(
            "Runtime override: `{}`",
            snapshot
                .current_runtime_override
                .as_deref()
                .unwrap_or("(none)")
        ),
    ];
    if snapshot.has_parent_fallback {
        lines.push(format!(
            "Inherited parent override: `{}`",
            snapshot
                .parent_runtime_override
                .as_deref()
                .unwrap_or("(none)")
        ));
    }
    lines.extend([
        format!(
            "Dispatch role default: `{}`",
            format_scoped_model(
                snapshot.dispatch_role_model.as_deref(),
                snapshot.dispatch_role_source,
                PARENT_DISPATCH_ROLE_OVERRIDE_SOURCE,
            )
        ),
        format!(
            "Role default: `{}`",
            snapshot.role_model.as_deref().unwrap_or("(none)")
        ),
        format!(
            "Default if cleared: **{}** (`{}`)",
            snapshot.fallback_model, snapshot.fallback_source
        ),
        format!("Runtime CLI: **{}**", runtime_status),
        format!("Binary: `{}`", binary_path),
        format!("Version: `{}`", version),
        "Applies: next turn".to_string(),
        model_hint(provider),
    ]);
    let mut msg = lines.join("\n");
    msg.push_str(&doctor_guidance_suffix(provider));
    msg
}

fn build_model_picker_options(
    provider: &ProviderKind,
    current_override: Option<&str>,
) -> Vec<serenity::CreateSelectMenuOption> {
    let mut options = vec![
        serenity::CreateSelectMenuOption::new("default", MODEL_DEFAULT_PICKER_VALUE)
            .description("기본값 사용 | override 해제")
            .default_selection(current_override.is_none()),
    ];

    if let Some(catalog) = catalog_for_provider(provider) {
        for option in catalog
            .options
            .iter()
            .filter(|option| !option.value.eq_ignore_ascii_case("default"))
        {
            let built = serenity::CreateSelectMenuOption::new(option.label, option.value)
                .description(picker_option_description(option))
                .default_selection(
                    current_override
                        .is_some_and(|active| active.eq_ignore_ascii_case(option.value)),
                );
            options.push(built);
        }
    }

    options
}

pub(in crate::services::discord) async fn build_model_picker_embed(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
    pending_selection: Option<&str>,
) -> serenity::CreateEmbed {
    let snapshot = effective_model_snapshot(discord_ctx, shared, channel_id).await;
    let reset_pending = shared
        .pending_model_session_resets
        .contains_key(&channel_id);
    let mut lines = vec![
        format!("Provider: **{}**", provider.display_name()),
        format!("Current Model: **{}**", snapshot.effective),
        format!(
            "현재 작업 상태: **{}**",
            picker_status_text(&snapshot, pending_selection, reset_pending)
        ),
    ];
    let doctor_hint = doctor_guidance_suffix(provider);
    if !doctor_hint.is_empty() {
        lines.push(doctor_hint.trim().to_string());
    }
    let description = lines.join("\n");

    serenity::CreateEmbed::new()
        .title("Model Picker")
        .description(description)
        .color(0x5865F2)
}

pub(in crate::services::discord) async fn build_model_picker_components(
    discord_ctx: &serenity::Context,
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
    pending_selection: Option<&str>,
) -> Vec<serenity::CreateActionRow> {
    let snapshot = effective_model_snapshot(discord_ctx, shared, channel_id).await;
    let selected_value = picker_selected_value(&snapshot, pending_selection);
    let pending_button_value =
        pending_selection.unwrap_or_else(|| selected_value.unwrap_or(MODEL_DEFAULT_PICKER_VALUE));
    let menu = serenity::CreateSelectMenu::new(
        MODEL_PICKER_CUSTOM_ID,
        serenity::CreateSelectMenuKind::String {
            options: build_model_picker_options(provider, selected_value),
        },
    )
    .placeholder("Select a model override")
    .min_values(1)
    .max_values(1);

    let save_button =
        serenity::CreateButton::new(build_model_picker_save_custom_id(pending_button_value))
            .label("Save")
            .style(serenity::ButtonStyle::Primary)
            .disabled(!has_pending_model_change(&snapshot, pending_selection));

    let default_button = serenity::CreateButton::new(MODEL_DEFAULT_CUSTOM_ID)
        .label("Default")
        .style(serenity::ButtonStyle::Secondary)
        .disabled(snapshot.current_runtime_override.is_none());

    let cancel_button = serenity::CreateButton::new(MODEL_CANCEL_CUSTOM_ID)
        .label("Cancel")
        .style(serenity::ButtonStyle::Secondary)
        .disabled(pending_selection.is_none());

    vec![
        serenity::CreateActionRow::SelectMenu(menu),
        serenity::CreateActionRow::Buttons(vec![save_button, default_button, cancel_button]),
    ]
}

async fn autocomplete_model<'a>(
    ctx: Context<'a>,
    partial: &'a str,
) -> Vec<serenity::AutocompleteChoice> {
    let mut choices = Vec::new();
    let partial_lower = partial.to_ascii_lowercase();
    let provider = &ctx.data().provider;

    for keyword in ["info", "list", "default"] {
        if partial.is_empty() || keyword.contains(&partial_lower) {
            let label = format!(
                "{} — {}",
                keyword,
                match keyword {
                    "info" => "show provider, model, and source",
                    "list" => "show model picker card",
                    "default" => "clear runtime override",
                    _ => "",
                }
            );
            choices.push(serenity::AutocompleteChoice::new(
                label,
                keyword.to_string(),
            ));
        }
    }

    if let Some(catalog) = catalog_for_provider(provider) {
        for option in catalog.options.iter() {
            if choices.len() >= 25 {
                break;
            }
            if option.value.eq_ignore_ascii_case("default")
                || !matches_catalog_query(option, partial)
            {
                continue;
            }
            let label = format!("{} · {}", option.label, option.value);
            choices.push(serenity::AutocompleteChoice::new(label, option.value));
        }
    }

    choices
}

/// /model — Set or view the model override for this channel
#[poise::command(slash_command, rename = "model")]
pub(in crate::services::discord) async fn cmd_model(
    ctx: Context<'_>,
    #[autocomplete = "autocomplete_model"]
    #[description = "Model name or 'default' to clear"]
    model: Option<String>,
) -> Result<(), Error> {
    let user_id = ctx.author().id;
    let user_name = &ctx.author().name;
    if !check_auth(user_id, user_name, &ctx.data().shared, &ctx.data().token).await {
        return Ok(());
    }

    let ts = chrono::Local::now().format("%H:%M:%S");
    let channel_id = ctx.channel_id();

    if !provider_supports_model_override(&ctx.data().provider) {
        println!("  [{ts}] ◀ [{user_name}] /model (unsupported provider)");
        ctx.say("Model override is only supported for Claude, Codex, and Gemini channels.")
            .await?;
        return Ok(());
    }

    match model {
        Some(m) => {
            println!("  [{ts}] ◀ [{user_name}] /model {m}");

            if normalize_keyword(&m, MODEL_LIST_KEYWORDS).is_some() {
                let embed = build_model_picker_embed(
                    ctx.serenity_context(),
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                    None,
                )
                .await;
                let components = build_model_picker_components(
                    ctx.serenity_context(),
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                    None,
                )
                .await;
                ctx.send(CreateReply::default().embed(embed).components(components))
                    .await?;
                return Ok(());
            }

            if normalize_keyword(&m, MODEL_INFO_KEYWORDS).is_some() {
                let msg = build_model_info_message(
                    ctx.serenity_context(),
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                )
                .await;
                ctx.say(msg).await?;
                return Ok(());
            }

            if normalize_keyword(&m, MODEL_CLEAR_KEYWORDS).is_some() {
                if let Err(message) = apply_model_override(
                    ctx.serenity_context(),
                    &ctx.data().shared,
                    &ctx.data().provider,
                    channel_id,
                    "default",
                    &ctx.data().token,
                )
                .await
                {
                    ctx.say(message).await?;
                    return Ok(());
                }
                let msg = build_model_status_message(
                    ctx.serenity_context(),
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                )
                .await;
                ctx.say(format!("Model override cleared.\n{}", msg)).await?;
                return Ok(());
            }

            let validated = match validate_model_input(&ctx.data().provider, &m) {
                Ok(model) => model,
                Err(message) => {
                    ctx.say(message).await?;
                    return Ok(());
                }
            };

            if let Err(message) = apply_model_override(
                ctx.serenity_context(),
                &ctx.data().shared,
                &ctx.data().provider,
                channel_id,
                &validated,
                &ctx.data().token,
            )
            .await
            {
                ctx.say(message).await?;
                return Ok(());
            }

            let msg = build_model_status_message(
                ctx.serenity_context(),
                &ctx.data().shared,
                channel_id,
                &ctx.data().provider,
            )
            .await;
            ctx.say(format!(
                "Model set to **{}** for this channel.\n{}",
                validated, msg
            ))
            .await?;
        }
        None => {
            println!("  [{ts}] ◀ [{user_name}] /model");
            let embed = build_model_picker_embed(
                ctx.serenity_context(),
                &ctx.data().shared,
                channel_id,
                &ctx.data().provider,
                None,
            )
            .await;
            let components = build_model_picker_components(
                ctx.serenity_context(),
                &ctx.data().shared,
                channel_id,
                &ctx.data().provider,
                None,
            )
            .await;
            ctx.send(CreateReply::default().embed(embed).components(components))
                .await?;
        }
    }
    Ok(())
}

/// /allowedtools — Show currently allowed tools
#[poise::command(slash_command, rename = "allowedtools")]
pub(in crate::services::discord) async fn cmd_allowedtools(ctx: Context<'_>) -> Result<(), Error> {
    let user_id = ctx.author().id;
    let user_name = &ctx.author().name;
    if !check_auth(user_id, user_name, &ctx.data().shared, &ctx.data().token).await {
        return Ok(());
    }

    let ts = chrono::Local::now().format("%H:%M:%S");
    println!("  [{ts}] ◀ [{user_name}] /allowedtools");

    let tools = {
        let settings = ctx.data().shared.settings.read().await;
        settings.allowed_tools.clone()
    };

    let mut msg = String::from("**Allowed Tools**\n\n");
    for tool in &tools {
        let (desc, destructive) = tool_info(tool);
        let badge = risk_badge(destructive);
        if badge.is_empty() {
            msg.push_str(&format!("`{}` — {}\n", tool, desc));
        } else {
            msg.push_str(&format!("`{}` {} — {}\n", tool, badge, desc));
        }
    }
    msg.push_str(&format!(
        "\n{} = destructive\nTotal: {}",
        risk_badge(true),
        tools.len()
    ));

    send_long_message_ctx(ctx, &msg).await?;
    Ok(())
}

/// /allowed <+/-tool> — Add or remove a tool
#[poise::command(slash_command, rename = "allowed")]
pub(in crate::services::discord) async fn cmd_allowed(
    ctx: Context<'_>,
    #[description = "Use +name to add, -name to remove (e.g. +Bash or -Bash)"] action: String,
) -> Result<(), Error> {
    let user_id = ctx.author().id;
    let user_name = &ctx.author().name;
    if !check_auth(user_id, user_name, &ctx.data().shared, &ctx.data().token).await {
        return Ok(());
    }

    let ts = chrono::Local::now().format("%H:%M:%S");
    println!("  [{ts}] ◀ [{user_name}] /allowed {action}");

    let arg = action.trim();
    let (op, raw_name) = if let Some(name) = arg.strip_prefix('+') {
        ('+', name.trim())
    } else if let Some(name) = arg.strip_prefix('-') {
        ('-', name.trim())
    } else {
        ctx.say("Use `+toolname` to add or `-toolname` to remove.\nExample: `/allowed +Bash`")
            .await?;
        return Ok(());
    };

    if raw_name.is_empty() {
        ctx.say("Tool name cannot be empty.").await?;
        return Ok(());
    }

    let Some(tool_name) = canonical_tool_name(raw_name).map(str::to_string) else {
        ctx.say(format!(
            "Unknown tool `{}`. Use `/allowedtools` to see valid tool names.",
            raw_name
        ))
        .await?;
        return Ok(());
    };

    let response_msg = {
        let mut settings = ctx.data().shared.settings.write().await;
        match op {
            '+' => {
                if settings.allowed_tools.iter().any(|t| t == &tool_name) {
                    format!("`{}` is already in the list.", tool_name)
                } else {
                    settings.allowed_tools.push(tool_name.clone());
                    save_bot_settings(&ctx.data().token, &settings);
                    format!("Added `{}`", tool_name)
                }
            }
            '-' => {
                let before_len = settings.allowed_tools.len();
                settings.allowed_tools.retain(|t| t != &tool_name);
                if settings.allowed_tools.len() < before_len {
                    save_bot_settings(&ctx.data().token, &settings);
                    format!("Removed `{}`", tool_name)
                } else {
                    format!("`{}` is not in the list.", tool_name)
                }
            }
            _ => unreachable!(),
        }
    };

    ctx.say(&response_msg).await?;
    Ok(())
}

/// /adduser @user — Allow another user to use the bot (owner only)
#[poise::command(slash_command, rename = "adduser")]
pub(in crate::services::discord) async fn cmd_adduser(
    ctx: Context<'_>,
    #[description = "User to add"] user: serenity::User,
) -> Result<(), Error> {
    let author_id = ctx.author().id;
    let author_name = &ctx.author().name;
    if !check_auth(
        author_id,
        author_name,
        &ctx.data().shared,
        &ctx.data().token,
    )
    .await
    {
        return Ok(());
    }
    if !check_owner(author_id, &ctx.data().shared).await {
        ctx.say("Only the owner can add users.").await?;
        return Ok(());
    }

    let target_id = user.id.get();
    let target_name = &user.name;

    let ts = chrono::Local::now().format("%H:%M:%S");
    println!("  [{ts}] ◀ [{author_name}] /adduser {target_name}");

    {
        let mut settings = ctx.data().shared.settings.write().await;
        if settings.allowed_user_ids.contains(&target_id) {
            ctx.say(format!("`{}` is already authorized.", target_name))
                .await?;
            return Ok(());
        }
        settings.allowed_user_ids.push(target_id);
        save_bot_settings(&ctx.data().token, &settings);
    }

    ctx.say(format!("Added `{}` as authorized user.", target_name))
        .await?;
    println!("  [{ts}] ▶ Added user: {target_name} (id:{target_id})");
    Ok(())
}

/// /removeuser @user — Remove a user's access (owner only)
#[poise::command(slash_command, rename = "removeuser")]
pub(in crate::services::discord) async fn cmd_removeuser(
    ctx: Context<'_>,
    #[description = "User to remove"] user: serenity::User,
) -> Result<(), Error> {
    let author_id = ctx.author().id;
    let author_name = &ctx.author().name;
    if !check_auth(
        author_id,
        author_name,
        &ctx.data().shared,
        &ctx.data().token,
    )
    .await
    {
        return Ok(());
    }
    if !check_owner(author_id, &ctx.data().shared).await {
        ctx.say("Only the owner can remove users.").await?;
        return Ok(());
    }

    let target_id = user.id.get();
    let target_name = &user.name;

    let ts = chrono::Local::now().format("%H:%M:%S");
    println!("  [{ts}] ◀ [{author_name}] /removeuser {target_name}");

    {
        let mut settings = ctx.data().shared.settings.write().await;
        let before_len = settings.allowed_user_ids.len();
        settings.allowed_user_ids.retain(|&id| id != target_id);
        if settings.allowed_user_ids.len() == before_len {
            ctx.say(format!("`{}` is not in the authorized list.", target_name))
                .await?;
            return Ok(());
        }
        save_bot_settings(&ctx.data().token, &settings);
    }

    ctx.say(format!("Removed `{}` from authorized users.", target_name))
        .await?;
    println!("  [{ts}] ▶ Removed user: {target_name} (id:{target_id})");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DISPATCH_ROLE_OVERRIDE_SOURCE, PARENT_DISPATCH_ROLE_OVERRIDE_SOURCE,
        PARENT_RUNTIME_OVERRIDE_SOURCE, RUNTIME_OVERRIDE_SOURCE, build_model_picker_options,
        MODEL_DEFAULT_PICKER_VALUE, MODEL_SAVE_DEFAULT_CUSTOM_ID, build_model_picker_save_custom_id,
        parse_model_picker_save_custom_id, picker_option_description, resolve_effective_model,
        resolve_fallback_model, serenity,
    };
    use crate::services::model_catalog::catalog_for_provider;
    use crate::services::provider::ProviderKind;
    use serde_json::Value;

    fn option_json(option: &serenity::CreateSelectMenuOption) -> Value {
        serde_json::to_value(option).expect("select menu option should serialize")
    }

    #[test]
    fn test_resolve_effective_model_prefers_dispatch_override_before_role_map() {
        let resolved = resolve_effective_model(
            None,
            RUNTIME_OVERRIDE_SOURCE,
            Some("dispatch-model"),
            DISPATCH_ROLE_OVERRIDE_SOURCE,
            Some("role-model"),
        );
        assert_eq!(
            resolved,
            ("dispatch-model".to_string(), "dispatch role override")
        );
    }

    #[test]
    fn test_resolve_effective_model_prefers_runtime_override_first() {
        let resolved = resolve_effective_model(
            Some("runtime-model"),
            RUNTIME_OVERRIDE_SOURCE,
            Some("dispatch-model"),
            DISPATCH_ROLE_OVERRIDE_SOURCE,
            Some("role-model"),
        );
        assert_eq!(resolved, ("runtime-model".to_string(), "runtime override"));
    }

    #[test]
    fn test_resolve_fallback_model_prefers_provider_default_last() {
        let resolved = resolve_fallback_model(None, DISPATCH_ROLE_OVERRIDE_SOURCE, None);
        assert_eq!(resolved, ("default".to_string(), "provider default"));
    }

    #[test]
    fn test_resolve_effective_model_uses_parent_runtime_source_label() {
        let resolved = resolve_effective_model(
            Some("parent-model"),
            PARENT_RUNTIME_OVERRIDE_SOURCE,
            Some("dispatch-model"),
            DISPATCH_ROLE_OVERRIDE_SOURCE,
            Some("role-model"),
        );
        assert_eq!(
            resolved,
            (
                "parent-model".to_string(),
                "parent channel runtime override"
            )
        );
    }

    #[test]
    fn test_resolve_fallback_model_uses_parent_dispatch_source_label() {
        let resolved = resolve_fallback_model(
            Some("dispatch-model"),
            PARENT_DISPATCH_ROLE_OVERRIDE_SOURCE,
            Some("role-model"),
        );
        assert_eq!(
            resolved,
            (
                "dispatch-model".to_string(),
                "parent channel dispatch role override"
            )
        );
    }

    #[test]
    fn test_build_model_picker_options_follows_catalog_entries() {
        let provider = ProviderKind::Codex;
        let options = build_model_picker_options(&provider, Some("gpt-5.4-mini"));
        let catalog = catalog_for_provider(&provider).expect("codex catalog should exist");

        assert_eq!(options.len(), catalog.options.len());

        let default_option = option_json(&options[0]);
        assert_eq!(default_option["label"], "default");
        assert_eq!(default_option["value"], "__default__");
        assert_eq!(default_option["default"], false);

        let built_values = options
            .iter()
            .skip(1)
            .map(|option| {
                option_json(option)["value"]
                    .as_str()
                    .expect("picker value should serialize as string")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let catalog_values = catalog
            .options
            .iter()
            .filter(|option| !option.value.eq_ignore_ascii_case("default"))
            .map(|option| option.value.to_string())
            .collect::<Vec<_>>();
        assert_eq!(built_values, catalog_values);

        let selected_option = options
            .iter()
            .skip(1)
            .find(|option| {
                option_json(option)["value"]
                    .as_str()
                    .is_some_and(|value| value == "gpt-5.4-mini")
            })
            .expect("current override should appear in picker");
        let selected_option = option_json(selected_option);
        let selected_catalog = catalog
            .options
            .iter()
            .find(|option| option.value == "gpt-5.4-mini")
            .expect("selected catalog entry should exist");

        assert_eq!(
            selected_option["description"],
            picker_option_description(selected_catalog)
        );
        assert_eq!(selected_option["default"], true);
    }

    #[test]
    fn test_picker_option_description_includes_pricing() {
        let catalog = catalog_for_provider(&ProviderKind::Gemini).expect("gemini catalog");
        let option = catalog
            .options
            .iter()
            .find(|option| option.value == "gemini-3.1-pro-preview")
            .expect("gemini 3.1 pro should exist");

        let description = picker_option_description(option);
        assert!(description.contains(" | "));
        assert!(description.contains("$2 / $12"));
    }

    #[test]
    fn test_model_save_custom_id_round_trips() {
        let custom_id = build_model_picker_save_custom_id("gemini-3-flash-preview");
        assert_eq!(
            parse_model_picker_save_custom_id(&custom_id),
            Some("gemini-3-flash-preview")
        );
    }

    #[test]
    fn test_model_save_default_custom_id_round_trips() {
        let custom_id = build_model_picker_save_custom_id(MODEL_DEFAULT_PICKER_VALUE);
        assert_eq!(custom_id, MODEL_SAVE_DEFAULT_CUSTOM_ID);
        assert_eq!(parse_model_picker_save_custom_id(&custom_id), Some("default"));
    }
}
