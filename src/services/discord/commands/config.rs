use std::sync::Arc;

use poise::CreateReply;
use poise::serenity_prelude as serenity;

use super::super::formatting::{canonical_tool_name, risk_badge, send_long_message_ctx, tool_info};
use super::super::settings::{resolve_role_binding, save_bot_settings};
use super::super::{Context, Error, SharedData, check_auth, check_owner};
use crate::services::provider::ProviderKind;

const MODEL_CLEAR_KEYWORDS: &[&str] = &["default", "none", "clear"];
const MODEL_INFO_KEYWORDS: &[&str] = &["info"];
const MODEL_LIST_KEYWORDS: &[&str] = &["list"];
pub(in crate::services::discord) const MODEL_PICKER_CUSTOM_ID: &str = "agentdesk:model-picker";

pub(in crate::services::discord) fn provider_supports_model_override(provider: &ProviderKind) -> bool {
    matches!(
        provider,
        ProviderKind::Claude | ProviderKind::Codex | ProviderKind::Gemini
    )
}

pub(in crate::services::discord) fn model_hint(provider: &ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Claude => "예: opus / sonnet / haiku",
        ProviderKind::Codex => "예: gpt-5-codex / o3 / o4-mini",
        ProviderKind::Gemini => "예: gemini-2.5-pro / gemini-2.5-flash",
        ProviderKind::Unsupported(_) => "모델 이름 또는 default",
    }
}

fn known_models(provider: &ProviderKind) -> &'static [&'static str] {
    match provider {
        ProviderKind::Claude => &["opus", "sonnet", "haiku"],
        ProviderKind::Codex => &["gpt-5-codex", "o3", "o4-mini"],
        ProviderKind::Gemini => &["gemini-2.5-pro", "gemini-2.5-flash"],
        ProviderKind::Unsupported(_) => &[],
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

pub(in crate::services::discord) fn is_clear_model_keyword(raw: &str) -> bool {
    normalize_keyword(raw, MODEL_CLEAR_KEYWORDS).is_some()
}

fn canonical_known_model(provider: &ProviderKind, raw: &str) -> Option<&'static str> {
    known_models(provider)
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(raw.trim()))
}

fn looks_like_model_identifier(raw: &str) -> bool {
    let trimmed = raw.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 64
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':'))
}

pub(in crate::services::discord) fn validate_model_input(
    provider: &ProviderKind,
    raw: &str,
) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("Model name cannot be empty.".to_string());
    }

    if let Some(canonical) = canonical_known_model(provider, trimmed) {
        return Ok(canonical.to_string());
    }

    if looks_like_model_identifier(trimmed) {
        return Ok(trimmed.to_string());
    }

    Err(format!(
        "Unrecognized model `{}` for {}.\n{}\nUse `/model list` to see known examples.",
        trimmed,
        provider.display_name(),
        model_hint(provider)
    ))
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

fn runtime_probe_detail(provider: &ProviderKind) -> (String, String, String) {
    match provider.probe_runtime() {
        Some(probe) => {
            let binary_path = probe.binary_path.unwrap_or_else(|| "(not found)".to_string());
            let version = probe.version.unwrap_or_else(|| "(version unavailable)".to_string());
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
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
) -> (Option<String>, Option<String>, String, String) {
    let override_model = shared.model_overrides.get(&channel_id).map(|v| v.clone());
    let ch_name = {
        let d = shared.core.lock().await;
        d.sessions
            .get(&channel_id)
            .and_then(|s| s.channel_name.clone())
    };
    let role_model = resolve_role_binding(channel_id, ch_name.as_deref()).and_then(|rb| rb.model);
    let effective = override_model
        .as_deref()
        .or(role_model.as_deref())
        .unwrap_or("(default)")
        .to_string();
    let source = if override_model.is_some() {
        "runtime override"
    } else if role_model.is_some() {
        "role-map"
    } else {
        "system default"
    }
    .to_string();

    (override_model, role_model, effective, source)
}

pub(in crate::services::discord) async fn build_model_status_message(
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
) -> String {
    let (override_model, role_model, effective, source) =
        effective_model_snapshot(shared, channel_id).await;

    format!(
        "Provider: **{}**\nModel: **{}**\nSource: **{}**\nRuntime override: `{}`\nRole default: `{}`\nApplies: next turn\n{}",
        provider.display_name(),
        effective,
        source,
        override_model.as_deref().unwrap_or("(none)"),
        role_model.as_deref().unwrap_or("(none)"),
        model_hint(provider)
    )
}

pub(in crate::services::discord) async fn build_model_info_message(
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
) -> String {
    let (override_model, role_model, effective, source) =
        effective_model_snapshot(shared, channel_id).await;
    let (runtime_status, binary_path, version) = runtime_probe_detail(provider);
    let mut msg = format!(
        "Provider: **{}**\nModel: **{}**\nSource: **{}**\nRuntime override: `{}`\nRole default: `{}`\nRuntime CLI: **{}**\nBinary: `{}`\nVersion: `{}`\nApplies: next turn\n{}",
        provider.display_name(),
        effective,
        source,
        override_model.as_deref().unwrap_or("(none)"),
        role_model.as_deref().unwrap_or("(none)"),
        runtime_status,
        binary_path,
        version,
        model_hint(provider)
    );
    msg.push_str(&doctor_guidance_suffix(provider));
    msg
}

pub(in crate::services::discord) fn build_model_list_message(provider: &ProviderKind) -> String {
    let examples = known_models(provider);
    let list = if examples.is_empty() {
        "(no known examples)".to_string()
    } else {
        examples
            .iter()
            .map(|m| format!("- `{}`", m))
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "**{} model examples**\n{}\n{}\nUse `/model <name>` to set one for this channel.{}",
        provider.display_name(),
        list,
        model_hint(provider),
        doctor_guidance_suffix(provider)
    )
}

fn build_model_picker_options(
    provider: &ProviderKind,
    current_override: Option<&str>,
) -> Vec<serenity::CreateSelectMenuOption> {
    let mut options = vec![
        serenity::CreateSelectMenuOption::new("default", "__default__")
            .description("clear runtime override")
            .default_selection(current_override.is_none()),
    ];

    for model in known_models(provider) {
        let option = serenity::CreateSelectMenuOption::new(*model, *model)
            .description(provider.display_name())
            .default_selection(
                current_override.is_some_and(|active| active.eq_ignore_ascii_case(model)),
            );
        options.push(option);
    }

    options
}

pub(in crate::services::discord) async fn build_model_picker_message(
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
) -> String {
    let mut msg = build_model_list_message(provider);
    let status = build_model_status_message(shared, channel_id, provider).await;
    msg.push_str("\n\nCurrent\n");
    msg.push_str(&status);
    msg.push_str("\nUse the select menu below or `/model <name>`.");
    msg
}

pub(in crate::services::discord) async fn build_model_picker_components(
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
    provider: &ProviderKind,
) -> Vec<serenity::CreateActionRow> {
    let (override_model, _, _, _) = effective_model_snapshot(shared, channel_id).await;
    let menu = serenity::CreateSelectMenu::new(
        MODEL_PICKER_CUSTOM_ID,
        serenity::CreateSelectMenuKind::String {
            options: build_model_picker_options(provider, override_model.as_deref()),
        },
    )
    .placeholder("Select a model override")
    .min_values(1)
    .max_values(1);

    vec![serenity::CreateActionRow::SelectMenu(menu)]
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
            let label = format!("{} — {}", keyword, match keyword {
                "info" => "show provider, model, and source",
                "list" => "show known model examples",
                "default" => "clear runtime override",
                _ => "",
            });
            choices.push(serenity::AutocompleteChoice::new(label, keyword.to_string()));
        }
    }

    for model in known_models(provider) {
        if choices.len() >= 25 {
            break;
        }
        if partial.is_empty() || model.to_ascii_lowercase().contains(&partial_lower) {
            let label = format!("{} — {}", model, provider.display_name());
            choices.push(serenity::AutocompleteChoice::new(label, (*model).to_string()));
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
                let content = build_model_picker_message(
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                )
                .await;
                let components = build_model_picker_components(
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                )
                .await;
                ctx.send(
                    CreateReply::default()
                        .content(content)
                        .components(components),
                )
                .await?;
                return Ok(());
            }

            if normalize_keyword(&m, MODEL_INFO_KEYWORDS).is_some() {
                let msg = build_model_info_message(
                    &ctx.data().shared,
                    channel_id,
                    &ctx.data().provider,
                )
                .await;
                ctx.say(msg).await?;
                return Ok(());
            }

            if normalize_keyword(&m, MODEL_CLEAR_KEYWORDS).is_some() {
                ctx.data().shared.model_overrides.remove(&channel_id);
                let msg = build_model_status_message(
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

            ctx.data()
                .shared
                .model_overrides
                .insert(channel_id, validated.clone());
            let msg = build_model_status_message(
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
            let msg =
                build_model_status_message(&ctx.data().shared, channel_id, &ctx.data().provider)
                    .await;
            ctx.say(msg).await?;
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
