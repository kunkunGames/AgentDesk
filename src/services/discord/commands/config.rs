use std::sync::Arc;

use poise::serenity_prelude as serenity;

use super::super::formatting::{canonical_tool_name, risk_badge, send_long_message_ctx, tool_info};
use super::super::settings::{resolve_role_binding, save_bot_settings};
use super::super::{Context, Error, SharedData, check_auth, check_owner};
use crate::services::model_catalog::{
    catalog_for_provider, matches_catalog_query, model_details_for_provider,
    normalize_model_override, render_catalog_values,
};
use crate::services::provider::ProviderKind;

const MODEL_EMBED_LIMIT_TITLE: usize = 256;
const MODEL_EMBED_LIMIT_DESCRIPTION: usize = 4096;
const MODEL_EMBED_LIMIT_FIELD_NAME: usize = 256;
const MODEL_EMBED_LIMIT_FIELD_VALUE: usize = 1024;

#[derive(Debug, Clone)]
pub(in crate::services::discord) struct ModelCommandResponse {
    plain: String,
    title: String,
    description: Option<String>,
    color: u32,
    fields: Vec<ModelCommandField>,
}

#[derive(Debug, Clone)]
struct ModelCommandField {
    name: String,
    value: String,
    inline: bool,
}

impl ModelCommandResponse {
    fn new(plain: String, title: impl Into<String>, color: u32) -> Self {
        Self {
            plain,
            title: title.into(),
            description: None,
            color,
            fields: Vec::new(),
        }
    }

    fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    fn field(mut self, name: impl Into<String>, value: impl Into<String>, inline: bool) -> Self {
        self.fields.push(ModelCommandField {
            name: name.into(),
            value: value.into(),
            inline,
        });
        self
    }

    pub(in crate::services::discord) fn plain_text(&self) -> &str {
        &self.plain
    }

    fn to_embed(&self) -> serenity::CreateEmbed {
        let mut embed = serenity::CreateEmbed::new()
            .title(truncate_embed_text(&self.title, MODEL_EMBED_LIMIT_TITLE))
            .color(self.color);

        if let Some(description) = &self.description {
            embed = embed.description(truncate_embed_text(
                description,
                MODEL_EMBED_LIMIT_DESCRIPTION,
            ));
        }

        for field in &self.fields {
            embed = embed.field(
                truncate_embed_text(&field.name, MODEL_EMBED_LIMIT_FIELD_NAME),
                truncate_embed_text(&field.value, MODEL_EMBED_LIMIT_FIELD_VALUE),
                field.inline,
            );
        }

        embed
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

fn provider_embed_color(provider: &ProviderKind) -> u32 {
    match provider.as_str() {
        "claude" => 0xD97706,
        "codex" => 0x16A34A,
        "gemini" => 0x2563EB,
        _ => 0x64748B,
    }
}

pub(in crate::services::discord) fn model_error_response(
    provider: &ProviderKind,
    message: impl Into<String>,
) -> ModelCommandResponse {
    let message = message.into();
    ModelCommandResponse::new(message.clone(), "Model Override Error", 0xDC2626)
        .description(format!(
            "{} channel could not update the model configuration.",
            provider.as_str()
        ))
        .field("Details", message, false)
}

pub(in crate::services::discord) async fn send_model_response_ctx(
    ctx: Context<'_>,
    response: ModelCommandResponse,
) -> Result<(), Error> {
    ctx.send(poise::CreateReply::default().embed(response.to_embed()))
        .await?;
    Ok(())
}

pub(in crate::services::discord) async fn send_model_response_raw(
    http: &serenity::Http,
    channel_id: serenity::ChannelId,
    reply_to: serenity::MessageId,
    response: &ModelCommandResponse,
) -> Result<(), serenity::Error> {
    channel_id
        .send_message(
            http,
            serenity::CreateMessage::new()
                .reference_message((channel_id, reply_to))
                .embed(response.to_embed()),
        )
        .await?;
    Ok(())
}

async fn autocomplete_model(ctx: Context<'_>, partial: &str) -> Vec<serenity::AutocompleteChoice> {
    let Some(catalog) = catalog_for_provider(&ctx.data().provider) else {
        return Vec::new();
    };

    catalog
        .options
        .iter()
        .filter(|option| matches_catalog_query(option, partial))
        .take(25)
        .map(|option| {
            let label = if option.value == "default" {
                "Default · clear override".to_string()
            } else {
                format!("{} · {}", option.label, option.value)
            };
            serenity::AutocompleteChoice::new(label, option.value)
        })
        .collect()
}

fn resolve_effective_model(
    override_model: Option<&str>,
    role_model: Option<&str>,
) -> (String, &'static str) {
    if let Some(model) = override_model {
        (model.to_string(), "runtime override")
    } else if let Some(model) = role_model {
        (model.to_string(), "role-map")
    } else {
        ("default".to_string(), "provider default")
    }
}

async fn resolve_role_model(
    shared: &Arc<SharedData>,
    channel_id: serenity::ChannelId,
) -> Option<String> {
    let channel_name = {
        let data = shared.core.lock().await;
        data.sessions
            .get(&channel_id)
            .and_then(|session| session.channel_name.clone())
    };

    resolve_role_binding(channel_id, channel_name.as_deref()).and_then(|binding| binding.model)
}

fn describe_effective_model_line(provider: &ProviderKind, model: &str) -> String {
    model_details_for_provider(provider, model).unwrap_or_else(|| {
        "Curated metadata is not available for this raw model override.".to_string()
    })
}

pub(in crate::services::discord) async fn describe_model_override(
    shared: &Arc<SharedData>,
    provider: &ProviderKind,
    channel_id: serenity::ChannelId,
) -> ModelCommandResponse {
    let Some(catalog) = catalog_for_provider(provider) else {
        return model_error_response(
            provider,
            format!(
                "Model override catalog is not available for provider `{}`.",
                provider.as_str()
            ),
        );
    };

    let override_model = shared
        .model_overrides
        .get(&channel_id)
        .map(|value| value.clone());
    let role_model = resolve_role_model(shared, channel_id).await;
    let (effective, source) =
        resolve_effective_model(override_model.as_deref(), role_model.as_deref());
    let options = render_catalog_values(provider).unwrap_or_default();
    let effective_details = describe_effective_model_line(provider, &effective);
    let default_details = model_details_for_provider(provider, "default")
        .unwrap_or_else(|| "Default model metadata is unavailable.".to_string());
    let next_turn = if shared
        .pending_model_session_resets
        .contains_key(&channel_id)
    {
        "fresh session pending"
    } else {
        "reuse current session if available"
    };

    let plain = format!(
        "**Model**\nProvider: **{}**\nEffective: **{}**\nDetails: {}\nSource: `{}`\nDefault: {}\nOptions: {}\nNext turn: {}",
        catalog.display_name,
        effective,
        effective_details,
        source,
        default_details,
        options,
        next_turn
    );

    ModelCommandResponse::new(
        plain,
        format!("{} Model", catalog.display_name),
        provider_embed_color(provider),
    )
    .description("Current model routing for this channel.")
    .field("Provider", catalog.display_name, true)
    .field("Effective", effective, true)
    .field("Source", source, true)
    .field("Details", effective_details, false)
    .field("Default", default_details, false)
    .field("Options", options, false)
    .field("Next Turn", next_turn, false)
}

pub(in crate::services::discord) async fn apply_model_override(
    shared: &Arc<SharedData>,
    provider: &ProviderKind,
    channel_id: serenity::ChannelId,
    raw: &str,
    bot_token: &str,
) -> Result<ModelCommandResponse, String> {
    if catalog_for_provider(provider).is_none() {
        return Err(format!(
            "Model override catalog is not available for provider `{}`.",
            provider.as_str()
        ));
    }

    let role_model = resolve_role_model(shared, channel_id).await;
    let previous_override = shared
        .model_overrides
        .get(&channel_id)
        .map(|value| value.clone());
    let (previous_effective, _) =
        resolve_effective_model(previous_override.as_deref(), role_model.as_deref());
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

    let (effective_next_turn, _) =
        resolve_effective_model(normalized.as_deref(), role_model.as_deref());
    let reset_required = previous_effective != effective_next_turn;
    if reset_required {
        shared.pending_model_session_resets.insert(channel_id, true);
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
    let effective_details = describe_effective_model_line(provider, &effective_next_turn);
    let default_details = model_details_for_provider(provider, "default")
        .unwrap_or_else(|| "Default model metadata is unavailable.".to_string());

    let plain = format!(
        "{}\nEffective next turn: **{}**\nDetails: {}\nDefault: {}\n{}",
        action_line, effective_next_turn, effective_details, default_details, reset_line
    );

    Ok(
        ModelCommandResponse::new(
            plain,
            "Model Override Updated",
            provider_embed_color(provider),
        )
        .description(action_line)
        .field("Effective Next Turn", effective_next_turn, true)
        .field(
            "Session Reset",
            if reset_required {
                "Fresh session will be started"
            } else {
                "Current session can be reused"
            },
            true,
        )
        .field("Provider", provider.as_str(), true)
        .field("Details", effective_details, false)
        .field("Default", default_details, false),
    )
}

/// /model — Set or view the model override for this channel
#[poise::command(slash_command, rename = "model")]
pub(in crate::services::discord) async fn cmd_model(
    ctx: Context<'_>,
    #[autocomplete = "autocomplete_model"]
    #[description = "CLI-safe model value or 'default' to clear"]
    model: Option<String>,
) -> Result<(), Error> {
    let user_id = ctx.author().id;
    let user_name = &ctx.author().name;
    if !check_auth(user_id, user_name, &ctx.data().shared, &ctx.data().token).await {
        return Ok(());
    }

    let ts = chrono::Local::now().format("%H:%M:%S");
    let channel_id = ctx.channel_id();

    match model {
        Some(raw) => {
            println!("  [{ts}] ◀ [{user_name}] /model {raw}");
            match apply_model_override(
                &ctx.data().shared,
                &ctx.data().provider,
                channel_id,
                &raw,
                &ctx.data().token,
            )
            .await
            {
                Ok(response) => {
                    send_model_response_ctx(ctx, response).await?;
                }
                Err(message) => {
                    send_model_response_ctx(ctx, model_error_response(&ctx.data().provider, message))
                        .await?;
                }
            }
        }
        None => {
            println!("  [{ts}] ◀ [{user_name}] /model");
            send_model_response_ctx(
                ctx,
                describe_model_override(&ctx.data().shared, &ctx.data().provider, channel_id).await,
            )
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
