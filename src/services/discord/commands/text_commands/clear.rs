//! Reply to text clear outcomes at the command boundary.

use crate::services::discord::Data;
use crate::services::discord::commands::{
    SESSION_CLEARED_RESPONSE, SoftClearNotifyMode, clear_channel_session_state,
};
use poise::serenity_prelude as serenity;

pub(super) async fn handle(
    ctx: &serenity::Context,
    msg: &serenity::Message,
    data: &Data,
    channel_id: serenity::ChannelId,
    cmd: &str,
) {
    let cleared = async {
        clear_channel_session_state(
            &ctx.http,
            &data.shared,
            &data.provider,
            channel_id,
            "!clear",
            SoftClearNotifyMode::Suppress,
        )
        .await?;
        let _ = msg.reply(&ctx.http, SESSION_CLEARED_RESPONSE).await;
        anyhow::Ok(())
    };
    // A returned error reaches the event log; the command must also answer the channel.
    if let Err(error) = cleared.await {
        tracing::warn!(
            event = "discord_command_failed",
            channel_id = channel_id.get(),
            user_id = msg.author.id.get(),
            user_name = %msg.author.name,
            command = cmd,
            reason = %error,
            "discord_command_failed"
        );
        let _ = msg.reply(&ctx.http, error.to_string()).await;
    }
}
