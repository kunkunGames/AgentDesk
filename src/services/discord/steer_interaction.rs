//! Interaction handler for the `/steer` cancel button (P1.5).
//!
//! The button's `custom_id` is `steer:cancel:<source_id>`, where `source_id` is
//! the steer intervention's `message_id` (== the Discord interaction id). On
//! click we authorise the user, then call the existing
//! `mailbox_cancel_soft_intervention` to remove the still-queued intervention
//! before it is delivered, and edit the card into the
//! `큐잉이 취소됨 : <instruction>` state. No new cancel core is introduced — this
//! mirrors `idle_recap_interaction.rs` and reuses the reaction-cancel path's
//! helper.

use poise::serenity_prelude as serenity;

use super::steering::{SteerLifecycle, parse_steer_cancel_source_id, steer_lifecycle_label};
use super::{Data, Error, check_auth, mailbox_cancel_soft_intervention};

pub(super) async fn handle_steer_cancel_interaction(
    ctx: &serenity::Context,
    component: &serenity::ComponentInteraction,
    data: &Data,
) -> Result<(), Error> {
    // Authorise the click with the same gate the `/steer` command uses. Without
    // this, anyone able to see the card could cancel another operator's steer.
    let user_id = component.user.id;
    let user_name = &component.user.name;
    if !check_auth(user_id, user_name, &data.shared, &data.token).await {
        let _ = component
            .create_response(
                ctx,
                serenity::CreateInteractionResponse::Message(
                    serenity::CreateInteractionResponseMessage::new()
                        .content("Not authorized for this bot.")
                        .ephemeral(true),
                ),
            )
            .await;
        return Ok(());
    }

    let Some(source_id) = parse_steer_cancel_source_id(&component.data.custom_id) else {
        // Malformed / sentinel id — acknowledge so the client doesn't time out.
        let _ = component
            .create_response(ctx, serenity::CreateInteractionResponse::Acknowledge)
            .await;
        return Ok(());
    };

    let channel_id = component.channel_id;
    // Reuse the existing soft-intervention cancel path (same as reaction-remove).
    // It removes the queued intervention and fires `apply_queue_exit_feedback`.
    let removed =
        mailbox_cancel_soft_intervention(&data.shared, &data.provider, channel_id, source_id).await;

    match removed {
        Some(intervention) => {
            // State 3 (REQ-013): `큐잉이 취소됨 : <instruction>`. The instruction is
            // recovered from the removed intervention so the label is exact even
            // though the custom_id carries only the source id.
            let cancelled_label =
                steer_lifecycle_label(SteerLifecycle::Cancelled, &intervention.text);
            let ts = chrono::Local::now().format("%H:%M:%S");
            tracing::info!(
                "  [{ts}] 📭 STEER-CANCEL: {cancelled_label} (channel {})",
                channel_id.get()
            );
            let _ = component
                .create_response(
                    ctx,
                    serenity::CreateInteractionResponse::UpdateMessage(
                        serenity::CreateInteractionResponseMessage::new()
                            .content(cancelled_label)
                            .components(Vec::new()),
                    ),
                )
                .await;
        }
        None => {
            // Already delivered or already cancelled — idempotent. Strip the
            // button so the stale card cannot be clicked again, but do not claim
            // a cancellation for an instruction that already reached the agent.
            let _ = component
                .create_response(
                    ctx,
                    serenity::CreateInteractionResponse::UpdateMessage(
                        serenity::CreateInteractionResponseMessage::new().components(Vec::new()),
                    ),
                )
                .await;
        }
    }

    Ok(())
}
