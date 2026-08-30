use super::inflight::load_inflight_state_read_only;
use super::relay_health::{RelayHealthSnapshot, RelayStallState};
use crate::services::agent_recovery::{self, DetectorSignal, ObserveInput};
use crate::services::discord::health::{self, HealthRegistry};
use crate::services::provider::ProviderKind;
use poise::serenity_prelude::ChannelId;
use serde_json::json;

/// Records a classified live-channel stall and, when recovery is configured,
/// starts the fenced fallback or restoration turn. This is deliberately called
/// only after the health registry's provider lock has been released: turn start
/// resolves a provider runtime and must never await while a health poll holds
/// that lock.
pub(in crate::services::discord) async fn observe_and_execute(
    registry: &HealthRegistry,
    snapshot: &RelayHealthSnapshot,
    stall: RelayStallState,
) -> bool {
    let channel_id = snapshot.channel_id.to_string();
    let turn_id = snapshot
        .mailbox_active_user_msg_id
        .unwrap_or(snapshot.channel_id)
        .to_string();
    let elapsed_secs = snapshot
        .mailbox_turn_age_secs
        .unwrap_or(0)
        .min(u64::from(u32::MAX)) as u32;
    let mailbox_outcome = agent_recovery::observe_mailbox_stall(
        &channel_id,
        &turn_id,
        stall.as_str(),
        elapsed_secs,
        snapshot.mailbox_has_cancel_token,
    );
    let spawn = mailbox_outcome
        .spawn
        .or_else(|| {
            (snapshot.tmux_alive == Some(false) && snapshot.mailbox_has_cancel_token).then(|| {
                agent_recovery::observe(ObserveInput {
                    channel_id: channel_id.clone(),
                    primary_turn_id: turn_id.clone(),
                    signal: DetectorSignal::TmuxSessionDead,
                })
                .spawn
            })
        })
        .flatten();
    if let Some(spawn) = spawn {
        return execute_fallback(registry, snapshot, spawn).await;
    }

    let Some(provider) = ProviderKind::from_str(&snapshot.provider) else {
        return false;
    };
    let owner_healthy = snapshot.tmux_alive == Some(true)
        && matches!(
            stall,
            RelayStallState::Healthy
                | RelayStallState::ActiveForegroundStream
                | RelayStallState::ExplicitBackgroundWork
        );
    let fallback_inflight =
        agent_recovery::fallback_provider(&channel_id).is_some_and(|fallback| {
            load_inflight_state_read_only(&fallback, snapshot.channel_id).is_some()
        });
    if let Some(plan) =
        agent_recovery::try_restore_owner(&channel_id, &provider, owner_healthy, fallback_inflight)
    {
        return execute_restore(registry, &provider, plan).await;
    }
    false
}

async fn execute_fallback(
    registry: &HealthRegistry,
    snapshot: &RelayHealthSnapshot,
    plan: agent_recovery::FallbackSpawnPlan,
) -> bool {
    let channel = ChannelId::new(snapshot.channel_id);
    // A claimed primary turn must be cancelled through the canonical runtime
    // path before the fallback can reserve the same mailbox. Failing open here
    // would permit two writers on one Discord channel, so leave the channel
    // unclaimed and let the next health poll retry instead.
    if snapshot.mailbox_has_cancel_token {
        let cancelled = health::force_kill_provider_channel_runtime(
            registry,
            &snapshot.provider,
            channel,
            "agent recovery fallback takeover",
            "agent_recovery_fallback_takeover",
        )
        .await;
        if !cancelled.is_some_and(|result| result.mailbox_foreground_free) {
            tracing::error!(
                channel_id = snapshot.channel_id,
                owner_provider = %snapshot.provider,
                fallback_provider = %plan.fallback_provider.as_str(),
                "agent recovery refused fallback start because owner mailbox was not fenced"
            );
            agent_recovery::abort(&plan.channel_id);
            return false;
        }
    }

    let metadata = recovery_metadata(
        "agent-recovery-fallback",
        &plan.fallback_agent_id,
        "fresh",
        "fallback",
    );
    match health::start_headless_agent_turn(
        registry,
        channel,
        plan.fallback_provider.clone(),
        plan.prompt,
        Some("agent-recovery-fallback".to_string()),
        Some(metadata),
        None,
    )
    .await
    {
        Ok(outcome) => {
            tracing::info!(
                channel_id = snapshot.channel_id,
                fallback_agent_id = %plan.fallback_agent_id,
                fallback_provider = %plan.fallback_provider.as_str(),
                turn_id = %outcome.turn_id,
                "agent recovery fallback turn started"
            );
            true
        }
        Err(error) => {
            tracing::error!(
                channel_id = snapshot.channel_id,
                fallback_agent_id = %plan.fallback_agent_id,
                error = %error,
                "agent recovery fallback turn could not start"
            );
            agent_recovery::abort(&plan.channel_id);
            false
        }
    }
}

async fn execute_restore(
    registry: &HealthRegistry,
    owner_provider: &ProviderKind,
    plan: agent_recovery::RestorePlan,
) -> bool {
    let Ok(channel_id) = plan.channel_id.parse::<u64>() else {
        tracing::error!(
            channel_id = %plan.channel_id,
            owner_agent_id = %plan.owner_agent_id,
            "agent recovery restore refused invalid channel id"
        );
        agent_recovery::abort(&plan.channel_id);
        return false;
    };
    let strategy = match plan.session_mode {
        agent_recovery::RestoreSessionMode::Resume => "persistent",
        agent_recovery::RestoreSessionMode::Fresh => "fresh",
    };
    let metadata = recovery_metadata(
        "agent-recovery-restore",
        &plan.owner_agent_id,
        strategy,
        "restore",
    );
    match health::start_headless_agent_turn(
        registry,
        ChannelId::new(channel_id),
        owner_provider.clone(),
        plan.packet,
        Some("agent-recovery-restore".to_string()),
        Some(metadata),
        None,
    )
    .await
    {
        Ok(outcome) => {
            tracing::info!(
                channel_id = %plan.channel_id,
                owner_agent_id = %plan.owner_agent_id,
                turn_id = %outcome.turn_id,
                "agent recovery owner restore turn started"
            );
            true
        }
        Err(error) => {
            tracing::error!(
                channel_id = %plan.channel_id,
                owner_agent_id = %plan.owner_agent_id,
                error = %error,
                "agent recovery owner restore turn could not start"
            );
            agent_recovery::abort(&plan.channel_id);
            false
        }
    }
}

fn recovery_metadata(
    routine_id: &str,
    agent_id: &str,
    execution_strategy: &str,
    recovery_mode: &str,
) -> serde_json::Value {
    json!({
        "routine_id": routine_id,
        "agent_id": agent_id,
        "execution_strategy": execution_strategy,
        "agent_recovery": { "mode": recovery_mode },
    })
}

#[cfg(test)]
mod tests {
    use super::recovery_metadata;

    #[test]
    fn recovery_metadata_is_a_role_bound_persistent_routine_when_requested() {
        let metadata =
            recovery_metadata("agent-recovery-restore", "claude", "persistent", "restore");
        assert_eq!(metadata["routine_id"], "agent-recovery-restore");
        assert_eq!(metadata["agent_id"], "claude");
        assert_eq!(metadata["execution_strategy"], "persistent");
        assert_eq!(metadata["agent_recovery"]["mode"], "restore");
    }
}
