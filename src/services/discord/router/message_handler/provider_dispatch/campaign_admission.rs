//! Authenticate a finite campaign admission window without replaying prepared intake.
use super::StreamingTurn;
use crate::db::campaigns::graph::learning::budget::{REVIEW_TASK_SQL, ReviewPolicy};
use crate::db::campaigns::graph::{Goal, contract_digest};
use crate::services::cluster::execution_capacity::AdmissionCheck;
use crate::services::discord::inflight;
use crate::services::provider::ProviderKind;
use crate::services::turn_orchestrator::{ChannelMailboxHandle, ChannelMailboxRegistry};
use chrono::{DateTime, Utc};
use poise::serenity_prelude::ChannelId;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(test)]
mod tests;

pub(super) struct CampaignAdmission<'a> {
    pool: &'a PgPool,
    dispatch: &'a str,
    provider: &'a ProviderKind,
    channel: u64,
    nonce: &'a str,
    token: &'a Arc<crate::services::provider::CancelToken>,
    mailbox: ChannelMailboxHandle,
    tmux: &'a str,
    user_message: u64,
    request_owner: u64,
    agent: String,
    deadline: Instant,
    identity: Value,
}

struct Observed {
    identity: Value,
    deadline: DateTime<Utc>,
    now: DateTime<Utc>,
}

impl<'a> CampaignAdmission<'a> {
    pub(super) fn capture(turn: &'a StreamingTurn<'a>) -> Result<Option<Self>, String> {
        let Some(dispatch) = turn.campaign_dispatch_id else {
            return Ok(None);
        };
        let pool = turn.pool.ok_or("campaign admission requires PostgreSQL")?;
        let agent = crate::services::platform::active_provider_context(turn.provider.as_str())
            .and_then(|context| context.agent_id);
        let nonce = turn.cancel.turn_nonce();
        let started = Instant::now();
        let observed = tokio::runtime::Handle::current().block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                inspect(
                    pool,
                    dispatch,
                    turn.provider,
                    turn.channel_id,
                    agent.as_deref(),
                ),
            )
            .await
            .map_err(|_| "campaign policy lookup timed out".to_string())?
        })?;
        let Some(observed) = observed else {
            return Ok(None);
        };
        let remaining = (observed.deadline - observed.now)
            .to_std()
            .map_err(|_| "campaign original deadline elapsed".to_string())?;
        let mailbox = ChannelMailboxRegistry::global_handle(ChannelId::new(turn.channel_id))
            .ok_or("campaign admission mailbox missing")?;
        let state = inflight::load_inflight_state_read_only_result(turn.provider, turn.channel_id)?
            .ok_or("campaign admission inflight owner missing")?;
        let value = Self {
            pool,
            dispatch,
            provider: turn.provider,
            channel: turn.channel_id,
            nonce: nonce
                .filter(|value| !value.is_empty())
                .ok_or("campaign turn nonce missing")?,
            token: &turn.cancel,
            mailbox,
            tmux: turn
                .tmux_session_name
                .filter(|name| !name.is_empty())
                .ok_or("campaign admission runtime name missing")?,
            user_message: state.user_msg_id,
            request_owner: state.request_owner_user_id,
            agent: agent.ok_or("campaign execution agent missing")?,
            deadline: started + remaining,
            identity: observed.identity,
        };
        tokio::runtime::Handle::current().block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5)
                    .min(value.deadline.saturating_duration_since(Instant::now())),
                value.owner(),
            )
            .await
            .map_err(|_| "campaign owner lookup timed out".to_string())?
        })?;
        if Instant::now() >= value.deadline {
            return Err("campaign original deadline elapsed during lookup".into());
        }
        Ok(Some(value))
    }

    async fn owner(&self) -> Result<(), String> {
        let current = ChannelMailboxRegistry::global_handle(ChannelId::new(self.channel))
            .ok_or("campaign admission mailbox missing")?;
        if !current.same_actor(&self.mailbox) {
            return Err("campaign admission mailbox incarnation changed".into());
        }
        let active = self
            .mailbox
            .try_snapshot()
            .await
            .map_err(|_| "campaign admission mailbox unreachable".to_string())?;
        let state = inflight::load_inflight_state_read_only_result(self.provider, self.channel)?
            .ok_or("campaign admission inflight owner missing")?;
        if state.provider != self.provider.as_str()
            || state.channel_id != self.channel
            || state.dispatch_id.as_deref() != Some(self.dispatch)
            || state.turn_nonce.as_deref() != Some(self.nonce)
            || state.terminal_delivery_completed()
            || state.rebind_origin
            || state.tmux_session_name.as_deref() != Some(self.tmux)
            || self.user_message == 0
            || self.request_owner == 0
            || state.user_msg_id != self.user_message
            || state.request_owner_user_id != self.request_owner
            || active.active_turn_nonce.as_deref() != Some(self.nonce)
            || active.active_user_message_id.map(|id| id.get()) != Some(self.user_message)
            || active.active_request_owner.map(|id| id.get()) != Some(self.request_owner)
            || !active
                .cancel_token
                .as_ref()
                .is_some_and(|token| Arc::ptr_eq(token, self.token))
        {
            return Err("campaign admission inflight ownership changed".into());
        }
        Ok(())
    }
}

impl AdmissionCheck for CampaignAdmission<'_> {
    fn deadline(&self) -> Instant {
        self.deadline
    }

    fn verify(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            self.owner().await?;
            let observed = inspect(
                self.pool,
                self.dispatch,
                self.provider,
                self.channel,
                Some(&self.agent),
            )
            .await?
            .ok_or("campaign admission task disappeared")?;
            if observed.identity != self.identity || observed.now >= observed.deadline {
                return Err("campaign admission policy or original deadline changed".into());
            }
            self.owner().await
        })
    }
}

async fn inspect(
    pool: &PgPool,
    dispatch: &str,
    provider: &ProviderKind,
    channel: u64,
    agent: Option<&str>,
) -> Result<Option<Observed>, String> {
    inspect_inner(pool, dispatch, provider, channel, agent)
        .await
        .map_err(|error| error.to_string())
}

async fn inspect_inner(
    pool: &PgPool,
    dispatch: &str,
    provider: &ProviderKind,
    channel: u64,
    agent: Option<&str>,
) -> anyhow::Result<Option<Observed>> {
    use anyhow::{Context, ensure};
    let rows = sqlx::query(&format!(
        "SELECT d.status,d.completed_at,d.created_at,d.dispatch_type,d.thread_id,d.to_agent_id,
            t.task_key,t.campaign_id,t.role,t.version,t.contract_digest,t.execution_profile,t.accepted,t.observation,
            k.id AS card_id,k.assigned_agent_id,k.description,k.metadata,
            s.spec,s.hold_reason,c.id AS current_campaign,c.document->>'status' AS campaign_status,
            review.snapshot AS review_snapshot,review.created_at AS review_created,review.status AS review_status,
            ({REVIEW_TASK_SQL}) AS captured_review,
            d.id=(SELECT latest.id FROM task_dispatches latest WHERE latest.kanban_card_id=k.id
                ORDER BY latest.created_at DESC,latest.id DESC LIMIT 1) AS latest,
            clock_timestamp() AS now
         FROM task_dispatches d LEFT JOIN campaign_graph_tasks t ON t.card_id=d.kanban_card_id
         LEFT JOIN kanban_cards k ON k.id=d.kanban_card_id
         LEFT JOIN campaign_execution_specs s ON s.campaign_id=t.campaign_id
         LEFT JOIN campaigns c ON c.id=t.campaign_id
         LEFT JOIN campaign_dag_reviews review ON review.task_key=t.task_key
         WHERE d.id=$1"))
        .bind(dispatch).fetch_all(pool).await?;
    ensure!(
        rows.len() == 1,
        "campaign admission dispatch is missing or ambiguous"
    );
    let row = &rows[0];
    // An ordinary intake dispatch keeps fail-fast behavior. Missing campaign
    // authority must never be mistaken for that ordinary non-campaign case.
    let Some(task_key) = row.try_get::<Option<String>, _>("task_key")? else {
        ensure!(
            row.try_get::<Option<Value>, _>("metadata")?
                .as_ref()
                .and_then(|metadata| metadata.get("graph_task_key"))
                .is_none(),
            "campaign admission task disappeared"
        );
        return Ok(None);
    };
    let campaign: String = row.try_get("campaign_id")?;
    ensure!(
        row.try_get::<Option<String>, _>("current_campaign")?
            .as_ref()
            == Some(&campaign)
            && row.try_get::<Option<Value>, _>("spec")?.is_some(),
        "campaign admission authority is missing"
    );
    let profile: Value = row
        .get::<Option<Value>, _>("execution_profile")
        .context("campaign execution profile missing")?;
    let description: String = row
        .get::<Option<String>, _>("description")
        .context("campaign prompt missing")?;
    let assigned: Option<String> = row.get("assigned_agent_id");
    ensure!(
        agent.is_some()
            && assigned.as_deref() == agent
            && row.get::<Option<String>, _>("to_agent_id") == assigned
            && row.get::<Option<String>, _>("thread_id").as_deref()
                == Some(channel.to_string().as_str())
            && profile["provider_requested"].as_str() == Some(provider.as_str())
            && profile["prompt_digest"]
                == crate::db::campaigns::graph::digest(description.as_bytes())
            && row.get::<bool, _>("latest")
            && !row.get::<bool, _>("accepted")
            && row.get::<Option<Value>, _>("observation").is_none()
            && row
                .get::<Option<DateTime<Utc>>, _>("completed_at")
                .is_none()
            && matches!(
                row.get::<Option<String>, _>("status").as_deref(),
                Some("pending" | "dispatched" | "running")
            )
            && matches!(
                row.get::<Option<String>, _>("dispatch_type").as_deref(),
                Some("implementation" | "rework")
            ),
        "campaign admission dispatch, prompt or executor changed"
    );
    let created: DateTime<Utc> = row
        .try_get::<Option<DateTime<Utc>>, _>("created_at")?
        .context("campaign dispatch creation timestamp missing")?;
    ensure!(
        row.get::<String, _>("role") != "template_review" || row.get::<bool, _>("captured_review"),
        "campaign review authority is missing"
    );
    let (deadline, captured) = if row.get::<bool, _>("captured_review") {
        ensure!(
            row.get::<Option<String>, _>("review_status").as_deref() == Some("pending"),
            "campaign review is terminal"
        );
        let snapshot: Value = row
            .get::<Option<Value>, _>("review_snapshot")
            .context("campaign review snapshot missing")?;
        let claimed = row
            .get::<Option<DateTime<Utc>>, _>("review_created")
            .context("campaign review claim missing")?;
        let policy = ReviewPolicy::from_snapshot(&snapshot, claimed)?;
        ensure!(
            profile["goal_digest"] == policy.goal_digest,
            "campaign review goal changed"
        );
        (policy.deadline, snapshot)
    } else {
        ensure!(
            matches!(
                row.get::<Option<String>, _>("campaign_status").as_deref(),
                Some("active" | "paused")
            ) && row.get::<Option<String>, _>("hold_reason").is_none(),
            "campaign is not admitted"
        );
        // A soft pause stops future admission, not an already owned dispatch.
        // Its dispatch status, original deadline and live cancel token fence it.
        let captured = profile
            .get("workflow_goal")
            .cloned()
            .unwrap_or_else(|| row.get("spec"));
        let goal: Goal = serde_json::from_value(captured.clone())?;
        goal.validate()?;
        ensure!(
            profile["goal_digest"] == contract_digest(&goal)?,
            "campaign original goal policy cannot be authenticated"
        );
        (
            created + chrono::Duration::seconds(i64::from(goal.budget.node_timeout_secs)),
            captured,
        )
    };
    let now: DateTime<Utc> = row.get("now");
    ensure!(
        created <= now && now < deadline,
        "campaign original dispatch deadline elapsed"
    );
    Ok(Some(Observed {
        deadline,
        now,
        identity: json!({
            "dispatch":dispatch,"task":task_key,"campaign":campaign,
            "card":row.get::<String,_>("card_id"),"created":created,"agent":assigned,"profile":profile,
            "contract":row.get::<String,_>("contract_digest"),"role":row.get::<String,_>("role"),
            "version":row.get::<Option<i64>,_>("version"),
            "captured":captured,"deadline":deadline
        }),
    }))
}
