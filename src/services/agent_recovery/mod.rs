//! Live Discord-channel recovery fallback.
//!
//! Same-channel exclusive intake. Mailbox handoff is never used.

pub mod checkpoint;
pub mod detector;
pub mod handoff;
pub mod policy;
pub mod restore;

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use sqlx::PgPool;

use crate::services::provider::ProviderKind;

pub use checkpoint::{
    ChannelRecoveryStatus, ChannelState, CheckpointEvent, CheckpointEventKind, CheckpointPayload,
    DEFAULT_MAX_CHECKPOINT_BYTES, DEFAULT_READ_EVENT_LIMIT, READ_BYTE_CAP,
};
pub use detector::{
    DetectorSignal, MailboxStallKind, classify_trigger, trigger_from_error_message,
};
pub use handoff::{FallbackSpawnPlan, RecoveryIntake, dual_processing, effective_handles};
pub use policy::{
    OrgAgentInput, OrgChannelInput, PolicyError, RecoveryCatalog, RecoveryConfigWire,
    RecoveryPolicy, TriggerKind, WorkspaceMode, build_recovery_catalog,
    load_org_recovery_catalog_from_yaml, validate_distinct_fallback_agent,
};
pub use restore::{
    RestorePlan, RestoreSessionMode, format_restore_packet, session_mode_for_provider,
};

use checkpoint::{
    last_n_events, load_checkpoint_events, load_locked_channel_states, persist_channel_state,
    persist_checkpoint_event, prepare_event,
};
use policy::ChannelRecoveryBinding;
use restore::build_restore_plan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObserveInput {
    pub channel_id: String,
    pub primary_turn_id: String,
    pub signal: DetectorSignal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObserveOutcome {
    pub trigger: Option<TriggerKind>,
    pub spawn: Option<FallbackSpawnPlan>,
}

#[derive(Debug)]
pub struct RecoveryRuntime {
    catalog: RecoveryCatalog,
    states: HashMap<String, ChannelState>,
    events: HashMap<String, Vec<CheckpointEvent>>,
    claimed_turns: HashMap<String, String>,
    open_keys: HashSet<(String, String)>,
    spawned: Vec<FallbackSpawnPlan>,
    mailbox_handoff_calls: u32,
    pending_restore: HashMap<String, String>,
    max_checkpoint_bytes: usize,
}

impl Default for RecoveryRuntime {
    fn default() -> Self {
        Self {
            catalog: RecoveryCatalog::default(),
            states: HashMap::new(),
            events: HashMap::new(),
            claimed_turns: HashMap::new(),
            open_keys: HashSet::new(),
            spawned: Vec::new(),
            mailbox_handoff_calls: 0,
            pending_restore: HashMap::new(),
            max_checkpoint_bytes: DEFAULT_MAX_CHECKPOINT_BYTES,
        }
    }
}

impl RecoveryRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_checkpoint_bytes(mut self, max_checkpoint_bytes: usize) -> Self {
        self.max_checkpoint_bytes = max_checkpoint_bytes.max(1);
        self
    }

    pub fn install_catalog(&mut self, catalog: RecoveryCatalog) {
        self.catalog = catalog;
    }

    pub fn catalog(&self) -> &RecoveryCatalog {
        &self.catalog
    }

    pub fn mailbox_handoff_calls(&self) -> u32 {
        self.mailbox_handoff_calls
    }

    pub fn spawned(&self) -> &[FallbackSpawnPlan] {
        &self.spawned
    }

    pub fn events(&self, channel_id: &str) -> &[CheckpointEvent] {
        self.events
            .get(channel_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn last_n(&self, channel_id: &str) -> Vec<CheckpointEvent> {
        last_n_events(
            self.events(channel_id),
            DEFAULT_READ_EVENT_LIMIT,
            READ_BYTE_CAP,
        )
    }

    pub fn claim_turn(&mut self, channel_id: &str, turn_id: &str) {
        self.claimed_turns
            .insert(channel_id.to_string(), turn_id.to_string());
    }

    pub fn inherit_workspace(&self, channel_id: &str) -> Option<String> {
        self.catalog
            .channels
            .get(channel_id)
            .map(|binding| binding.workspace.clone())
    }

    pub fn channel_recovery_intake(
        &self,
        provider: &ProviderKind,
        channel_id: &str,
    ) -> Option<RecoveryIntake> {
        let state = self.states.get(channel_id)?;
        if !state.lock_held() {
            return None;
        }
        let holder_provider = self.catalog.agent_provider(&state.active_writer_agent_id)?;
        if provider == &holder_provider {
            Some(RecoveryIntake::Allow)
        } else {
            Some(RecoveryIntake::Skip)
        }
    }

    pub fn allows_cli_turn(&self, channel_id: &str, agent_id: &str) -> bool {
        match self.states.get(channel_id) {
            Some(state) if state.lock_held() => state.active_writer_agent_id == agent_id,
            _ => true,
        }
    }

    pub fn allows_cli_turn_for_provider(&self, channel_id: &str, provider: &ProviderKind) -> bool {
        match self.states.get(channel_id) {
            Some(state) if state.lock_held() => {
                self.catalog
                    .agent_provider(&state.active_writer_agent_id)
                    .as_ref()
                    == Some(provider)
            }
            _ => true,
        }
    }

    pub fn fallback_prompt_prefix(&self, channel_id: &str) -> Option<String> {
        let state = self.states.get(channel_id)?;
        if !state.lock_held() {
            return None;
        }
        let binding = self.catalog.channels.get(channel_id)?;
        Some(handoff::format_fallback_prompt(
            binding,
            &self.last_n(channel_id),
        ))
    }

    pub fn take_restore_packet(&mut self, channel_id: &str) -> Option<String> {
        self.pending_restore.remove(channel_id)
    }

    pub fn note_owner_progress(
        &mut self,
        channel_id: &str,
        payload: CheckpointPayload,
    ) -> Result<Option<CheckpointEvent>, checkpoint::CheckpointError> {
        let Some(binding) = self.catalog.channels.get(channel_id).cloned() else {
            return Ok(None);
        };
        if binding.policy.as_ref().is_none_or(|policy| !policy.enabled) {
            return Ok(None);
        }
        let event = self.append_event(
            channel_id,
            &binding.owner_agent_id,
            CheckpointEventKind::OwnerProgress,
            payload,
        )?;
        Ok(Some(event))
    }

    pub fn observe(&mut self, input: ObserveInput) -> ObserveOutcome {
        let Some(binding) = self.catalog.channels.get(&input.channel_id).cloned() else {
            return ObserveOutcome::default();
        };
        let policy = binding.policy.as_ref();
        let trigger = classify_trigger(&input.signal, policy);
        let Some(trigger) = trigger else {
            return ObserveOutcome::default();
        };
        let key = (input.channel_id.clone(), input.primary_turn_id.clone());
        if self.open_keys.contains(&key) {
            return ObserveOutcome {
                trigger: Some(trigger),
                spawn: None,
            };
        }
        if self
            .states
            .get(&input.channel_id)
            .is_some_and(ChannelState::lock_held)
        {
            return ObserveOutcome {
                trigger: Some(trigger),
                spawn: None,
            };
        }
        let Some(policy) = policy.cloned() else {
            return ObserveOutcome::default();
        };
        let Some(fallback_provider) = self.catalog.agent_provider(&policy.fallback_agent_id) else {
            return ObserveOutcome::default();
        };
        self.claimed_turns
            .insert(input.channel_id.clone(), input.primary_turn_id.clone());
        let stall_payload = CheckpointPayload::compact(
            binding.owner_agent_id.clone(),
            "",
            format!("stalled: {}", trigger.as_str()),
            "",
            Vec::new(),
            "fallback continues on the original channel",
            "",
        );
        if self
            .append_event(
                &input.channel_id,
                &binding.owner_agent_id,
                CheckpointEventKind::Stall,
                stall_payload,
            )
            .is_err()
        {
            return ObserveOutcome {
                trigger: Some(trigger),
                spawn: None,
            };
        }
        let state = self.ensure_state(&binding, &policy.fallback_agent_id);
        state.status = ChannelRecoveryStatus::FallbackRunning;
        state.active_writer_agent_id = policy.fallback_agent_id.clone();
        state.primary_turn_id = Some(input.primary_turn_id.clone());
        self.open_keys.insert(key);
        let events = self.last_n(&input.channel_id);
        let Some(spawn) = FallbackSpawnPlan::from_binding(&binding, fallback_provider, &events)
        else {
            return ObserveOutcome {
                trigger: Some(trigger),
                spawn: None,
            };
        };
        self.spawned.push(spawn.clone());
        ObserveOutcome {
            trigger: Some(trigger),
            spawn: Some(spawn),
        }
    }

    pub fn note_fallback_progress(
        &mut self,
        channel_id: &str,
        kind: CheckpointEventKind,
        payload: CheckpointPayload,
    ) -> Result<Option<CheckpointEvent>, checkpoint::CheckpointError> {
        let Some(state) = self.states.get(channel_id) else {
            return Ok(None);
        };
        if !state.lock_held() {
            return Ok(None);
        }
        let writer = state.active_writer_agent_id.clone();
        let event = self.append_event(channel_id, &writer, kind, payload)?;
        if kind == CheckpointEventKind::Complete
            && let Some(state) = self.states.get_mut(channel_id)
        {
            state.status = ChannelRecoveryStatus::FallbackDone;
        }
        Ok(Some(event))
    }

    pub fn fallback_provider(&self, channel_id: &str) -> Option<ProviderKind> {
        let binding = self.catalog.channels.get(channel_id)?;
        let policy = binding.policy.as_ref()?;
        self.catalog.agent_provider(&policy.fallback_agent_id)
    }

    pub fn is_fallback_writer(&self, channel_id: &str, provider: &ProviderKind) -> bool {
        let Some(state) = self.states.get(channel_id) else {
            return false;
        };
        if !state.lock_held() {
            return false;
        }
        self.catalog
            .agent_provider(&state.active_writer_agent_id)
            .as_ref()
            == Some(provider)
            && self
                .catalog
                .channels
                .get(channel_id)
                .is_some_and(|binding| binding.owner_provider != *provider)
    }

    pub fn owner_may_retake(&self, channel_id: &str) -> bool {
        let Some(state) = self.states.get(channel_id) else {
            return false;
        };
        if !state.lock_held() {
            return false;
        }
        if state.status == ChannelRecoveryStatus::FallbackDone {
            return true;
        }
        self.events(channel_id)
            .iter()
            .rev()
            .find(|event| event.kind == CheckpointEventKind::Stall)
            .is_some_and(|event| event.payload.progress.contains("process_death"))
    }

    pub fn try_restore_owner(
        &mut self,
        channel_id: &str,
        observing_provider: &ProviderKind,
        owner_healthy: bool,
        fallback_inflight: bool,
    ) -> Option<RestorePlan> {
        let binding = self.catalog.channels.get(channel_id)?;
        if binding.owner_provider != *observing_provider {
            return None;
        }
        if !self
            .states
            .get(channel_id)
            .is_some_and(ChannelState::lock_held)
        {
            return None;
        }
        if !owner_healthy || fallback_inflight || !self.owner_may_retake(channel_id) {
            return None;
        }
        self.restore_owner(channel_id, true, "owner available")
    }

    pub fn restore_owner(
        &mut self,
        channel_id: &str,
        fallback_succeeded: bool,
        summary: &str,
    ) -> Option<RestorePlan> {
        let binding = self.catalog.channels.get(channel_id)?.clone();
        let policy = binding.policy.as_ref()?;
        if self
            .events(channel_id)
            .iter()
            .all(|event| event.kind != CheckpointEventKind::Complete)
        {
            let payload = CheckpointPayload::compact(
                policy.fallback_agent_id.clone(),
                "",
                if fallback_succeeded {
                    "fallback complete"
                } else {
                    "fallback failed"
                },
                "",
                Vec::new(),
                "",
                "",
            );
            let _ = self.append_event(
                channel_id,
                &policy.fallback_agent_id,
                CheckpointEventKind::Complete,
                payload,
            );
        }
        let events = self.last_n(channel_id);
        let mut plan = build_restore_plan(&binding, &events, fallback_succeeded, summary);
        let restore_payload = events
            .last()
            .map(|event| event.payload.clone())
            .unwrap_or_else(|| {
                CheckpointPayload::compact(
                    binding.owner_agent_id.clone(),
                    "",
                    "unknown",
                    "",
                    Vec::new(),
                    "마지막 사용자 메시지부터 재확인",
                    "",
                )
            });
        let _ = self.append_event(
            channel_id,
            &binding.owner_agent_id,
            CheckpointEventKind::Restore,
            restore_payload,
        );
        if let Some(state) = self.states.get_mut(channel_id) {
            state.status = ChannelRecoveryStatus::Restored;
            state.active_writer_agent_id = binding.owner_agent_id.clone();
        }
        self.open_keys
            .retain(|(held_channel, _)| held_channel != channel_id);
        self.pending_restore
            .insert(channel_id.to_string(), plan.packet.clone());
        plan.owner_intake = self.channel_recovery_intake(&binding.owner_provider, channel_id);
        if let Some(fallback_provider) = self.catalog.agent_provider(&policy.fallback_agent_id) {
            plan.fallback_intake = self.channel_recovery_intake(&fallback_provider, channel_id);
        }
        Some(plan)
    }

    pub fn abort(&mut self, channel_id: &str) {
        if let Some(state) = self.states.get_mut(channel_id) {
            state.status = ChannelRecoveryStatus::Aborted;
            state.active_writer_agent_id = state.owner_agent_id.clone();
        }
        self.open_keys
            .retain(|(held_channel, _)| held_channel != channel_id);
    }

    fn ensure_state(
        &mut self,
        binding: &ChannelRecoveryBinding,
        fallback_agent_id: &str,
    ) -> &mut ChannelState {
        self.states
            .entry(binding.channel_id.clone())
            .or_insert_with(|| ChannelState {
                channel_id: binding.channel_id.clone(),
                status: ChannelRecoveryStatus::Owner,
                owner_agent_id: binding.owner_agent_id.clone(),
                fallback_agent_id: fallback_agent_id.to_string(),
                active_writer_agent_id: binding.owner_agent_id.clone(),
                workspace: binding.policy.as_ref().map_or_else(
                    || "inherit".to_string(),
                    |policy| policy.workspace_mode.as_str().to_string(),
                ),
                primary_turn_id: None,
                next_seq: 0,
            })
    }

    fn append_event(
        &mut self,
        channel_id: &str,
        writer_agent_id: &str,
        kind: CheckpointEventKind,
        payload: CheckpointPayload,
    ) -> Result<CheckpointEvent, checkpoint::CheckpointError> {
        let binding = self
            .catalog
            .channels
            .get(channel_id)
            .cloned()
            .ok_or_else(|| checkpoint::CheckpointError::Serialize("unknown channel".to_string()))?;
        let fallback_agent_id = binding
            .policy
            .as_ref()
            .map(|policy| policy.fallback_agent_id.clone())
            .unwrap_or_default();
        let max_checkpoint_bytes = self.max_checkpoint_bytes;
        let seq = {
            let state = self.ensure_state(&binding, &fallback_agent_id);
            state.next_seq += 1;
            state.next_seq
        };
        let event = prepare_event(
            channel_id,
            seq,
            writer_agent_id,
            kind,
            payload,
            max_checkpoint_bytes,
        )?;
        self.events
            .entry(channel_id.to_string())
            .or_default()
            .push(event.clone());
        let persisted = self
            .states
            .get(channel_id)
            .cloned()
            .expect("channel recovery state exists after ensure_state");
        persist_in_background(event.clone(), persisted);
        Ok(event)
    }
}

fn global_runtime() -> &'static Mutex<RecoveryRuntime> {
    static RUNTIME: OnceLock<Mutex<RecoveryRuntime>> = OnceLock::new();
    RUNTIME.get_or_init(|| Mutex::new(RecoveryRuntime::default()))
}

fn lock_runtime() -> std::sync::MutexGuard<'static, RecoveryRuntime> {
    global_runtime()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

static PG_POOL: OnceLock<Mutex<Option<PgPool>>> = OnceLock::new();

fn pg_pool_slot() -> &'static Mutex<Option<PgPool>> {
    PG_POOL.get_or_init(|| Mutex::new(None))
}

pub fn attach_pg_pool(pool: PgPool) {
    let mut slot = pg_pool_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *slot = Some(pool);
}

pub fn install_catalog(catalog: RecoveryCatalog) {
    lock_runtime().install_catalog(catalog);
}

pub fn channel_recovery_intake(
    provider: &ProviderKind,
    channel_id: &str,
) -> Option<RecoveryIntake> {
    lock_runtime().channel_recovery_intake(provider, channel_id)
}

pub fn allows_cli_turn(channel_id: &str, agent_id: &str) -> bool {
    lock_runtime().allows_cli_turn(channel_id, agent_id)
}

pub fn allows_cli_turn_for_provider(channel_id: &str, provider: &ProviderKind) -> bool {
    lock_runtime().allows_cli_turn_for_provider(channel_id, provider)
}

pub fn inherit_workspace(channel_id: &str) -> Option<String> {
    lock_runtime().inherit_workspace(channel_id)
}

pub fn fallback_prompt_prefix(channel_id: &str) -> Option<String> {
    lock_runtime().fallback_prompt_prefix(channel_id)
}

pub fn take_restore_packet(channel_id: &str) -> Option<String> {
    lock_runtime().take_restore_packet(channel_id)
}

pub fn fallback_provider(channel_id: &str) -> Option<ProviderKind> {
    lock_runtime().fallback_provider(channel_id)
}

pub fn is_fallback_writer(channel_id: &str, provider: &ProviderKind) -> bool {
    lock_runtime().is_fallback_writer(channel_id, provider)
}

pub fn try_restore_owner(
    channel_id: &str,
    observing_provider: &ProviderKind,
    owner_healthy: bool,
    fallback_inflight: bool,
) -> Option<RestorePlan> {
    lock_runtime().try_restore_owner(
        channel_id,
        observing_provider,
        owner_healthy,
        fallback_inflight,
    )
}

pub fn note_fallback_progress(
    channel_id: &str,
    kind: CheckpointEventKind,
    payload: CheckpointPayload,
) -> Result<Option<CheckpointEvent>, checkpoint::CheckpointError> {
    lock_runtime().note_fallback_progress(channel_id, kind, payload)
}

pub fn observe(input: ObserveInput) -> ObserveOutcome {
    lock_runtime().observe(input)
}

pub fn note_owner_progress(
    channel_id: &str,
    payload: CheckpointPayload,
) -> Result<Option<CheckpointEvent>, checkpoint::CheckpointError> {
    lock_runtime().note_owner_progress(channel_id, payload)
}

pub fn note_provider_error(channel_id: &str, turn_id: &str, error: &str) -> ObserveOutcome {
    let Some(signal) = trigger_from_error_message(error) else {
        return ObserveOutcome::default();
    };
    observe(ObserveInput {
        channel_id: channel_id.to_string(),
        primary_turn_id: turn_id.to_string(),
        signal,
    })
}

pub fn observe_mailbox_stall(
    channel_id: &str,
    turn_id: &str,
    kind_name: &str,
    elapsed_secs: u32,
    claimed_turn: bool,
) -> ObserveOutcome {
    observe(ObserveInput {
        channel_id: channel_id.to_string(),
        primary_turn_id: turn_id.to_string(),
        signal: DetectorSignal::Mailbox {
            kind: detector::mailbox_kind_from_name(kind_name),
            elapsed_secs,
            claimed_turn,
        },
    })
}

pub async fn hydrate_from_pg(pool: &PgPool) {
    attach_pg_pool(pool.clone());
    let Ok(states) = load_locked_channel_states(pool).await else {
        return;
    };
    let mut loaded = Vec::new();
    for state in states {
        let events =
            load_checkpoint_events(pool, &state.channel_id, DEFAULT_READ_EVENT_LIMIT as i64)
                .await
                .unwrap_or_default();
        loaded.push((state, events));
    }
    let mut runtime = lock_runtime();
    for (state, events) in loaded {
        let channel_id = state.channel_id.clone();
        runtime.events.insert(channel_id.clone(), events);
        if let Some(turn_id) = state.primary_turn_id.clone() {
            runtime
                .open_keys
                .insert((channel_id.clone(), turn_id.clone()));
            runtime.claimed_turns.insert(channel_id.clone(), turn_id);
        }
        runtime.states.insert(channel_id, state);
    }
}

fn persist_in_background(event: CheckpointEvent, state: ChannelState) {
    let pool = {
        let slot = pg_pool_slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slot.clone()
    };
    let Some(pool) = pool else {
        return;
    };
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = persist_channel_state(&pool, &state).await {
            tracing::error!(
                channel_id = %state.channel_id,
                error = %error,
                "failed to persist agent recovery channel state"
            );
            return;
        }
        if let Err(error) = persist_checkpoint_event(&pool, &event).await {
            tracing::error!(
                channel_id = %event.channel_id,
                error = %error,
                "failed to persist agent recovery checkpoint event"
            );
        }
    });
}

#[cfg(test)]
mod tests;
