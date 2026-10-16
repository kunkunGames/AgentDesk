//! Read-only runtime ownership for channel diagnostics. A provider/channel
//! inflight file is shared evidence, not proof that every runtime owns it.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use poise::serenity_prelude::ChannelId;
use serde::Serialize;

use super::super::{HealthRegistry, ProviderEntry, redaction};
use crate::services::discord;
use crate::services::provider::ProviderKind;

const OWNER_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_REPORTED_CANDIDATES: usize = 16;

/// Unresolved selection reports no relay verdict. Existing internal Option
/// callers abstain; the HTTP handler distinguishes absence from uncertainty.
pub enum WatcherStateLookup {
    Selected {
        snapshot: Box<super::WatcherStateSnapshot>,
        selection: ChannelOwnerSelection,
    },
    Absent(ChannelOwnerSelection),
    Ambiguous(ChannelOwnerSelection),
    Unobserved(ChannelOwnerSelection),
}

impl WatcherStateLookup {
    pub(super) fn into_snapshot(self) -> Option<super::WatcherStateSnapshot> {
        match self {
            Self::Selected { snapshot, .. } => Some(*snapshot),
            Self::Absent(_) | Self::Ambiguous(_) | Self::Unobserved(_) => None,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct ChannelOwnerSelection {
    status: &'static str,
    source: Option<&'static str>,
    reason: &'static str,
    channel_id: u64,
    candidate_count: usize,
    candidates_truncated: bool,
    candidates: Vec<OwnerEvidence>,
}

#[derive(Clone, Serialize)]
struct OwnerEvidence {
    provider: String,
    mailbox_has_cancel_token: bool,
    mailbox_user_message_id: Option<u64>,
    inflight_user_message_id: Option<u64>,
    inflight_current_message_id: Option<u64>,
    tmux_session: Option<String>,
    local_watcher_present: bool,
    watcher_owner_channel_id: Option<u64>,
    current_mailbox_episode_matches: bool,
    persisted_inflight_present: bool,
    persisted_inflight_observed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Episode {
    user_message_id: u64,
    mailbox_anchor_message_id: u64,
    current_message_id: u64,
    turn_nonce: Option<String>,
    tmux_session: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Observation {
    provider: String,
    active_mailbox: bool,
    user_message_id: Option<u64>,
    turn_nonce: Option<String>,
    token_tmux: Option<String>,
    episode: Option<Episode>,
    episode_unobserved: bool,
    watcher_tmux: Option<String>,
    watcher_owner: Option<u64>,
    pending_queue: bool,
    thread_binding: bool,
    relay_coord: bool,
}

impl Observation {
    fn current_mailbox_episode(&self) -> bool {
        if !self.active_mailbox {
            return false;
        }
        if self.episode_unobserved {
            return false;
        }
        let Some(episode) = self.episode.as_ref() else {
            // Prelaunch turns can own a named executor before persistence.
            return self.user_message_id.is_some() && self.token_tmux.is_some();
        };
        let nonce_matches = match (self.turn_nonce.as_deref(), episode.turn_nonce.as_deref()) {
            (Some(mailbox), Some(inflight)) => mailbox == inflight,
            _ => true, // Legacy rows retain their existing message identity.
        };
        let tmux_matches = match (self.token_tmux.as_deref(), episode.tmux_session.as_deref()) {
            (Some(mailbox), Some(inflight)) => mailbox == inflight,
            _ => true,
        };
        self.user_message_id == Some(episode.mailbox_anchor_message_id)
            && nonce_matches
            && tmux_matches
    }

    fn current_watcher(&self, channel: u64) -> bool {
        self.watcher_owner == Some(channel)
            && self.watcher_tmux.is_some()
            && match self
                .episode
                .as_ref()
                .and_then(|e| e.tmux_session.as_deref())
            {
                Some(inflight) => self.watcher_tmux.as_deref() == Some(inflight),
                None => true,
            }
    }

    fn has_evidence(&self) -> bool {
        self.active_mailbox
            || self.episode.is_some()
            || self.episode_unobserved
            || self.watcher_tmux.is_some()
            || self.pending_queue
            || self.thread_binding
            || self.relay_coord
    }

    fn public_evidence(&self) -> OwnerEvidence {
        OwnerEvidence {
            provider: bounded(&self.provider, 128),
            mailbox_has_cancel_token: self.active_mailbox,
            mailbox_user_message_id: self.user_message_id.and_then(redaction::visible_message_id),
            inflight_user_message_id: self
                .episode
                .as_ref()
                .and_then(|e| redaction::visible_message_id(e.user_message_id)),
            inflight_current_message_id: self
                .episode
                .as_ref()
                .and_then(|e| redaction::visible_message_id(e.current_message_id)),
            tmux_session: self
                .token_tmux
                .as_ref()
                .or_else(|| self.episode.as_ref().and_then(|e| e.tmux_session.as_ref()))
                .or(self.watcher_tmux.as_ref())
                .map(|name| bounded(name, 256)),
            local_watcher_present: self.watcher_tmux.is_some(),
            watcher_owner_channel_id: self.watcher_owner,
            current_mailbox_episode_matches: self.current_mailbox_episode(),
            persisted_inflight_present: self.episode.is_some(),
            persisted_inflight_observed: !self.episode_unobserved,
        }
    }
}

fn bounded(value: &str, maximum: usize) -> String {
    value.chars().take(maximum).collect()
}

#[derive(Debug, PartialEq, Eq)]
enum Decision {
    Selected(usize, &'static str),
    Absent,
    Ambiguous,
    Unobserved,
    ResolveProvider,
}

fn choose(channel: u64, observations: &[Observation]) -> Decision {
    let active: Vec<_> = observations
        .iter()
        .enumerate()
        .filter(|(_, o)| o.active_mailbox)
        .collect();
    if active.len() > 1 {
        return Decision::Ambiguous;
    }
    if observations.iter().any(|o| o.episode_unobserved) {
        return Decision::Unobserved;
    }
    if let Some((index, observation)) = active.first() {
        return if observation.current_mailbox_episode() {
            Decision::Selected(*index, "current_mailbox_episode")
        } else {
            // A conflicting or incomplete live owner cannot be overridden by
            // a different runtime's watcher, settings or persisted file.
            Decision::ResolveProvider
        };
    }
    for (source, predicate) in [
        (
            "local_watcher",
            Observation::current_watcher as fn(&Observation, u64) -> bool,
        ),
        ("pending_mailbox_queue", |o: &Observation, _| {
            o.pending_queue
        }),
        ("thread_channel_binding", |o: &Observation, _| {
            o.thread_binding
        }),
    ] {
        let indices: Vec<_> = observations
            .iter()
            .enumerate()
            .filter(|(_, o)| predicate(o, channel))
            .map(|(i, _)| i)
            .collect();
        if indices.len() > 1 {
            return Decision::Ambiguous;
        }
        if let Some(index) = indices.first() {
            return Decision::Selected(*index, source);
        }
    }
    if observations.iter().any(Observation::has_evidence) {
        Decision::ResolveProvider
    } else {
        Decision::Absent
    }
}

fn report(
    channel: u64,
    observations: &[Observation],
    status: &'static str,
    source: Option<&'static str>,
    reason: &'static str,
) -> ChannelOwnerSelection {
    let mut evidence: Vec<_> = observations
        .iter()
        .filter(|o| o.has_evidence())
        .map(Observation::public_evidence)
        .collect();
    evidence.sort_by(|a, b| {
        (
            &a.provider,
            &a.tmux_session,
            a.mailbox_user_message_id,
            a.local_watcher_present,
        )
            .cmp(&(
                &b.provider,
                &b.tmux_session,
                b.mailbox_user_message_id,
                b.local_watcher_present,
            ))
    });
    let candidate_count = evidence.len();
    evidence.truncate(MAX_REPORTED_CANDIDATES);
    ChannelOwnerSelection {
        status,
        source,
        reason,
        channel_id: channel,
        candidate_count,
        candidates_truncated: candidate_count > evidence.len(),
        candidates: evidence,
    }
}

async fn observe(
    entry: &ProviderEntry,
    channel: ChannelId,
    episodes: &mut HashMap<String, Result<Option<Episode>, ()>>,
) -> Observation {
    let provider = ProviderKind::from_str(&entry.name);
    let name = provider
        .as_ref()
        .map(ProviderKind::as_str)
        .unwrap_or(&entry.name)
        .to_string();
    let episode = episodes
        .entry(name.clone())
        .or_insert_with(|| {
            let Some(kind) = provider.as_ref() else {
                return Ok(None);
            };
            let Some(row) =
                discord::inflight::load_inflight_state_read_only_result(kind, channel.get())
                    .map_err(|_| ())?
            else {
                return Ok(None);
            };
            Ok(Some(Episode {
                user_message_id: row.user_msg_id,
                mailbox_anchor_message_id: row.effective_finalizer_turn_id(),
                current_message_id: row.current_msg_id,
                turn_nonce: row.turn_nonce,
                tmux_session: row.tmux_session_name,
            }))
        })
        .clone();
    let mailbox = discord::mailbox_snapshot(&entry.shared, channel).await;
    let watcher = entry.shared.tmux_watchers.channel_binding(&channel);
    Observation {
        provider: name,
        active_mailbox: mailbox.cancel_token.is_some(),
        user_message_id: mailbox.active_user_message_id.map(|id| id.get()),
        turn_nonce: mailbox.active_turn_nonce.clone(),
        token_tmux: mailbox
            .cancel_token
            .as_ref()
            .and_then(|token| token.tmux_session_name()),
        episode_unobserved: episode.is_err(),
        episode: episode.ok().flatten(),
        watcher_tmux: watcher.as_ref().map(|w| w.tmux_session_name.clone()),
        watcher_owner: watcher.as_ref().map(|w| w.owner_channel_id.get()),
        pending_queue: !mailbox.intervention_queue.is_empty(),
        thread_binding: entry.shared.dispatch.thread_parents.contains_key(&channel)
            || entry
                .shared
                .dispatch
                .thread_parents
                .iter()
                .any(|parent| *parent.value() == channel),
        relay_coord: entry.shared.tmux_relay_coords.contains_key(&channel),
    }
}

struct Plan {
    entry: ProviderEntry,
    observed: Observation,
    selection: ChannelOwnerSelection,
}

async fn resolve(
    registry: &HealthRegistry,
    channel: ChannelId,
    filter: Option<&ProviderKind>,
) -> Result<Plan, WatcherStateLookup> {
    let mut entries: Vec<ProviderEntry> = Vec::new();
    for entry in registry.provider_entries_snapshot().await {
        if filter.is_some_and(|kind| !entry.name.eq_ignore_ascii_case(kind.as_str())) {
            continue;
        }
        if !entries.iter().any(|other| {
            other.name.eq_ignore_ascii_case(&entry.name)
                && Arc::ptr_eq(&other.shared, &entry.shared)
        }) {
            entries.push(entry);
        }
    }
    let mut episodes = HashMap::new();
    let mut observations = Vec::with_capacity(entries.len());
    for entry in &entries {
        observations.push(observe(entry, channel, &mut episodes).await);
    }
    let selected = match choose(channel.get(), &observations) {
        Decision::Selected(index, source) => (index, source),
        Decision::Absent => {
            return Err(WatcherStateLookup::Absent(report(
                channel.get(),
                &observations,
                "absent",
                None,
                "no_channel_evidence",
            )));
        }
        Decision::Ambiguous => {
            return Err(WatcherStateLookup::Ambiguous(report(
                channel.get(),
                &observations,
                "ambiguous",
                None,
                "multiple_current_runtime_owners",
            )));
        }
        Decision::Unobserved => {
            return Err(WatcherStateLookup::Unobserved(report(
                channel.get(),
                &observations,
                "unobserved",
                None,
                "persisted_inflight_observation_failed",
            )));
        }
        Decision::ResolveProvider => {
            let mut resolved = Vec::new();
            let mut tried = Vec::new();
            for observation in &observations {
                if !observation.has_evidence() || tried.contains(&observation.provider) {
                    continue;
                }
                tried.push(observation.provider.clone());
                let Some(kind) = ProviderKind::from_str(&observation.provider) else {
                    continue;
                };
                if let Some(shared) = registry
                    .shared_for_provider_on_channel(&kind, channel)
                    .await
                    && let Some(index) = entries.iter().position(|entry| {
                        entry.name.eq_ignore_ascii_case(kind.as_str())
                            && Arc::ptr_eq(&entry.shared, &shared)
                    })
                    && observations[index].has_evidence()
                {
                    resolved.push(index);
                }
            }
            if resolved.len() > 1 {
                return Err(WatcherStateLookup::Ambiguous(report(
                    channel.get(),
                    &observations,
                    "ambiguous",
                    None,
                    "multiple_provider_channel_owners",
                )));
            }
            let Some(index) = resolved.first().copied() else {
                return Err(WatcherStateLookup::Unobserved(report(
                    channel.get(),
                    &observations,
                    "unobserved",
                    None,
                    "current_runtime_owner_not_established",
                )));
            };
            if observations.iter().enumerate().any(|(other, observation)| {
                observation.active_mailbox
                    && (other != index
                        || observation.episode.is_some() && !observation.current_mailbox_episode())
            }) {
                return Err(WatcherStateLookup::Unobserved(report(
                    channel.get(),
                    &observations,
                    "unobserved",
                    None,
                    "active_mailbox_episode_conflicts_with_runtime_resolution",
                )));
            }
            (index, "provider_channel_binding")
        }
    };
    Ok(Plan {
        entry: entries[selected.0].clone(),
        observed: observations[selected.0].clone(),
        selection: report(
            channel.get(),
            &observations,
            "selected",
            Some(selected.1),
            "unique_current_runtime_owner",
        ),
    })
}

pub(super) async fn lookup(
    registry: &HealthRegistry,
    channel: ChannelId,
    filter: Option<&ProviderKind>,
) -> WatcherStateLookup {
    let generation = registry.registration_generation();
    let plan = match tokio::time::timeout(
        OWNER_OBSERVATION_TIMEOUT,
        resolve(registry, channel, filter),
    )
    .await
    {
        Ok(Ok(plan)) => plan,
        Ok(Err(outcome)) => return outcome,
        Err(_) => {
            return WatcherStateLookup::Unobserved(report(
                channel.get(),
                &[],
                "unobserved",
                None,
                "runtime_owner_observation_timed_out",
            ));
        }
    };
    snapshot_selected(registry, channel, filter, generation, plan).await
}

async fn snapshot_selected(
    registry: &HealthRegistry,
    channel: ChannelId,
    filter: Option<&ProviderKind>,
    generation: u64,
    plan: Plan,
) -> WatcherStateLookup {
    let snapshot = super::watcher_state_snapshot_for_shared(
        &plan.observed.provider,
        plan.entry.shared.clone(),
        channel,
        registry.started_at_unix(),
    )
    .await;
    // Re-resolve the whole candidate set: another already-registered runtime
    // can acquire a mailbox while the selected runtime is being probed.
    let after = match tokio::time::timeout(
        OWNER_OBSERVATION_TIMEOUT,
        resolve(registry, channel, filter),
    )
    .await
    {
        Ok(Ok(after)) => after,
        Ok(Err(WatcherStateLookup::Ambiguous(selection))) => {
            return WatcherStateLookup::Ambiguous(selection);
        }
        Ok(Err(_)) => {
            return WatcherStateLookup::Unobserved(
                plan.selection
                    .unobserved("runtime_owner_changed_during_snapshot"),
            );
        }
        Err(_) => {
            return WatcherStateLookup::Unobserved(
                plan.selection
                    .unobserved("runtime_owner_revalidation_timed_out"),
            );
        }
    };
    if generation != registry.registration_generation()
        || !Arc::ptr_eq(&after.entry.shared, &plan.entry.shared)
        || after.observed != plan.observed
    {
        return WatcherStateLookup::Unobserved(
            plan.selection
                .unobserved("runtime_owner_changed_during_snapshot"),
        );
    }
    match snapshot {
        Some(snapshot) => WatcherStateLookup::Selected {
            snapshot: Box::new(snapshot),
            selection: plan.selection,
        },
        None => WatcherStateLookup::Unobserved(
            plan.selection
                .unobserved("selected_runtime_snapshot_not_observed"),
        ),
    }
}

impl ChannelOwnerSelection {
    fn unobserved(mut self, reason: &'static str) -> Self {
        self.status = "unobserved";
        self.reason = reason;
        self
    }
}

#[cfg(test)]
mod tests;
