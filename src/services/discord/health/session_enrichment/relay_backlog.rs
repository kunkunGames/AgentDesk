//! Bounded, read-only evidence about output which the relay can actually send.
//! Raw transcript growth includes reasoning, tools and telemetry. It is not a
//! delivery obligation. Unknown coverage or unknown preview delivery stays
//! unknown; neither a heartbeat nor an in-flight buffer is a send receipt.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;

use crate::services::agent_protocol::RuntimeHandoffKind;
use crate::services::discord::inflight::InflightTurnState;
use crate::services::provider::ProviderKind;

use super::super::liveness_authority::CaptureCoordinateObservation;

#[cfg(unix)]
mod source_scan;
#[cfg(unix)]
use source_scan::scan_file;
#[cfg(all(test, unix))]
use source_scan::scan_records;

#[cfg(not(unix))]
fn scan_file(_request: &ScanRequest) -> ScanResult {
    ScanResult::Unknown(BacklogUnknownReason::SourceIdentityUnresolved)
}

const MAX_SCAN_BYTES: u64 = 256 * 1024;
const MAX_RECORD_BYTES: usize = 32 * 1024;
const MAX_OBSERVERS: usize = 512;
const RESAMPLE_AFTER: Duration = Duration::from_secs(5);
const READ_DEADLINE: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::services::discord) enum BacklogUnknownReason {
    NotObserved,
    InflightNotObserved,
    ProviderOrRuntimeUnresolved,
    MissingSource,
    SourceBindingMismatch,
    SourceIdentityUnresolved,
    MissingFrontier,
    GenerationUnresolved,
    TurnStartUnresolved,
    InvalidRange,
    ScanBudgetExceeded,
    RecordBudgetExceeded,
    PartialRecord,
    MalformedRecord,
    UnsupportedRecord,
    AmbiguousTurnBoundary,
    SourceChanged,
    DeliveryBufferMismatch,
    PreviewDeliveryUnconfirmed,
    ReadBusy,
    ReadTimedOut,
    ReadFailed,
    ResampleDeferred,
    ObservationSuperseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::services::discord) enum BacklogClearReason {
    SilentTurn,
    TerminalDelivered,
    NoEligibleOutput,
    ConfirmedOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::services::discord) enum PendingOutputKind {
    AssistantText,
    TerminalAnswer,
}

/// `pending_age_ms` is a conservative process-local observation age, not turn
/// age, transcript mtime, provider timestamp, or outbound heartbeat age.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(in crate::services::discord) enum RelayBacklogEvidence {
    KnownClear {
        reason: BacklogClearReason,
    },
    Pending {
        kind: PendingOutputKind,
        source_end_offset: u64,
        observed_since_unix_ms: i64,
        pending_age_ms: u64,
        age_basis: &'static str,
    },
    Unknown {
        reason: BacklogUnknownReason,
    },
}

impl RelayBacklogEvidence {
    pub(in crate::services::discord) fn unobserved() -> Self {
        Self::unknown(BacklogUnknownReason::NotObserved)
    }

    pub(super) fn unknown(reason: BacklogUnknownReason) -> Self {
        Self::Unknown { reason }
    }

    fn clear(reason: BacklogClearReason) -> Self {
        Self::KnownClear { reason }
    }

    pub(super) fn overdue(&self, stale_ms: i64) -> bool {
        matches!(self, Self::Pending { pending_age_ms, .. }
            if *pending_age_ms >= stale_ms.max(0) as u64)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceIdentity {
    provider: String,
    channel: u64,
    user_message: u64,
    finalizer_turn: u64,
    started_at: String,
    runtime: RuntimeHandoffKind,
    tmux: String,
    path: String,
    file_id: (u64, u64),
    generation: i64,
    reset_incarnation: u64,
    turn_start: u64,
}

#[derive(Clone)]
struct ScanRequest {
    identity: SourceIdentity,
    capture_end: u64,
    capture_modified: Option<SystemTime>,
    frontier: u64,
    full_response: String,
    response_sent_offset: usize,
    policy_clear: Option<BacklogClearReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PendingSpan {
    end: u64,
    kind: PendingOutputKind,
    record_hash: u64,
}

#[derive(Clone, Debug)]
enum ScanResult {
    Clear(BacklogClearReason),
    Pending(PendingSpan),
    Unknown(BacklogUnknownReason),
}

struct PendingClock {
    span: PendingSpan,
    first_seen: Instant,
    first_seen_unix_ms: i64,
}

struct CacheEntry {
    identity: SourceIdentity,
    fingerprint: u64,
    sampled_at: Instant,
    serial: u64,
    result: ScanResult,
    pending: Option<PendingClock>,
}

#[derive(Default)]
struct ObserverCache {
    entries: HashMap<(String, u64), CacheEntry>,
    next_serial: u64,
}

impl ObserverCache {
    fn publish(
        entry: &mut CacheEntry,
        result: ScanResult,
        now: Instant,
        now_ms: i64,
    ) -> RelayBacklogEvidence {
        match &result {
            ScanResult::Pending(span) => {
                if !entry
                    .pending
                    .as_ref()
                    .is_some_and(|clock| clock.span == *span)
                {
                    entry.pending = Some(PendingClock {
                        span: *span,
                        first_seen: now,
                        first_seen_unix_ms: now_ms,
                    });
                }
            }
            ScanResult::Clear(_) => entry.pending = None,
            // Incomplete observations neither establish clear delivery nor
            // erase the age of the same previously witnessed source span.
            ScanResult::Unknown(_) => {}
        }
        entry.result = result;
        evidence_for(entry, now)
    }
}

fn evidence_for(entry: &CacheEntry, now: Instant) -> RelayBacklogEvidence {
    match &entry.result {
        ScanResult::Clear(reason) => RelayBacklogEvidence::clear(*reason),
        ScanResult::Unknown(reason) => RelayBacklogEvidence::unknown(*reason),
        ScanResult::Pending(span) => {
            let Some(clock) = entry.pending.as_ref().filter(|clock| clock.span == *span) else {
                return RelayBacklogEvidence::unobserved();
            };
            RelayBacklogEvidence::Pending {
                kind: span.kind,
                source_end_offset: span.end,
                observed_since_unix_ms: clock.first_seen_unix_ms,
                pending_age_ms: now
                    .saturating_duration_since(clock.first_seen)
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
                age_basis: "observed_since",
            }
        }
    }
}

fn cache() -> &'static Mutex<ObserverCache> {
    static CACHE: OnceLock<Mutex<ObserverCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ObserverCache::default()))
}

fn readers() -> &'static Arc<tokio::sync::Semaphore> {
    static READERS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    READERS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(2)))
}

fn fingerprint(request: &ScanRequest) -> u64 {
    let mut hash = DefaultHasher::new();
    request.capture_end.hash(&mut hash);
    request.capture_modified.hash(&mut hash);
    request.frontier.hash(&mut hash);
    request.full_response.hash(&mut hash);
    request.response_sent_offset.hash(&mut hash);
    request.policy_clear.hash(&mut hash);
    hash.finish()
}

/// One coherent watcher/source sample, captured before the bounded read.
pub(super) struct RelayObservation<'a> {
    pub(super) provider: Option<&'a ProviderKind>,
    pub(super) channel: u64,
    pub(super) state: Option<&'a InflightTurnState>,
    pub(super) watcher_path: Option<&'a str>,
    pub(super) coordinate: &'a CaptureCoordinateObservation,
    pub(super) capture_modified: Option<SystemTime>,
    pub(super) frontier: Option<u64>,
    pub(super) generation: Option<i64>,
    pub(super) reset_incarnation: Option<u64>,
    pub(super) binding_matches: bool,
}

pub(super) async fn observe(observation: RelayObservation<'_>) -> RelayBacklogEvidence {
    let cache_key = (
        observation
            .provider
            .map(|kind| kind.as_str())
            .unwrap_or("")
            .to_owned(),
        observation.channel,
    );
    let request = match make_request(observation) {
        Ok(request) => request,
        Err(evidence) => {
            // Confirmed clear delivery/suppression ends the episode. Unknown
            // observations preserve its clock; a later valid request fences
            // that clock by the complete source/execution identity.
            if matches!(evidence, RelayBacklogEvidence::KnownClear { .. }) {
                cache()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entries
                    .remove(&cache_key);
            }
            return evidence;
        }
    };
    let now = Instant::now();
    let input_fingerprint = fingerprint(&request);
    let serial;
    {
        let mut cache = cache().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = cache.entries.get(&cache_key) {
            if entry.identity == request.identity
                && now.duration_since(entry.sampled_at) < RESAMPLE_AFTER
            {
                return if entry.fingerprint == input_fingerprint {
                    evidence_for(entry, now)
                } else {
                    RelayBacklogEvidence::unknown(BacklogUnknownReason::ResampleDeferred)
                };
            }
        }
        if !cache.entries.contains_key(&cache_key) && cache.entries.len() >= MAX_OBSERVERS {
            if let Some(oldest) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.sampled_at)
                .map(|(key, _)| key.clone())
            {
                cache.entries.remove(&oldest);
            }
        }
        cache.next_serial = cache.next_serial.wrapping_add(1);
        serial = cache.next_serial;
        let pending = cache
            .entries
            .remove(&cache_key)
            .filter(|entry| entry.identity == request.identity)
            .and_then(|entry| entry.pending);
        cache.entries.insert(
            cache_key.clone(),
            CacheEntry {
                identity: request.identity.clone(),
                fingerprint: input_fingerprint,
                sampled_at: now,
                serial,
                result: ScanResult::Unknown(BacklogUnknownReason::ReadBusy),
                pending,
            },
        );
    }
    let result = match readers().clone().try_acquire_owned() {
        Err(_) => ScanResult::Unknown(BacklogUnknownReason::ReadBusy),
        Ok(permit) => {
            let read_request = request.clone();
            let task = tokio::task::spawn_blocking(move || {
                // spawn_blocking is not canceled by timeout. The permit lives
                // in this closure until I/O really ends, limiting late readers.
                let _permit = permit;
                scan_file(&read_request)
            });
            match tokio::time::timeout(READ_DEADLINE, task).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => ScanResult::Unknown(BacklogUnknownReason::ReadFailed),
                Err(_) => ScanResult::Unknown(BacklogUnknownReason::ReadTimedOut),
            }
        }
    };
    let mut cache = cache().lock().unwrap_or_else(|e| e.into_inner());
    let Some(entry) = cache
        .entries
        .get_mut(&cache_key)
        .filter(|entry| entry.serial == serial && entry.identity == request.identity)
    else {
        return RelayBacklogEvidence::unknown(BacklogUnknownReason::ObservationSuperseded);
    };
    ObserverCache::publish(
        entry,
        result,
        Instant::now(),
        chrono::Utc::now().timestamp_millis(),
    )
}

fn make_request(
    RelayObservation {
        provider,
        channel,
        state,
        watcher_path,
        coordinate,
        capture_modified,
        frontier,
        generation,
        reset_incarnation,
        binding_matches,
    }: RelayObservation<'_>,
) -> Result<ScanRequest, RelayBacklogEvidence> {
    // The read-only loader returns None for both absent and unreadable rows.
    // That is no usable turn witness, not a positive delivery measurement.
    let Some(state) = state else {
        return Err(RelayBacklogEvidence::unknown(
            BacklogUnknownReason::InflightNotObserved,
        ));
    };
    let unknown = |reason| Err(RelayBacklogEvidence::unknown(reason));
    let Some(provider) =
        provider.filter(|kind| kind.as_str() == state.provider && state.channel_id == channel)
    else {
        return unknown(BacklogUnknownReason::ProviderOrRuntimeUnresolved);
    };
    // A row flag describes this execution only. It cannot clear another
    // watcher's backlog until the source and generation witnesses agree.
    let policy_clear = if state.silent_turn {
        Some(BacklogClearReason::SilentTurn)
    } else if state.terminal_delivery_completed() {
        Some(BacklogClearReason::TerminalDelivered)
    } else {
        None
    };
    let Some(runtime) = state
        .runtime_kind
        .filter(|_| !state.runtime_kind_unknown_on_disk)
    else {
        return unknown(BacklogUnknownReason::ProviderOrRuntimeUnresolved);
    };
    if !matches!(
        (provider, runtime),
        (ProviderKind::Codex, RuntimeHandoffKind::CodexTui)
            | (
                ProviderKind::Claude,
                RuntimeHandoffKind::ClaudeTui | RuntimeHandoffKind::LegacyTmuxWrapper
            )
    ) {
        return unknown(BacklogUnknownReason::ProviderOrRuntimeUnresolved);
    }
    let Some(path) = state.output_path.as_deref().filter(|path| !path.is_empty()) else {
        return unknown(BacklogUnknownReason::MissingSource);
    };
    if !binding_matches || watcher_path != Some(path) {
        return unknown(BacklogUnknownReason::SourceBindingMismatch);
    }
    let Some(file_id) = coordinate.file_id else {
        return unknown(BacklogUnknownReason::SourceIdentityUnresolved);
    };
    let Some(capture_end) = coordinate.offset else {
        return unknown(BacklogUnknownReason::MissingSource);
    };
    if capture_modified.is_none() {
        return unknown(BacklogUnknownReason::SourceIdentityUnresolved);
    }
    let Some(frontier) = frontier else {
        return unknown(BacklogUnknownReason::MissingFrontier);
    };
    let Some((generation, reset_incarnation)) = generation
        .filter(|generation| *generation != 0)
        .zip(reset_incarnation)
    else {
        return unknown(BacklogUnknownReason::GenerationUnresolved);
    };
    if state
        .last_watcher_relayed_generation_mtime_ns
        .is_some_and(|saved| saved != 0 && saved != generation)
        || state
            .tui_terminal_generation_mtime_ns
            .is_some_and(|saved| saved != generation)
        || state
            .tui_terminal_source_file_identity
            .is_some_and(|saved| saved != file_id)
    {
        return unknown(BacklogUnknownReason::GenerationUnresolved);
    }
    let Some(turn_start) = state.turn_start_offset else {
        return unknown(BacklogUnknownReason::TurnStartUnresolved);
    };
    if frontier > capture_end || turn_start > capture_end {
        return unknown(BacklogUnknownReason::InvalidRange);
    }
    if policy_clear.is_none()
        && frontier < capture_end
        && (capture_end - turn_start > MAX_SCAN_BYTES
            || state.full_response.len() as u64 > MAX_SCAN_BYTES)
    {
        return unknown(BacklogUnknownReason::ScanBudgetExceeded);
    }
    if state.response_sent_offset > state.full_response.len()
        || !state
            .full_response
            .is_char_boundary(state.response_sent_offset)
    {
        return unknown(BacklogUnknownReason::DeliveryBufferMismatch);
    }
    let Some(tmux) = state
        .tmux_session_name
        .clone()
        .filter(|tmux| !tmux.is_empty())
    else {
        return unknown(BacklogUnknownReason::SourceBindingMismatch);
    };
    Ok(ScanRequest {
        identity: SourceIdentity {
            provider: provider.as_str().to_owned(),
            channel,
            user_message: state.user_msg_id,
            finalizer_turn: state.finalizer_turn_id,
            started_at: state.started_at.clone(),
            runtime,
            tmux,
            path: path.to_owned(),
            file_id,
            generation,
            reset_incarnation,
            turn_start,
        },
        capture_end,
        capture_modified,
        frontier,
        full_response: if policy_clear.is_some() || frontier == capture_end {
            String::new()
        } else {
            state.full_response.clone()
        },
        response_sent_offset: if policy_clear.is_some() || frontier == capture_end {
            0
        } else {
            state.response_sent_offset
        },
        policy_clear,
    })
}

#[cfg(all(test, unix))]
mod tests;
