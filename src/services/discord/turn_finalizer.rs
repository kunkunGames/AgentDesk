//! EPIC #3016 — Single-authority `TurnFinalizer`.
//!
//! Today finalization is a distributed handshake: `mailbox_finish_turn`
//! (mod.rs) is called directly from ~17 sites, the bridge↔watcher handoff
//! uses a per-handle `mailbox_finalize_owed` CAS, and `global_active` is
//! decremented in ≥10 places. The races this caused (bridge-revoke vs
//! watcher-swap, and the silent gate-timeout SKIP that defers finalize to a
//! never-firing 1800s sweeper) are the root cause of the recurring "turn
//! never finalizes / inflight stuck" bug.
//!
//! This module introduces ONE actor that OWNS the side-effects of finalize as
//! an atomic, exactly-once unit:
//!   (1) inflight clear (honouring `PlannedRestartSkipped`),
//!   (2) mailbox cancel_token release via `mailbox_finish_turn`,
//!   (3) `global_active` decrement — gated on `removed_token.is_some()`,
//!   (4) the trailing terminal side-effects that today follow
//!       `mailbox_finish_turn` inline at each call-site (watchdog override
//!       clear, `dispatch_thread_parents` retain, voice barge-in drain,
//!       `dispatch_role_overrides` cleanup).
//!
//! Exactly-once is decided in a single place — the actor task's handling of a
//! `Terminal` message — via a per-`TurnKey` ledger phase gate
//! (`Pending → Finalizing → Finalized`). Because the gate runs inside one
//! task there is no CAS, no AcqRel handoff, no `mailbox_finalize_owed`.
//!
//! Landing is incremental (EPIC §4). Phase 1 ships this dormant: `do_finalize`
//! reproduces today's exact sequence and no call-site submits yet. Phases 2-3
//! rewire the bridge and watcher terminals to `submit(...)`. Because
//! `mailbox_finish_turn` is idempotent (returns `removed_token = None` on the
//! second call) and the counter decrement is gated on `removed_token.is_some()`,
//! a transitional double-finalize during the incremental window is a harmless
//! no-op — never an underflow, never a double Discord notice.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serenity::model::id::ChannelId;
// `tokio::time::Instant` (not `std::time::Instant`) so deadlines respect the
// paused/virtual test clock and the production `interval` clock alike.
use tokio::time::Instant;
use tokio::sync::{mpsc, oneshot};

use crate::services::discord::inflight::RelayOwnerKind;
use crate::services::provider::{CancelToken, ProviderKind};

use super::SharedData;

/// How often the reconciler `Tick` fires to re-check deadline-armed
/// gate-timeout entries and garbage-collect the ledger.
const RECONCILE_INTERVAL: Duration = Duration::from_millis(1000);

/// Bounded backstop for a gate-timeout whose pane is still busy and whose
/// relay owner is still alive. The reconciler finalizes once the pane
/// quiesces, the owner dies, OR this deadline elapses — seconds, NOT the
/// 1800s placeholder-sweeper horizon the old silent SKIP deferred to. The
/// hosted-TUI pre-submit busy guard remains the correctness floor that stops
/// follow-up input from being injected into a still-busy pane, so this only
/// guarantees finalize eventually fires.
const GATE_BACKSTOP: Duration = Duration::from_secs(8);

/// TTL after which a `Finalized` ledger entry is garbage-collected so the
/// ledger stays bounded while still suppressing a late double-submit.
const FINALIZED_TTL: Duration = Duration::from_secs(60);

/// Identity carried by a submission. Holds `user_msg_id` for telemetry, but
/// the LEDGER MATCH is channel + generation only (see `LedgerKey`): the
/// mailbox/cancel_token is channel-scoped with a single active turn, so a
/// terminal that only knows the channel (`user_msg_id == 0`, recovery/orphan
/// paths) MUST resolve to the same ledger entry a `Start` registered with the
/// real message id. Including `user_msg_id` in the map key would split them and
/// break the exactly-once `AlreadyFinalized` guarantee.
#[derive(Clone, Copy, Debug)]
pub(in crate::services::discord) struct TurnKey {
    pub(in crate::services::discord) channel_id: ChannelId,
    /// 0 == "unknown identity" (recovery/orphan paths). Informational only —
    /// never part of the ledger match.
    pub(in crate::services::discord) user_msg_id: u64,
    pub(in crate::services::discord) generation: u64,
}

impl TurnKey {
    pub(in crate::services::discord) fn new(
        channel_id: ChannelId,
        user_msg_id: u64,
        generation: u64,
    ) -> Self {
        Self {
            channel_id,
            user_msg_id,
            generation,
        }
    }

    /// The channel-scoped, generation-guarded match key. `user_msg_id` is
    /// deliberately excluded so id-0 (channel-only) terminals collapse onto the
    /// turn a real-id `Start` registered.
    fn ledger_key(&self) -> LedgerKey {
        LedgerKey {
            channel_id: self.channel_id,
            generation: self.generation,
        }
    }
}

/// The exact ledger match: channel + restart generation. Matches today's
/// channel-scoped single-active-turn mailbox semantics.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct LedgerKey {
    channel_id: ChannelId,
    generation: u64,
}

/// Every actor submits ONE of these terminal events. The finalizer's ledger
/// gate decides exactly-once. Phase 1 wires the variants the bridge/watcher
/// terminals need (Phases 2-3); the remaining sweeper/recovery variants land
/// in Phase 4 and are listed here so the matrix is explicit.
#[derive(Clone, Debug)]
pub(in crate::services::discord) enum TerminalEvent {
    /// Normal completion — bridge or watcher relayed (or intentionally
    /// suppressed) terminal output and the turn is done.
    Complete,
    /// `/!stop`, reaction, watchdog. `mark_completion_cleanup()` is skipped
    /// (the cancel is a real mid-stream stop) but `cancelled.store(true)`
    /// still fires.
    Cancel,
    /// #2293/#2780 hosted-TUI quiescence gate. `pane_quiescent == Some(false)`
    /// with a still-alive relay owner is the ONLY event that defers: it arms a
    /// bounded deadline and the reconciler finalizes when the precondition
    /// clears or the deadline elapses. Replaces the silent watcher SKIP.
    GateTimeout { pane_quiescent: Option<bool> },
    /// No output owner and an empty response — finalize once and emit the
    /// observability event from inside the finalizer.
    RelayMiss,
}

impl TerminalEvent {
    fn is_cancel(&self) -> bool {
        matches!(self, TerminalEvent::Cancel)
    }
}

/// Per-submission knobs that keep each routed call-site behaviourally
/// identical to its pre-#3016 inline sequence during the incremental window.
#[derive(Clone, Copy, Debug)]
pub(in crate::services::discord) struct FinalizeContext {
    /// Whether `do_finalize` clears inflight as part of the finalize. Bridge
    /// branches clear inflight elsewhere in their own flow, so they submit
    /// `false`; the watcher cleared inflight inline before finalizing, so it
    /// submits `true`. Preserving the per-site behaviour avoids a double-clear
    /// or a missed clear during Phases 2-3.
    pub(in crate::services::discord) clear_inflight: bool,
    /// Whether to drain voice barge-in deferred prompts as part of finalize.
    /// Both bridge branches and the watcher run this today; sweeper/recovery
    /// paths (Phase 4) will not.
    pub(in crate::services::discord) drain_voice: bool,
    /// Whether to schedule a deferred idle-queue kickoff when the finalize
    /// leaves a pending soft-queue (gated on `mailbox_online && has_pending`).
    /// The watcher's `finish_restored_watcher_active_turn` did this; the
    /// bridge branches deferred kickoff to a later site, so they submit
    /// `false`.
    pub(in crate::services::discord) kickoff_queue: bool,
}

impl FinalizeContext {
    /// Bridge non-delegation / missing-handoff branches: bridge owns the
    /// inflight clear elsewhere, drains voice, defers queue kickoff.
    pub(in crate::services::discord) fn bridge() -> Self {
        Self {
            clear_inflight: false,
            drain_voice: true,
            kickoff_queue: false,
        }
    }

    /// Watcher terminal: cleared inflight inline today, drains voice via the
    /// shared path, kicks off the queue when backlog remains.
    pub(in crate::services::discord) fn watcher() -> Self {
        Self {
            clear_inflight: true,
            drain_voice: true,
            kickoff_queue: true,
        }
    }
}

/// Outcome of a terminal submission. The submitter uses this to decide whether
/// it owns the post-finalize bookkeeping (it performed the one finalize) or
/// must do nothing (someone else already finalized this turn).
pub(in crate::services::discord) enum FinalizeOutcome {
    /// This submission performed the one finalize.
    Finalized {
        removed_token: Option<Arc<CancelToken>>,
        has_pending: bool,
        mailbox_online: bool,
    },
    /// Another actor already finalized this turn; the submitter must do
    /// nothing (no counter touch, no Discord notice, no re-relay).
    AlreadyFinalized,
    /// Gate-timeout with a still-busy pane and a live relay owner — recorded
    /// with a bounded deadline; the reconciler will finalize when the
    /// precondition clears.
    Deferred,
}

/// Ledger phase for a single turn. Owned solely by the actor task; the
/// check-and-set on this enum is the one place exactly-once is decided.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Pending,
    Finalizing,
    Finalized,
}

struct LedgerEntry {
    phase: Phase,
    relay_owner: RelayOwnerKind,
    provider: ProviderKind,
    /// The originating identity, retained so the deadline-armed reconciler can
    /// reconstruct a `TurnKey` for `do_finalize` (channel-scoped, so any of the
    /// colliding ids is equivalent).
    turn_key: TurnKey,
    /// Backstop deadline for a deferred gate-timeout. `None` unless a
    /// `GateTimeout{Some(false)}` armed it.
    terminal_deadline: Option<Instant>,
    /// When the entry reached `Finalized`, for TTL-based GC.
    finalized_at: Option<Instant>,
}

/// Messages the actor task drains. Each carries `Arc<SharedData>` so the
/// finalize side-effects can run inside the task without the finalizer holding
/// a `Weak<SharedData>` back-reference (which would re-introduce an Arc cycle
/// and ordering ambiguity).
enum FinalizeMsg {
    /// #3018 register: the bridge submits this synchronously at intake/handoff
    /// before the watcher can submit a terminal, so the ledger knows the turn
    /// exists and message arrival order replaces the deleted Release/AcqRel
    /// `mailbox_finalize_owed.store` ordering.
    Start {
        key: TurnKey,
        provider: ProviderKind,
        relay_owner: RelayOwnerKind,
    },
    Terminal {
        key: TurnKey,
        provider: ProviderKind,
        event: TerminalEvent,
        ctx: FinalizeContext,
        shared: Arc<SharedData>,
        ack: oneshot::Sender<FinalizeOutcome>,
    },
}

/// A per-runtime actor, held as `Arc<TurnFinalizer>` on `SharedData`. One
/// owning task drains the `mpsc`; all public methods are cheap
/// submit-or-await wrappers.
pub(in crate::services::discord) struct TurnFinalizer {
    tx: mpsc::UnboundedSender<FinalizeMsg>,
}

impl TurnFinalizer {
    /// Spawn the owning actor task and return the handle. Cheap and idle until
    /// the first submission, so it is safe to construct unconditionally at
    /// every `SharedData` construction site (incl. tests).
    ///
    /// The actor task is only spawned when a Tokio runtime handle is available.
    /// Some synchronous unit tests build `SharedData` outside any runtime via
    /// `make_shared_data_for_tests` and never finalize a turn; there `spawn`
    /// must not panic (`tokio::spawn` requires a reactor). When no runtime is
    /// present we skip the task — the unbounded sender simply buffers the
    /// (never-sent, in practice) messages, so the dormant finalizer stays inert
    /// instead of crashing the test process.
    pub(in crate::services::discord) fn spawn() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(actor_loop(rx));
        }
        Arc::new(Self { tx })
    }

    /// #3018 — register a turn so the ledger knows it exists before any
    /// terminal can arrive. Idempotent: a second `Start` for a key already in
    /// the ledger only refreshes the relay owner.
    pub(in crate::services::discord) fn register_start(
        &self,
        key: TurnKey,
        provider: ProviderKind,
        relay_owner: RelayOwnerKind,
    ) {
        // UnboundedSender::send only fails if the actor task is gone (process
        // teardown); dropping the Start there is harmless because no terminal
        // will be awaited either.
        let _ = self.tx.send(FinalizeMsg::Start {
            key,
            provider,
            relay_owner,
        });
    }

    /// Submit a terminal event and await the exactly-once decision. Returns
    /// `AlreadyFinalized` if another actor already finalized this turn.
    pub(in crate::services::discord) async fn submit_terminal(
        &self,
        key: TurnKey,
        provider: ProviderKind,
        event: TerminalEvent,
        ctx: FinalizeContext,
        shared: Arc<SharedData>,
    ) -> FinalizeOutcome {
        let (ack, rx) = oneshot::channel();
        if self
            .tx
            .send(FinalizeMsg::Terminal {
                key,
                provider,
                event,
                ctx,
                shared,
                ack,
            })
            .is_err()
        {
            // Actor task gone (teardown). Treat as already-finalized so the
            // submitter does no further bookkeeping.
            return FinalizeOutcome::AlreadyFinalized;
        }
        rx.await.unwrap_or(FinalizeOutcome::AlreadyFinalized)
    }
}

/// The single owning task. Owns the ledger and a NON-owning cached handle to
/// `SharedData`, and drives the reconcile timer in the same task via `select!`
/// so the deadline-armed gate-timeout finalize fires deterministically (no
/// cross-task nudge ordering to reason about).
async fn actor_loop(mut rx: mpsc::UnboundedReceiver<FinalizeMsg>) {
    let mut ledger: HashMap<LedgerKey, LedgerEntry> = HashMap::new();
    // A `Weak` (NOT `Arc`) so the actor never keeps `SharedData` alive: the
    // cycle would otherwise be SharedData → Arc<TurnFinalizer> → sender →
    // actor → cached Arc<SharedData>, leaking the whole runtime across a
    // provider restart. The reconcile pass upgrades on demand; if the upgrade
    // fails the runtime is shutting down and there is nothing left to finalize.
    let mut cached_shared: Option<std::sync::Weak<SharedData>> = None;
    let mut reconcile_timer = tokio::time::interval(RECONCILE_INTERVAL);
    reconcile_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_msg = rx.recv() => {
                let Some(msg) = maybe_msg else { break };
                match msg {
                    FinalizeMsg::Start {
                        key,
                        provider,
                        relay_owner,
                    } => {
                        ledger
                            .entry(key.ledger_key())
                            .and_modify(|e| {
                                // Only refresh the owner while still live; never
                                // resurrect a finalized turn.
                                if e.phase != Phase::Finalized {
                                    e.relay_owner = relay_owner;
                                    e.provider = provider.clone();
                                    e.turn_key = key;
                                }
                            })
                            .or_insert(LedgerEntry {
                                phase: Phase::Pending,
                                relay_owner,
                                provider,
                                turn_key: key,
                                terminal_deadline: None,
                                finalized_at: None,
                            });
                    }
                    FinalizeMsg::Terminal {
                        key,
                        provider,
                        event,
                        ctx,
                        shared,
                        ack,
                    } => {
                        cached_shared = Some(Arc::downgrade(&shared));
                        let outcome =
                            handle_terminal(&mut ledger, key, provider, event, ctx, &shared)
                                .await;
                        let _ = ack.send(outcome);
                    }
                }
            }
            _ = reconcile_timer.tick() => {
                if let Some(shared) = cached_shared.as_ref().and_then(std::sync::Weak::upgrade) {
                    reconcile(&mut ledger, &shared).await;
                }
            }
        }
    }
}

/// Exactly-once gate + immediate-finalize / deferral decision. Runs inside the
/// actor task, so the check-and-set on `Phase` needs no synchronization.
async fn handle_terminal(
    ledger: &mut HashMap<LedgerKey, LedgerEntry>,
    key: TurnKey,
    provider: ProviderKind,
    event: TerminalEvent,
    ctx: FinalizeContext,
    shared: &Arc<SharedData>,
) -> FinalizeOutcome {
    let ledger_key = key.ledger_key();
    // Resolve (or lazily create) the ledger entry. A terminal for an
    // unregistered turn is the orphan path (post-restart inflight on disk, no
    // live `Start`); we still finalize it driven by the inflight identity —
    // idempotent `mailbox_finish_turn` returns `removed_token = None` so the
    // counter is untouched and inflight is cleared exactly once.
    let entry = ledger.entry(ledger_key).or_insert(LedgerEntry {
        phase: Phase::Pending,
        relay_owner: RelayOwnerKind::None,
        provider,
        turn_key: key,
        terminal_deadline: None,
        finalized_at: None,
    });

    match entry.phase {
        Phase::Finalizing | Phase::Finalized => {
            return FinalizeOutcome::AlreadyFinalized;
        }
        Phase::Pending => {}
    }

    // Gate-timeout with a still-busy pane AND a live relay owner is the only
    // event that defers instead of finalizing now.
    if let TerminalEvent::GateTimeout {
        pane_quiescent: Some(false),
    } = event
    {
        if entry.relay_owner != RelayOwnerKind::None {
            entry.terminal_deadline = Some(Instant::now() + GATE_BACKSTOP);
            return FinalizeOutcome::Deferred;
        }
        // No live relay owner → nothing will drive the pane to quiescence;
        // finalize now rather than wait out the backstop.
    }

    // Flip Pending → Finalizing, run the side-effects, flip → Finalized.
    entry.phase = Phase::Finalizing;
    let provider = entry.provider.clone();
    let outcome = do_finalize(key, provider, &event, ctx, shared).await;
    if let Some(entry) = ledger.get_mut(&ledger_key) {
        entry.phase = Phase::Finalized;
        entry.finalized_at = Some(Instant::now());
        entry.terminal_deadline = None;
    }
    outcome
}

/// The single owner of finalize's side-effects. Reproduces today's exact
/// `clear_inflight (per-site) + mailbox_finish_turn + counter-- + trailing
/// terminal side-effects` sequence so each routed call-site stays
/// behaviourally identical during the incremental landing.
async fn do_finalize(
    key: TurnKey,
    provider: ProviderKind,
    event: &TerminalEvent,
    ctx: FinalizeContext,
    shared: &Arc<SharedData>,
) -> FinalizeOutcome {
    let channel_id = key.channel_id;

    // (A) inflight clear — per-site behaviour preserved. Watcher cleared
    //     inflight inline before finalizing; bridge clears it elsewhere.
    if ctx.clear_inflight {
        super::inflight::clear_inflight_state(&provider, channel_id.get());
    }

    // (B) mailbox cancel_token release — the routed sites' single
    //     `mailbox_finish_turn`. Idempotent: returns `removed_token = None` on
    //     a second call, which is what makes a transitional double-finalize a
    //     no-op during Phases 2-3.
    let finish = super::mailbox_finish_turn(shared, &provider, channel_id).await;

    if let Some(token) = finish.removed_token.as_ref() {
        // A normal completion releases lingering token observers via
        // `mark_completion_cleanup` so provider watchdogs don't treat the
        // post-terminal `cancelled` flip as a live mid-stream cancel. A real
        // cancel must NOT mark completion-cleanup.
        if !event.is_cancel() {
            token.mark_completion_cleanup();
        }
        // Stop any lingering watchdog timer from firing on a newer turn's
        // token.
        token
            .cancelled
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // (C) #3019 active-counter — decrement ONLY here, ONLY when this submission
    //     actually removed the active turn. Gating on `removed_token.is_some()`
    //     is what guarantees no underflow even under a transitional
    //     double-call.
    if finish.removed_token.is_some() {
        super::saturating_decrement_global_active(shared);
    }

    // (D) trailing terminal side-effects that today follow `mailbox_finish_turn`
    //     inline at the bridge/watcher call-sites. Moved here so they cannot
    //     diverge between the routed paths.
    super::clear_watchdog_deadline_override(channel_id.get()).await;
    shared
        .dispatch_thread_parents
        .retain(|_, thread| *thread != channel_id);

    let voice_deferred_enqueued = if ctx.drain_voice {
        shared
            .voice_barge_in
            .drain_deferred_after_turn(shared, &provider, channel_id)
            .await
    } else {
        false
    };
    let has_pending_after_voice = finish.has_pending || voice_deferred_enqueued;
    if !has_pending_after_voice {
        shared.dispatch_role_overrides.remove(&channel_id);
    }

    // (E) optional deferred queue kickoff (watcher path), gated exactly as
    //     `finish_restored_watcher_active_turn` did.
    if ctx.kickoff_queue && finish.mailbox_online && has_pending_after_voice {
        super::schedule_deferred_idle_queue_kickoff(
            shared.clone(),
            provider.clone(),
            channel_id,
            "turn_finalizer terminal completion with queued backlog",
        );
    }

    // (F) relay-miss observability — emitted from inside the finalizer so the
    //     signal fires exactly once per finalize regardless of submitter.
    if matches!(event, TerminalEvent::RelayMiss) {
        crate::services::observability::emit_inflight_lifecycle_event(
            provider.as_str(),
            channel_id.get(),
            None,
            None,
            None,
            "relay_miss_finalized",
            serde_json::json!({
                "removed_token": finish.removed_token.is_some(),
                "has_pending": has_pending_after_voice,
            }),
        );
    }

    FinalizeOutcome::Finalized {
        removed_token: finish.removed_token,
        has_pending: has_pending_after_voice,
        mailbox_online: finish.mailbox_online,
    }
}

/// The one reconciler. Finalizes deadline-armed gate-timeouts whose backstop
/// elapsed (the deadline-armed path; pane-quiescence/owner-death re-checks land
/// with the watcher rewire in Phase 3 and Phase 4 respectively) and
/// garbage-collects `Finalized` entries past their TTL so the ledger stays
/// bounded.
async fn reconcile(ledger: &mut HashMap<LedgerKey, LedgerEntry>, shared: &Arc<SharedData>) {
    let now = Instant::now();

    // Collect deadline-elapsed gate-timeout entries to finalize. We must not
    // hold a `&mut` borrow across the `do_finalize` await, so snapshot first.
    // The stored `turn_key` carries the identity `do_finalize` needs.
    let due: Vec<(LedgerKey, TurnKey, ProviderKind)> = ledger
        .iter()
        .filter_map(|(ledger_key, entry)| {
            if entry.phase == Phase::Pending
                && let Some(deadline) = entry.terminal_deadline
                && now >= deadline
            {
                Some((*ledger_key, entry.turn_key, entry.provider.clone()))
            } else {
                None
            }
        })
        .collect();

    for (ledger_key, turn_key, provider) in due {
        if let Some(entry) = ledger.get_mut(&ledger_key) {
            if entry.phase != Phase::Pending {
                continue;
            }
            entry.phase = Phase::Finalizing;
        }
        // Backstop finalize uses watcher-style context (inflight clear) because
        // the deferred gate-timeout originates from the watcher terminal.
        let _ = do_finalize(
            turn_key,
            provider,
            &TerminalEvent::GateTimeout {
                pane_quiescent: Some(true),
            },
            FinalizeContext::watcher(),
            shared,
        )
        .await;
        if let Some(entry) = ledger.get_mut(&ledger_key) {
            entry.phase = Phase::Finalized;
            entry.finalized_at = Some(now);
            entry.terminal_deadline = None;
        }
    }

    // GC finalized entries past their TTL.
    ledger.retain(|_, entry| {
        !(entry.phase == Phase::Finalized
            && entry
                .finalized_at
                .is_some_and(|t| now.duration_since(t) >= FINALIZED_TTL))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn key(ch: u64) -> TurnKey {
        TurnKey::new(ChannelId::new(ch), 1, 0)
    }

    /// A `Complete` on a registered Pending turn finalizes exactly once and the
    /// late loser receives `AlreadyFinalized`.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn exactly_once_complete_then_late_complete() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        shared.global_active.store(1, Ordering::Relaxed);
        let fin = TurnFinalizer::spawn();
        let k = key(101);
        fin.register_start(k, ProviderKind::Claude, RelayOwnerKind::Watcher);

        let first = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(first, FinalizeOutcome::Finalized { .. }));

        let second = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(second, FinalizeOutcome::AlreadyFinalized));
    }

    /// A turn registered with the real `user_msg_id` and a terminal submitted
    /// with the channel-only `user_msg_id == 0` (recovery/orphan path) MUST
    /// resolve to the SAME ledger entry, so the channel-only terminal finalizes
    /// the registered turn and a later real-id terminal loses the exactly-once
    /// gate. Regression for the codex P2: keying on `user_msg_id` would split
    /// them and duplicate the finalize side-effects.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn unknown_user_msg_id_collapses_onto_registered_turn() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        let fin = TurnFinalizer::spawn();
        let ch = ChannelId::new(606);
        let registered = TurnKey::new(ch, 99_999, 0);
        let channel_only = TurnKey::new(ch, 0, 0);
        fin.register_start(registered, ProviderKind::Claude, RelayOwnerKind::Watcher);

        // Channel-only (id 0) terminal finalizes the registered turn.
        let first = fin
            .submit_terminal(
                channel_only,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(first, FinalizeOutcome::Finalized { .. }));

        // Real-id terminal now hits the same entry → AlreadyFinalized.
        let second = fin
            .submit_terminal(
                registered,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(second, FinalizeOutcome::AlreadyFinalized));
    }

    /// The counter is decremented at most once even under a double terminal
    /// submission — `removed_token.is_some()` gates the decrement and the
    /// saturating helper can never underflow.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn no_underflow_on_double_terminal() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        // No active mailbox turn → mailbox_finish_turn returns removed_token=None.
        shared.global_active.store(0, Ordering::Relaxed);
        let fin = TurnFinalizer::spawn();
        let k = key(202);

        let _ = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::bridge(),
                shared.clone(),
            )
            .await;
        let _ = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::bridge(),
                shared.clone(),
            )
            .await;

        // Never underflows below zero.
        assert_eq!(shared.global_active.load(Ordering::Relaxed), 0);
    }

    /// A gate-timeout with a busy pane and a live relay owner defers; once the
    /// backstop deadline elapses the reconciler finalizes exactly once.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn gate_timeout_pane_busy_finalizes_after_backstop() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        let fin = TurnFinalizer::spawn();
        let k = key(303);
        fin.register_start(k, ProviderKind::Claude, RelayOwnerKind::Watcher);

        let deferred = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::GateTimeout {
                    pane_quiescent: Some(false),
                },
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(deferred, FinalizeOutcome::Deferred));

        // Sleep past GATE_BACKSTOP. Under `start_paused` the runtime
        // auto-advances the clock once all tasks are idle on timers, which lets
        // the actor's own reconcile interval fire and finalize the deferred
        // entry. A couple of extra reconcile intervals guarantees the pass ran.
        tokio::time::sleep(GATE_BACKSTOP + RECONCILE_INTERVAL * 3).await;
        tokio::task::yield_now().await;

        // A late terminal now sees Finalized → AlreadyFinalized, proving the
        // reconciler finalized the deferred entry.
        let late = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(late, FinalizeOutcome::AlreadyFinalized));
    }

    /// A gate-timeout whose pane is already quiescent finalizes immediately
    /// (no deferral).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn gate_timeout_pane_quiescent_finalizes_now() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        let fin = TurnFinalizer::spawn();
        let k = key(404);
        fin.register_start(k, ProviderKind::Claude, RelayOwnerKind::Watcher);

        let outcome = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::GateTimeout {
                    pane_quiescent: Some(true),
                },
                FinalizeContext::watcher(),
                shared.clone(),
            )
            .await;
        assert!(matches!(outcome, FinalizeOutcome::Finalized { .. }));
    }

    /// A cancel submission does not double-apply completion cleanup and yields
    /// the exactly-once gate (a late Complete loses).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cancel_then_late_complete_already_finalized() {
        let shared = super::super::make_shared_data_for_tests_with_storage(None, None);
        let fin = TurnFinalizer::spawn();
        let k = key(505);
        fin.register_start(k, ProviderKind::Claude, RelayOwnerKind::Watcher);

        let cancelled = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Cancel,
                FinalizeContext::bridge(),
                shared.clone(),
            )
            .await;
        assert!(matches!(cancelled, FinalizeOutcome::Finalized { .. }));

        let late = fin
            .submit_terminal(
                k,
                ProviderKind::Claude,
                TerminalEvent::Complete,
                FinalizeContext::bridge(),
                shared.clone(),
            )
            .await;
        assert!(matches!(late, FinalizeOutcome::AlreadyFinalized));
    }
}
