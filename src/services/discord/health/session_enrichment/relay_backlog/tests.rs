//! Synthetic provider records exercise the production parsers and receipt policy.
//! No provider process, transcript, tmux session, global observer cache or HTTP.

use super::*;
use crate::services::agent_protocol::StreamMessage;
use crate::services::codex_tui::rollout_tail::RolloutRecordDecoder;
use serde_json::{Value, json};

fn records(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn native_text(text: &str, phase: &str) -> Value {
    json!({"type":"response_item","payload":{
        "type":"message","role":"assistant","phase":phase,
        "content":[{"type":"output_text","text":text}]
    }})
}

fn native_complete(text: &str) -> Value {
    json!({"type":"event_msg","payload":{
        "type":"task_complete","turn_id":"synthetic-turn","last_agent_message":text
    }})
}

fn native_tool() -> Value {
    json!({"type":"response_item","payload":{
        "type":"function_call","name":"exec_command","arguments":"{}","call_id":"synthetic-call"
    }})
}

fn native_tool_output() -> Value {
    json!({"type":"response_item","payload":{
        "type":"function_call_output","call_id":"synthetic-call","output":"synthetic output"
    }})
}

fn native_progress() -> Value {
    // Same native schema as progress_event_msg_refreshes_drain_clock.
    json!({"type":"event_msg","payload":{
        "type":"exec_command_progress","message":"still running"
    }})
}

fn identity(runtime: RuntimeHandoffKind) -> SourceIdentity {
    SourceIdentity {
        provider: if runtime == RuntimeHandoffKind::CodexTui {
            "codex"
        } else {
            "claude"
        }
        .into(),
        channel: 42,
        user_message: 7,
        finalizer_turn: 7,
        started_at: "2000-01-01T00:00:00Z".into(),
        runtime,
        tmux: "synthetic-backlog".into(),
        path: "synthetic-backlog.jsonl".into(),
        file_id: (1, 2),
        generation: 3,
        reset_incarnation: 4,
        turn_start: 0,
    }
}

fn request(bytes: &[u8], runtime: RuntimeHandoffKind) -> ScanRequest {
    ScanRequest {
        identity: identity(runtime),
        capture_end: bytes.len() as u64,
        capture_modified: Some(std::time::UNIX_EPOCH),
        frontier: 0,
        full_response: String::new(),
        response_sent_offset: 0,
        policy_clear: None,
    }
}

fn assert_clear(result: ScanResult, expected: BacklogClearReason) {
    match result {
        ScanResult::Clear(reason) => assert_eq!(reason, expected),
        other => panic!("expected measured clear {expected:?}, got {other:?}"),
    }
}

fn assert_unknown(result: ScanResult, expected: BacklogUnknownReason) {
    match result {
        ScanResult::Unknown(reason) => assert_eq!(reason, expected),
        other => panic!("expected unknown {expected:?}, got {other:?}"),
    }
}

fn pending(result: ScanResult, kind: PendingOutputKind, end: u64) -> PendingSpan {
    match result {
        ScanResult::Pending(span) => {
            assert_eq!(span.kind, kind);
            assert_eq!(span.end, end);
            span
        }
        other => panic!("expected pending {kind:?} at {end}, got {other:?}"),
    }
}

#[test]
fn native_tool_reasoning_and_progress_bytes_are_not_answer_obligations() {
    // All records are real native schemas used by existing rollout-tail tests.
    // Their contents are synthetic; a ToolUse is not an assistant text block.
    let values = [
        json!({"type":"session_meta","payload":{"id":"synthetic-native","cwd":"/synthetic"}}),
        json!({"type":"turn_context","payload":{"cwd":"/synthetic","model":"synthetic-model","effort":"high"}}),
        json!({"type":"response_item","payload":{"type":"reasoning"}}),
        native_tool(),
        native_tool_output(),
        native_progress(),
        json!({"type":"event_msg","payload":{"type":"token_count","info":{}}}),
    ];
    for end in 1..=values.len() {
        let bytes = records(&values[..end]);
        assert!(
            bytes.len() > 0,
            "raw capture grows independently of answers"
        );
        let req = request(&bytes, RuntimeHandoffKind::CodexTui);
        assert_eq!(req.frontier, 0, "reading is not a delivery receipt");
        assert_clear(
            scan_records(&req, &bytes),
            BacklogClearReason::NoEligibleOutput,
        );
    }
}

#[test]
fn canonical_claude_tools_thinking_and_system_status_have_no_answer_backlog() {
    let bytes = records(&[
        json!({"type":"system","subtype":"init","session_id":"synthetic-claude"}),
        json!({"type":"assistant","message":{"content":[{"type":"thinking","thinking":"synthetic"}]}}),
        json!({"type":"assistant","message":{"content":[{
            "type":"tool_use","id":"synthetic-tool","name":"Bash","input":{"command":"synthetic"}
        }]}}),
        json!({"type":"user","message":{"content":[{
            "type":"tool_result","tool_use_id":"synthetic-tool","content":"synthetic output"
        }]}}),
        json!({"type":"system","subtype":"turn_duration","durationMs":31_000}),
    ]);
    for runtime in [
        RuntimeHandoffKind::ClaudeTui,
        RuntimeHandoffKind::LegacyTmuxWrapper,
    ] {
        assert_clear(
            scan_records(&request(&bytes, runtime), &bytes),
            BacklogClearReason::NoEligibleOutput,
        );
    }
}

#[test]
fn unconsumed_native_and_claude_text_establish_exact_source_spans() {
    for (runtime, row) in [
        (
            RuntimeHandoffKind::CodexTui,
            native_text("answer 후", "final_answer"),
        ),
        (
            RuntimeHandoffKind::ClaudeTui,
            json!({"type":"assistant","message":{"content":[{"type":"text","text":"answer 후"}]}}),
        ),
    ] {
        let bytes = records(&[row]);
        pending(
            scan_records(&request(&bytes, runtime), &bytes),
            PendingOutputKind::AssistantText,
            bytes.len() as u64,
        );
    }
}

#[test]
fn canonical_claude_text_after_a_tool_in_the_same_record_is_not_lost() {
    let bytes = records(&[json!({"type":"assistant","message":{"content":[
        {"type":"tool_use","id":"synthetic-tool","name":"Bash","input":{}},
        {"type":"text","text":"answer after tool"}
    ]}})]);
    pending(
        scan_records(&request(&bytes, RuntimeHandoffKind::ClaudeTui), &bytes),
        PendingOutputKind::AssistantText,
        bytes.len() as u64,
    );
}

#[test]
fn persisted_preview_without_a_send_offset_remains_unknown() {
    let bytes = records(&[native_text("preview", "commentary")]);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.full_response = "preview".into();
    assert_unknown(
        scan_records(&req, &bytes),
        BacklogUnknownReason::PreviewDeliveryUnconfirmed,
    );
    let evidence = RelayBacklogEvidence::unknown(BacklogUnknownReason::PreviewDeliveryUnconfirmed);
    assert!(
        !evidence.overdue(30_000),
        "a persisted buffer must not become a false delivery fault"
    );
}

#[test]
fn frozen_response_sent_offset_proves_a_nonterminal_preview_clear() {
    let bytes = records(&[native_text("frozen prefix 후", "commentary")]);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.full_response = "frozen prefix 후".into();
    req.response_sent_offset = req.full_response.len();
    assert_clear(
        scan_records(&req, &bytes),
        BacklogClearReason::ConfirmedOutput,
    );
    assert_eq!(
        req.frontier, 0,
        "preview coverage does not forge a terminal receipt"
    );
}

#[test]
fn commentary_text_is_eligible_even_when_native_completion_is_absent() {
    let row = native_text("working", "commentary");
    let mut decoder = RolloutRecordDecoder::default();
    let messages = decoder.decode(&row).unwrap();
    assert!(
        messages
            .iter()
            .any(|message| matches!(message, StreamMessage::Text { .. }))
    );
    assert!(
        !messages
            .iter()
            .any(|message| matches!(message, StreamMessage::Done { .. }))
    );
    let bytes = records(&[row]);
    pending(
        scan_records(&request(&bytes, RuntimeHandoffKind::CodexTui), &bytes),
        PendingOutputKind::AssistantText,
        bytes.len() as u64,
    );
}

#[test]
fn completion_after_frontier_still_requires_terminal_delivery_of_prior_text() {
    let first = records(&[native_text("final answer", "final_answer")]);
    let bytes = records(&[
        native_text("final answer", "final_answer"),
        native_complete("final answer"),
    ]);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.frontier = first.len() as u64;
    req.full_response = "final answer".into();
    req.response_sent_offset = req.full_response.len();
    pending(
        scan_records(&req, &bytes),
        PendingOutputKind::TerminalAnswer,
        bytes.len() as u64,
    );
}

#[test]
fn matching_tool_output_releases_native_completion_at_its_own_range() {
    let prefix = [
        native_text("done", "final_answer"),
        native_tool(),
        native_complete("done"),
    ];
    let bytes = records(&prefix);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.full_response = "done".into();
    req.response_sent_offset = 4;
    assert_clear(
        scan_records(&req, &bytes),
        BacklogClearReason::ConfirmedOutput,
    );
    let mut completed = prefix.to_vec();
    completed.push(native_tool_output());
    let completed = records(&completed);
    req.capture_end = completed.len() as u64;
    pending(
        scan_records(&req, &completed),
        PendingOutputKind::TerminalAnswer,
        completed.len() as u64,
    );
}

#[test]
fn telemetry_after_an_already_delivered_completion_does_not_reemit_old_obligation() {
    let complete = [
        native_text("old completed answer", "final_answer"),
        native_complete("old completed answer"),
    ];
    let confirmed = records(&complete).len() as u64;
    let mut rows = complete.to_vec();
    rows.extend([
        native_progress(),
        json!({"type":"event_msg","payload":{"type":"token_count","info":{}}}),
    ]);
    let bytes = records(&rows);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.frontier = confirmed;
    assert!(
        req.capture_end > confirmed,
        "telemetry alone advances raw capture"
    );
    assert_clear(
        scan_records(&req, &bytes),
        BacklogClearReason::NoEligibleOutput,
    );
}

#[test]
fn task_complete_fallback_text_is_eligible_but_empty_fallback_is_not_an_answer() {
    let nonempty = records(&[native_complete("fallback answer")]);
    pending(
        scan_records(&request(&nonempty, RuntimeHandoffKind::CodexTui), &nonempty),
        PendingOutputKind::TerminalAnswer,
        nonempty.len() as u64,
    );
    let empty = records(&[native_complete("")]);
    assert_clear(
        scan_records(&request(&empty, RuntimeHandoffKind::CodexTui), &empty),
        BacklogClearReason::NoEligibleOutput,
    );
}

#[test]
fn normalized_claude_result_is_a_terminal_obligation_after_preview_coverage() {
    let bytes = records(&[
        json!({"type":"assistant","message":{"content":[{"type":"text","text":"done"}]}}),
        json!({"type":"result","subtype":"success","result":"done","session_id":"synthetic-claude"}),
    ]);
    let mut req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    req.full_response = "done".into();
    req.response_sent_offset = 4;
    pending(
        scan_records(&req, &bytes),
        PendingOutputKind::TerminalAnswer,
        bytes.len() as u64,
    );
}

#[test]
fn normalized_soft_stop_does_not_mask_a_later_hard_result() {
    let bytes = records(&[
        json!({"type":"system","subtype":"stop_hook_summary","session_id":"synthetic-claude"}),
        json!({"type":"assistant","message":{"content":[{"type":"text","text":"late answer"}]}}),
        json!({"type":"result","subtype":"success","result":"late answer","session_id":"synthetic-claude"}),
    ]);
    let mut req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    req.full_response = "late answer".into();
    req.response_sent_offset = req.full_response.len();
    pending(
        scan_records(&req, &bytes),
        PendingOutputKind::TerminalAnswer,
        bytes.len() as u64,
    );
}

#[test]
fn normalized_interleaved_text_and_tool_blocks_match_the_delivered_composed_body() {
    let bytes = records(&[json!({"type":"assistant","message":{"content":[
        {"type":"text","text":"First answer."},
        {"type":"tool_use","id":"synthetic-tool","name":"Bash","input":{}},
        {"type":"text","text":"Second answer."}
    ]}})]);
    let mut req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    req.full_response = "First answer.\n\nSecond answer.".into();
    req.response_sent_offset = req.full_response.len();
    assert_clear(
        scan_records(&req, &bytes),
        BacklogClearReason::ConfirmedOutput,
    );
    req.response_sent_offset = 0;
    assert_unknown(
        scan_records(&req, &bytes),
        BacklogUnknownReason::PreviewDeliveryUnconfirmed,
    );
}

#[test]
fn normalized_empty_hard_result_still_requires_delivery_of_frozen_assistant_body() {
    let bytes = records(&[
        json!({"type":"assistant","message":{"content":[{"type":"text","text":"completed answer"}]}}),
        json!({"type":"result","subtype":"success","result":"","session_id":"synthetic-claude"}),
    ]);
    let mut req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    req.full_response = "completed answer".into();
    req.response_sent_offset = req.full_response.len();
    pending(
        scan_records(&req, &bytes),
        PendingOutputKind::TerminalAnswer,
        bytes.len() as u64,
    );
}

#[test]
fn normalized_phantom_response_does_not_create_text_or_terminal_obligation() {
    let bytes = records(&[
        json!({"type":"assistant","message":{"content":[{"type":"text","text":"No response requested."}]}}),
        json!({"type":"result","subtype":"success","result":"","session_id":"synthetic-claude"}),
    ]);
    let req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    assert!(
        req.full_response.is_empty(),
        "the actual inflight buffer contains no user-facing response"
    );
    assert_clear(
        scan_records(&req, &bytes),
        BacklogClearReason::NoEligibleOutput,
    );
}

#[test]
fn normalized_new_prompt_after_confirmed_answer_is_ambiguous_but_tool_result_is_not() {
    let old = [
        json!({"type":"assistant","message":{"content":[{"type":"text","text":"old answer"}]}}),
        json!({"type":"result","subtype":"success","result":"old answer","session_id":"synthetic-claude"}),
    ];
    let frontier = records(&old).len() as u64;
    let mut new_turn = old.to_vec();
    // The watcher's existing user-message boundary recognizes this prompt.
    new_turn.push(json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"new question"}]}}));
    new_turn.push(
        json!({"type":"result","subtype":"success","result":"","session_id":"synthetic-claude"}),
    );
    let bytes = records(&new_turn);
    let mut req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    req.frontier = frontier;
    assert_unknown(
        scan_records(&req, &bytes),
        BacklogUnknownReason::AmbiguousTurnBoundary,
    );

    let mut same_turn = old.to_vec();
    // A tool result is provider-role=user, but carries no genuine prompt text.
    same_turn.push(json!({"type":"user","message":{"role":"user","content":[{
        "type":"tool_result","tool_use_id":"synthetic-tool","content":"tool output"
    }]}}));
    same_turn.push(
        json!({"type":"result","subtype":"success","result":"","session_id":"synthetic-claude"}),
    );
    let bytes = records(&same_turn);
    req.capture_end = bytes.len() as u64;
    assert_clear(
        scan_records(&req, &bytes),
        BacklogClearReason::NoEligibleOutput,
    );
}

#[test]
fn incomplete_or_unsupported_records_never_measure_empty_backlog() {
    let unsupported_native = records(&[json!({"type":"response_item","payload":{
        "type":"message","role":"assistant","content":[{"type":"output_image","image":"synthetic"}]
    }})]);
    let unsupported_claude = records(&[
        json!({"type":"assistant","message":{"content":[{"type":"image","source":{}}]}}),
    ]);
    let cases = [
        (
            RuntimeHandoffKind::CodexTui,
            unsupported_native,
            BacklogUnknownReason::UnsupportedRecord,
        ),
        (
            RuntimeHandoffKind::ClaudeTui,
            unsupported_claude,
            BacklogUnknownReason::UnsupportedRecord,
        ),
        (
            RuntimeHandoffKind::CodexTui,
            b"{malformed}\n".to_vec(),
            BacklogUnknownReason::MalformedRecord,
        ),
        (
            RuntimeHandoffKind::CodexTui,
            b"{\"type\":\"event_msg\"".to_vec(),
            BacklogUnknownReason::PartialRecord,
        ),
        (
            RuntimeHandoffKind::CodexTui,
            vec![0xff, b'\n'],
            BacklogUnknownReason::MalformedRecord,
        ),
        (
            RuntimeHandoffKind::CodexTui,
            vec![b'x'; MAX_RECORD_BYTES + 1]
                .into_iter()
                .chain([b'\n'])
                .collect(),
            BacklogUnknownReason::RecordBudgetExceeded,
        ),
        (
            RuntimeHandoffKind::CodexTui,
            vec![b'\n'; MAX_SCAN_BYTES as usize + 1],
            BacklogUnknownReason::ScanBudgetExceeded,
        ),
    ];
    for (runtime, bytes, reason) in cases {
        assert_unknown(scan_records(&request(&bytes, runtime), &bytes), reason);
        assert!(!RelayBacklogEvidence::unknown(reason).overdue(30_000));
    }
}

#[test]
fn frontier_in_the_middle_of_a_complete_record_is_unknown() {
    let bytes = records(&[native_text("answer", "final_answer")]);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.frontier = 1;
    assert_unknown(
        scan_records(&req, &bytes),
        BacklogUnknownReason::PartialRecord,
    );
}

#[test]
fn a_second_native_turn_boundary_in_one_scan_is_unknown() {
    let bytes = records(&[
        native_text("first turn", "final_answer"),
        native_complete("first turn"),
        json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"second"}}),
        native_text("second turn", "final_answer"),
    ]);
    assert_unknown(
        scan_records(&request(&bytes, RuntimeHandoffKind::CodexTui), &bytes),
        BacklogUnknownReason::AmbiguousTurnBoundary,
    );
}

fn clock_entry(req: &ScanRequest, now: Instant) -> CacheEntry {
    CacheEntry {
        identity: req.identity.clone(),
        fingerprint: fingerprint(req),
        sampled_at: now,
        serial: 1,
        result: ScanResult::Clear(BacklogClearReason::NoEligibleOutput),
        pending: None,
    }
}

fn assert_age(evidence: &RelayBacklogEvidence, expected: u64, first_seen_ms: i64) {
    match evidence {
        RelayBacklogEvidence::Pending {
            pending_age_ms,
            observed_since_unix_ms,
            age_basis,
            ..
        } => {
            assert_eq!(*pending_age_ms, expected);
            assert_eq!(*observed_since_unix_ms, first_seen_ms);
            assert_eq!(*age_basis, "observed_since");
        }
        other => panic!("expected observed pending age, got {other:?}"),
    }
}

#[test]
fn backlog_grace_uses_first_observation_and_has_exact_thirty_second_boundary() {
    let bytes = records(&[native_text("new answer", "final_answer")]);
    let req = request(&bytes, RuntimeHandoffKind::CodexTui);
    let span = pending(
        scan_records(&req, &bytes),
        PendingOutputKind::AssistantText,
        bytes.len() as u64,
    );
    let quiet_start = Instant::now();
    let first_seen = quiet_start + Duration::from_secs(86_400);
    let mut entry = clock_entry(&req, quiet_start);
    let first = ObserverCache::publish(&mut entry, ScanResult::Pending(span), first_seen, 1_000);
    assert_age(&first, 0, 1_000);
    assert!(
        !first.overdue(30_000),
        "an old started_at and long quiet interval grant the new output fresh grace"
    );
    let before = evidence_for(&entry, first_seen + Duration::from_millis(29_999));
    assert_age(&before, 29_999, 1_000);
    assert!(!before.overdue(30_000));
    let boundary = evidence_for(&entry, first_seen + Duration::from_millis(30_000));
    assert_age(&boundary, 30_000, 1_000);
    assert!(boundary.overdue(30_000));
}

#[test]
fn progress_and_unknown_observations_preserve_the_same_pending_clock() {
    let first = records(&[native_text("waiting for delivery", "final_answer")]);
    let req = request(&first, RuntimeHandoffKind::CodexTui);
    let span = pending(
        scan_records(&req, &first),
        PendingOutputKind::AssistantText,
        first.len() as u64,
    );
    let now = Instant::now();
    let mut entry = clock_entry(&req, now);
    ObserverCache::publish(&mut entry, ScanResult::Pending(span), now, 2_000);
    let later = records(&[
        native_text("waiting for delivery", "final_answer"),
        native_progress(),
        native_tool(),
        native_tool_output(),
    ]);
    let later_req = request(&later, RuntimeHandoffKind::CodexTui);
    let same_span = pending(
        scan_records(&later_req, &later),
        PendingOutputKind::AssistantText,
        first.len() as u64,
    );
    assert_eq!(
        same_span, span,
        "appended tool/progress records must not refresh pending answer age"
    );
    let progressed = ObserverCache::publish(
        &mut entry,
        ScanResult::Pending(same_span),
        now + Duration::from_secs(20),
        22_000,
    );
    assert_age(&progressed, 20_000, 2_000);
    let unknown = ObserverCache::publish(
        &mut entry,
        ScanResult::Unknown(BacklogUnknownReason::ReadTimedOut),
        now + Duration::from_secs(25),
        27_000,
    );
    assert!(
        !unknown.overdue(30_000),
        "unknown remains unknown while retaining internal witnessed age"
    );
    let recovered = ObserverCache::publish(
        &mut entry,
        ScanResult::Pending(span),
        now + Duration::from_secs(30),
        32_000,
    );
    assert_age(&recovered, 30_000, 2_000);
    assert!(recovered.overdue(30_000));
}

#[test]
fn identical_text_at_a_later_source_range_gets_a_new_pending_episode() {
    let bytes = records(&[native_text("same answer", "final_answer")]);
    let req = request(&bytes, RuntimeHandoffKind::CodexTui);
    let old = pending(
        scan_records(&req, &bytes),
        PendingOutputKind::AssistantText,
        bytes.len() as u64,
    );
    let mut later_req = req.clone();
    later_req.identity.turn_start = bytes.len() as u64;
    later_req.frontier = bytes.len() as u64;
    later_req.capture_end = 2 * bytes.len() as u64;
    let new = pending(
        scan_records(&later_req, &bytes),
        PendingOutputKind::AssistantText,
        2 * bytes.len() as u64,
    );
    assert_eq!(old.record_hash, new.record_hash);
    assert_ne!(
        old, new,
        "text equality cannot identify a source-range obligation"
    );
    let now = Instant::now();
    let mut entry = clock_entry(&req, now);
    ObserverCache::publish(&mut entry, ScanResult::Pending(old), now, 10);
    let evidence = ObserverCache::publish(
        &mut entry,
        ScanResult::Pending(new),
        now + Duration::from_secs(60),
        60_010,
    );
    assert_age(&evidence, 0, 60_010);
}

#[test]
fn measured_clear_resets_pending_age_before_a_later_answer() {
    let bytes = records(&[native_text("answer", "final_answer")]);
    let req = request(&bytes, RuntimeHandoffKind::CodexTui);
    let span = pending(
        scan_records(&req, &bytes),
        PendingOutputKind::AssistantText,
        bytes.len() as u64,
    );
    let now = Instant::now();
    let mut entry = clock_entry(&req, now);
    ObserverCache::publish(&mut entry, ScanResult::Pending(span), now, 1);
    ObserverCache::publish(
        &mut entry,
        ScanResult::Clear(BacklogClearReason::ConfirmedOutput),
        now + Duration::from_secs(40),
        40_001,
    );
    assert!(entry.pending.is_none());
    let next = ObserverCache::publish(
        &mut entry,
        ScanResult::Pending(span),
        now + Duration::from_secs(41),
        41_001,
    );
    assert_age(&next, 0, 41_001);
}

fn valid_state() -> InflightTurnState {
    let mut state = InflightTurnState::new(
        ProviderKind::Codex,
        42,
        None,
        1,
        7,
        8,
        "synthetic request".into(),
        Some("synthetic-session".into()),
        Some("synthetic-backlog".into()),
        Some("synthetic-backlog.jsonl".into()),
        None,
        0,
    );
    state.runtime_kind = Some(RuntimeHandoffKind::CodexTui);
    state.turn_start_offset = Some(0);
    state
}

fn coordinate() -> CaptureCoordinateObservation {
    CaptureCoordinateObservation {
        offset: Some(100),
        path_hash: 0,
        file_id: Some((1, 2)),
        status: super::super::super::liveness_authority::CoordinateStatus::Observed,
    }
}

fn gated_request(state: &InflightTurnState) -> Result<ScanRequest, RelayBacklogEvidence> {
    make_request(RelayObservation {
        provider: Some(&ProviderKind::Codex),
        channel: 42,
        state: Some(state),
        watcher_path: Some("synthetic-backlog.jsonl"),
        coordinate: &coordinate(),
        capture_modified: Some(std::time::UNIX_EPOCH),
        frontier: Some(0),
        generation: Some(3),
        reset_incarnation: Some(4),
        binding_matches: true,
    })
}

fn assert_gate_unknown(
    result: Result<ScanRequest, RelayBacklogEvidence>,
    expected: BacklogUnknownReason,
) {
    match result {
        Err(RelayBacklogEvidence::Unknown { reason }) => assert_eq!(reason, expected),
        Err(other) => panic!("expected unknown {expected:?}, got {other:?}"),
        Ok(_) => panic!("an unresolved input must not authorize a scan: {expected:?}"),
    }
}

#[test]
fn silent_and_terminal_policy_clear_require_a_coherent_source_request() {
    for (terminal, reason) in [
        (false, BacklogClearReason::SilentTurn),
        (true, BacklogClearReason::TerminalDelivered),
    ] {
        let mut state = valid_state();
        state.silent_turn = !terminal;
        state.terminal_delivery_committed = terminal;
        let request = gated_request(&state)
            .expect("coherent source request carries policy intent, not a delivery proof");
        assert_eq!(request.policy_clear, Some(reason));
        state.output_path = None;
        assert_gate_unknown(gated_request(&state), BacklogUnknownReason::MissingSource);
    }
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: None,
            channel: 42,
            state: None,
            watcher_path: None,
            coordinate: &coordinate(),
            capture_modified: Some(std::time::UNIX_EPOCH),
            frontier: None,
            generation: None,
            reset_incarnation: None,
            binding_matches: false,
        }),
        BacklogUnknownReason::InflightNotObserved,
    );
}

#[test]
fn foreign_channel_or_provider_cannot_use_silent_or_terminal_delivery_as_clear_evidence() {
    for terminal in [false, true] {
        let mut state = valid_state();
        state.silent_turn = !terminal;
        state.terminal_delivery_committed = terminal;
        state.channel_id = 43;
        assert_gate_unknown(
            gated_request(&state),
            BacklogUnknownReason::ProviderOrRuntimeUnresolved,
        );
        state.channel_id = 42;
        state.provider = "claude".into();
        assert_gate_unknown(
            gated_request(&state),
            BacklogUnknownReason::ProviderOrRuntimeUnresolved,
        );
    }
}

#[test]
fn missing_runtime_source_generation_or_turn_binding_remains_unknown() {
    let valid = valid_state();
    assert!(gated_request(&valid).is_ok());
    let cases: [(fn(&mut InflightTurnState), BacklogUnknownReason); 8] = [
        (
            |s| s.runtime_kind = None,
            BacklogUnknownReason::ProviderOrRuntimeUnresolved,
        ),
        (
            |s| s.runtime_kind_unknown_on_disk = true,
            BacklogUnknownReason::ProviderOrRuntimeUnresolved,
        ),
        (
            |s| s.provider = "claude".into(),
            BacklogUnknownReason::ProviderOrRuntimeUnresolved,
        ),
        (
            |s| s.output_path = None,
            BacklogUnknownReason::MissingSource,
        ),
        (
            |s| s.tmux_session_name = None,
            BacklogUnknownReason::SourceBindingMismatch,
        ),
        (
            |s| s.turn_start_offset = None,
            BacklogUnknownReason::TurnStartUnresolved,
        ),
        (
            |s| s.last_watcher_relayed_generation_mtime_ns = Some(99),
            BacklogUnknownReason::GenerationUnresolved,
        ),
        (
            |s| s.tui_terminal_source_file_identity = Some((99, 100)),
            BacklogUnknownReason::GenerationUnresolved,
        ),
    ];
    for (mutate, reason) in cases {
        let mut state = valid.clone();
        mutate(&mut state);
        assert_gate_unknown(gated_request(&state), reason);
    }
    let mut missing_id = coordinate();
    missing_id.file_id = None;
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: Some(&ProviderKind::Codex),
            channel: 42,
            state: Some(&valid),
            watcher_path: Some("synthetic-backlog.jsonl"),
            coordinate: &missing_id,
            capture_modified: Some(std::time::UNIX_EPOCH),
            frontier: Some(0),
            generation: Some(3),
            reset_incarnation: Some(4),
            binding_matches: true,
        }),
        BacklogUnknownReason::SourceIdentityUnresolved,
    );
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: Some(&ProviderKind::Codex),
            channel: 42,
            state: Some(&valid),
            watcher_path: Some("synthetic-backlog.jsonl"),
            coordinate: &coordinate(),
            capture_modified: Some(std::time::UNIX_EPOCH),
            frontier: None,
            generation: Some(3),
            reset_incarnation: Some(4),
            binding_matches: true,
        }),
        BacklogUnknownReason::MissingFrontier,
    );
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: Some(&ProviderKind::Codex),
            channel: 42,
            state: Some(&valid),
            watcher_path: Some("synthetic-backlog.jsonl"),
            coordinate: &coordinate(),
            capture_modified: Some(std::time::UNIX_EPOCH),
            frontier: Some(0),
            generation: Some(0),
            reset_incarnation: Some(4),
            binding_matches: true,
        }),
        BacklogUnknownReason::GenerationUnresolved,
    );
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: Some(&ProviderKind::Codex),
            channel: 42,
            state: Some(&valid),
            watcher_path: Some("synthetic-backlog.jsonl"),
            coordinate: &coordinate(),
            capture_modified: Some(std::time::UNIX_EPOCH),
            frontier: Some(0),
            generation: Some(3),
            reset_incarnation: Some(4),
            binding_matches: false,
        }),
        BacklogUnknownReason::SourceBindingMismatch,
    );
}

#[test]
fn invalid_delivery_buffers_and_out_of_range_frontiers_are_unknown() {
    let mut state = valid_state();
    state.full_response = "후".into();
    state.response_sent_offset = 1;
    assert_gate_unknown(
        gated_request(&state),
        BacklogUnknownReason::DeliveryBufferMismatch,
    );
    state.response_sent_offset = state.full_response.len() + 1;
    assert_gate_unknown(
        gated_request(&state),
        BacklogUnknownReason::DeliveryBufferMismatch,
    );
    state.response_sent_offset = 0;
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: Some(&ProviderKind::Codex),
            channel: 42,
            state: Some(&state),
            watcher_path: Some("synthetic-backlog.jsonl"),
            coordinate: &coordinate(),
            capture_modified: Some(std::time::UNIX_EPOCH),
            frontier: Some(101),
            generation: Some(3),
            reset_incarnation: Some(4),
            binding_matches: true,
        }),
        BacklogUnknownReason::InvalidRange,
    );
    let bytes = records(&[native_text("answer", "final_answer")]);
    let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
    req.full_response = "a different response".into();
    assert_unknown(
        scan_records(&req, &bytes),
        BacklogUnknownReason::DeliveryBufferMismatch,
    );
}

#[test]
fn same_length_source_overwrite_changes_the_observation_fingerprint() {
    let bytes = records(&[native_text("answer", "final_answer")]);
    let before = request(&bytes, RuntimeHandoffKind::CodexTui);
    let mut overwritten = before.clone();
    overwritten.capture_modified = Some(std::time::UNIX_EPOCH + Duration::from_secs(1));
    assert_eq!(
        before.identity, overwritten.identity,
        "same inode and binding"
    );
    assert_eq!(
        before.capture_end, overwritten.capture_end,
        "same length overwrite"
    );
    assert_ne!(
        fingerprint(&before),
        fingerprint(&overwritten),
        "an unchanged length/frontier must not reuse clear evidence for overwritten source bytes"
    );
}

#[test]
fn missing_capture_modification_witness_cannot_authorize_a_scan() {
    let state = valid_state();
    assert_gate_unknown(
        make_request(RelayObservation {
            provider: Some(&ProviderKind::Codex),
            channel: 42,
            state: Some(&state),
            watcher_path: Some("synthetic-backlog.jsonl"),
            coordinate: &coordinate(),
            capture_modified: None,
            frontier: Some(0),
            generation: Some(3),
            reset_incarnation: Some(4),
            binding_matches: true,
        }),
        BacklogUnknownReason::SourceIdentityUnresolved,
    );
}

fn enrichment_from_evidence(
    req: &ScanRequest,
    evidence: RelayBacklogEvidence,
) -> super::super::SessionEnrichment {
    use crate::services::discord::relay_health::{
        CoordFrontierObservation, DurableFrontierObservation, FrontierProvenance,
    };
    let mut capture = coordinate();
    capture.offset = Some(req.capture_end);
    let mut row = valid_state();
    row.started_at = "2000-01-01T00:00:00Z".into();
    super::super::SessionEnrichment {
        inflight: Some(row),
        attached: true,
        watcher_attached: true,
        watcher_attached_stale: false,
        has_relay_coord: true,
        watcher_owner_channel_id: Some(42),
        watcher_output_path: Some(req.identity.path.clone()),
        tmux_session: Some(req.identity.tmux.clone()),
        inflight_state_present: true,
        tmux_session_mismatch: false,
        last_relay_offset: req.frontier,
        last_relay_ts_ms: 0,
        reconnect_count: 0,
        last_capture_offset: Some(req.capture_end),
        capture_coordinate: capture,
        unread_bytes: Some(req.capture_end - req.frontier),
        relay_stale: true,
        capture_lagged: super::super::eligible_capture_lagged(true, true, &evidence),
        relay_backlog: evidence,
        frontier_provenance: FrontierProvenance::observe(
            CoordFrontierObservation::PresentZero,
            DurableFrontierObservation::RowUnrelayed {
                turn_start_offset: 0,
            },
        ),
    }
}

fn classify_owned_foreground(
    enrichment: &super::super::SessionEnrichment,
) -> crate::services::discord::relay_health::RelayStallState {
    use crate::services::discord::relay_health::{
        RelayActiveTurn, RelayHealthSnapshot, RelayStallClassifier,
    };
    let snapshot = RelayHealthSnapshot {
        active_turn: RelayActiveTurn::Foreground,
        bridge_inflight_present: enrichment.inflight_state_present,
        mailbox_has_cancel_token: true,
        tmux_alive: Some(true),
        watcher_attached: enrichment.watcher_attached,
        watcher_attached_stale: enrichment.watcher_attached_stale,
        watcher_owns_live_relay: true,
        last_capture_offset: enrichment.last_capture_offset,
        last_relay_offset: enrichment.last_relay_offset,
        unread_bytes: enrichment.unread_bytes,
        // The most recent outbound progress is deliberately fresh. It is not
        // a receipt for the independently witnessed pending assistant span.
        last_outbound_activity_ms: Some(1_030_000),
        desynced: enrichment.desynced(true, enrichment.attached),
        ..RelayHealthSnapshot::test_snapshot()
    };
    RelayStallClassifier::classify(&snapshot)
}

#[test]
fn native_parser_observation_age_desync_and_classifier_preserve_actual_delivery_lag() {
    use crate::services::discord::relay_health::RelayStallState;
    let progress = records(&[
        json!({"type":"turn_context","payload":{"cwd":"/synthetic","model":"synthetic-model"}}),
        json!({"type":"response_item","payload":{"type":"reasoning"}}),
        native_tool(),
        native_tool_output(),
        native_progress(),
    ]);
    let progress_req = request(&progress, RuntimeHandoffKind::CodexTui);
    let now = Instant::now();
    let mut progress_clock = clock_entry(&progress_req, now);
    let progress_evidence = ObserverCache::publish(
        &mut progress_clock,
        scan_records(&progress_req, &progress),
        now + Duration::from_secs(86_400),
        1_000_000,
    );
    let measured = enrichment_from_evidence(&progress_req, progress_evidence);
    assert!(measured.last_capture_offset.unwrap() > measured.last_relay_offset);
    assert!(
        measured.relay_stale,
        "old turn start and zero delivery frontier are insufficient"
    );
    assert!(!measured.desynced(true, true));
    assert_eq!(
        classify_owned_foreground(&measured),
        RelayStallState::ActiveForegroundStream
    );

    let partial = b"{\"type\":\"response_item\"";
    let partial_req = request(partial, RuntimeHandoffKind::CodexTui);
    let mut partial_clock = clock_entry(&partial_req, now);
    let unknown = ObserverCache::publish(
        &mut partial_clock,
        scan_records(&partial_req, partial),
        now + Duration::from_secs(86_400),
        1_000_000,
    );
    assert!(matches!(
        unknown,
        RelayBacklogEvidence::Unknown {
            reason: BacklogUnknownReason::PartialRecord
        }
    ));
    let mut unmeasured = enrichment_from_evidence(&partial_req, unknown);
    assert!(!unmeasured.desynced(true, true));
    assert_eq!(
        classify_owned_foreground(&unmeasured),
        RelayStallState::ActiveForegroundStream
    );

    let answer = records(&[native_text("eligible but not consumed", "final_answer")]);
    let answer_req = request(&answer, RuntimeHandoffKind::CodexTui);
    let mut answer_clock = clock_entry(&answer_req, now);
    ObserverCache::publish(
        &mut answer_clock,
        scan_records(&answer_req, &answer),
        now,
        1_000_000,
    );
    let within_grace = enrichment_from_evidence(
        &answer_req,
        evidence_for(&answer_clock, now + Duration::from_millis(29_999)),
    );
    assert!(!within_grace.desynced(true, true));
    assert_eq!(
        classify_owned_foreground(&within_grace),
        RelayStallState::ActiveForegroundStream
    );
    let overdue = enrichment_from_evidence(
        &answer_req,
        evidence_for(&answer_clock, now + Duration::from_millis(30_000)),
    );
    assert!(overdue.desynced(true, true));
    assert_eq!(
        classify_owned_foreground(&overdue),
        RelayStallState::TmuxAliveRelayDead,
        "a fresh outbound progress heartbeat must not mask overdue eligible assistant output"
    );

    unmeasured.tmux_session_mismatch = true;
    assert!(
        unmeasured.desynced(true, true),
        "independent session mismatch survives unknown backlog"
    );
    unmeasured.tmux_session_mismatch = false;
    unmeasured.attached = false;
    unmeasured.watcher_attached = false;
    assert!(
        unmeasured.desynced(true, false),
        "an independently witnessed stale orphan survives unknown backlog"
    );
    assert_eq!(
        classify_owned_foreground(&unmeasured),
        RelayStallState::TmuxAliveRelayDead
    );
}

#[test]
fn normalized_result_only_completion_cannot_hide_a_later_prompt_epoch() {
    let old = json!({"type":"result","subtype":"success","result":"old result-only answer","session_id":"synthetic-claude"});
    let frontier = records(&[old.clone()]).len() as u64;
    let bytes = records(&[
        old,
        json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"new question"}]}}),
        json!({"type":"result","subtype":"success","result":"new result-only answer","session_id":"synthetic-claude"}),
    ]);
    let mut req = request(&bytes, RuntimeHandoffKind::ClaudeTui);
    req.frontier = frontier;
    assert_unknown(
        scan_records(&req, &bytes),
        BacklogUnknownReason::AmbiguousTurnBoundary,
    );
}

#[test]
fn native_fallback_only_completion_cannot_hide_a_later_turn_epoch() {
    let old = native_complete("old fallback-only answer");
    let frontier = records(&[old.clone()]).len() as u64;
    let boundaries = [
        json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"new-turn"}}),
        json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"new question"}]}}),
        json!({"type":"turn_context","payload":{"cwd":"/synthetic","model":"synthetic-model","turn_id":"new-turn"}}),
    ];
    for boundary in boundaries {
        let bytes = records(&[
            old.clone(),
            boundary,
            native_complete("new fallback-only answer"),
        ]);
        let mut req = request(&bytes, RuntimeHandoffKind::CodexTui);
        req.frontier = frontier;
        assert_unknown(
            scan_records(&req, &bytes),
            BacklogUnknownReason::AmbiguousTurnBoundary,
        );
    }
}

#[test]
fn normalized_missing_or_numeric_text_is_unsupported_instead_of_measured_clear() {
    for block in [json!({"type":"text"}), json!({"type":"text","text":7})] {
        let bytes = records(&[json!({"type":"assistant","message":{"content":[block]}})]);
        assert_unknown(
            scan_records(&request(&bytes, RuntimeHandoffKind::ClaudeTui), &bytes),
            BacklogUnknownReason::UnsupportedRecord,
        );
    }
}
