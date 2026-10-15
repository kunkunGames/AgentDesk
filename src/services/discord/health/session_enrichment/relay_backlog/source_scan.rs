//! Provider replay and file-descriptor witnesses for the bounded observer.

use serde_json::Value;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom};
use std::time::SystemTime;

use crate::services::agent_protocol::{RuntimeHandoffKind, StreamMessage};
use crate::services::codex_tui::rollout_tail::RolloutRecordDecoder;
use crate::services::discord::response_sanitizer::strip_leading_tui_response_chrome;
use crate::services::discord::semantic_boundaries::semantic_chunk_separator_needed;
use crate::services::session_backend::{
    StreamLineState, observe_stream_context, parse_stream_message_with_state,
};

use super::{
    BacklogClearReason, BacklogUnknownReason, MAX_RECORD_BYTES, MAX_SCAN_BYTES, PendingOutputKind,
    PendingSpan, ScanRequest, ScanResult,
};

#[derive(PartialEq, Eq)]
struct FileWitness {
    id: Option<(u64, u64)>,
    len: u64,
    modified: Option<SystemTime>,
}

fn file_witness(metadata: &std::fs::Metadata) -> FileWitness {
    #[cfg(unix)]
    let id = {
        use std::os::unix::fs::MetadataExt;
        Some((metadata.dev(), metadata.ino()))
    };
    #[cfg(not(unix))]
    let id = None;
    FileWitness {
        id,
        len: metadata.len(),
        modified: metadata.modified().ok(),
    }
}

pub(super) fn scan_file(request: &ScanRequest) -> ScanResult {
    let read = || -> Result<ScanResult, BacklogUnknownReason> {
        let generation = || {
            crate::services::discord::tmux::read_generation_file_mtime_ns(&request.identity.tmux)
        };
        if generation() != request.identity.generation {
            return Err(BacklogUnknownReason::GenerationUnresolved);
        }
        let mut file = std::fs::File::open(&request.identity.path)
            .map_err(|_| BacklogUnknownReason::ReadFailed)?;
        let before = file_witness(
            &file
                .metadata()
                .map_err(|_| BacklogUnknownReason::ReadFailed)?,
        );
        if before.id != Some(request.identity.file_id)
            || before.len != request.capture_end
            || before.modified != request.capture_modified
        {
            return Err(BacklogUnknownReason::SourceChanged);
        }
        if request.identity.turn_start > 0 {
            file.seek(SeekFrom::Start(request.identity.turn_start - 1))
                .map_err(|_| BacklogUnknownReason::ReadFailed)?;
            let mut boundary = [0];
            file.read_exact(&mut boundary)
                .map_err(|_| BacklogUnknownReason::ReadFailed)?;
            if boundary[0] != b'\n' {
                return Err(BacklogUnknownReason::PartialRecord);
            }
        }
        let result = if let Some(reason) = request.policy_clear {
            // Suppression/terminal receipts are scoped to the coherently
            // witnessed row and source, never provider/channel flags alone.
            ScanResult::Clear(reason)
        } else if request.frontier == request.capture_end {
            ScanResult::Clear(BacklogClearReason::ConfirmedOutput)
        } else {
            file.seek(SeekFrom::Start(request.identity.turn_start))
                .map_err(|_| BacklogUnknownReason::ReadFailed)?;
            let mut bytes = vec![0; (request.capture_end - request.identity.turn_start) as usize];
            file.read_exact(&mut bytes)
                .map_err(|_| BacklogUnknownReason::ReadFailed)?;
            scan_records(request, &bytes)
        };
        let after = file_witness(
            &file
                .metadata()
                .map_err(|_| BacklogUnknownReason::ReadFailed)?,
        );
        let path_after = file_witness(
            &std::fs::metadata(&request.identity.path)
                .map_err(|_| BacklogUnknownReason::SourceChanged)?,
        );
        if before != after || after != path_after || generation() != request.identity.generation {
            return Err(BacklogUnknownReason::SourceChanged);
        }
        Ok(result)
    };
    read().unwrap_or_else(ScanResult::Unknown)
}

fn native_schema_supported(record: &Value) -> bool {
    if record.get("type").and_then(Value::as_str) != Some("response_item") {
        return true;
    }
    let Some(payload) = record.get("payload") else {
        return false;
    };
    if payload.get("type").and_then(Value::as_str) != Some("message")
        || payload.get("role").and_then(Value::as_str) != Some("assistant")
    {
        return true;
    }
    payload
        .get("content")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().all(|item| {
                matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("output_text" | "text")
                ) && item.get("text").is_some_and(Value::is_string)
            })
        })
}

fn is_native_turn_boundary(record: &Value) -> bool {
    (record.get("type").and_then(Value::as_str) == Some("response_item")
        && record.pointer("/payload/type").and_then(Value::as_str) == Some("message")
        && record.pointer("/payload/role").and_then(Value::as_str) == Some("user"))
        || (record.get("type").and_then(Value::as_str) == Some("event_msg")
            && matches!(
                record.pointer("/payload/type").and_then(Value::as_str),
                Some("task_started" | "user_message")
            ))
}

fn normalized_messages(
    record: &Value,
    state: &mut StreamLineState,
) -> Result<Vec<StreamMessage>, BacklogUnknownReason> {
    observe_stream_context(record, state);
    match record.get("type").and_then(Value::as_str) {
        Some("assistant") => {
            let items = record
                .pointer("/message/content")
                .and_then(Value::as_array)
                .ok_or(BacklogUnknownReason::UnsupportedRecord)?;
            let mut messages = Vec::new();
            for item in items {
                if !matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("text" | "tool_use" | "thinking")
                ) {
                    return Err(BacklogUnknownReason::UnsupportedRecord);
                }
                if item.get("type").and_then(Value::as_str) == Some("text")
                    && !item.get("text").is_some_and(Value::is_string)
                {
                    return Err(BacklogUnknownReason::UnsupportedRecord);
                }
                let mut projected = record.clone();
                projected["message"]["content"] = Value::Array(vec![item.clone()]);
                if let Some(message) = parse_stream_message_with_state(&projected, state) {
                    messages.push(message);
                }
            }
            Ok(messages)
        }
        Some("user" | "system" | "result") => {
            if let Some(message) = parse_stream_message_with_state(record, state) {
                Ok(vec![message])
            } else if record.get("type").and_then(Value::as_str) == Some("user") {
                Ok(Vec::new())
            } else {
                Err(BacklogUnknownReason::UnsupportedRecord)
            }
        }
        _ => Err(BacklogUnknownReason::UnsupportedRecord),
    }
}

pub(super) fn scan_records(request: &ScanRequest, bytes: &[u8]) -> ScanResult {
    if bytes.len() as u64 > MAX_SCAN_BYTES {
        return ScanResult::Unknown(BacklogUnknownReason::ScanBudgetExceeded);
    }
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return ScanResult::Unknown(BacklogUnknownReason::PartialRecord);
    }
    let native = request.identity.runtime == RuntimeHandoffKind::CodexTui;
    let mut decoder = RolloutRecordDecoder::default();
    let mut normalized = StreamLineState::new();
    let mut first_done = false;
    let mut body = String::new();
    let mut text_spans: Vec<(PendingSpan, usize)> = Vec::new();
    let mut terminal = None;
    let mut end = request.identity.turn_start;
    let mut frontier_boundary = request.frontier <= end;
    let mut substantive_record_seen = false;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if line.len() > MAX_RECORD_BYTES {
            return ScanResult::Unknown(BacklogUnknownReason::RecordBudgetExceeded);
        }
        end += line.len() as u64;
        if end == request.frontier {
            frontier_boundary = true;
        }
        let Ok(line_text) = std::str::from_utf8(line) else {
            return ScanResult::Unknown(BacklogUnknownReason::MalformedRecord);
        };
        if line_text.trim().is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line_text) else {
            return ScanResult::Unknown(BacklogUnknownReason::MalformedRecord);
        };
        let messages = if native {
            // Codex writes turn_context alongside native rollout events. It
            // is context metadata, not a StreamMessage. A later context epoch
            // cannot be replayed with the current-turn decoder's old state.
            if record.get("type").and_then(Value::as_str) == Some("turn_context") {
                if substantive_record_seen || first_done {
                    return ScanResult::Unknown(BacklogUnknownReason::AmbiguousTurnBoundary);
                }
                continue;
            }
            if !native_schema_supported(&record) {
                return ScanResult::Unknown(BacklogUnknownReason::UnsupportedRecord);
            }
            if is_native_turn_boundary(&record) && (substantive_record_seen || first_done) {
                return ScanResult::Unknown(BacklogUnknownReason::AmbiguousTurnBoundary);
            }
            let Some(messages) = decoder.decode(&record) else {
                return ScanResult::Unknown(BacklogUnknownReason::UnsupportedRecord);
            };
            messages
        } else {
            if record.get("type").and_then(Value::as_str) == Some("user")
                && (substantive_record_seen || first_done)
                && crate::services::discord::tmux::tmux_output_stream::watcher_user_event_is_prompt_boundary(&record)
            {
                return ScanResult::Unknown(BacklogUnknownReason::AmbiguousTurnBoundary);
            }
            match normalized_messages(&record, &mut normalized) {
                Ok(messages) => messages,
                Err(reason) => return ScanResult::Unknown(reason),
            }
        };
        let mut hash = DefaultHasher::new();
        line.hash(&mut hash);
        let record_hash = hash.finish();
        for message in messages {
            match message {
                StreamMessage::Text { content } if !content.trim().is_empty() => {
                    substantive_record_seen = true;
                    if native {
                        body = decoder.response().to_owned();
                    } else {
                        // Match the watcher's discrete assistant-block
                        // composition; intra-block deltas are not admitted by
                        // this envelope parser.
                        if !body.is_empty() && semantic_chunk_separator_needed(&body, &content) {
                            body.push_str("\n\n");
                        }
                        body.push_str(&content);
                    }
                    let has_relayable_text =
                        native || !strip_leading_tui_response_chrome(&body).trim().is_empty();
                    if has_relayable_text && end > request.frontier {
                        text_spans.push((
                            PendingSpan {
                                end,
                                kind: PendingOutputKind::AssistantText,
                                record_hash,
                            },
                            body.len(),
                        ));
                    }
                }
                StreamMessage::Done { result, .. } if !first_done => {
                    // The native decoder may repeat Done on later telemetry.
                    // Only the first genuine terminal source record qualifies.
                    // Normalized stop_hook_summary emits an empty, soft Done
                    // before late text/hard result. It must not consume the
                    // completion latch for that eventual answer.
                    let hard_result =
                        !native && record.get("type").and_then(Value::as_str) == Some("result");
                    let has_terminal_body = if native {
                        !result.trim().is_empty()
                    } else {
                        !strip_leading_tui_response_chrome(&result).trim().is_empty()
                            || (hard_result
                                && !strip_leading_tui_response_chrome(&body).trim().is_empty())
                    };
                    if native || hard_result || has_terminal_body {
                        first_done = true;
                    }
                    if has_terminal_body && end > request.frontier {
                        terminal = Some(PendingSpan {
                            end,
                            kind: PendingOutputKind::TerminalAnswer,
                            record_hash,
                        });
                    }
                }
                StreamMessage::ToolUse { .. }
                | StreamMessage::ToolResult { .. }
                | StreamMessage::Thinking { .. } => {
                    substantive_record_seen = true;
                }
                StreamMessage::Error { .. } => {
                    return ScanResult::Unknown(BacklogUnknownReason::UnsupportedRecord);
                }
                _ => {}
            }
        }
    }
    if !frontier_boundary {
        return ScanResult::Unknown(BacklogUnknownReason::PartialRecord);
    }
    // Explicit, nonempty completion remains an obligation even when its text
    // was previewed. Conversely, buffer persistence alone proves no send.
    if text_spans.is_empty() {
        return terminal
            .map(ScanResult::Pending)
            .unwrap_or(ScanResult::Clear(BacklogClearReason::NoEligibleOutput));
    }
    if !(body.starts_with(&request.full_response) || request.full_response.starts_with(&body)) {
        return terminal
            .map(ScanResult::Pending)
            .unwrap_or(ScanResult::Unknown(
                BacklogUnknownReason::DeliveryBufferMismatch,
            ));
    }
    let mut preview_unconfirmed = false;
    for (span, body_end) in text_spans {
        if body_end > request.full_response.len() {
            return ScanResult::Pending(
                terminal
                    .filter(|terminal| terminal.end < span.end)
                    .unwrap_or(span),
            );
        }
        if body_end > request.response_sent_offset {
            preview_unconfirmed = true;
        }
    }
    if let Some(span) = terminal {
        ScanResult::Pending(span)
    } else if preview_unconfirmed {
        ScanResult::Unknown(BacklogUnknownReason::PreviewDeliveryUnconfirmed)
    } else {
        ScanResult::Clear(BacklogClearReason::ConfirmedOutput)
    }
}
