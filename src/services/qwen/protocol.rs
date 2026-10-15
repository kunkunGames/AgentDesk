//! Shared framing rules for direct and tmux Qwen CLI streams.
use serde_json::Value;

pub(crate) fn is_session_start(frame: &Value) -> bool {
    frame.get("type").and_then(Value::as_str) == Some("system")
        && matches!(
            frame.get("subtype").and_then(Value::as_str),
            Some("init" | "session_start")
        )
}

pub(crate) fn is_partial_content(frame: &Value) -> bool {
    // Metadata such as goal_state also arrives inside stream_event. It does
    // not replace complete assistant messages when partial output is disabled.
    frame.get("type").and_then(Value::as_str) == Some("stream_event")
        && matches!(
            frame.pointer("/event/type").and_then(Value::as_str),
            Some("content_block_start" | "content_block_delta" | "content_block_stop")
        )
}

#[cfg(test)]
mod tests {
    use super::super::{QwenAttemptState, StreamMessage, process_qwen_json_event};
    use serde_json::json;

    #[test]
    fn direct_stream_accepts_both_session_frames() {
        for subtype in ["init", "session_start"] {
            let mut state = QwenAttemptState::default();
            process_qwen_json_event(
                &json!({"type":"system","subtype":subtype,"session_id":"owned-session","model":"fixture-model"}),
                &mut state,
            );
            assert_eq!(state.last_session_id.as_deref(), Some("owned-session"));
            assert_eq!(state.current_model.as_deref(), Some("fixture-model"));
        }
    }

    #[test]
    fn metadata_preserves_full_tool_message_and_identity() {
        let mut state = QwenAttemptState::default();
        process_qwen_json_event(
            &json!({"type":"stream_event","event":{"type":"goal_state"}}),
            &mut state,
        );
        process_qwen_json_event(
            &json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"read_file","input":{"path":"input.json"}}]}}),
            &mut state,
        );
        assert!(!state.partial_stream_seen);
        assert!(state.buffered_messages.iter().any(|message| matches!(
            message,
            StreamMessage::ToolUse { tool_use_id: Some(id), .. } if id == "call-1"
        )));
    }
}
