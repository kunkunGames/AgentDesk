use super::*;

fn frame(state: &mut TurnNormalizationState, value: Value) -> Vec<Value> {
    normalize_qwen_line(&value.to_string(), state)
}

#[test]
fn headless_init_and_legacy_session_start_share_session_identity() {
    for subtype in ["init", "session_start"] {
        let mut state = TurnNormalizationState::default();
        let raw = json!({"type":"system","subtype":subtype,"session_id":"owned-session","model":"fixture-model"});
        let normalized = frame(&mut state, raw.clone());
        assert_eq!(normalized[0]["subtype"], "init");
        assert_eq!(normalized[0]["session_id"], "owned-session");
        assert_eq!(state.current_model.as_deref(), Some("fixture-model"));
        assert!(frame(&mut state, raw).is_empty());
    }
}

#[test]
fn goal_metadata_does_not_hide_full_assistant_tools_or_results() {
    let mut state = TurnNormalizationState::default();
    assert!(
        frame(
            &mut state,
            json!({"type":"stream_event","event":{"type":"goal_state","goal_state":{}}})
        )
        .is_empty()
    );
    let use_frame = frame(
        &mut state,
        json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":"call-1","name":"write_file","input":{"file_path":"output.json","content":"{}"}}]}}),
    );
    assert_eq!(use_frame[0]["message"]["content"][0]["id"], "call-1");
    let result = frame(
        &mut state,
        json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"call-1","content":"written","is_error":false}]}}),
    );
    assert_eq!(result[0]["message"]["content"][0]["tool_use_id"], "call-1");
}

#[test]
fn partial_tools_keep_call_identity_and_do_not_duplicate_full_message() {
    let mut state = TurnNormalizationState::default();
    for event in [
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"stream-call","name":"read_file","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"input.json\"}"}}),
    ] {
        assert!(frame(&mut state, json!({"type":"stream_event","event":event})).is_empty());
    }
    let result = frame(
        &mut state,
        json!({"type":"stream_event","event":{"type":"content_block_stop","index":0}}),
    );
    let block = &result[0]["message"]["content"][0];
    assert_eq!(block["id"], "stream-call");
    assert_eq!(block["input"], json!({"path":"input.json"}));
    assert!(frame(&mut state,json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":"stream-call","name":"read_file","input":{"path":"input.json"}}]}})).is_empty());
}
