//! Hash-only graph observations from the existing normalized watcher stream.
use super::*;
use crate::services::discord::inflight::{InflightTurnIdentity, InflightTurnState};

#[derive(Clone, Default)]
pub(super) struct GraphEvents(Vec<serde_json::Value>);

impl GraphEvents {
    pub(super) fn record(&mut self, value: &serde_json::Value) {
        if let Some(observation) = crate::db::campaigns::graph::metrics::control_observation(value)
        {
            if self.0.len() < 256 {
                self.0
                    .push(serde_json::json!({"execution_observation":observation}));
            }
        }
        let Some(blocks) = value.pointer("/message/content").and_then(|v| v.as_array()) else {
            return;
        };
        for block in blocks {
            let (id, fingerprint, output) = match (value["type"].as_str(), block["type"].as_str()) {
                (Some("assistant"), Some("tool_use")) => {
                    let Some(name) = block["name"].as_str() else {
                        continue;
                    };
                    let Some(input) = block.get("input") else {
                        continue;
                    };
                    let hash = crate::db::campaigns::graph::contract_digest(&serde_json::json!([
                        name, input
                    ]))
                    .ok();
                    (block["id"].as_str(), hash, None)
                }
                (Some("user"), Some("tool_result")) => {
                    let Some(content) = block.get("content") else {
                        continue;
                    };
                    let hash = crate::db::campaigns::graph::contract_digest(content).ok();
                    (block["tool_use_id"].as_str(), None, hash)
                }
                _ => continue,
            };
            let Some(id) = id.filter(|id| !id.is_empty() && id.len() <= 256) else {
                continue;
            };
            if self.0.len() >= 256 {
                tracing::warn!("graph watcher observation batch full; coverage is incomplete");
                break;
            }
            self.0.push(
                serde_json::json!({"event_id":id,"fingerprint":fingerprint,"output_digest":output}),
            );
        }
    }
}

fn owned_dispatch<'a>(
    expected: Option<&InflightTurnIdentity>,
    pinned: Option<&'a InflightTurnState>,
    current: Option<&InflightTurnState>,
) -> Option<&'a str> {
    let (expected, pinned, current) = (expected?, pinned?, current?);
    if expected.is_unnameable()
        || !expected.matches_state(pinned)
        || !expected.matches_state(current)
        || pinned.dispatch_id != current.dispatch_id
    {
        return None;
    }
    pinned.dispatch_id.as_deref()
}

fn execution_observation(events: &[serde_json::Value]) -> Option<serde_json::Value> {
    let mut model = serde_json::Value::Null;
    let mut usage = serde_json::Value::Null;
    for observed in events.iter().filter_map(|e| e.get("execution_observation")) {
        if observed["model"].is_string() {
            model = observed["model"].clone();
        }
        if observed["usage"].is_object() {
            usage = observed["usage"].clone();
        }
    }
    (!model.is_null() || !usage.is_null()).then(|| serde_json::json!({"model":model,"usage":usage}))
}

impl WatcherToolState {
    pub(in crate::services::discord) async fn flush_graph_progress(
        &mut self,
        pool: Option<&sqlx::PgPool>,
        provider: &ProviderKind,
        channel: u64,
        expected: Option<&InflightTurnIdentity>,
        pinned: Option<&InflightTurnState>,
    ) {
        let events = std::mem::take(&mut self.graph_events.0);
        if events.is_empty() || pool.is_none() {
            return;
        }
        let current = crate::services::discord::inflight::load_inflight_state(provider, channel);
        let Some(dispatch) = owned_dispatch(expected, pinned, current.as_ref()) else {
            return;
        };
        crate::db::campaigns::graph::progress::watcher_batch(pool.unwrap(), dispatch, &events)
            .await;
        if let Some(observed) = execution_observation(&events) {
            let write=sqlx::query("INSERT INTO campaign_dispatch_observations(dispatch_id,provider,model,usage) SELECT $1,$2,$3,$4 WHERE EXISTS(SELECT 1 FROM campaign_graph_tasks t JOIN task_dispatches d ON d.kanban_card_id=t.card_id WHERE d.id=$1) ON CONFLICT(dispatch_id) DO UPDATE SET model=COALESCE(EXCLUDED.model,campaign_dispatch_observations.model),usage=COALESCE(EXCLUDED.usage,campaign_dispatch_observations.usage),observed_at=NOW()")
                .bind(dispatch).bind(provider.as_str()).bind(observed["model"].as_str())
                .bind(observed.get("usage").filter(|v|!v.is_null()).cloned()).execute(pool.unwrap());
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_millis(500), write).await,
                Ok(Ok(_))
            ) {
                tracing::warn!(%dispatch,"Campaign execution observation unavailable; model and usage coverage incomplete");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_header_survives_a_usage_only_result_in_the_same_batch() {
        let mut events = GraphEvents::default();
        events.record(
            &serde_json::json!({"type":"system","subtype":"init","model":"verified-model"}),
        );
        events.record(
            &serde_json::json!({"type":"result","usage":{"input_tokens":12,"output_tokens":8}}),
        );
        let observed = execution_observation(&events.0).unwrap();
        assert_eq!(observed["model"], "verified-model");
        assert_eq!(observed["usage"]["output_tokens"], 8);
    }

    #[test]
    fn graph_watcher_requires_the_same_pinned_turn_and_dispatch() {
        let mut pinned = InflightTurnState::new(
            ProviderKind::Codex,
            7,
            None,
            1,
            2,
            3,
            String::new(),
            None,
            Some("graph-pane".into()),
            None,
            None,
            10,
        );
        pinned.dispatch_id = Some("owned".into());
        let identity = InflightTurnIdentity::from_state(&pinned);
        assert_eq!(
            owned_dispatch(Some(&identity), Some(&pinned), Some(&pinned)),
            Some("owned")
        );
        let mut replaced = pinned.clone();
        replaced.dispatch_id = Some("new-dispatch".into());
        assert!(owned_dispatch(Some(&identity), Some(&pinned), Some(&replaced)).is_none());
        replaced = pinned.clone();
        replaced.turn_start_offset = Some(11);
        assert!(owned_dispatch(Some(&identity), Some(&pinned), Some(&replaced)).is_none());
        assert!(owned_dispatch(Some(&identity), Some(&pinned), None).is_none());
        assert!(owned_dispatch(None, Some(&pinned), Some(&pinned)).is_none());
    }

    #[test]
    fn native_codex_tool_messages_reach_graph_observation_buffer() {
        use crate::services::agent_protocol::StreamMessage;
        let mut tools = WatcherToolState::new();
        let mut state = StreamLineState::new();
        let mut response = String::new();
        process_native_codex_messages(
            vec![
                StreamMessage::ToolUse {
                    name: "exec".into(),
                    input: "{\"cmd\":\"check\"}".into(),
                    tool_use_id: Some("native-call".into()),
                },
                StreamMessage::ToolResult {
                    content: "ok".into(),
                    is_error: false,
                    tool_use_id: Some("native-call".into()),
                },
            ],
            &mut state,
            &mut response,
            &mut tools,
        );
        assert_eq!(tools.graph_events.0.len(), 2);
        assert!(
            tools
                .graph_events
                .0
                .iter()
                .all(|v| v["event_id"] == "native-call")
        );
        assert!(tools.graph_events.0[1]["output_digest"].is_string());
    }

    #[test]
    fn graph_watcher_hashes_tool_events_and_ignores_prose_and_missing_ids() {
        let mut events = GraphEvents::default();
        events.record(
            &serde_json::json!({"type":"assistant","message":{"content":[
                {"type":"tool_use","id":"call-1","name":"exec","input":{"cmd":"private input"}},
                {"type":"text","text":"not a tool"},
                {"type":"tool_use","name":"exec","input":{}}
            ]}}),
        );
        events.record(&serde_json::json!({"type":"user","message":{"content":[
            {"type":"tool_result","tool_use_id":"call-1","content":"private output"}
        ]}}));
        assert_eq!(events.0.len(), 2);
        assert_eq!(events.0[0]["event_id"], "call-1");
        assert_eq!(events.0[1]["event_id"], "call-1");
        assert_eq!(events.0[0]["fingerprint"].as_str().unwrap().len(), 64);
        assert_eq!(events.0[1]["output_digest"].as_str().unwrap().len(), 64);
        assert!(
            !serde_json::to_string(&events.0)
                .unwrap()
                .contains("private")
        );
    }
}
