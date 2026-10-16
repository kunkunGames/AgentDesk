//! Transcript event wire representation and persisted turn outcome markers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTranscriptEventKind {
    User,
    Assistant,
    Thinking,
    ToolUse,
    ToolResult,
    Result,
    Error,
    Task,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTranscriptEvent {
    pub kind: SessionTranscriptEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default)]
    pub is_error: bool,
}

impl SessionTranscriptEvent {
    /// Persist the bridge's final decision atomically with its transcript.
    /// The asynchronous analytics queue is not completion authority.
    pub(crate) fn turn_outcome(outcome: &str) -> Self {
        Self {
            kind: SessionTranscriptEventKind::Result,
            tool_name: None,
            summary: Some("agentdesk.turn_outcome.v1".into()),
            content: String::new(),
            status: Some(outcome.into()),
            is_error: false,
        }
    }

    pub(crate) fn recorded_turn_outcome(events: &[Self]) -> Option<&str> {
        events
            .iter()
            .rev()
            .find(|event| {
                event.kind == SessionTranscriptEventKind::Result
                    && event.summary.as_deref() == Some("agentdesk.turn_outcome.v1")
                    && event.tool_name.is_none()
            })
            .and_then(|event| event.status.as_deref())
    }
}
