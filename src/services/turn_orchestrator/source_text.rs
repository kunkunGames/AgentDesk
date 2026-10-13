//! Preserve per-message text segments while queued interventions are merged.
use super::{Intervention, SourceMessageTextSegment};

pub(super) fn join_source_text_segments(segments: &[SourceMessageTextSegment]) -> String {
    let mut text = String::new();
    for segment in segments {
        if segment.text.is_empty() {
            continue;
        }
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&segment.text);
    }
    text
}

pub(super) fn ensure_source_text_segments(intervention: &mut Intervention) {
    intervention.source_text_segments = intervention.source_text_segments();
}

pub(super) fn push_unique_source_text_segments(
    existing: &mut Vec<SourceMessageTextSegment>,
    incoming: impl IntoIterator<Item = SourceMessageTextSegment>,
) {
    for incoming in incoming {
        if !existing
            .iter()
            .any(|segment| segment.message_id == incoming.message_id)
        {
            existing.push(incoming);
        }
    }
}
