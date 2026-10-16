use super::*;
use crate::services::cluster::attachment_transfer::uploads::PendingUploads;
use crate::voice::prompt::VoiceTranscriptAnnouncement;

#[test]
fn intake_entry_futures_stay_within_worker_stack_budget() {
    fn handler_size<F>(
        _: impl FnOnce(
            &'static IntakeDeps<'static>,
            bool,
            IntakeRequest,
            (bool, Option<adk_thread::Route>),
            PendingUploads,
            Option<VoiceTranscriptAnnouncement>,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    fn core_size<F>(
        _: impl FnOnce(
            &'static Arc<serenity::http::Http>,
            &'static Arc<SharedData>,
            &'static str,
            IntakeRequest,
            PendingUploads,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    fn campaign_size<F>(
        _: impl FnOnce(Option<&'static sqlx::PgPool>, Option<&'static str>) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }

    // Measure compiled future types without constructing a runtime or polling
    // any work. Actual intake tests still exercise construction and polling on
    // the default thread stack; these limits detect renewed inline growth.
    let entry = handler_size(handle_text_message);
    let admitted = handler_size(handle_text_message_admitted);
    let core = core_size(execute_intake_turn_core);
    let campaign = campaign_size(context::campaign_graph_prompt);
    eprintln!(
        "INTAKE_FUTURE_BYTES entry={entry} admitted={admitted} core={core} campaign={campaign}"
    );
    assert!(entry <= 64 * 1024, "entry embeds {entry} bytes");
    assert!(core <= 64 * 1024, "worker core embeds {core} bytes");
    assert!(
        admitted <= 256 * 1024,
        "admitted turn retains {admitted} bytes before nested polls"
    );
    assert!(
        campaign <= 1024,
        "campaign boundary embeds {campaign} bytes"
    );
}
