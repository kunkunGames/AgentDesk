//! A streamed answer may edit its anchor before the one terminal answer edit.

pub(super) fn assert_terminal_answer(
    edits: &[(&str, &str, &str)],
    anchor_path: &str,
    answer: &str,
) {
    assert!(
        !edits.is_empty()
            && edits
                .iter()
                .all(|(method, path, _)| *method == "PATCH" && path.ends_with(anchor_path)),
        "every answer edit must target its original anchor: {edits:?}"
    );
    assert_eq!(
        edits.last().unwrap().2,
        answer,
        "the last edit must contain the exact terminal answer"
    );
    assert_eq!(
        edits
            .iter()
            .filter(|(_, _, content)| *content == answer)
            .count(),
        1,
        "the exact terminal answer must be delivered once; progress edits are separate: {edits:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::assert_terminal_answer;

    const ANCHOR: &str = "/channels/6903901/messages/5704900001";
    const ANSWER: &str = "LIVE_DISCOVERY_6903_ANSWER";
    const PROGRESS: &str = "LIVE_DISCOVERY_6903_ANSWER\n\n⠋ LIVE_DISCOVERY_6903_ANSWER";

    #[test]
    fn one_terminal_edit_is_one_answer() {
        assert_terminal_answer(&[("PATCH", ANCHOR, ANSWER)], ANCHOR, ANSWER);
    }

    #[test]
    fn observed_progress_then_terminal_is_one_answer() {
        assert_terminal_answer(
            &[("PATCH", ANCHOR, PROGRESS), ("PATCH", ANCHOR, ANSWER)],
            ANCHOR,
            ANSWER,
        );
    }

    #[test]
    fn several_progress_edits_keep_one_terminal_answer() {
        assert_terminal_answer(
            &[
                ("PATCH", ANCHOR, "LIVE_DISCOVERY_6903_ANSWER\n\nworking"),
                ("PATCH", ANCHOR, PROGRESS),
                ("PATCH", ANCHOR, ANSWER),
            ],
            ANCHOR,
            ANSWER,
        );
    }

    fn rejected(edits: &[(&str, &str, &str)]) {
        assert!(
            std::panic::catch_unwind(|| assert_terminal_answer(edits, ANCHOR, ANSWER)).is_err()
        );
    }

    #[test]
    fn a_repeated_terminal_edit_is_rejected() {
        rejected(&[("PATCH", ANCHOR, ANSWER), ("PATCH", ANCHOR, ANSWER)]);
    }

    #[test]
    fn a_new_message_instead_of_an_edit_is_rejected() {
        rejected(&[("POST", ANCHOR, ANSWER)]);
    }

    #[test]
    fn a_second_anchor_is_rejected() {
        rejected(&[
            ("PATCH", ANCHOR, PROGRESS),
            ("PATCH", "/channels/6903901/messages/5704900002", ANSWER),
        ]);
    }

    #[test]
    fn progress_without_a_terminal_answer_is_rejected() {
        rejected(&[("PATCH", ANCHOR, PROGRESS)]);
    }

    #[test]
    fn progress_after_the_terminal_answer_is_rejected() {
        rejected(&[("PATCH", ANCHOR, ANSWER), ("PATCH", ANCHOR, PROGRESS)]);
    }

    #[test]
    fn an_empty_delivery_is_rejected() {
        rejected(&[]);
    }
}
