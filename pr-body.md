What changed:
Updated tracing format for error messages in `src/services/dispatches/wait_queue.rs`, `src/services/message_outbox.rs`, and `src/services/discord/discord_io.rs` to improve log structure. Specifically, changed the usage of `{error}` or `error,` to `error = %error,` or `error = %e,` in `tracing::warn!` macros.

Why:
To ensure the error value is properly captured and formatted as a structured field in tracing logs, rather than just being formatted into the log message string. This improves observability and log structure for message delivery and dispatch outbox behavior.

WorkFingerprint:
Agent Name: Courier
Category Boundary: `src/services/message_outbox.rs`, `src/services/dispatches/wait_queue.rs`, `src/services/discord/discord_io.rs`
Primary Files: `src/services/dispatches/wait_queue.rs`, `src/services/message_outbox.rs`, `src/services/discord/discord_io.rs`
Invariant Protected: Tracing logs for queue outboxes correctly format error outputs without losing message contexts.
Public API Impact: None
Docs Impact: None
Verification Plan: Run cargo check to verify rust compiler accepts the changes without error.
Related PRs/Issues: None

Duplicate/Overlap Check:
Checked open PRs across branches matching `jules/courier/`. Only one PR found `jules/courier/fix-tracing-warn-error-format-8858862390488137022` which is the original version of this change, from which these changes were rebuilt cleanly on top of `main`.

Verification Commands and Results:
- `git diff --check`: No whitespace errors.
- `cargo check --all-targets`: Passed successfully.

Skipped Checks:
- `cargo test --all`: Due to execution environment timeouts, isolated test compilation checks passed.
- `./scripts/verify-dashboard.sh`: Skipped since no dashboard files were changed.
- `python3 scripts/generate_inventory_docs.py`: Skipped since no generated docs were affected.

Risk:
Very low. Modifies tracing log formats.

Rollback Notes:
Revert the commit if it causes any logging-related regressions.
