What changed: Updated the `check_stale_zero_byte_db_files` check in `src/cli/doctor/orchestrator.rs` to include the `--repair-sqlite-cache` flag in the recommended fix command for the non-Postgres (SQLite) codepath. Also added standard consistency fields (`subsystem`, `fix_safety`, `security_exposure`) that the Postgres-path equivalent check already includes.

Why: Without this change, operators in environments without Postgres enabled are told to run `agentdesk doctor --fix` which skips the stale SQLite cache cleanup unless `--repair-sqlite-cache` is explicitly provided, leading to confusion and unrepairable zero-byte db files until they discover the flag in the manual or other branches. It also ensures the diagnostic correctly classifies its `subsystem` and safety constraints.

WorkFingerprint:
- Agent: Doctor
- Category: src/cli/doctor/**
- Primary files: src/cli/doctor/orchestrator.rs
- Invariant protected: Doctor behavior should inspect and report actionable information.
- Public API impact: JSON struct fields expanded to include missing properties for `stale_db_files` warning.
- Docs impact: None
- Related PRs/issues: Avoided overlap with `jules/doctor/remove-sqlite-from-db-unavailable-next-step-14778426102243551507` (which touches `health.rs` instead) and `jules/doctor/stale-sqlite-repair-message-8939958958679141096` (which appears to touch something similar, but `check_stale_zero_byte_db_files` was still missing the flag in `main` as of right now).

Duplicate/overlap check: Verified against active branches using `git branch -r | grep jules/doctor`. While `jules/doctor/stale-sqlite-repair-message-8939958958679141096` exists, `check_stale_zero_byte_db_files` was clearly still lacking the `--repair-sqlite-cache` flag in the current `main` base, proving the fix is not applied. No active `gh` tool to check exact PR state, so assumed safe based on `main`.

Verification commands/results:
- `git diff --check` passed.
- `cargo check --all-targets` and `cargo test` failed/timed out due to environment issues, not related to this simple text change.

Skipped checks with reasons:
- `cargo check --all-targets`: Internal rustc/toolchain error in environment.
- `cargo test`: Timed out after 400s in environment.

Risk and rollback notes: Extremely low risk since this only changes string output formatting and adds standard `Check` fields that are already supported and used in the companion codepath. Rollback simply reverts the text changes.
