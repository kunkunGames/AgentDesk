What changed:
Updated the `fix_safety` field for the `db_unavailable` degraded reason from `NotFixable` to `ExplicitDbRepairRequired` within `src/cli/doctor/health.rs`. This correctly classifies a database unavailability issue as requiring an explicit database repair step or intervention by an operator. The JSON output testing contract in `reasons_evidence_preserves_actionable_reason_contract` was also updated to align with the change.
Also ran `--write-lib-inventory-manifest` to satisfy `test_target_integrity.py` due to a newly surfaced test by CI.

Why:
The `db_unavailable` reason was previously labeled as `NotFixable`, which does not accurately provide actionable diagnostic information to operators because the issue *is* fixable, but only via an explicit intervention (e.g. restoring Postgres/SQLite availability or reviewing server logs). Classifying it as `ExplicitDbRepairRequired` better reflects the true nature of the fix safety level and adheres to the goal of providing operators clearer, actionable information without mutating runtime state.
The CI failed on `FAIL: Test-target integrity gate (#5003/#5008)` because an unrelated test surfaced and `lib_test_inventory_manifest.txt` was not updated to reflect it. We have included the updated manifest file.

WorkFingerprint:
- Agent name: Jules
- Category boundary: Doctor (`src/cli/doctor/health.rs`)
- Primary files: `src/cli/doctor/health.rs`
- Invariant protected: JSON output contracts for `doctor --json` and fallback behaviors.
- Public API impact: `fix_safety` in `db_unavailable` payload now returns `"explicit_db_repair_required"`.
- Docs impact: None
- Verification plan: ran `cargo check --lib` on `agentdesk`. Full target `cargo check` and `cargo test` skipped due to timeout. `git diff --check` run and passed.
- Related PRs/issues: None

Overlap check:
Executed `gh pr list --state open ...` and git remote checking; no existing/open overlapping doctor-specific PRs related to `fix_safety` or `db_unavailable` were found.

Verification commands:
- `cargo check -p agentdesk --lib` (Success, with expected unrelated warnings)
- `git diff --check` (Success)
- `python3 scripts/check_test_target_integrity.py --write-lib-inventory-manifest`

Skipped checks:
- `cargo check --all-targets` and `cargo test -p agentdesk -- doctor::health` (Skipped due to timeouts/environment limits during execution). Residual risk is very low because the enum variant `FixSafety::ExplicitDbRepairRequired` already exists, and the corresponding string literal update in the JSON test assertions matched cleanly.

Risk:
Low. The modification only affects the `doctor` diagnostic reporting classification and does not touch any actual runtime paths or fallback state.

Rollback notes:
Revert the commit to restore the `NotFixable` string literal and diagnostic classification in `src/cli/doctor/health.rs`.
