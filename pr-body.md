What changed:
Translated all Korean CLI diagnostic strings in `src/cli/doctor/orchestrator.rs` into English.

Why:
CLI diagnostics and doctor reports should use English strings for consistency, avoiding hardcoded localized strings per memory guidelines.

WorkFingerprint:
- Agent: Doctor
- Category Boundary: `src/cli/doctor/**`
- Primary Files: `src/cli/doctor/orchestrator.rs`
- Invariant Protected: Doctor output semantics and diagnostic strings.
- Public API Impact: None, purely internal CLI output message formatting.
- Docs Impact: None.
- Verification Plan: `cargo check --all-targets` and `./scripts/verify-dashboard.sh` and `npm run test:policies`.
- Related PRs/Issues: None.

Duplicate/Overlap Check:
Checked open PRs using `git branch -r` and found no existing PR that addresses this diagnostic localization fix.

Verification Commands and Results:
- `cargo check --all-targets` completed successfully without any compilation errors in `src/cli/doctor/orchestrator.rs`.
- `npm run test:policies` passed 206/206 tests.
- `./scripts/verify-dashboard.sh` successfully built and executed all 304 UI tests.
- `python3 scripts/generate_inventory_docs.py` ran with no drift observed.

Skipped Checks:
- cargo test target for doctor was not executed as it was not present.

Risk and Rollback:
Low risk; modifies read-only stdout string outputs. No logical changes. Rollback involves reverting the single commit on `src/cli/doctor/orchestrator.rs`.
