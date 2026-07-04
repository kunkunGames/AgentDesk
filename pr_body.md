## What changed
Translated multiple hardcoded Korean diagnostic strings to English within `src/cli/doctor/orchestrator.rs`.

## Why
This improves the clarity of actionable diagnostic messages for operators reading `agentdesk doctor` output, making the tool more accessible to a broader audience without mutating any runtime state.

## WorkFingerprint
- **Agent Name:** Doctor
- **Category Boundary:** `src/cli/doctor/**`
- **Primary Files:** `src/cli/doctor/orchestrator.rs`
- **Invariant Protected:** Diagnostic output contracts and `DoctorProfile` JSON fields remain unchanged; purely cosmetic string translations.
- **Public API Impact:** None.
- **Docs Impact:** None.
- **Verification Plan:** `git diff --check`, `cargo check --all-targets` (via `./scripts/ci-script-checks.sh` as fallback).
- **Related PRs/Issues:** Addressed potential overlaps.

## Duplicate/Overlap Check
Executed `git branch -a | grep -i doctor`. Identified multiple existing PRs related to translations (e.g., `localize-messages`, `english-diagnostics`), but verified that the specific strings translated in this PR were still untranslated on `main` and not actively being fixed in conflicting PRs.

## Verification commands and results
- `git diff --check`: Clean.
- `./scripts/ci-script-checks.sh`: Passed successfully.

## Skipped checks
- `npm run test:policies`: Skipped, no JS policy files changed.
- `./scripts/verify-dashboard.sh`: Skipped, no frontend changes.
- `python3 scripts/generate_inventory_docs.py`: Skipped, no changes to module structures or routing that require inventory updates.

## Risk
Low. The changes are strictly cosmetic and confined to static strings used in diagnostic warnings and errors.

## Rollback notes
Revert the commit modifying `src/cli/doctor/orchestrator.rs`.
