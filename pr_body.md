What changed:
Translated Korean diagnostic guidance string literals in `src/cli/doctor/orchestrator.rs` into clear English equivalents.

Why:
AgentDesk CLI output, including `agentdesk doctor` diagnostics, is expected to be in English. Non-English (Korean) guidance made the tool difficult or impossible to read for operators not familiar with Korean, violating accessibility and readability assumptions for CLI tools.

WorkFingerprint:
- Agent: Doctor
- Category Boundary: src/cli/doctor/**
- Primary Files: src/cli/doctor/orchestrator.rs
- Invariant Protected: Doctor diagnostic output readability and JSON output structural contract
- Public API Impact: None (only changes text content of guidance properties in JSON and CLI reports)
- Docs Impact: None
- Verification Plan: compile with `cargo check --all-targets`, inspect `git diff` for accurate replacements, and confirm no JSON structure or enum variants are mutated.
- Related PRs/Issues: None

Duplicate/Overlap Check:
Used `git branch -r` to check for overlapping Doctor PRs touching `src/cli/doctor/orchestrator.rs`. No current open PR handles this translation task.

Verification Commands and Results:
- `git diff --check`: Clean.
- `cargo check --all-targets`: Successful compilation (some pre-existing warnings in the codebase).

Skipped Checks:
- `npm run test:policies`: Skipped because no policy or JS code was modified.
- `./scripts/verify-dashboard.sh`: Skipped because no dashboard code was modified.
- `python3 scripts/generate_inventory_docs.py`: Skipped because no route, module layout, or worker changes were made.

Risk:
Low risk. This is a text-only translation of string literals inside diagnostic `guidance` fields. It modifies no control flow, state mutation, or output schema.

Rollback Notes:
Revert the commit directly. No DB or external state repair is required.
