## What changed
Removed the unused `#[allow(dead_code)]` attribute from `resolve_parent_dispatch_context` in `src/dispatch/dispatch_context.rs`.

## Why
The comment indicated it was wired into `create_dispatch_core_internal` under #850. The function is now actively used, meaning the `dead_code` attribute is no longer applicable. Removing it cleans up code hygiene and reduces technical debt in the dispatch lifecycle context.

## WorkFingerprint
- Agent: Dispatcher
- Category Boundary: `src/dispatch/**`
- Primary Files: `src/dispatch/dispatch_context.rs`
- Invariant Protected: Dispatch lifecycle code hygiene.
- Public API Impact: None
- Docs Impact: None
- Verification Plan: `cargo check`
- Related PRs/Issues: #850

## Duplicate/Overlap Check
Checked using GitHub CLI for open PRs under the `dispatcher` context and verified no overlap exists for this file cleanup.

## Verification Commands and Results
- `cargo check --all-targets` and `cargo test` targets (verified clean compilation on relevant package)

## Skipped Checks
No specific checks skipped where applicable.

## Risk
Low risk, only removing an obsolete attribute. No runtime behavior change.

## Rollback Notes
Revert the commit to add the attribute back if necessary.
