What changed:
Updated the domain category mapping for `src/server/routes/domains/onboarding.rs` from `admin` to `onboarding`.

Why:
The `onboarding.rs` file represents the onboarding domain boundary, but its file comment incorrectly listed `// Category: admin`. Updating this clarifies ownership and domain categorization, fulfilling the DomainKeeper mission to align route and configuration ownership correctly without refactoring behavior.

WorkFingerprint:
Agent: DomainKeeper
Category Boundary: src/server/routes/domains/**
Primary Files: src/server/routes/domains/onboarding.rs
Invariant Protected: Domain ownership comments should reflect the actual module purpose and routing structure.
Public API Impact: None.
Docs Impact: None (the generated route inventory scripts were run and confirm the docs output does not rely on this particular comment field).
Related PRs/Issues: None.

Duplicate/Overlap Check:
Checked `gh pr list --state open` and `git ls-remote --heads origin`. Verified there are no overlapping open PRs related to `onboarding.rs` domain ownership updates.

Verification commands and results:
- `git diff --check`: Clean output.
- `cargo check`: Executed, built and finished correctly with 0 errors.
- `python3 scripts/generate_inventory_docs.py`: Ran cleanly without changes, confirming generated inventories were not decoupled.

Skipped checks with reasons:
- `npm run test:policies`: Skipped because no Javascript policies were modified.
- `./scripts/verify-dashboard.sh`: Skipped because no dashboard/frontend files were modified.

Risk:
Very low. This is a comment-only change to align category metadata with file name and context.

Rollback notes:
Revert the single commit to restore the comment. No side effects.
