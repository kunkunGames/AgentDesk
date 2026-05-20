What changed: Created an empty no-change report commit to cleanly skip candidate selection.

Why: An overlapping PR already exists for moving hooks routes to the integrations domain (e.g. `origin/jules/api-routemaster/move-hooks-routes-to-integrations-11691982998671490737` and others).

WorkFingerprint:
- agent: ApiRoutemaster
- category: src/server/routes/**
- target: hooks routes
- invariant protected: no-change overlap report
- verification: empty commit

Duplicate/overlap check: Verified overlapping branches exist by running `git branch -r | grep api-routemaster`.

Verification commands and results:
- `cargo check --all-targets` (failed due to internal command runner errors, fell back to `cargo check` which succeeded)
- `git diff --check` (clean)

Skipped checks with reasons:
- Python script verification skipped because no files were modified.
- `cargo test` skipped as there are no code changes to test.

Risk: None.
Rollback notes: Delete the branch.
