What changed:
Added the `observability_target = spec.target` field to all 10 worker lifecycle `tracing::info!`, `tracing::warn!`, and `tracing::error!` logging macros in `src/server/worker_registry.rs`. Regenerated `docs/generated/worker-inventory.md`.

Why:
PR #193 initially added target observability to worker supervision start/skip logs, but the open branch became contaminated with unrelated stale files. We recreated this change cleanly from `main` to extend observability targets to all lifecycle log sites consistently (start, skip, stop, and error paths), preserving worker idempotency while improving metrics/log consistency.

WorkFingerprint:
- Agent: WorkerRegistry
- Boundary: `src/server/worker_registry.rs`, `docs/generated/worker-inventory.md`
- Primary files: `src/server/worker_registry.rs`, `docs/generated/worker-inventory.md`
- Invariant protected: Idempotent worker lifecycle and restart safety.
- Public API impact: None.
- Docs impact: `docs/generated/worker-inventory.md` lines shifted to reflect source file changes.
- Verification plan: Check code format, compile, verify no unintended logic shifts.
- Related PRs/issues: Replaces the core goal of #193.

Duplicate/Overlap Check:
Checked open branches on origin. Branch `remotes/origin/jules/worker-registry/observability-target-16140987696892580792` had contaminated files (like `policies/auto-queue.js`). This PR recreates the changes cleanly on a new branch from `main` to avoid overlap.

Verification Commands & Results:
- `git diff --check` (Passed cleanly)
- `cargo check --all-targets` (Compiled successfully, existing non-blocking warnings remain unchanged)
- `python3 scripts/generate_inventory_docs.py` (Verified and updated `docs/generated/worker-inventory.md` line number drifts)
- Code Review Tool (Received "#Correct#")

Skipped Checks:
`npm run test:policies` and `./scripts/verify-dashboard.sh` were skipped as they are not affected by Rust backend logging macros. `cargo test --package agentdesk --lib -- server::worker_registry` timed out in sandbox limits, but `cargo check --all-targets` verified the typing/macro syntax, and visual code review confirmed the safety.

Risk:
Low. Only adds a field to structured `tracing` macros. No control flow changes.

Rollback Notes:
`git revert` the commit if log ingest systems reject the new field.
