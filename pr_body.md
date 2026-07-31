What changed: Extended `worker-local` Tokio supervisor tracing events in `src/server/worker_recovery.rs` to include the missing `target` field. Fixed one logging event missing the `target` field in `src/server/worker_registry.rs`.

Why: PR #193 introduced target observability to some supervisor startup and skip logs, but did not extend them consistently to recovery boundaries. This closes the gap by standardizing `target = spec.target` across all worker supervision lifecycles.

WorkFingerprint:
- Agent: WorkerRegistry
- Category boundary: src/server/worker_registry.rs, src/server/worker_recovery.rs
- Primary files: src/server/worker_recovery.rs, src/server/worker_registry.rs
- Invariant protected: Worker supervision logging schema consistency
- Public API impact: None
- Docs impact: None
- Verification plan: cargo check --all-targets, cargo test (internal error skipped)
- Related PRs: Follow up to PR #193

Overlap check: Found previous contaminated branches for PR #193. Proceeding with clean branch. Checked using git fetch && git branch -a.

Verification commands and results:
- `cargo check --all-targets` (passed, zero errors)
- `python3 scripts/generate_inventory_docs.py` (passed)

Skipped checks with reasons:
- `cargo test` (fails with an internal error locally on my system)

Risk: Extremely low. This is a purely cosmetic log metadata addition.
Rollback notes: Revert the commit.
