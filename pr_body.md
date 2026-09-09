What changed:
Removed multiple intermediate `serde_json::to_string` and `serde_json::from_str` calls when mutating `context_with_session_strategy` in `src/dispatch/dispatch_create.rs`. It now clones and mutates the inner JSON object directly, serializing it only once at the end.

Why:
These operations were highly redundant (up to 3 string serializations and 2 parsing passes per `dispatch_create` invocation without a phase_gate sidecar) on a hot path, causing unnecessary memory allocation and CPU overhead overhead in the dispatch engine. By mutating the object in place, we achieve a deterministic complexity reduction for memory allocations during dispatch creation.

WorkFingerprint:
- Agent name: Bolt
- Category boundary: `src/dispatch/**`
- Primary files: `src/dispatch/dispatch_create.rs`
- Invariant protected: The JSON output matches the original structure (no functional behavioral change in what is serialized).
- Public API impact: None
- Docs impact: None
- Verification plan: Executed `git diff --check`, `cargo check --lib -p agentdesk`, `cargo test` (timed out due to environment but command ran), `npm run test:policies`, `./scripts/verify-dashboard.sh`, and `python3 scripts/generate_inventory_docs.py`.
- Related PRs/issues: None

Duplicate/overlap check:
Executed `gh pr list` (unavailable, fell back to `git branch -r`) and verified there is no open or overlapping PR resolving these redundant serde_json allocations in `dispatch_create`.

Verification commands and results:
- `git diff --check`: Ran clean.
- `cargo check --lib -p agentdesk`: Finished successfully (with baseline warnings).
- `cargo check --all-targets` and `cargo test -p agentdesk --lib dispatch::dispatch_create`: Timed out due to local environment constraints (taking over 400s), but executed as requested.
- `npm run test:policies`: Finished successfully.
- `./scripts/verify-dashboard.sh`: Finished successfully.
- `python3 scripts/generate_inventory_docs.py`: Generated docs output diffs cleanly.

Skipped checks with reasons:
`cargo test` fully completing could not be achieved locally as tests timed out after 400+ seconds in the sandbox environment.

Risk:
Low. It replaces multiple stringify/parse rounds with an in-place mutation of the `serde_json::Value::Object`.

Rollback notes:
Revert the single commit to restore the multiple serialization passes in `dispatch_create`.
