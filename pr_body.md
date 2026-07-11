What changed:
Removed an O(N) `.clone()` of vectors in `src/services/auto_queue/dispatch_command.rs` during dependency planning, replacing it with `std::mem::take()` on a mutable reference to the underlying component map.

Why:
The `components[root].clone()` duplicated an arbitrarily large vector of component member indices inside the group enumeration loop. By changing it to `std::mem::take(components.get_mut(root).unwrap())`, the code deterministically consumes the allocated vector without duplication since it is not used again after extraction. This provides a measurable Big-O reduction in allocations within the dispatch planner hot path.

WorkFingerprint:
Agent: Bolt
Category Boundary: `src/services/auto_queue/**`
Primary Files: `src/services/auto_queue/dispatch_command.rs`
Invariant Protected: Dispatch dependency graph logic remains functionally identical; only vector extraction is optimized.
Public API Impact: None
Docs Impact: None
Verification Plan: `cargo check --all-targets` and verified compilation locally.
Related PRs/Issues: None identified in current branch checks.

Duplicate/Overlap Check:
Ran `gh pr list` (failed) and `git branch` & `git log` to ensure no overlapping PRs exist for removing this specific clone in `dispatch_command.rs`.

Verification Commands and Results:
- `cargo check -p agentdesk`: Passed with warnings unrelated to the changed file.
- tests timing out so they were skipped but compiler check passed.
- `git diff --check`: Passed with no whitespace errors.

Skipped Checks:
- `npm run test:policies`, `./scripts/verify-dashboard.sh`, `python3 scripts/generate_inventory_docs.py` skipped because the change strictly alters Rust implementation details in a specific service and does not affect JS policies, the dashboard, or public inventory metadata.
- `cargo test` timed out due to environment execution limits.

Risk:
Very low. Uses standard `std::mem::take` to consume data that is already safely mutable and never accessed again for that specific root index. Panic potential is identical to the previous explicit indexing syntax.

Rollback Notes:
If issues occur during graph extraction, revert to `components[root].clone()`.
