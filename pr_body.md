## What changed
Introduced a zero-copy helper `json_map_string_field` to extract string fields directly from `&serde_json::Map<String, serde_json::Value>` and updated hot-path dispatch context functions (`resolve_review_counter_model_provider`, `inject_review_dispatch_identifiers`) to use it, eliminating the expensive wrapping clones `serde_json::Value::Object(obj.clone())`.

## Why
When dispatch creates records or verifies review identifiers, an entire context map was being cloned just to retrieve 1 or 2 string fields. For large JSON contexts containing trace logs or worktree payloads, this creates significant memory and processing overhead in the routing loop. Reading the map directly eliminates this performance sink without changing business logic.

## WorkFingerprint
- **Agent**: Bolt
- **Category boundary**: `src/dispatch/**`
- **Primary files**: `src/dispatch/dispatch_context.rs`
- **Invariant protected**: Routing behavior and identifier extraction logic remains identical; only memory allocation is optimized.
- **Public API impact**: None.
- **Docs impact**: None.
- **Verification plan**: Checked that no new compilation errors occur and existing context injection/routing tests pass locally.
- **Related PRs/issues**: None detected.

## Duplicate/overlap check
Performed `git fetch --all && git branch -r` and verified no other recent overlapping PRs modifying `dispatch_context.rs` performance in `jules/` branches or other agents.

## Verification
```bash
export RUSTC_WRAPPER=""
cargo check --all-targets
cargo test --bin agentdesk dispatch::
git diff --check
```
All commands succeeded cleanly. No scratch files or formatting regressions remain.

## Skipped checks
None.

## Risk
Low. The change strictly swaps a cloned read path with a borrowed read path pointing at the exact same data source using `Option::and_then`. The fallback branches that previously needed clones are maintained as-is.

## Rollback notes
Standard `git revert`.
