What changed:
Replaced the custom `expand_home_dir` function in `src/services/qwen.rs` with the existing `crate::runtime_layout::expand_user_path` helper. Kept the fallback behavior byte-for-byte identical for non-tilde paths by only passing strings starting with `~` to the helper, which ensures paths with trailing whitespace remain unchanged.

Why:
Consolidates tilde-expansion logic across the codebase to the `runtime_layout` helper, reducing local complexity in the `qwen.rs` file.

WorkFingerprint:
- Agent: Refiner
- Boundary: `src/services/qwen.rs`
- Invariant protected: Behavior preservation of working directory expansion. Non-tilde paths remain exactly the same without unexpected trimming.
- Public API impact: None
- Docs impact: None
- Verification plan: Cargo check and test the library.

Duplicate/Overlap Check:
Checked open PRs (via `git branch -r`) before starting. There are no active PRs overlapping with `qwen.rs` path expansion.

Verification Commands and Results:
- `git diff --check`: Clean
- `cargo check --all-targets`: Clean build, no new warnings in `qwen.rs`.
- `cargo test -p agentdesk qwen_project_cache_key`: Passed. Tests for `qwen.rs` compilation passed.

Skipped Checks:
- `npm run test:policies`, `./scripts/verify-dashboard.sh`, `python3 scripts/generate_inventory_docs.py` skipped because there were no JS policy, dashboard, or route/inventory changes.

Risk:
Low. The change explicitly preserves byte-for-byte fallback for non-tilde paths by checking `starts_with('~')` before invoking the helper (which trims whitespace). The helper successfully delegates to the standard home directory resolution logic.

Rollback Notes:
Revert the PR. No DB migrations or generated docs to unroll.
