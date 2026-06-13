What changed:
1. Replaced `.unwrap_or_default()` with explicit `try_get` error handling in `src/server/routes/dispatches/thread_reuse.rs` (`link_dispatch_thread` and `get_card_thread`).
2. Fixed a policy JS unit test failure in `policies/00-escalation.js` by guarding `agentdesk.registerPolicy({ ... })` with a `typeof agentdesk !== "undefined"` check.

Why:
1. Silently defaulting to an empty string on missing or null columns (`kanban_card_id`, `to_agent_id`, `card_id`, `dispatch_agent_id`) could lead to corrupted downstream logic and state-transition ordering. The code now returns an explicit 500 error.
2. Fixes the `agentdesk.registerPolicy is not a function` error which was causing the `test:policies` CI step to fail in the test harness when executing `00-escalation.js`.

WorkFingerprint:
- Agent: Dispatcher
- Category Boundary: src/server/routes/dispatches*
- Primary Files: src/server/routes/dispatches/thread_reuse.rs, policies/00-escalation.js
- Invariant Protected: Dispatch lifecycle data parsing validity and explicit error handling on route entrypoints; policy test pipeline stability.
- Public API Impact: None.
- Docs Impact: None.
- Verification Plan: Local cargo check, git diff, and npm test.

Duplicate/Overlap Check:
Checked `git branch -a`. This correctly replaces previous iterations like `fix-thread-reuse-silent-unwrap-16808908297257248051`.

Verification Commands & Results:
- `git diff --check`: Clean.
- `cargo check --all-targets`: Passed.
- `npm run test:policies`: Passed (144/144).

Skipped Checks:
`python3 scripts/generate_inventory_docs.py` - Not generating docs.
`./scripts/verify-dashboard.sh` - No frontend files changed.

Risk:
Low.

Rollback Notes:
`git revert` the single commit.
