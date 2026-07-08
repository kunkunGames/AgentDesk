What changed:
Optimized the `onDispatchCompleted` function in `policies/auto-queue.js` by removing redundant `JSON.parse` operations for `gateDispatch.context` and `gateDispatch.result`. It now avoids parsing when `gateDispatch.id === dispatch.id` because the data is already parsed as `context` and `result` during the function execution.

Why:
The `onDispatchCompleted` hook runs frequently on the hot path (as a part of the auto queue). In cases where `gateDispatch.id === dispatch.id`, iterating over `gateDispatches` previously performed double `JSON.parse()` for exactly the same JSON string, consuming extra time and allocating duplicate objects in memory. By reusing `context` and `result`, we reduce parsing time and allocation counts for the triggering dispatch.

WorkFingerprint:
* Agent name: Bolt
* Category boundary: `policies/auto-queue.js`
* Primary files: `policies/auto-queue.js`
* Invariant protected: Behavior of `gateContext` and `gateResult` parsing.
* Public API impact: None
* Docs impact: None
* Verification plan: Run `npm run test:policies` to verify the logic does not break any current phase gate or auto-queue checks. Also run dashboard verification script and tests.
* Related PRs/issues: None directly overlapping.

Duplicate/overlap check:
Checked open pull requests for any overlap on `auto-queue.js` optimization and `onDispatchCompleted`. No overlapping open PR was found.

Verification commands and results:
- `git diff --check`: Clean output.
- `npm run test:policies`: Passed successfully (181 tests passed).
- `./scripts/verify-dashboard.sh`: Passed successfully.
- `npm run -C dashboard test`: Passed successfully.

Skipped checks with reasons:
- `cargo check --all-targets` / rust tests: Rust build environment was not fully available (`cargo` not in PATH), but it was skipped since the change applies strictly to `policies/auto-queue.js` and does not impact any Rust logic.

Risk:
Low. It's a localized logical reuse of variables that are strictly identical. If `gateDispatch.id === dispatch.id`, we bypass identical DB retrieval strings and utilize the already-parsed `context` and `result` variables.

Rollback notes:
Revert the single `if (gateDispatch.id === dispatch.id)` condition inside `policies/auto-queue.js`.
