What changed:
Registered `src/services/discord/gateway.rs` in `scripts/giant_file_registry.toml` and refreshed the `ARCHITECTURE.md` output to resolve generated-inventory and CI drift.

Why:
The `src/services/discord/gateway.rs` file crossed the 1000-line production threshold but was missing from the registry, which caused `scripts/generate_inventory_docs.py` to fail CI checks. Additionally, the `ARCHITECTURE.md` was stale with missing files like `cluster_role.rs` and `edge_case_tests.rs`. Both issues have been corrected to keep CI and documentation green.

WorkFingerprint:
- Agent: Redline
- Category boundary: `scripts/generate_inventory_docs.py`, `docs/generated/**`, `ARCHITECTURE.md`, `scripts/giant_file_registry.toml`.
- Primary files: `ARCHITECTURE.md`, `scripts/giant_file_registry.toml`
- Invariant protected: Generated inventory and registry docs must match current code state.
- Public API impact: None
- Docs impact: Updated `ARCHITECTURE.md` directory structure.
- Verification plan: Run Python generator script and `cargo check`.
- Related PRs/issues: None directly overlapping with this exact set.

Duplicate/overlap check:
Checked open branches via `git branch -r` and specifically inspected branches containing `inventory` or `refresh` keywords. Found older refresh PRs but none addressing the recent addition of `src/services/discord/gateway.rs` nor the `cluster_role.rs`/`edge_case_tests.rs` architecture output.

Verifications:
- `python3 scripts/generate_inventory_docs.py --check`: Initially flagged drift and failing missing registry file. After the fix, reported `up to date`.
- `git diff --check`: No trailing spaces or git-level issues.
- `cargo check --bin agentdesk`: Compiled successfully (skipped `--all-targets` due to earlier command-level runtime errors, but the specific fix here required no Rust logic change).

Skipped checks:
- `npm run test:policies`: Not run as this PR does not modify JS policies.
- `./scripts/verify-dashboard.sh`: Not run as this PR does not modify frontend assets.

Risk:
Low. Modifies only documentation and a single generator configuration registry.

Rollback notes:
Revert the commit; CI checks would then flag the missing file in the registry and stale architecture docs.
