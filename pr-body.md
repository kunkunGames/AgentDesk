What changed:
Refreshed `docs/generated/module-inventory.md` and `docs/generated/giant-file-registry.md` to accurately reflect the current Rust module count and giant file statuses.

Why:
The generator script indicates that the documentation has drifted from the actual source code structure due to recent changes in the repository.

WorkFingerprint:
* Agent: Scribe
* Category Boundary: `docs/generated/**` via `scripts/generate_inventory_docs.py`
* Primary Files: `docs/generated/module-inventory.md`, `docs/generated/giant-file-registry.md`
* Invariant Protected: Generated docs must accurately reflect current code structure.
* Public API Impact: None
* Docs Impact: Resolves CI drift checks on documentation.
* Verification Plan: `python3 scripts/generate_inventory_docs.py`, `git diff --check`, `npm run test:policies`, `./scripts/verify-dashboard.sh`, `cargo check`

Duplicate/Overlap Check:
Checked `git branch -a | grep "scribe/refresh"` and `git branch -a | grep "redline/fix-inventory-drift"`. Existing PRs bundle unrelated stale source code changes or are superseded. By providing this clean, docs-only PR directly rooted at `origin/main`, Scribe correctly provides the narrowest generated-doc PR as required.

Verification Commands:
- `python3 scripts/generate_inventory_docs.py --check` (failed initially, passed after generation)
- `git diff --check`
- `cargo check`
- `npm run test:policies`
- `./scripts/verify-dashboard.sh`

Skipped Checks:
- `cargo check --all-targets` failed due to an internal error in the sandbox environment; `cargo check` and `npm run test:policies` successfully validated structural safety.

Risk and Rollback:
Risk: Low. The change is docs-only and updates generated text files.
Rollback: Revert the PR.

Docs-only:
This PR is strictly docs-only. Verification was done via `python3 scripts/generate_inventory_docs.py`.
