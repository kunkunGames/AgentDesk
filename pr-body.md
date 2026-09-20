What changed:
Updated the generated module inventory (`docs/generated/module-inventory.md`) and added `docs/generated/giant-file-registry.md` as output by the generator script. No other files were changed as route and worker inventories remained consistent with the code.

Why:
Routine Cartographer-Lite task to keep generated architecture, module, route, and worker inventories aligned with the current code without mixing unrelated product changes into the inventory PR.

WorkFingerprint:
- Agent: Cartographer-Lite
- Boundary: docs/generated/module-inventory.md, docs/generated/giant-file-registry.md
- Primary files: docs/generated/module-inventory.md, docs/generated/giant-file-registry.md
- Invariant protected: Generated inventories strictly reflect codebase state.
- Public API impact: None
- Docs impact: Updates generated inventories only.
- Verification plan: ran python3 scripts/generate_inventory_docs.py, checked git diff.
- Related PRs/issues: duplicate check performed.

Duplicate/overlap check:
Executed `git branch -a` and examined logs to verify no existing open Cartographer-Lite/Redline/Scribe PR already covered this exact drift, as `gh pr list` was unavailable.

Verification commands and results:
- `python3 scripts/generate_inventory_docs.py`: generated new outputs correctly.
- `git diff --check`: no trailing whitespace or check errors.

Skipped checks with reasons:
- `cargo check --all-targets` and `cargo test`: cargo check exited with an internal error (not related to this repo/PR, pure workspace error) and since no `.rs` code or script code changed, it is safe to skip this check as we only update generated `.md` docs.
- `npm run test:policies` and `./scripts/verify-dashboard.sh`: skipped because no policy or dashboard changes were made.

Risk:
Low. Only touches generated documentation artifacts and does not modify any source code or runtime behavior.

Rollback notes:
Revert the PR and regenerate the inventory if needed.
