What changed:
Regenerated documentation files `docs/generated/module-inventory.md`, `docs/generated/route-inventory.md`, and `docs/generated/giant-file-registry.md` using `scripts/generate_inventory_docs.py` to match the current Rust module and HTTP route source of truth.

Why:
These files had drifted from the current repository state. Cartographer-Lite is tasked with keeping them synchronized with the code.

WorkFingerprint:
- Agent: Cartographer-Lite
- Boundary: `scripts/generate_inventory_docs.py`, `ARCHITECTURE.md`, `docs/generated/module-inventory.md`, `docs/generated/route-inventory.md`, `docs/generated/worker-inventory.md`, `docs/generated/giant-file-registry.md`
- Primary files: `docs/generated/module-inventory.md`, `docs/generated/route-inventory.md`, `docs/generated/giant-file-registry.md`
- Invariant protected: Generated code and architecture inventory matches actual source of truth without mixing in unrelated code changes.
- Public API impact: None.
- Docs impact: Updates generated inventories to accurately reflect the system state.
- Verification plan: Check that `generate_inventory_docs.py` produces the included changes cleanly. Use `git diff --check`.
- Related PRs/Issues: Addressed as a daily scheduled drift-fix.

Duplicate/overlap check:
Checked open branches via `git branch -r`. No existing currently mergeable PR handles this exact base drift without including unrelated artifacts or scratch files. Older overlap PRs (e.g. from Redline/Scribe) were reviewed, and a fresh dedicated update was produced.

Verification commands/results:
- `python3 scripts/generate_inventory_docs.py`: Ran successfully and generated the updated files.
- `git diff --check`: Clean (no whitespace errors).

Skipped checks:
- `cargo check --all-targets`: Skipped because there are no Rust code changes. (An internal environment issue caused an error, but this PR is purely Markdown docs updates).

Risk:
Low. Only generated documentation files are updated. No runtime or behavioral code is changed.

Rollback notes:
`git revert <commit-hash>` to restore the old generated inventories.
