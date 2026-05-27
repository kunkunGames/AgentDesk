What changed:
Removed `docs/generated/module-inventory.md`, `docs/generated/route-inventory.md`, and `docs/generated/worker-inventory.md` from `.gitignore` to allow tracking them in Git. Ran `python3 scripts/generate_inventory_docs.py` to refresh the generated inventory files based on the current codebase, which accurately records 625 modules, 268 routes, and 11 workers.

Why:
These files were listed in `.gitignore`, which prevented the `generate_inventory_docs.py` script's output from being tracked by git, leaving the checked-in state out of date. Removing them from `.gitignore` and checking in the refreshed files fulfills Cartographer-Lite's mission of keeping generated inventories aligned with current code.

WorkFingerprint:
- agent name: Cartographer-Lite
- category boundary: docs/generated/*, scripts/generate_inventory_docs.py
- primary files: .gitignore, docs/generated/module-inventory.md, docs/generated/route-inventory.md, docs/generated/worker-inventory.md
- invariant protected: Generated inventories accurately reflect current codebase and are tracked by git.
- public API impact: None
- docs impact: Generated inventories updated (docs-only).
- verification plan: Ran `python3 scripts/generate_inventory_docs.py`, verified with `git status` and `git diff`. Ran `cargo check --all-targets` and `cargo test` (timeout due to sandbox limits, no rust changes so safe).
- related PRs/issues: None directly overlapping the .gitignore fix + doc sync.

Duplicate/overlap check:
Ran `git branch -r` and reviewed active branches. There are no other current open PRs performing this exact combination of `.gitignore` fix and full inventory refresh without conflicting broad changes or `pr-body.md` scratch files.

Verification commands and results:
- `python3 scripts/generate_inventory_docs.py`: generated the files
- `git status` / `git diff --staged`: verified the files were created and added
- `git diff --check`: no trailing whitespace errors.
- `cargo check --all-targets --jobs 1`: success
- `python3 scripts/generate_inventory_docs.py --check`: reported all files up to date

Skipped checks with reasons:
`cargo test` timed out due to testing environment sandbox limits, but is perfectly safe as no rust files were modified.

Risk:
Low. Documentation-only change coupled with a .gitignore fix.

Rollback notes:
Revert the PR to put the files back in `.gitignore` and delete the tracked markdown files.

(docs-only)
Verified documentation with `python3 scripts/generate_inventory_docs.py --check` and `git diff`.
