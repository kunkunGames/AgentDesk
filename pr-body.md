## Boundary fingerprint

- Agent: Redline
- Boundary: Generated inventory docs drift
- Primary files: `docs/generated/module-inventory.md`
- CI job / failure signature: Script check / generate_inventory_docs.py drift check
- Related PRs/issues checked: None matching open Redline PRs or active inventory drift PRs in `git log`.
- Why this is non-overlapping: This directly addresses the out-of-date table rows for module lengths found via `python3 scripts/generate_inventory_docs.py --check` and does not intersect with other agent concerns.

## Summary
Resolves inventory drift that causes CI script checks to fail.

## CI failure signature
```
stale generated doc: docs/generated/module-inventory.md
--- docs/generated/module-inventory.md (current)
+++ docs/generated/module-inventory.md (expected)
```

## Root cause
The module-inventory docs were not accurately updated after recent file changes, resulting in a mismatch between the expected line counts and actual files.

## Fix
Ran `python3 scripts/generate_inventory_docs.py` to regenerate the documentation.

## Why this does not weaken coverage
This is purely an inventory documentation regeneration to align expected metrics.

## Verification commands and results
`python3 scripts/generate_inventory_docs.py --check`

```
up to date: ARCHITECTURE.md
up to date: docs/generated/module-inventory.md
up to date: docs/generated/route-inventory.md
up to date: docs/generated/worker-inventory.md
```
