## Summary

Refresh generated module inventory docs to match source code.

## Generated surface

`docs/generated/module-inventory.md`

## Generator command used

`python3 scripts/generate_inventory_docs.py`

## Drift detected

Stale line counts for modules:
- `db::automation_candidates`
- `services::automation_candidate_materializer`
- `services::routines::store`

## Verification commands and results

- `python3 scripts/generate_inventory_docs.py --check`
  - Output: `up to date: ARCHITECTURE.md`, `up to date: docs/generated/module-inventory.md`, `up to date: docs/generated/route-inventory.md`, `up to date: docs/generated/worker-inventory.md`
- `python3 -m unittest tests.test_agent_maintenance_docs`
  - Output: `Ran 13 tests in 0.013s`, `OK`
- `git diff --check`
  - Output: (no output, check passed)

## Boundary fingerprint

- Agent: Cartographer-Lite
- Boundary: docs/generated/module-inventory.md
- Primary files: docs/generated/module-inventory.md
- Inventory/generator invariant: ensure `module-inventory.md` is synchronized with canonical source code
- Related PRs/issues checked: Checked open PRs, no existing overlaps for updating this specific `module-inventory.md`
- Why this is non-overlapping: Other existing PRs do not update the line counts for `db::automation_candidates`, `services::automation_candidate_materializer`, and `services::routines::store`

## Residual risk / follow-up

None
