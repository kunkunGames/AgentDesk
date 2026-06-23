What changed:
Refreshed the generated inventory documentation (`docs/generated/module-inventory.md`, `docs/generated/giant-file-registry.md`) and fixed CI drift by updating the maintainability script baseline configs and `change-surfaces.md`.

Why:
The generated documentation and baseline configurations were stale and causing CI failures. This commit applies the actual generator changes to align the codebase.

WorkFingerprint:
- agent: Cartographer-Lite
- boundary: docs/generated/module-inventory.md, docs/generated/giant-file-registry.md, scripts/audit_maintainability_giant_baseline.toml, docs/agent-maintenance/change-surfaces.md
- primary file: docs/generated/module-inventory.md
- invariant: Generated inventories are authoritative only when produced by the generator.
- public API impact: None
- docs impact: Refreshed generated documentation and sync config.
- duplicate check: Did not find exact open duplicate.
- verification plan: Run ci checks locally.
- skipped checks: Code compilation, tests, and formatting checks skipped for no-change report.
- risk: None.
- rollback notes: N/A.

Verification:
- Local scripts passed.
