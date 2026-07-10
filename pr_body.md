What changed:
Extracted 10 analytics and receipt-related routes from the `admin` domain into a new, dedicated `analytics` domain. Updated the router composition in `src/server/routes/domains/mod.rs` and `src/server/routes/mod.rs` to register the new module. Regenerated routing inventory docs.

Why:
The `admin` domain router was becoming a dumping ground for disparate metrics, receipts, and analytics endpoints. Extracting these into a dedicated `analytics` domain clarifies ownership boundaries and makes route registration easier to reason about, fulfilling the DomainKeeper mission without altering public paths, authentication requirements, or underlying behavior.

WorkFingerprint:
- Agent: DomainKeeper
- Category Boundary: src/server/routes/domains/**, src/server/routes/mod.rs, docs/generated/**
- Primary Files: src/server/routes/domains/analytics.rs, src/server/routes/domains/admin.rs, src/server/routes/domains/mod.rs, src/server/routes/mod.rs
- Invariant Protected: Domain ownership mapping in Axum routers matches API architecture boundaries.
- Public API Impact: None. All extracted routes remain unchanged in path, method, and auth requirements (wrapped in `protected_api_domain`).
- Docs Impact: `docs/generated/route-inventory.md` updated to reflect the new `src/server/routes/domains/analytics.rs` handler source mapping.
- Verification Plan: Run `cargo check --all-targets`, `npm run test:policies`, `./scripts/verify-dashboard.sh`, `python3 scripts/generate_inventory_docs.py`, and `git diff --check`.
- Related Issues/PRs: None overlapping.

Duplicate/Overlap Check:
Checked open PRs via `gh pr list` / `git log` and branch names. No overlapping DomainKeeper PRs found touching these route boundaries.

Verification Commands and Results:
- `git diff --check`: No whitespace errors.
- `cargo check --all-targets`: Passed (with pre-existing dead code warnings in unrelated modules).
- `npm run test:policies && ./scripts/verify-dashboard.sh`: Passed successfully. Dashboard builds and tests complete without failure.
- `python3 scripts/generate_inventory_docs.py`: Successfully regenerated `route-inventory.md` and `module-inventory.md`.

Skipped Checks:
None.

Risk:
Low. The routes retain their exact paths and middleware (via `protected_api_domain`). No handler logic was modified.

Rollback Notes:
Revert the commit. No database migrations or external state changes are required to rollback.
