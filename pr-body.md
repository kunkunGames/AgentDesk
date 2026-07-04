What changed:
Moved the `/skills/*` routes (`/skills/catalog`, `/skills/ranking`, `/skills/prune`) from `src/server/routes/domains/ops.rs` to `src/server/routes/domains/admin.rs`, updating the `skills_api` imports accordingly. Also re-generated the route-inventory docs and module-inventory docs to reflect this move.

Why:
The `/skills` endpoints fall under the `admin` category according to the docs taxonomy (`src/server/routes/docs/taxonomy.rs`). Currently, they were registered in `ops.rs` which didn't match their intended domain classification. Moving them ensures correct domain boundary enforcement without broad refactoring.

WorkFingerprint:
* agent name: DomainKeeper
* category boundary: `src/server/routes/domains/**`, `docs/generated/**`
* primary files: `src/server/routes/domains/admin.rs`, `src/server/routes/domains/ops.rs`, `docs/generated/route-inventory.md`, `docs/generated/module-inventory.md`
* invariant protected: API routing boundary enforcement
* public API impact: None, purely internal router relocation
* docs impact: Updated generated inventory docs for the relocated routes
* verification plan: check the route definitions, ensure cargo check passes, check `npm run test:policies`, `./scripts/verify-dashboard.sh` and python3 inventory generator
* related PRs/issues: None

Duplicate/overlap check:
Executed `git branch -r` and verified there are no open PRs overlapping with this specific route migration under the `jules/domainkeeper/*` namespace or other open branches modifying the `/skills` routes.

Verification commands and results:
* `cargo check --all-targets` passed.
* `npm run test:policies` passed.
* `./scripts/verify-dashboard.sh` passed.
* `python3 scripts/generate_inventory_docs.py` ran and updated `route-inventory.md` & `module-inventory.md` with correct line references.
* `git diff --check` passed.

Skipped checks:
None.

Risk:
Very low. Moving route registration files between modules in axum does not change the public-facing API path or behavior.

Rollback notes:
Revert the PR to restore the routes in `ops.rs` and update the inventory docs.
