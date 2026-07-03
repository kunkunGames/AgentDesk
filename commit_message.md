FacadePilot: migrate long-turn-monitor to use typed KV facade

What changed:
- Replaced `agentdesk.db.query/execute` for `long_turn_tier` and `long_turn_watchdog_extension` with `agentdesk.kv.get/set/delete` in `policies/timeouts/long-turn-monitor.js`.
- Updated test expectations in `policies/__tests__/timeouts.test.js` to correctly stub `kv: new Map()` state instead of mocking `SELECT value FROM kv_meta` DB query strings.
- Refreshed generated inventory docs.

Why:
- To follow the Typed Facade migration guidelines (`docs/policy-typed-facade.md` & `docs/generated/policy-db-inventory.md`), reducing brittle raw-DB queries from policy runtime code.

WorkFingerprint:
- Agent: FacadePilot
- Boundary: policies/**, policies/__tests__/**
- Primary files: `policies/timeouts/long-turn-monitor.js`, `policies/__tests__/timeouts.test.js`
- Invariant protected: KV meta storage interactions in policies must use typed wrappers.
- Public API impact: None.
- Docs impact: Refreshed inventory stats.
- Verification plan: Executed `npm run test:policies` and `python3 scripts/generate_inventory_docs.py`.
- Duplicate overlap check: Checked existing GH branches manually, none overlapping.

Verification:
- `git diff --check` passed.
- `npm run test:policies` passed locally.
- `python3 scripts/generate_inventory_docs.py` ran.

Skipped checks:
- No rust code changed, so skipped `cargo check --all-targets`.

Risk:
- Low. Tests ensure equivalent logic behavior. `!= null` check used appropriately for string payloads.

Rollback notes:
- Revert the JS file and the test file changes.
