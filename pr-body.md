What changed:
Renamed the `0110_auto_queue_cleanup_tasks_card_rollback.sql` migration to `0115` to resolve a duplicate migration version conflict with `0110_intake_outbox_dispatched_status.sql` and properly sequence it after `0114`. Updated the migration checksum manifest and SQL execution surface inventory baselines to account for the rename and updated schema. Made a minor fix to `scripts/check_postgres_migration_checksums.py` to handle both `"migrations"` and `"protected_migrations"` keys in `immutable-checksums.json` gracefully.

Why:
The duplicate version numbering `0110` caused `sqlx` and the migration checksum guards to fail, breaking CI checks. The rename resolves the collision and preserves the strict migration ledger. The checksum manifest generator logic required adjusting to not erase old `immutable-checksums.json` structure during automated sync.

WorkFingerprint:
- Agent: Steward
- Boundary: PR templates and contribution docs, scripts that check PR hygiene, migrations
- Primary files: `migrations/postgres/0115_auto_queue_cleanup_tasks_card_rollback.sql`, `migrations/postgres/immutable-checksums.json`, `scripts/sql_execution_surface_inventory.json`
- Public API impact: None
- Docs impact: None
- Verification commands and results: Run `./scripts/ci-script-checks.sh` passing all relevant verification (shell lint, PG audit, Postgres checksum guard, SQL inventory baseline).
- Skipped checks with reasons: `cargo check` skipped due to environment limitations but out of scope for these script fixes.
- Risk: Low, simple migration rename and ledger fixes.
- Rollback notes: Revert migration rename and manifest/inventory changes.
- Queue hygiene invariant: Ensure strict linear ordering of postgres migrations and correct recording.
- Related PRs/issues checked: None explicitly mentioned in the request, but fixes a recent `main` conflict.
- Duplicate/overlap check: No overlapping PRs were detected.
- Why this is non-overlapping: No other PRs are addressing the specific `0110` migration duplication in `AgentDesk`.
