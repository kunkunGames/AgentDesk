# PostgreSQL migration histories

The campaign runtime and upstream independently used migration versions 131 onward.
The database ledger selects an immutable history by its stored SHA384 checksums.
Never renumber applied migrations, rewrite receipts, or repair their checksums to
make a different catalog appear compatible.

| History | Immutable catalog | Convergence |
| --- | --- | --- |
| Upstream | `migrations/postgres` through 140 | Upstream 146 adds campaign capabilities |
| Campaign v5 | Common upstream 1–130, then `migrations/postgres_campaign_v5` 131–145 | Campaign 146 adds upstream capabilities |

Both 146 migrations are ordered, exact copies of the missing historical SQL.
`migrations/convergence.json` records each input and output hash, and
`python scripts/check_migration_convergence.py` verifies coverage and bytes.
Campaign convergence excludes upstream 132, which deletes legacy Office data.
Those unused tables remain available for retention and recovery.

Every subsequent migration belongs in `migrations/postgres_shared`, starting at
147. Do not append to either historical catalog. The shared catalog must behave
correctly with either retained legacy Office tables or no Office tables.
The immutable checksum guard covers all three directories.

An empty database defaults to upstream only when no application relation,
routine, or user type exists in the effective search path. Extension-owned
objects do not make a database occupied. Existing common-only history requires
an explicit `AGENTDESK_PG_MIGRATION_LINEAGE=upstream` or `campaign_v5` after
operator verification. A conflicting or invalid selection fails before DDL.
Normally, deployments with version 131 or later need no environment override.

Selection, receipt verification, and execution hold the same SQLx advisory lock
on a dedicated connection. The connection is closed on failure or cancellation.
Receipts must be an exact successful catalog prefix; historical version gaps and
Down migrations are handled according to the actual catalog. A normal pending
suffix is different from a missing intermediate receipt. Doctor reports the
latter as invalid history, including when future versions are also present.

Before upgrading a deployed database, stop mutation admission, take and verify a
backup, restore it into an isolated PostgreSQL instance, run the candidate
migration twice, and compare the preexisting ledger and business rows. Apply only
the candidate built and tested from the recorded source manifest. After 146,
rolling back the binary alone is not supported by old binaries with a 145 catalog;
use a compatible forward fix or an explicitly coordinated full restore.
