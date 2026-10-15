# Shared PostgreSQL migrations

Forward migrations after the lineage-specific convergence must start at 0147. They are appended to both immutable histories by src/db/postgres/migration_catalog.rs. Never fill historical gaps or add an upstream migration at 0141 through 0145.
