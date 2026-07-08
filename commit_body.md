ApiRoutemaster: move hooks routes to ops domain

What changed:
Moved `/hook/reset-status`, `/hook/skill-usage`, and `/hook/session/{sessionKey}` routes from the `integrations` domain (`src/server/routes/domains/integrations.rs`) to the `ops` domain (`src/server/routes/domains/ops.rs`). Updated the generated route inventory.

Why:
These routes manage the agent's internal state and session lifecycle rather than external integrations. Moving them to the `ops` domain aligns with their true purpose and keeps the `integrations` domain focused on external services (GitHub, Discord, etc.).

WorkFingerprint:
Agent Name: ApiRoutemaster
Category Boundary: src/server/routes/**, docs/generated/**
Primary Invariant: Route paths, methods, handlers, and auth remain identical. Only the registration domain changes.
Public API Impact: None, paths are preserved.

Overlap Check:
Checked open PRs, no overlapping PRs found touching these domain route configurations.

Verification:
- `cargo check --all-targets` passed
- `python3 scripts/generate_inventory_docs.py` ran to update docs/generated/route-inventory.md

Risk & Rollback:
Risk is low as paths remain the same. Rollback by reverting this commit.
