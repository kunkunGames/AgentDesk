# Steward Queue Hygiene Journal

- **Generated Docs Drift Collision**: Scribe agents frequently create duplicate PRs attempting to refresh generated inventories (e.g. `docs/generated/module-inventory.md`) when code changes alter module sizes.
- **Action**: When Steward detects duplicate generated-docs PRs, it should recommend maintainers close all but one. To prevent future overlap, Scribe agents should check for existing open PRs touching the same generated files before opening new ones.
