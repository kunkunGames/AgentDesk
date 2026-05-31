What changed:
Added missing `id` and `htmlFor` attributes to input fields and their respective labels in `AgentFormModal.tsx` and `DepartmentFormModal.tsx`. Also added a visually hidden (`sr-only`) label for the department prompt textarea in `DepartmentFormModal.tsx` to ensure screen readers can announce it correctly.

Why:
These changes resolve accessibility issues where form inputs lacked accessible names because their labels were not properly associated using `htmlFor`. This improves the dashboard's usability for keyboard and screen reader users and aligns with the memory requirements to maintain accessible names.

WorkFingerprint:
- agent name: Accessor
- category boundary: dashboard/src/**
- primary files: dashboard/src/components/agent-manager/AgentFormModal.tsx, dashboard/src/components/agent-manager/DepartmentFormModal.tsx
- invariant protected: Keep layout dimensions stable; accessibility changes must not introduce visual jitter.
- public API impact: None
- docs impact: None
- verification plan: verified dashboard builds and tests pass using `./scripts/verify-dashboard.sh` and playwright
- related PRs/issues: None

Duplicate/overlap check:
Checked open PRs (using `git branch -r`) and found no overlapping accessibility improvements for `AgentFormModal` or `DepartmentFormModal`. Previous overlapping PRs #196 and #202 were closed.

Verification commands and results:
- `./scripts/verify-dashboard.sh`: Successfully built dashboard and passed all tests.
- `git diff --check`: No whitespace or conflict marker issues.

Skipped checks with reasons:
- `npm run test:policies`, `cargo check`, etc.: Skipped because no Rust, policy, or API files were modified. The changes are strictly localized to React dashboard UI components.

Risk and rollback notes:
- Risk is extremely low, as this change only modifies HTML attributes for accessibility and does not alter any component logic or styling (except adding an `sr-only` class to a hidden label).
- Rollback: Revert the PR to remove the `id` and `htmlFor` attributes if any unforeseen issue occurs with the new IDs.
