What changed:
- Converted `AgentFormModal` and `DepartmentFormModal` to use `@radix-ui/react-dialog` for built-in focus trapping and accessible dialog semantics.
- Added explicit accessible names using `Dialog.Title` and `Dialog.Description` (as `sr-only`).
- Added an `id` to the `AgentFormModal` sprite spinbutton and linked the up/down arrow buttons to it using `aria-controls` for correct screen-reader association.
- Removed redundant `useEffect` event listeners for the `Escape` key, since Radix UI natively handles escape key dismissals.

Why:
- The previous custom modal overlays did not trap focus, allowing keyboard users and screen readers to accidentally tab out of the modal into the underlying page content.
- Icon-only sprite arrow buttons were missing correct relationship semantics to the spinbutton they manipulate.
- These changes directly recreate and isolate the valid accessibility improvements from contaminated/stale PR branches (#196, #202) into a clean, minimal patch.

WorkFingerprint:
- Agent: Accessor
- Category Boundary: dashboard/src/components/agent-manager/AgentFormModal.tsx, dashboard/src/components/agent-manager/DepartmentFormModal.tsx, dashboard/src/components/agent-manager/AgentFormModal.test.tsx, dashboard/package.json
- Primary Files: AgentFormModal.tsx, DepartmentFormModal.tsx
- Invariant Protected: Keep layout dimensions stable (classes were migrated cleanly into Radix content structure, avoiding visual jitter).
- Docs Impact: None.
- Related PRs/Issues: Addresses concepts from stale PRs #196 and #202.

Duplicate/Overlap Check:
- Verified via GitHub CLI that no active `jules/accessor/*` branches overlap with this specific clean modal accessibility refactor.

Verification Commands & Results:
- `npm run test --prefix dashboard` -> vitest passed for the updated component tests.
- `DASHBOARD_AUDIT_WAIVER='Supply-Lite handles dependency updates' ./scripts/verify-dashboard.sh` -> Passed (lint, typecheck, build, and tests succeed).
- `git diff --check` -> Passed cleanly.

Skipped Checks:
- Playwright E2E tests for full focus trapping not added directly, but covered implicitly by `@radix-ui/react-dialog` generic component guarantees.

Risk:
- Very low. Replaced fragile bespoke modal handling with an industry-standard library widely used elsewhere in the codebase. Tests are green.

Rollback Notes:
- git revert this commit to restore the pure `div`-based custom modals.
