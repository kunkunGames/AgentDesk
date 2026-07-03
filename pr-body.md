## What changed
- Added `id` prop passing to `EmojiPicker` and linked it via `htmlFor` to its `<label>` in both `AgentFormModal.tsx` and `DepartmentFormModal.tsx` for proper screen reader association.
- Updated `AgentFormModal.tsx`'s sprite selector `spinbutton` to explicitly announce "No sprite (Emoji fallback)" via `aria-valuetext` when falling back to emoji.
- Added `aria-hidden="true"` to visual preview images inside the sprite selector to prevent redundant announcements by screen readers.
- Added `tabIndex={-1}` to the increment/decrement buttons inside the `spinbutton` so they don't break the standard single-tab-stop pattern.

## Why
To fix known modal accessibility issues and provide complete accessible names, correct selected-state semantics, and clean keyboard navigation within the Agent and Department form modals, maintaining consistent product terminology across components.

## WorkFingerprint
- **Agent**: Accessor
- **Category Boundary**: `dashboard/src/**`
- **Primary Files**: `dashboard/src/components/agent-manager/AgentFormModal.tsx`, `dashboard/src/components/agent-manager/DepartmentFormModal.tsx`, `dashboard/src/components/agent-manager/EmojiPicker.tsx`
- **Invariant Protected**: Dashboard accessibility (keyboard focus, ARIA labeling, semantic association)
- **Public API Impact**: None
- **Docs Impact**: None
- **Verification Plan**: Dashboard verification script (`verify-dashboard.sh`) and local vitest.
- **Related PRs/Issues**: Resurrects intent from closed/contaminated PRs #196 and #202.

## Duplicate/Overlap Check
Executed `git branch -a` and verified no currently active overlapping PRs or branches exist modifying `AgentFormModal` or `DepartmentFormModal` for these specific accessibility targets.

## Verification Commands and Results
- `git diff --check`: Clean
- `./scripts/verify-dashboard.sh`: Passed (48 test files, 300 tests passed)
- `cargo check --all-targets`: Skipped (no Rust changes)
- `python3 scripts/generate_inventory_docs.py`: Skipped (no generator or structural changes)

## Skipped Checks
- `cargo check --all-targets` and `generate_inventory_docs.py` were skipped as the changes are exclusively scoped to isolated React component files in the dashboard.

## Risk
Low. Modifications are limited to HTML attributes (`id`, `htmlFor`, `aria-*`, `tabIndex`) within isolated React modals. Visual layout and state logic remain untouched.

## Rollback Notes
Revert the commit touching `AgentFormModal.tsx`, `DepartmentFormModal.tsx`, and `EmojiPicker.tsx`.
