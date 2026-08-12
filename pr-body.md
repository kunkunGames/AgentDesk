**What changed**
Updated the `aria-label` for the icon and color selectors in both `OfficeManagerModal.tsx` and `OfficeManagerView.tsx` to explicitly combine the semantic accessible name with its current state (e.g., "선택된 아이콘: 🏢" instead of just "아이콘 🏢" when selected). Added corresponding tests to `OfficeManagerIconPicker.test.tsx` to assert this format.

**Why**
The previous implementation relied solely on `aria-checked` to communicate state. When dealing with abstract concepts like sprites, emojis, or colors in a list of `role="radio"` elements, combining the state with the noun in the label provides better context for screen reader users and creates consistent product terminology for selectors across modals.

**WorkFingerprint**
- Agent: Accessor
- Category Boundary: dashboard/src/**
- Primary Files: `dashboard/src/components/OfficeManagerModal.tsx`, `dashboard/src/components/OfficeManagerView.tsx`, `dashboard/src/components/OfficeManagerIconPicker.test.tsx`
- Invariant Protected: Accessible names and selected-state semantics for icon/color selectors.
- Public API Impact: None
- Docs Impact: None
- Verification Plan: `./scripts/verify-dashboard.sh`, frontend visual verification via playwright, and `git diff --check`.
- Duplicate Check: Ran `git branch -a` and verified no other branches have this specific fix (there is an old branch `modal-accessibility-selectors-recreate` but this is intended to recreate a clean PR from `main`).

**Verification**
- Run `git diff --check`
- Run `./scripts/verify-dashboard.sh`
- Visually verified via playwright screenshot.

**Skipped checks**
- `cargo check` and `cargo test` timed out on the sandbox environment, but these were skipped as no backend Rust code was modified.

**Risk**
Low. Changes are strictly confined to `aria-label` attributes in localized dashboard strings.

**Rollback**
Revert the PR. No data migrations or schema changes are involved.
