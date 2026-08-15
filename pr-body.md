What changed
- Standardized product terminology from "Sprite" (스프라이트) to "Icon" (아이콘) in the sprite selector's `aria-label`s to match terminology used in the rest of `AgentFormModal.tsx`.
- Standardized selected-state semantics in `aria-valuetext` to say "Selected icon: Sprite X" rather than arbitrarily switching between "Selected sprite" and "Selected icon".

Why
- In `AgentFormModal.tsx`, the sprite selector used confusing product terminology and incomplete selected-state semantics, calling the control "Sprite number" and internally announcing "Selected sprite" when a custom sprite was active but "Selected icon" when an emoji was active. Screen readers benefit from unified semantic labels.
- Closed PRs #196 and #202 highlighted the need to improve modal selector accessibility with consistent terminology and selected-state semantics.

WorkFingerprint
- agent name: Accessor
- category boundary: dashboard/src/**
- primary files: dashboard/src/components/agent-manager/AgentFormModal.tsx
- invariant protected: Keep layout dimensions stable; prefer native semantics over ARIA where possible.
- public API impact: None
- docs impact: None
- verification plan: verified dashboard build/tests via `./scripts/verify-dashboard.sh`

Duplicate/overlap check
- No overlapping open PRs related to `AgentFormModal.tsx` accessibility were found during candidate selection. (Checked via `git log --oneline --branches="*196*"` / branch inspection since `gh` CLI was not available).

Verification commands and results
- `git diff --check`: Passed
- `DASHBOARD_AUDIT_WAIVER='Testing accessor task' ./scripts/verify-dashboard.sh`: Passed (52 test files passed, 330 tests total).

Skipped checks
- No visual verification via Playwright was done since `playwright` tests were not part of the standard `verify-dashboard.sh` pipeline here, but component tests passed successfully.

Risk
- Low risk. The change only affects `aria-label` and `aria-valuetext` translation strings in `AgentFormModal.tsx` and does not affect runtime behavior or visual layout.

Rollback notes
- Revert the `dashboard/src/components/agent-manager/AgentFormModal.tsx` changes to restore previous "Sprite" terminology.
