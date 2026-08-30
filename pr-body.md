What changed:
Added `Home`, `End`, `PageUp`, and `PageDown` keyboard handlers to the icon selector spinbutton in `dashboard/src/components/agent-manager/AgentFormModal.tsx` and bounded the value between 1 and 40. Also added `aria-selected` semantic attribute to the emoji picker in `dashboard/src/components/agent-manager/EmojiPickerLibraryPanel.tsx` to properly convey the selected state.

Why:
The `AgentFormModal` icon selector lacked full spinbutton keyboard accessibility, missing the standard `Home`, `End`, `PageUp`, and `PageDown` semantics. Furthermore, the emoji picker (`EmojiPickerLibraryPanel`) lacked a clear selected-state ARIA attribute (`aria-selected`), using only `aria-pressed`. This change aligns with PR #196/#202 learnings for consistent modal accessibility while keeping terms properly localized as "아이콘".

WorkFingerprint:
- Agent: Accessor
- Boundary: dashboard/src/components/agent-manager/**
- Primary files: dashboard/src/components/agent-manager/AgentFormModal.tsx, dashboard/src/components/agent-manager/EmojiPickerLibraryPanel.tsx
- Public API impact: None
- Docs impact: None
- Accessibility invariant: Spinbutton has complete standard keyboard behaviors; emojis convey proper selected-state semantics.
- Related PRs/issues checked: Checked for PR #196 and #202 branches.
- Non-overlapping reason: Verified no overlapping active `Accessor` branches.

Duplicate/overlap check:
Checked using `git fetch origin && git branch -r` for overlapping branches (e.g. PR #196/#202 modal topics). None found active.

Verification commands and results:
- `git diff --check` (clean)
- `./scripts/verify-dashboard.sh` (passed successfully, all 335 tests passed)

Skipped checks and reasons:
- `gh` API duplicate check was skipped and fell back to `git branch -r` due to lack of `gh` CLI availability.
- Playwright/E2E test run since there isn't a dedicated one matching this specific component locally beyond the `vitest` unit test suite which passed.

Risk and rollback notes:
- Risk: Low, dashboard frontend logic update constrained to React component states.
- Rollback: Revert the commit.
