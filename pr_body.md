What changed:
Updated accessible names and selected-state semantics for the emoji/sprite selectors in the dashboard modals (`AgentFormModal.tsx` and `DepartmentFormModal.tsx`). Changed "아이콘 번호" / "아이콘 선택" to "스프라이트 번호" / "스프라이트 선택" in `AgentFormModal.tsx`'s spinbutton to clarify product terminology versus the explicit "Emoji/Icon" picker. Also delegated the `aria-label` computation for `EmojiPicker` to its own internal defaults, removing redundant overrides in the parents, and enhanced the default text to include the selection state (e.g., "Selected icon: 🤖, change icon").

Why:
These changes fulfill the requirement to include both accessible names and selected-state semantics when improving sprite or emoji selectors in the dashboard, standardizing product terminology across labels and providing clearer context for screen reader users.

WorkFingerprint:
Accessor, dashboard/src/**, AgentFormModal.tsx/DepartmentFormModal.tsx/EmojiPicker.tsx, dashboard UI semantics, no public API impact, no docs impact, checked overlap, verified using verify-dashboard.sh script, related to previously closed PRs #196/#202.

Duplicate/overlap check:
Checked open PRs matching `accessor` and determined this clean semantic update addresses the previously dropped modal accessibility intent without overlapping any currently open PR.

Verification commands and results:
- `./scripts/verify-dashboard.sh` - Completed successfully (tests pass, including newly updated tests for EmojiPicker accessible names).

Skipped checks with reasons:
N/A - all relevant scripts were run.

Risk:
Low. No layout dimensions were changed (avoiding visual jitter) and no native component structures were drastically altered; updates primarily focus on semantic labeling.

Rollback notes:
If issues arise, revert the commit from branch `jules/accessor/modal-accessibility-semantics`.
