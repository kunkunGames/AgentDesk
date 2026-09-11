What changed:
- Converted `EmojiPickerLibraryPanel` toggle buttons to use `aria-pressed` instead of `aria-current` to accurately represent their toggle state semantic.
- Excluded duplicate "Previous/Next Sprite" arrow buttons from sequential keyboard navigation by adding `tabIndex={-1}` inside `AgentFormModal`.
- Hid duplicate visual previews (image/emoji elements) from assistive technologies using `aria-hidden="true"` inside `AgentFormModal`'s spinbutton, relying on the container's `aria-valuetext` instead.
- Unified the `aria-valuetext` localization mechanism to use the local `tr` instance in `AgentFormModal.tsx`.

Why:
These changes implement the missing selected-state semantic requirements on modal selectors, removing redundant nested focus targets, and standardizing aria labels. This aligns the emoji picker and the sprite selector with ARIA recommendations for toggle buttons and spinbuttons.

WorkFingerprint:
- Agent: Accessor
- Boundary: dashboard/src/components/agent-manager/
- Primary files: `dashboard/src/components/agent-manager/AgentFormModal.tsx`, `dashboard/src/components/agent-manager/EmojiPickerLibraryPanel.tsx`
- Invariant protected: Selected-state semantics and keyboard focus ordering within modal selectors.
- Public API impact: None
- Docs impact: None
- Verification plan: Visual and lint check via verify-dashboard.sh and git diff --check.
- Related PRs/issues: Closed/stale #196, #202; and relates to a previous clean attempt at modal selector a11y.

Overlap Check:
- Verified via `git branch -r` and git log. The branch `origin/jules/accessor/modal-selector-a11y-1487321874384798187` generated a no-change report and is no longer an active overlapping code change. No other PRs are currently editing these two specific modal files in conflicting ways.

Verification Commands & Results:
- `git diff --check`: Clean
- `./scripts/verify-dashboard.sh`: Passed

Skipped Checks:
- `cargo check`/`npm run test:policies`/`generate_inventory_docs.py`: Skipped because changes are entirely contained within the dashboard TypeScript components.

Risk: Low. Changes are purely ARIA attribute additions and `tabIndex` updates inside the dashboard.
Rollback Notes: Can be reverted by reverting the commit. No side effects.
