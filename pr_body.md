What changed:
- Fixed `aria-valuetext` localization mapping in `AgentFormModal.tsx`.
- Hid redundant inner decorative images from screen readers (`aria-hidden="true"` and `alt=""`) within the sprite selection slider, since the parent slider handles text announcements.
- Excluded intermediate interactive buttons inside `role="spinbutton"` from sequential tab navigation (`tabIndex={-1}`) to prevent duplicate focus steps, aligning with standard slider interaction.
- Changed `aria-current="true"` to `aria-pressed="true"` in `EmojiPickerLibraryPanel.tsx` since the component acts functionally as a toggle button.

Why:
These changes resolve duplicate/unnecessary screen reader read-outs in modals and improve keyboard navigation behavior within custom slider implementations. This PR recreates and consolidates the intent of closed PRs #196 and #202 from current `main`.

WorkFingerprint:
- Agent: Accessor
- Boundary: dashboard/src/components/agent-manager/**
- Primary Files: AgentFormModal.tsx, EmojiPickerLibraryPanel.tsx
- Invariant Protected: Accessible names and selected-state semantics for modal elements. No visual jitter or layout changes introduced.
- Public API impact: None
- Docs impact: None
- Verification plan: Run `./scripts/verify-dashboard.sh` and ensure all tests pass.
- Related PRs/issues: Recreates the intent of #196 and #202.

Duplicate/Overlap Check:
- Verified open PRs; there are no other open PRs overlapping this specific accessibility change scope at this time, excluding the closed #196/#202 branches.

Verifications:
- `git diff --check`: Passed
- `./scripts/verify-dashboard.sh`: Passed

Skipped checks:
- Rust tests, policy tests, and generated inventory checks were skipped because this PR exclusively modifies the dashboard React UI.

Risk and Rollback:
- Risk is extremely low, limited to `aria-*` tags and `tabIndex` changes within React components.
- Rollback can be performed securely by reverting this PR without risking application data or stability.
