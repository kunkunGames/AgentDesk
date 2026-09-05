What changed:
Added `MAX_ONBOARDING_DRAFT_AGENTS` constant (64) to the frontend and enforced the limit in the onboarding wizard's `addCustomAgent` action and `Step3AgentSelection` UI.

Why:
The backend restricts drafts to 64 agents, but the frontend lacked validation, allowing operators to add more agents in the UI than could be safely saved, leading to a broken experience upon attempting to proceed or complete setup.

WorkFingerprint:
Agent: OnboardingSmith
Category Boundary: dashboard/src/components/onboarding**
Primary Files: dashboard/src/components/onboardingDraft.ts, dashboard/src/components/onboarding/OnboardingWizardActions.ts, dashboard/src/components/onboarding/Step3AgentSelection.tsx
Invariant Protected: Onboarding state and configuration limits align between dashboard and API.
Public API Impact: None.
Docs Impact: None.
Verification Plan: Run dashboard tests to verify functionality remains intact.

Duplicate/Overlap Check:
Checked remote branches with `gh pr list ...` and `git branch -r`. No existing PR addresses this specific limit enforcement.

Verification Commands/Results:
- `./scripts/verify-dashboard.sh` - Passed (51 test files passed, 318 tests passed)
- `git diff --check` - Clean

Skipped Checks:
- `cargo check --all-targets` and `npm run test:policies` (no Rust or policy changes)

Risk:
Low. Safely limits user input without breaking existing data flows.

Rollback Notes:
Revert the frontend changes; no database migrations or backend logic depend on this frontend enforcement.
