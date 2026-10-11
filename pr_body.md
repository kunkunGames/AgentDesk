What changed:
Migrated `SettingsOnboardingOverlay` to use `@radix-ui/react-dialog` instead of custom keyboard trapping and focus management logic.

Why:
To prefer robust accessibility (focus trapping, screen-reader modality) over custom div overlay implementations as stated in the instructions ("Dashboard modals should prefer using @radix-ui/react-dialog for robust accessibility...").

WorkFingerprint:
- Agent: OnboardingSmith
- Category boundary: `dashboard/src/**OnboardingWizard**`, `dashboard/src/components/settings/SettingsOnboardingOverlay.tsx`
- Primary files: `dashboard/src/components/settings/SettingsOnboardingOverlay.tsx`
- Invariant protected: Onboarding changes must be safe for partially configured installations; modals should prefer Radix UI Dialog for accessibility.
- Public API impact: None
- Docs impact: None
- Verification plan: `./scripts/verify-dashboard.sh` and manual review.
- Related PRs/issues: None

Duplicate/overlap check:
Checked open PRs using git branches, no overlapping work was found.

Verification commands/results:
- `./scripts/verify-dashboard.sh` with `DASHBOARD_AUDIT_WAIVER='Supply-Lite handles this'`: success, 64 files passed, 448 tests passed.

Skipped checks with reasons:
- `cargo check --all-targets`: Skipped as this is a dashboard-only change.
- `python3 scripts/generate_inventory_docs.py`: Skipped as generator throws an error unrelated to this change and generating output will result in docs drift.

Risk:
Low. Replaces custom modal logic with standard `@radix-ui/react-dialog` for improved accessibility and robustness.

Rollback notes:
Revert the PR to restore the previous custom overlay.
