What changed:
Added `autoComplete="off"`, `spellCheck="false"`, and `data-1p-ignore` attributes to the three `<input type="password">` bot token fields in the Step 1 onboarding wizard UI (`dashboard/src/components/onboarding/Step1BotConnection.tsx`).

Why:
These fields collect sensitive Discord tokens, which triggers many browser password managers (e.g. 1Password) to aggressively try to save them as passwords or auto-fill them on subsequent visits. This disrupts the onboarding flow. Explicitly disabling autocomplete and adding the `data-1p-ignore` attribute prevents this interference, creating a safer and clearer progress path for operators setting up their environment.

WorkFingerprint:
- Agent: OnboardingSmith
- Category Boundary: `dashboard/src/**OnboardingWizard**`
- Primary Files: `dashboard/src/components/onboarding/Step1BotConnection.tsx`
- Invariant Protected: Keep dashboard and API behavior aligned, prevent secret autofill issues.
- Public API Impact: None.
- Docs Impact: None.
- Verification Plan: Run `./scripts/verify-dashboard.sh` and `git diff --check`.
- Related PRs/Issues: None.

Duplicate/Overlap Check:
Ran `gh pr list --state open` in bash. Checked for OnboardingSmith, DomainKeeper, or Accessor overlapping tasks. No PRs modifying the Step 1 token inputs were found.

Verification Commands & Results:
- `./scripts/verify-dashboard.sh`: Pass (Builds correctly, 300 tests passed).
- `git diff --check`: Pass (No whitespace errors).

Skipped Checks:
- `cargo check --all-targets`: Skipped because no Rust code was changed.
- `npm run test:policies`: Skipped because no policy JS code was changed.
- `python3 scripts/generate_inventory_docs.py`: Skipped because no generator source or inputs changed.

Risk:
Very low. Modifies HTML input attributes to opt out of browser features; does not change core React logic or token submission logic.

Rollback Notes:
Revert the PR to restore the previous `<input>` field attributes.
