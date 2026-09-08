What changed:
Updated `vitest` dependency in `dashboard` from `^4.1.1` to `^4.1.11`.
Updated `package-lock.json` via `npm install -D vitest@^4.1.11`.

Why:
Routine toolchain hygiene maintenance. There is a moderate security advisory for `@vitest/mocker` (Path Traversal / Arbitrary File Read) affecting versions prior to `4.1.10`. Updating `vitest` to `^4.1.11` resolves this advisory without introducing structural risk.

WorkFingerprint:
Agent: Supply-Lite
Category boundary: package.json files
Primary files: dashboard/package.json, dashboard/package-lock.json
Invariant protected: Maintain secure and up-to-date dashboard build tooling without breaking the build.
API/docs impact: None.
Verification plan: Run `./scripts/verify-dashboard.sh` and `git diff --check`.
Related PRs/issues: None.

Duplicate/overlap check:
Checked open branches via `git branch -a`. There are no overlapping `jules/supply-lite` branches for dashboard package updates.

Verification commands and results:
- `git diff --check` (Passed cleanly)
- `./scripts/verify-dashboard.sh` (Passed successfully, tests passed)
- `npm audit --prefix dashboard` (0 vulnerabilities)

Skipped checks with reasons:
`cargo check` and other rust-specific tests were skipped because this is purely a frontend JavaScript/Node tooling update.

Risk:
Low. Minor toolchain bump isolated to build-time `devDependency` dependencies.

Rollback notes:
Revert the commit or downgrade to `vitest@4.1.1`.
