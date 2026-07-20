What changed:
Updated all GitHub Actions workflows (`.github/workflows/*.yml`) to use `node-version-file: ".nvmrc"` instead of hardcoding `node-version: "22"`. Simplified `.nvmrc` to contain exactly `22` rather than `22.15.0`. Also, explicitly tracked `.nvmrc` locally.

Why:
To improve repository toolchain hygiene. It centralizes the Node version declaration into a single source of truth (`.nvmrc`), aligning local developer setup with the CI configuration, exactly mirroring how `rust-toolchain.toml` aligns the Rust version. This ensures that CI workflows and local environments stay precisely in sync without having to modify multiple YAML files for a single bump.

WorkFingerprint:
- Agent Name: Supply-Lite
- Category Boundary: CI setup (`.github/workflows/*.yml`, `.nvmrc`)
- Primary Files: `.github/workflows/ci-main.yml`, `.github/workflows/ci-nightly.yml`, `.github/workflows/ci-pr.yml`, `.nvmrc`
- Invariant Protected: Toolchain consistency across local and CI environments.
- Public API Impact: None
- Docs Impact: None
- Verification Plan: Run local node dashboard verification (`./scripts/verify-dashboard.sh`) and policy tests (`npm run test:policies`) to ensure Node operations are unaffected.
- Related PRs: Follow-up to `upstream-pr/add-nvmrc` which introduced the file but missed hooking it into CI.

Duplicate/Overlap Check:
Used `git ls-remote --heads origin`. Saw `upstream-pr/add-nvmrc` but no open `ci-node-version-file` or workflow refactoring PRs.

Verification Commands/Results:
- `git diff --check`: Clean
- `./scripts/verify-dashboard.sh`: Passed successfully. (304 tests passed, built successfully).
- `npm run test:policies`: Passed successfully. (196 tests passed).

Skipped Checks:
`cargo check --all-targets` was skipped as this is exclusively a Node.js CI setup change and does not affect the Rust build pipeline.
`python3 scripts/generate_inventory_docs.py` was skipped because generated documentation drift is not affected by CI configuration changes.

Risk:
Low. It's a configuration refactoring using standard GitHub Action features (`node-version-file`). The local version `22` natively maps to Node 22.x in `actions/setup-node`.

Rollback Notes:
Revert the commit to restore the hardcoded `node-version: "22"` declarations in the workflow YAML files.
