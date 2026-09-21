# AgentDesk Test Plan & Queue Hygiene Guidelines

## Queue Hygiene & Merge-Readiness
- **Duplicate Checks:** Before starting work, check open PRs for duplicates. If your generated inventory refresh or PR overlaps with existing open PRs, stop and report a no-change overlap. If the `gh` CLI tool is unavailable, fallback to fetching remotes and inspecting branch names (e.g., using `git branch -a`) to avoid duplicating work.
- **No-Change Avoidance & Reporting:** A no-change result should NOT become a PR unless it explicitly changes a concrete queue-hygiene artifact. Otherwise, produce a report only (e.g., no changed files plus no durable artifact is a close candidate). Prefer ending the run with a no-change report rather than opening an empty PR when overlap is found.
- **Strict No-Change Verification:** A "no-change" report MUST have exactly zero changed files. Verify using `gh pr view --json files`. If a PR claims "no change" but modifies files (e.g. migrations, routines, source code), it is unsafe. If an empty no-change PR is unavoidably created, its body must explicitly list the exact overlapping PR numbers and branches.
- **Infrastructure CI Failures:** If a CI failure is determined to be an infrastructure issue or runner cancellation (e.g., 'The runner has received a shutdown signal') with no code fix applicable, produce an empty commit no-change report explaining the cancellation.
- **Stale Branch Cleanup:** Treat low-signal open PR volume and stale broad branches as queue debt. Explicitly close or recommend closing stale broad branches rather than attempting to salvage them in place.
- **Clean Workspace & Baggage Audit:** When using tools that generate scratch files or creating ad-hoc test scripts (e.g., `test_*.rs`, `test.sh`, `plan.md`, `pr-body.md`), always run a final changed-file audit (e.g. `git status`) before committing to ensure stray artifacts are not accidentally included, preventing repository pollution. Do not commit scratch PR body files such as `pr-body.md`; put PR text directly in the GitHub PR body. If the diff contains unrelated files, old merged work, or broad stale branch baggage, abandon the branch and report that a clean PR is required.
- **False Verification Guard:** Do not falsely claim verification (e.g. PostgreSQL, Discord, tmux, provider runtime, browser, CI) unless it was actually executed. If a required check cannot run in the environment, state the exact reason in the skipped checks section and explain the residual risk.

## PR Body Requirements
Every PR must include:
- What changed
- Why
- WorkFingerprint (Agent, Boundary, Primary files, Queue hygiene invariant, Related PRs/issues, Non-overlapping reason)
- Duplicate/overlap check
- Verification commands and results (If the change is docs-only, explicitly state 'docs-only' and list the source files or commands used to verify the documentation)
- Skipped checks and reasons
- Risk and rollback notes

## Verification Commands
- **Rust Changes:** `cargo check --all-targets`, `cargo test <narrow-target>`. When executing commands that might terminate the shell session (like `cargo check --all-targets`), wrap them in `bash -c '<command>'` to prevent session loss and preserve environment state.
- **Dashboard Changes:** `./scripts/verify-dashboard.sh`
- **Policy Changes:** `npm run test:policies`
- **Scripts:** `shellcheck`
- **Generated Docs:** `python3 scripts/generate_inventory_docs.py` (only if the PR explicitly owns generated inventory)

## Review Process
- Ensure generated-inventory refresh PRs contain an explicit duplicate-PR guard in the body.
- Changes must be concrete enough for agents or humans to follow.
