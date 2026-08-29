# AgentDesk Test Plan & Queue Hygiene Guidelines

## Queue Hygiene & Merge-Readiness
- **Duplicate Checks:** Before starting work, check open PRs for duplicates. If your generated inventory refresh or PR overlaps with existing open PRs, stop and report a no-change overlap. When checking for overlapping open PRs, if the `gh` CLI is unavailable, query the live GitHub Pulls API (e.g., via `curl -s https://api.github.com/repos/<owner>/<repo>/pulls?state=open`) or fetch remotes and inspect branch names using `git branch -r` and logs to ensure no duplicate work is performed. Re-check overlap immediately before opening or updating a PR.
- **Strict No-Change Verification:** A "no-change" report MUST have exactly zero changed files. When producing a no-change overlap report, use an empty commit with the message format `<Agent>: no-change overlap report for <topic>`. Verify using `gh pr view --json files`. If a PR claims "no change" but modifies files (e.g. migrations, routines), it is unsafe. If an empty no-change PR is unavoidably created, its body must explicitly list the exact overlapping PR numbers and branches.
- **Stale Branch Cleanup:** Treat low-signal or stale broad branches as queue debt. Explicitly close or recommend closing stale broad branches rather than attempting to salvage them in place. Run a final changed-file audit before creating or updating a PR. If the diff contains unrelated files, old merged work, scratch files, or broad stale branch baggage, abandon the branch and report that a clean PR from main is required. A no-change result should NOT become a PR unless it explicitly changes a queue-hygiene artifact.
- **Clean Workspace (Scratch Files):** When using tools that generate scratch files or creating ad-hoc test scripts (e.g., `test_*.rs`, `test.sh`, `plan.md`, `pr-body.md`, `patch.diff`, `prs.json`), always run a final changed-file audit (e.g. `git status`) before committing to ensure stray artifacts are not accidentally included, preventing repository pollution. Do not commit scratch PR body files such as `pr-body.md`; put PR text directly in the GitHub PR body.

## Execution Plan Guidelines
- **Actionable Steps:** When formulating execution plans with the `set_plan` tool, focus strictly on concrete, actionable execution steps that correspond to tool calls. Omit high-level, abstract thought processes like 'Understand the problem'. Explicitly prohibit stream-of-consciousness conversational text.
- **Dedicated Verification Step:** Plans for code changes must name the relevant test, lint, build, or generated-drift commands before final review and push. Keep the commands proportional to the changed surface; docs-only work may use focused document and diff checks.
- **Groundedness Rule:** Base all steps, tool calls, and reports solely on confirmed facts and exact tool outputs, avoiding assumptions about truncated data or unverified branch names.
- **Completeness Rule:** When modifying Rust code, the testing step must explicitly state: 'Run `cargo check --all-targets` and the narrowest relevant `cargo test` target to verify the Rust changes'. Testing (e.g., running test scripts) must be a distinct and explicit step placed *before* the pre-commit phase, never bundled into the final PR submission.
- **Specificity Rule:** When formulating a no-change report, do not create a separate narrative step to 'formulate' the report. Incorporate the report details directly into the final submission step using the `done` tool. Execution commands must be granular, forward-looking, and actionable (e.g., 'Run `git checkout -b ...`' instead of 'Create a branch') and exclude mental observations or completed exploration actions.
- **Pre-Commit Rule:** Create a separate, distinct step immediately before the final submission step to 'Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.' rather than embedding it as a sub-bullet.
- **Finalization Step:** Describe finalization in environment-neutral terms and include a final diff/status review. Execution plans involving PR submission must avoid vague instructions like 'an appropriate description'. Instead, explicitly require drafting the PR body to include all mandatory sections (What changed, Why, WorkFingerprint, duplicate/overlap check, verification commands/results, skipped checks, risk, and rollback notes).
- **No-Change Handling:** When no repository files changed, stop and report the result using an empty commit without creating a PR. When deciding to stop and produce a no-change report, explicitly restore any locally modified files (e.g., using `git restore .`) to ensure the working tree remains clean before generating the empty no-change overlap report commit. Clean up all temporary scratch files (e.g., plan.md, python/bash scripts, prs.json) to avoid accidentally generating a patch containing them. Even when producing a no-change report, a final verification stage to run all relevant baseline tests (e.g., `cargo check --all-targets`, `npm run test:policies`, `./scripts/verify-dashboard.sh`) must be explicitly included in the plan before the pre-commit step to verify the repository baseline. Only an external workflow that explicitly requires a no-change trace may use the existing Strict No-Change Verification exception, and the PR body must state why the exception applies. When producing a no-change report, incorporate the justification for the no-change report directly into the finalization report summary instead of generating separate conversational plan steps for it.

## PR Body Requirements
Every PR must include:
- What changed
- Why
- WorkFingerprint (Agent, Boundary, Primary files, Public API impact, Docs impact, Queue hygiene invariant, Related PRs/issues, Non-overlapping reason)
- Duplicate/overlap check
- Verification commands and results
- Skipped checks and reasons
- Risk and rollback notes

## Verification Commands
- **Rust Changes:** `cargo check --all-targets`, `cargo test <narrow-target>`
- **Dashboard Changes:** `./scripts/verify-dashboard.sh`
- **Policy Changes:** `npm run test:policies`
- **Scripts:** `shellcheck`
- **Generated Docs:** `python3 scripts/generate_inventory_docs.py` (only if the PR explicitly owns generated inventory)

If a required check cannot run in the environment, state the exact reason and the residual risk in the PR body.

## Review Process
- Ensure generated-inventory refresh PRs contain an explicit duplicate-PR guard in the body.
- Changes must be concrete enough for agents or humans to follow.
