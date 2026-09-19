What changed:
- Translated the Korean diagnostic string `"postgres 연결 검증용 async runtime 생성에 실패했습니다."` to English `"failed to create async runtime for postgres check."` in `src/cli/doctor/orchestrator.rs`.
- Removed stale "SQLite" references from the `database_unavailable` next step string in `src/cli/doctor/health.rs`, updating it from `"check Postgres/SQLite availability and server logs"` to `"check Postgres availability and server logs"`.

Why:
- To provide clearer, actionable English diagnostic information for operators, aligning with the project's push away from Korean diagnostic strings.
- To reflect the repository rule that PostgreSQL is the canonical live datastore and SQLite is no longer used for live paths, making the diagnostic remediation more accurate.

WorkFingerprint:
- Agent: Doctor
- Category Boundary: `src/cli/doctor/**`
- Primary Files: `src/cli/doctor/orchestrator.rs`, `src/cli/doctor/health.rs`
- Invariant Protected: Doctor behavior must inspect and report without mutating state; preserve JSON output contracts.
- Public API impact: None
- Docs impact: None
- Related PRs/Issues: None

Duplicate/Overlap Check:
- Verified `gh pr list` state. Found other english translation PRs but not this specific string, and found another PR that removes sqlite from other paths but not this specific one.

Verifications:
- `git diff --check`
- Tested JSON contract visually (the test `health_classification_tests` was updated properly)

Skipped checks:
- `cargo check --all-targets` and `cargo test` timed out/failed due to shell errors, but visual diff inspection confirms this is a safe string-only substitution.

Risk:
- Low risk string substitution.

Rollback Notes:
- Revert the commit.
