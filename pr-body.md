What changed:
Updated `scripts/analyze_prs.py` to flag empty PRs (0 changed files) that lack explicit no-change intent in their title. It also updates the inventory refresh check to trigger if the diff only contains generated inventory docs, avoiding false negatives on PRs without "inventory" and "refresh" in the title.

Why:
To improve PR hygiene and treat low-signal volume as queue debt, as seen in PR review lessons from the 2026-05-13 queue cleanup. Empty PRs must be explicitly flagged and require an overlap reference. Also, PRs that only modify generated inventory docs should be flagged as inventory refreshes and properly guarded for duplicates, regardless of their title.

WorkFingerprint:
- Agent: Steward
- Boundary: PR templates and contribution docs, scripts that check PR hygiene
- Primary files: `scripts/analyze_prs.py`
- Public API impact: None
- Docs impact: None
- Verification commands and results: `python3 -m unittest tests.test_analyze_prs` (Ran 74 tests in 0.026s, OK)
- Skipped checks with reasons: `./scripts/ci-script-checks.sh` was skipped due to an out-of-boundary failing migration check on the test environment (`migrations/postgres/0110_auto_queue_cleanup_tasks_card_rollback.sql` conflict).
- Risk: Low, this only updates a local PR analysis script to be stricter about what constitutes an inventory refresh or an empty PR.
- Rollback notes: Revert the `scripts/analyze_prs.py` changes.
- Queue hygiene invariant: PR duplicates and stale branches are checked and avoided.
- Related PRs/issues checked: None explicitly mentioned in the request, but checked for overlapping open PRs in `jules/steward/`.
- Duplicate/overlap check: No overlapping PRs were detected.
- Why this is non-overlapping: No other PRs are modifying the specific empty PR or inventory refresh PR logic in `scripts/analyze_prs.py`.
