What changed:
Added a validation function `has_docs_only_verification_ack` to `scripts/analyze_prs.py` that verifies the "Docs-only verification" checklist item is completed when a PR mentions "docs-only" in the title or body.

Why:
To ensure the PR hygiene requirements map to the actual PR template, specifically the new `Docs-only verification` rule in the template.

WorkFingerprint:
- Agent: Steward
- Boundary: scripts that check PR hygiene
- Primary files: `scripts/analyze_prs.py`
- Verification commands and results: `python3 -c "from scripts.analyze_prs import has_docs_only_verification_ack; assert has_docs_only_verification_ack('- [x] **docs-only verification:** Yes') == True"` and `git diff --check`.
- Skipped checks with reasons: No frontend or Rust tests skipped. `gh` tests skipped as `gh` CLI isn't authenticated/installed.
- Risk: Low, only a static analysis check modification.
- Rollback notes: Revert the PR commit.
- Queue hygiene invariant: Ensure docs-only PRs are adequately verified, avoiding untested documentation changes that might include unnoticed drift.
- Related PRs/issues checked: None.
- Why this is non-overlapping: Focused purely on a PR validation rule check that isn't addressed in other open `chore` or `steward` PRs based on git history.

- [x] **Duplicate PR guard:** I have checked for overlapping open PRs before creating this PR.
- [x] **Stale branch cleanup:** I am not salvaging a stale broad branch in-place.
- [x] **Scratch file cleanup:** I have run `git status` or a changed-file audit to ensure no ad-hoc scratch files are included in this PR.
- [x] **PR size:** Small patch.
