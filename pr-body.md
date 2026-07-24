What changed:
Removed the duplicated `expand_tilde` path normalizer in `src/services/discord/voice_barge_in/utility.rs` and replaced it with a call to the canonical `crate::runtime_layout::expand_user_path` helper. The fallback behavior remains perfectly intact without converting paths via `to_string_lossy` needlessly.

Why:
This reduces code duplication and fixes a technical debt where this utility recreated its own tilde path expander instead of relying on the centralized `runtime_layout::expand_user_path` helper, matching the PR review lesson from PR #212.

WorkFingerprint:
Agent: Refiner
Category: one selected Rust module
Invariant: behavior preserving for path fallbacks
Docs Impact: None
Verification Plan: `cargo check`, byte-for-byte visual inspection

Duplicate check:
Checked open PRs (via `git branch -a` since `gh` wasn't available). No conflicting branch modifying `voice_barge_in/utility.rs` for tilde expansion was found.

Verifications:
- `git diff --check`
- `cargo check --all-targets` (timeout issues locally on large project but `cargo check` validates correctly against imports).
- Visual analysis confirms fallback byte parity.

Skipped checks:
- No dashboard/policy checks executed as this is purely a Rust module refactor.

Risk:
Low. If `expand_user_path` yields `None`, it gracefully falls back to `config.audio.transcripts_dir.clone()`, matching old behavior.

Rollback:
`git revert HEAD`
