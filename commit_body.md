What changed:
Removed the duplicate `dirs::home_dir()` usage in `src/services/discord/voice_barge_in.rs`'s local `expand_tilde` helper and replaced it with `crate::runtime_layout::expand_user_path("~")`.

Why:
The `dirs::home_dir()` approach directly bypassed the project's canonical `runtime_layout::expand_user_path` helper logic for user paths. By replacing it, we reduce logic duplication and ensure all paths use the same centralized environment resolution, without altering the byte-for-byte behavior for non-tilde fallback paths.

WorkFingerprint:
- Agent: Refiner
- Category boundary: src/services/discord/voice_barge_in.rs
- Primary files: src/services/discord/voice_barge_in.rs
- Invariant protected: Behavior-preserving refactor without trimming whitespace or stripping valid paths unnecessarily.
- Public API impact: None
- Docs impact: None
- Verification plan: Check compilation via `cargo check --all-targets` and clean diffs.
- Related issues/PRs: None

Duplicate/overlap check:
Checked open PRs on remote via `git branch -a` and found no existing Refiner PR targeting `src/services/discord/voice_barge_in.rs` or `expand_tilde` deduplication for this surface.

Verification commands and results:
- `cargo check --all-targets`: Passed (warnings unrelated to changes).
- `git diff --check`: Clean.

Skipped checks with reasons:
- `./scripts/verify-dashboard.sh`: Dashboard untouched.
- `npm run test:policies`: Policies untouched.
- `scripts/generate_inventory_docs.py`: No new inventory endpoints modified.

Risk:
Low risk; modifies path resolution for a specific config value (`config.audio.transcripts_dir`) in a single helper safely, while maintaining identical non-tilde behavior.

Rollback notes:
Revert the commit to restore `dirs::home_dir()`.
