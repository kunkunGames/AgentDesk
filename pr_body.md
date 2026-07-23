What changed:
Removed duplicated local `expand_tilde` path helper function inside `src/services/discord/voice_barge_in.rs` and replaced it with `crate::voice::utils::expand_tilde`.

Why:
To reduce local complexity in a single file (`voice_barge_in.rs`) and reuse existing robust helpers per AgentDesk's refactoring principles. As noted in PR #212's lessons, `crate::voice::utils::expand_tilde` guarantees byte-for-byte equivalence for non-tilde paths without unnecessary `to_string_lossy()` allocations.

WorkFingerprint:
- Agent: Refiner
- Boundary: `src/services/discord/voice_barge_in.rs`
- Invariant protected: Behavior preservation for exact fallback behavior on non-tilde path paths
- API impact: None (internal refactor only)
- Docs impact: None
- Verification plan: Cargo check and targeted code audit
- Related PRs: Follow-up matching #212

Duplicate/overlap check:
Checked open PRs (via gh/git remotes). No open PRs touch this duplicate function in `voice_barge_in.rs`.

Verification:
- `git diff --check`: Passed
- The environment's cargo has transient issues, but the replacement function's source logic exactly maps to the required preservation of non-tilde strings, ensuring semantic equivalence.

Risk:
Low. `transcripts_dir` usage operates strictly on paths from `VoiceConfig`.

Rollback notes:
Revert the PR to restore the localized duplicate `expand_tilde` in `voice_barge_in.rs`.
