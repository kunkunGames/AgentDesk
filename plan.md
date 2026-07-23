1. **Refactor `src/services/discord/voice_barge_in.rs`:**
   - Remove the local `expand_tilde(path: &Path)` function.
   - Update `transcript_dirs_from_config` to call `crate::voice::utils::expand_tilde` directly.
   - This explicitly matches the prompt's `Good Refiner candidates should explicitly name the existing helper being reused and prove fallback behavior stays byte-for-byte equivalent for non-tilde paths; do not route every path through a helper if that trims whitespace, uses to_string_lossy, or otherwise rebuilds a valid path that was previously returned unchanged.`
   - The local `expand_tilde` in `voice_barge_in.rs` *does* use `to_string_lossy()`, which rebuilds the path, so removing it and replacing it with `crate::voice::utils::expand_tilde` which iterates over components *avoids* the `to_string_lossy` conversion for non-tilde paths!
2. **Include Pre-commit Check**
   - Run verification locally (`cargo check --all-targets`). Note: due to the current sandbox issue, I'll still include the instructions. I will use `cargo check` and `git diff --check`.
