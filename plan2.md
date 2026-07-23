So in `role_map.rs`, we can redefine `expand_tilde` as:

```rust
fn expand_tilde(path: &str) -> String {
    if !path.starts_with('~') {
        return path.to_string();
    }
    if path == "~" || path.starts_with("~/") {
        if let Some(expanded) = crate::runtime_layout::expand_user_path(path) {
            return expanded.to_string_lossy().into_owned();
        }
    }
    path.to_string()
}
```
Wait, the existing `expand_tilde` in `src/services/discord/role_map.rs` is ALREADY byte-for-byte equivalent!
```rust
fn expand_tilde(path: &str) -> String {
    if path == "~" || path.starts_with("~/") {
        if let Some(expanded) = crate::runtime_layout::expand_user_path(path) {
            return expanded.to_string_lossy().into_owned();
        }
    }
    path.to_string()
}
```
It already returns `path.to_string()` for non-tilde paths! So what should I refactor?
I should remove the duplicate `expand_tilde` functions in:
- `src/services/discord/org_schema.rs`
- `src/services/discord/agentdesk_config.rs`
- `src/services/discord/voice_barge_in.rs`
Wait! In `org_schema.rs` and `agentdesk_config.rs`, they check `~\\` too!
```rust
fn expand_tilde(path: &str) -> String {
    if path == "~" || path.starts_with("~/") || path.starts_with("~\\") {
        if let Some(expanded) = crate::runtime_layout::expand_user_path(path) {
            return expanded.to_string_lossy().into_owned();
        }
    }
    path.to_string()
}
```

Wait, `crate::utils::format::expand_tilde_path` returns `PathBuf`.
If I use `crate::utils::format::expand_tilde_path` but only for when it starts with `~`, it would be byte-for-byte equivalent.
But the prompt says:
`Good Refiner candidates should explicitly name the existing helper being reused and prove fallback behavior stays byte-for-byte equivalent for non-tilde paths; do not route every path through a helper if that trims whitespace, uses to_string_lossy, or otherwise rebuilds a valid path that was previously returned unchanged.`

Let's look at `src/services/discord/voice_barge_in.rs`:
```rust
fn transcript_dirs_from_config(config: &VoiceConfig) -> Vec<PathBuf> {
    vec![expand_tilde(&config.audio.transcripts_dir)]
}

fn expand_tilde(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    if raw == "~" {
        return dirs::home_dir().unwrap_or_else(|| path.to_path_buf());
    }
    if let Some(rest) = raw.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest);
    }
    path.to_path_buf()
}
```
This is EXACTLY the same pattern as what was fixed in `voice/utils.rs` by #212!
The issue in `voice_barge_in.rs` is that it uses its own `expand_tilde(path: &Path) -> PathBuf`!
Wait, `voice_barge_in.rs` can just use `crate::voice::utils::expand_tilde` !
Let's check if `crate::voice::utils::expand_tilde` is visible. Yes, `pub(crate) fn expand_tilde(path: &Path) -> PathBuf`.
