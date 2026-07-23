Wait!
`src/services/discord/voice_barge_in/progress_playback.rs` ALREADY uses `crate::voice::utils::expand_tilde`.
But `src/services/discord/voice_barge_in.rs` has its OWN duplicate `expand_tilde`:
```rust
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
If we look closely, `voice_barge_in.rs`'s `expand_tilde` uses `to_string_lossy()`, which allocates and rebuilds if it contains invalid UTF-8. It also doesn't use `expand_user_path`.
Replacing it with `crate::voice::utils::expand_tilde` accomplishes exactly what #212 did!

Let's check `voice/utils.rs`:
```rust
pub(crate) fn expand_tilde(path: &Path) -> PathBuf {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::Normal(first)) if first == "~") {
        return path.to_path_buf();
    }

    let Some(mut expanded) = crate::runtime_layout::expand_user_path("~") else {
        return path.to_path_buf();
    };
    for component in components {
        expanded.push(component.as_os_str());
    }
    expanded
}
```
This avoids `to_string_lossy()` and only expands exactly `~` as the first component!
Is `crate::voice::utils::expand_tilde` perfectly safe to replace `voice_barge_in.rs`'s `expand_tilde`?
Wait, `voice_barge_in.rs` `expand_tilde` is called in `transcript_dirs_from_config`:
```rust
fn transcript_dirs_from_config(config: &VoiceConfig) -> Vec<PathBuf> {
    vec![expand_tilde(&config.audio.transcripts_dir)]
}
```
If `config.audio.transcripts_dir` does not start with `~`, it returns `path.to_path_buf()`. Which is exactly what `crate::voice::utils::expand_tilde` does!
Also, it removes a duplicate tilde expansion in a voice-related file (`voice_barge_in.rs`).

Let's read `src/services/discord/voice_barge_in.rs` again:
