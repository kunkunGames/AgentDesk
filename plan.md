1. **Refactor duplicate `expand_tilde` in `src/services/discord/voice_barge_in.rs`:**
   - Replace the local `expand_tilde` implementation in `src/services/discord/voice_barge_in.rs` with `crate::voice::utils::expand_tilde` (or `crate::runtime_layout::expand_user_path` directly, guarded correctly).
   - Actually, using `crate::voice::utils::expand_tilde` is exactly what PR #212 did for the voice module ("replaced duplicate `voice/utils.rs` tilde expansion with `runtime_layout::expand_user_path`").
   - Looking closely, `src/services/discord/voice_barge_in.rs` has its own `expand_tilde` function that reimplements tilde expansion using `dirs::home_dir()`.
   - We will replace `expand_tilde` in `src/services/discord/voice_barge_in.rs` to just use `crate::voice::utils::expand_tilde`.

Let's double check `src/cli/dcserver.rs`.
```rust
                                let expanded = if ws.starts_with("~/") {
                                    if let Some(home) = dirs::home_dir() {
                                        format!("{}{}", home.display(), &ws[1..])
                                    } else {
                                        ws.to_string()
                                    }
                                } else {
                                    ws.to_string()
                                };
```
This is also a great candidate. We can use `crate::runtime_layout::expand_user_path` guarded by `starts_with("~/")` to preserve byte-for-byte fallback for non-tilde paths.

Let's pick `src/cli/dcserver.rs`. It's a very narrow refactor in one file.

Wait, the instructions say:
"PR #212 replaces duplicate `voice/utils.rs` tilde expansion with `runtime_layout::expand_user_path`. Good Refiner candidates should explicitly name the existing helper being reused and prove fallback behavior stays byte-for-byte equivalent for non-tilde paths"

I will refactor `src/cli/dcserver.rs` to use `crate::runtime_layout::expand_user_path`.

Plan:
1. Open `src/cli/dcserver.rs`.
2. Locate the manual tilde expansion in `load_agentdesk_config_with_path` (or rather `migrate_workspaces_for_session`).
3. Replace:
```rust
                                let expanded = if ws.starts_with("~/") {
                                    if let Some(home) = dirs::home_dir() {
                                        format!("{}{}", home.display(), &ws[1..])
                                    } else {
                                        ws.to_string()
                                    }
                                } else {
                                    ws.to_string()
                                };
```
with:
```rust
                                let expanded = if ws == "~" || ws.starts_with("~/") || ws.starts_with("~\\") {
                                    if let Some(path) = crate::runtime_layout::expand_user_path(ws) {
                                        path.to_string_lossy().into_owned()
                                    } else {
                                        ws.to_string()
                                    }
                                } else {
                                    ws.to_string()
                                };
```
Wait, the original logic in `dcserver.rs` ONLY checks `ws.starts_with("~/")`.
If I want byte-for-byte equivalence, I should only expand `~/`, or is it better to support `~` and `~\` since the existing helper does that? The prompt says "prove fallback behavior stays byte-for-byte equivalent for **non-tilde** paths". For non-tilde paths, both original and new will return `ws.to_string()`.

Let's choose `src/cli/dcserver.rs`.
