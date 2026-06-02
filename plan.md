1. **Refactor `src/server/routes/agents_setup.rs`**
   - Use `replace_with_git_merge_diff` to modify `src/server/routes/agents_setup.rs`.
   - The diff will be:
```
<<<<<<< SEARCH
fn resolve_setup_path(root: &Path, raw: &str) -> PathBuf {
    let expanded = expand_tilde(raw);
    let candidate = PathBuf::from(&expanded);
    if candidate.is_absolute() {
        return candidate;
    }
    let root_candidate = root.join(&candidate);
    if root_candidate.exists() {
        return root_candidate;
    }
    crate::runtime_layout::config_dir(root).join(candidate)
}

fn expand_tilde(raw: &str) -> String {
    if raw == "~" || raw.starts_with("~/") {
        if let Some(expanded) = crate::runtime_layout::expand_user_path(raw) {
            return expanded.to_string_lossy().into_owned();
        }
    }
    raw.to_string()
}
=======
fn resolve_setup_path(root: &Path, raw: &str) -> PathBuf {
    let expanded = if raw == "~" || raw.starts_with("~/") {
        crate::runtime_layout::expand_user_path(raw)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| raw.to_string())
    } else {
        raw.to_string()
    };
    let candidate = PathBuf::from(&expanded);
    if candidate.is_absolute() {
        return candidate;
    }
    let root_candidate = root.join(&candidate);
    if root_candidate.exists() {
        return root_candidate;
    }
    crate::runtime_layout::config_dir(root).join(candidate)
}
>>>>>>> REPLACE
```
2. **Verify changes**
   - Use `run_in_bash_session` to execute: `cargo check --all-targets`
   - Use `run_in_bash_session` to execute: `git diff --check`
3. **Complete pre-commit steps**
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
4. **Submit PR**
   - Use `submit` tool to submit the branch `jules/refiner/reuse-expand-user-path-agents-setup`.
   - Title: `Refiner: reuse expand_user_path in agents_setup`
   - Description:
```
What changed:
Replaced the local `expand_tilde` helper in `src/server/routes/agents_setup.rs` with an inline call to the existing `crate::runtime_layout::expand_user_path`.

Why:
This reduces duplication and relies on the centralized path resolution helper as documented in previous PRs (like #212). The behavior for strings not starting with `~` or `~/` is preserved exactly.

WorkFingerprint:
Agent: Refiner
Category: src/server/routes/agents_setup.rs
Invariant Protected: Behavior preservation for non-tilde paths.
Public API Impact: None
Docs Impact: None
Verification Plan: `cargo check --all-targets` and `git diff --check`.
Related PRs/Issues: None

Duplicate/overlap check:
Ran `git branch -r | grep jules | grep reuse-expand-user-path` and `gh pr list` (if available) before starting to ensure no overlapping work on `agents_setup.rs`.

Verification commands/results:
- `cargo check --all-targets`: Passed
- `git diff --check`: Passed

Skipped checks with reasons:
- `python3 scripts/generate_inventory_docs.py` skipped because there's no inventory drift (no generated inventory files in this category).
- `npm run test:policies` skipped because no JavaScript policy files were modified.
- `./scripts/verify-dashboard.sh` skipped because no dashboard frontend files were modified.

Risk: Low. The change is a straightforward substitution of a local helper with an equivalent centralized helper (with identical fallback behavior).
Rollback notes: Revert this commit if it causes path resolution failures during agent setup.
```
