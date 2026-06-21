1.  **Overview**: Replace hardcoded Korean strings with clear, accurate English messages in `src/cli/doctor/orchestrator.rs`.
2.  **Files**: Modify `src/cli/doctor/orchestrator.rs`.
3.  **Action**: Replace specific Korean guidance messages with their English equivalents using `replace_with_git_merge_diff`.
4.  **Testing/Verification**: Run `cargo check --all-targets`, `git diff --check`, and potentially the `agentdesk doctor` tests to verify no syntax errors were introduced and that formatting holds up.
5.  **Pre-commit Steps**: Ensure `pre_commit_instructions` are followed to guarantee quality and checks pass.
6.  **Submit**: Make the PR commit.
