1. **Explore the codebase and understand the task**
    - Done. The goal is to improve one runtime/install/config/doctor reliability boundary. The memory instructions specifically say: "When adding new error or diagnostic messages (e.g., in the CLI doctor checks), they must be written in English to maintain consistency, avoiding hardcoded non-English localizations."
    - We found a lot of hardcoded Korean text in `src/cli/doctor/orchestrator.rs`.
2. **Translate Korean strings in `src/cli/doctor/orchestrator.rs`**
    - Done. Applied translations to all Korean strings found using `grep '[가-힣]' src/cli/doctor/orchestrator.rs`.
3. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
    - Run tests: `cargo test --bin agentdesk doctor::`, `cargo clippy`, `cargo check`.
    - Done.
4. **Submit the change**
    - Done. I will use the PR template and instructions given.
