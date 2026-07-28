Plan:
1.  **Modify `WorkerLocalTerminalSignal` in `src/server/worker_registry.rs`**:
    *   Add `target: &'static str` to the `WorkerLocalTerminalSignal` struct.
2.  **Update JSON serialization in `leader_only_worker_status_json`**:
    *   Include `"target": signal.target,` in the `serde_json::json!` for `last_worker_local_signal`.
3.  **Update population of `WorkerLocalTerminalSignal`**:
    *   In `record_worker_local_terminal_signal`, when creating `WorkerLocalTerminalSignal`, add `target: spec.target`.
4.  **Run verification commands**:
    *   Run `python3 scripts/generate_inventory_docs.py` (though changing JSON schema doesn't affect `WorkerSpec`, we must run the generator to check for drift or inventory output changes).
    *   Run `cargo check --all-targets` and narrow tests if any.
    *   Run `npm run test:policies`, `./scripts/verify-dashboard.sh`.
    *   Commit changes explicitly specifying this handles PR #193 feedback to add `target` observability cleanly without touching `worker_recovery.rs`.
