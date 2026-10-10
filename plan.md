1. **Analyze the Problem**:
    - The `observability_target` field is currently present in log lines related to leader-only workers (`src/server/worker_registry/registry.rs`, `src/server/worker_registry/status.rs`) but missing from worker-local worker log paths (`src/server/worker_recovery.rs`).
    - PR #193 added target observability to worker supervision start/skip logs. We need to extend this field `observability_target = spec.target` to all lifecycle log sites consistently.

2. **Identify Modification Points**:
    - `src/server/worker_recovery.rs` around lines:
      - `236` (`tracing::error!`)
      - `269` (`tracing::warn!`)
      - `314` (`tracing::info!`)
      - `324` (`tracing::warn!`)

3. **Apply the Fix**:
    - Add `observability_target = spec.target,` to all identified `tracing::` macros in `src/server/worker_recovery.rs`.

4. **Verify the Fix**:
    - Run `cargo check --all-targets` to ensure Rust compiles successfully.
    - Check for overlapping open PRs in `gh` output (simulated or checking open branches). I did check branches before.

5. **Pre-commit Instructions**:
    - Complete pre-commit steps to make sure proper testing, verifications, reviews and reflections are done.

6. **Submit PR**:
    - Commit and submit changes following the format `WorkerRegistry: <concise worker supervision improvement>`.
