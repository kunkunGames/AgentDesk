What changed:
Added `src/services/cluster/intake_router_hook/agent_execution_node_tests.rs`, `src/services/cluster/intake_router_hook/edge_case_tests.rs`, `src/services/discord/queue_io/transport/tests.rs` and `src/services/discord/turn_bridge/terminal_outcome_delivery/delivery_epilogue_tests/rest_delivery_tests.rs` to the `PINNED_BASENAME_TEST_FILES` list in `scripts/test_only_module_skip_pin.py`.

Why:
These newly introduced test files caused the "Durable frontier writer per-file call-site allowlist" check and "Library test sweep" (test lane generation constraint check) to fail, because any new test-only Rust module file must be explicitly added to `PINNED_BASENAME_TEST_FILES` to satisfy the writer gate's pinned file expectation exactly.

WorkFingerprint:
- Agent: Redline
- Primary files: `scripts/test_only_module_skip_pin.py`
- Category boundary: `scripts/check_durable_frontier_writer_call_sites.py`
- Invariant protected: The exact count and path list of skipped test files must match the pins.
- Public API impact: None
- Docs impact: None
- Verification plan: Run `python3 scripts/check_durable_frontier_writer_call_sites.py` and `python3 scripts/check_writer_gate_ci_wiring.py`.

Duplicate/overlap check:
Checked open branches via `git branch -r` and specifically inspected branches containing `writer-gate` or `test-only-module` keywords. Found none addressing these specific files.

Verifications:
- `python3 scripts/check_durable_frontier_writer_call_sites.py`: Passed cleanly without finding drift.
- `python3 scripts/check_writer_gate_ci_wiring.py`: Passed cleanly.
- `python3 -m unittest tests/test_analyze_prs.py`: Executed to ensure the PR hygiene isn't blocked. Passed.

Skipped checks:
- `cargo check --all-targets`: Not strictly necessary since the drift was just a Python script exclusion list update.
- `./scripts/verify-dashboard.sh`: Not run as this PR does not modify frontend assets.

Risk:
Low. Only updates test metadata for the writer gate.

Rollback notes:
Revert the commit; CI checks would then flag the test drift.
