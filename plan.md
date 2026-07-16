1. **Change substring filters to index prefix conditions in `fetch_delivery_kv_guard_batch_pg`**.
    - In `src/reconcile.rs` (lines ~930 and ~936), the cursor pagination over `kv_meta` uses `SUBSTRING(key FROM ...) > $1` which prevents using the primary key index efficiently for the cursor jump.
    - Rewrite to `key > 'dispatch_reserving:' || $1` and `key > 'dispatch_notified:' || $1`.
2. **Apply similar index prefix rewrites in `recover_expired_dispatch_reserving_pg` and `recover_orphan_dispatch_notified_pg`**.
    - In `recover_expired_dispatch_reserving_pg` (line 538), replace `WHERE e.dispatch_id = SUBSTRING(m.key FROM LENGTH('dispatch_reserving:') + 1)` with `WHERE m.key = 'dispatch_reserving:' || e.dispatch_id`.
    - In `recover_orphan_dispatch_notified_pg` (line 713), replace `WHERE td.id = SUBSTRING(m.key FROM LENGTH('dispatch_notified:') + 1)` with `WHERE m.key = 'dispatch_notified:' || td.id`.
3. **Verify the change**
    - Use `run_in_bash_session` to run `cargo check --all-targets` and the narrowest relevant `cargo test` target (`cargo test reconcile`) for the reconcile changes to ensure SQL queries compile properly in Rust and have no regressions.
4. **Complete pre-commit steps**
    - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
5. **Submit the PR**
    - Use `submit` to commit the changes, ensuring the WorkFingerprint is included in the PR body.
