1. **Fix `upsert_turn_stat` in `src/db/memento_feedback_stats.rs`**
   - The function `upsert_turn_stat` currently takes `_db: &Db` and unconditionally returns an error "sqlite memento feedback stats backend is unavailable in production".
   - The system is mostly PostgreSQL backed.
   - Update `src/db/memento_feedback_stats.rs` to implement an async `upsert_turn_stat_pg` that uses `sqlx::PgPool` to upsert into `memento_feedback_turn_stats`.
   - Update `upsert_turn_stat` signature to take `pg_pool: Option<&sqlx::PgPool>` (or make a new `upsert_turn_stat_pg` taking `&sqlx::PgPool` and just change the callers to use it). Let's make `upsert_turn_stat_pg` an async function that accepts `&sqlx::PgPool`. The table schema from `migrations/postgres/0001_initial_schema.sql` is:
     ```sql
     CREATE TABLE IF NOT EXISTS memento_feedback_turn_stats (
         turn_id                     TEXT PRIMARY KEY,
         stat_date                   TEXT NOT NULL,
         agent_id                    TEXT NOT NULL,
         provider                    TEXT NOT NULL,
         recall_count                INTEGER NOT NULL DEFAULT 0,
         manual_tool_feedback_count  INTEGER NOT NULL DEFAULT 0,
         manual_covered_recall_count INTEGER NOT NULL DEFAULT 0,
         auto_tool_feedback_count    INTEGER NOT NULL DEFAULT 0,
         covered_recall_count        INTEGER NOT NULL DEFAULT 0,
         created_at                  TIMESTAMPTZ DEFAULT NOW()
     );
     ```

2. **Update `turn_bridge/mod.rs` call site**
   - The call site in `src/services/discord/turn_bridge/mod.rs` currently does:
     ```rust
        if let (Some(db), Some(analysis)) =
            (None::<&crate::db::Db>, recall_feedback_analysis.as_ref())
            && analysis.recall_count > 0
        {
            let stat = crate::db::memento_feedback_stats::MementoFeedbackTurnStat { ... };
            if let Err(error) = crate::db::memento_feedback_stats::upsert_turn_stat(db, &stat) {
                ...
            }
        }
     ```
   - It needs to be updated to use `shared_owned.pg_pool.as_ref()` and `.await`:
     ```rust
        if let (Some(pg_pool), Some(analysis)) =
            (shared_owned.pg_pool.as_ref(), recall_feedback_analysis.as_ref())
            && analysis.recall_count > 0
        {
            let stat = crate::db::memento_feedback_stats::MementoFeedbackTurnStat { ... };
            if let Err(error) = crate::db::memento_feedback_stats::upsert_turn_stat_pg(pg_pool, &stat).await {
                let ts = chrono::Local::now().format("%H:%M:%S");
                tracing::warn!("  [{ts}] ⚠ failed to persist memento feedback stats: {error}");
            }
        }
     ```

3. **Verify the change.**
   - Run `cargo check --all-targets` to make sure it compiles.
   - Run `cargo test --package agentdesk --lib`
   - Run `git diff --check`.

4. **Complete pre-commit steps.**
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.

5. **Submit PR.**
   - Commit and submit.
