-- #5071 T2-M stage 2: atomically widen the status CHECK and install the
-- concurrently-built open-route fence from 0110 under its stable name.
--
-- Do not apply this file directly with autocommit psql: atomicity depends on
-- SQLx's transaction, and a later failure can leave the swap partially committed.
--
-- SQLx runs this file and its migration bookkeeping in one transaction. The
-- first ALTER and the ordinary DROP INDEX require ACCESS EXCLUSIVE on
-- intake_outbox. From the time 0111's request enters the lock queue, later
-- conflicting reads and writes can also wait behind it. Once acquired, that lock
-- blocks reads and writes through commit. The acquired-lock execution interval
-- remains catalog-only: ADD CHECK NOT VALID skips validation scanning, and 0110
-- completed the heap scans beforehand. Total traffic blocking is dominated by
-- the remaining duration of preceding transactions plus the O(1) catalog and
-- commit work.
--
-- Failure-state inventory across the two migrations:
--   * 0110 fails: the old CHECK and old fence remain; an INVALID temporary index
--     may remain and may add update/uniqueness overhead. The old invariant is
--     intact, and 0110's fail-closed recovery is required.
--   * 0110 builds a valid index but is not recorded: the valid temporary fence
--     coexists with the old CHECK/fence. This is consistent because the old
--     CHECK rejects dispatched; use 0110's recovery before proceeding.
--   * 0110 is recorded and 0111 has not committed: both valid fences coexist
--     with the old CHECK. This is the same consistent intermediate state.
--   * Any statement or SQLx bookkeeping in 0111 fails: its transaction rolls
--     back to that consistent intermediate state; rerunning 0111 is safe.
--   * 0111 commits: the widened CHECK and widened fence become visible together
--     under the stable discriminator name, and SQLx records 0111 atomically.

-- The replacement is a strict superset of the valid 0052 domain, so every
-- existing row is already known to satisfy it. NOT VALID avoids a redundant
-- validation scan while enforcing the replacement on new or updated rows.
-- Validate it in a separate low-traffic migration, under PostgreSQL's SHARE
-- UPDATE EXCLUSIVE lock, before T2-W activates the dispatched writer.
ALTER TABLE intake_outbox
    DROP CONSTRAINT intake_outbox_status_check;

ALTER TABLE intake_outbox
    ADD CONSTRAINT intake_outbox_status_check CHECK (status IN (
        'pending',
        'claimed',
        'accepted',
        'spawned',
        'dispatched',
        'done',
        'failed_pre_accept',
        'failed_post_accept'
    )) NOT VALID;

-- Keep the discriminator name stable for Rust's 23505 (UNIQUE violation)
-- classification in src/db/intake_outbox.rs.
DROP INDEX intake_outbox_one_open_route_per_channel;

ALTER INDEX intake_outbox_one_open_route_per_channel_t2m
    RENAME TO intake_outbox_one_open_route_per_channel;
