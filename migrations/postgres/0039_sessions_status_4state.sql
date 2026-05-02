-- Forward replay for databases that already recorded legacy version 30 as
-- 0030_dispatch_outbox_claims. Keep both original 0030 files immutable and
-- apply the sessions status normalization under a fresh version only when
-- the runtime migrator detects the dispatch migration was the applied v30.

UPDATE sessions
   SET status = CASE
       WHEN status IS NULL OR BTRIM(status) = '' THEN 'idle'
       WHEN LOWER(BTRIM(status)) = 'working' AND COALESCE(active_children, 0) > 0 THEN 'awaiting_bg'
       WHEN LOWER(BTRIM(status)) = 'working' THEN 'turn_active'
       WHEN LOWER(BTRIM(status)) = 'idle' AND COALESCE(active_children, 0) > 0 THEN 'awaiting_bg'
       WHEN LOWER(BTRIM(status)) = 'idle' AND thread_channel_id IS NOT NULL THEN 'awaiting_user'
       WHEN LOWER(BTRIM(status)) IN ('turn_active', 'awaiting_bg', 'awaiting_user', 'idle', 'disconnected', 'aborted')
           THEN LOWER(BTRIM(status))
       WHEN active_dispatch_id IS NOT NULL THEN 'turn_active'
       ELSE 'idle'
   END;

ALTER TABLE sessions
    ALTER COLUMN status SET DEFAULT 'disconnected';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'sessions'::regclass
           AND conname = 'sessions_status_known_check'
    ) THEN
        ALTER TABLE sessions
            ADD CONSTRAINT sessions_status_known_check
            CHECK (status IN (
                'turn_active',
                'awaiting_bg',
                'awaiting_user',
                'idle',
                'disconnected',
                'aborted'
            ));
    END IF;
END $$;
