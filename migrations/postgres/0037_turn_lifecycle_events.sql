CREATE TABLE IF NOT EXISTS turn_lifecycle_events (
    id           BIGSERIAL PRIMARY KEY,
    turn_id      TEXT NOT NULL,
    channel_id   TEXT NOT NULL,
    session_key  TEXT,
    dispatch_id  TEXT,
    kind         TEXT NOT NULL,
    severity     TEXT NOT NULL,
    summary      TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_turn_lifecycle_events_turn_id
    ON turn_lifecycle_events(turn_id);

CREATE INDEX IF NOT EXISTS idx_turn_lifecycle_events_channel_id
    ON turn_lifecycle_events(channel_id);

CREATE INDEX IF NOT EXISTS idx_turn_lifecycle_events_created_at
    ON turn_lifecycle_events(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_turn_lifecycle_events_turn_channel_created_at
    ON turn_lifecycle_events(turn_id, channel_id, created_at DESC);
