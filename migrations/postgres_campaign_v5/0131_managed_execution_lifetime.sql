-- An expiring delivery lease cannot prove an asynchronous writer stopped.
-- Keep the existing execution lease as the admission owner; no second queue.
ALTER TABLE node_execution_leases
    ADD COLUMN managed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN resource_key TEXT,
    ADD COLUMN execution_state TEXT NOT NULL DEFAULT 'reserved'
        CHECK (execution_state IN ('reserved', 'running', 'unknown', 'quiesced')),
    ADD COLUMN deadline_at TIMESTAMPTZ,
    ADD COLUMN max_tool_calls INTEGER,
    ADD COLUMN permission_ids TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN provider_session_id TEXT,
    ADD COLUMN runtime_identity JSONB CHECK (runtime_identity IS NULL OR jsonb_typeof(runtime_identity)='object'),
    ADD COLUMN stop_observation JSONB CHECK (stop_observation IS NULL OR jsonb_typeof(stop_observation)='object'),
    ADD COLUMN stop_observed_at TIMESTAMPTZ,
    ADD COLUMN stop_reason TEXT,
    ADD CONSTRAINT managed_execution_requires_runtime CHECK (
        NOT managed OR execution_state='reserved'
        OR (runtime_identity IS NOT NULL AND COALESCE(runtime_identity->>'version'='1',FALSE))
    ),
    ADD CONSTRAINT managed_execution_has_bounds CHECK (
        NOT managed OR (resource_key IS NOT NULL AND deadline_at IS NOT NULL
                        AND max_tool_calls IS NOT NULL
                        AND max_tool_calls BETWEEN 1 AND 10000)
    );
CREATE INDEX node_execution_resource ON node_execution_leases(instance_id, resource_key);

CREATE OR REPLACE FUNCTION node_execution_occupancy(node_id TEXT)
RETURNS TABLE(provider TEXT, channel_id TEXT) LANGUAGE sql VOLATILE AS $$
    SELECT provider, channel_id FROM intake_outbox
     WHERE target_instance_id=node_id
       AND status IN ('pending','claimed','accepted','spawned','dispatched')
    UNION
    SELECT provider, channel_id FROM node_execution_leases
     WHERE instance_id=node_id AND (managed OR expires_at>clock_timestamp())
$$;

CREATE OR REPLACE FUNCTION guard_provider_execution_capacity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- This also holds the node advisory lock through the conflict check/insert.
    PERFORM reserve_node_execution(NEW.instance_id,NEW.provider,NEW.channel_id);
    IF NEW.resource_key IS NOT NULL AND EXISTS (
        SELECT 1 FROM node_execution_leases occupied_lease
         WHERE occupied_lease.instance_id=NEW.instance_id AND occupied_lease.resource_key=NEW.resource_key
           AND (NEW.managed OR occupied_lease.managed)
           AND (occupied_lease.managed OR occupied_lease.expires_at>clock_timestamp())
    ) THEN
        RAISE EXCEPTION 'managed execution resource remains occupied'
          USING ERRCODE='23514', CONSTRAINT='managed_execution_resource_available';
    END IF;
    RETURN NEW;
END
$$;

-- A stale TTL sweeper (including an older binary) must not delete an unknown
-- writer. Only an explicit, nonce-fenced quiescence transition permits release.
CREATE FUNCTION retain_unquiesced_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.managed AND OLD.execution_state IN ('running','unknown') THEN
        RETURN NULL;
    END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER retain_unquiesced_execution BEFORE DELETE ON node_execution_leases
    FOR EACH ROW EXECUTE FUNCTION retain_unquiesced_execution();
