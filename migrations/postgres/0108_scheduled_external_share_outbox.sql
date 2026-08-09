-- Scheduled push fan-out for external providers.
--
-- Provider targets stay encrypted while a reservation is active. At fire time
-- the scheduler atomically creates both the Discord message_outbox row and an
-- encrypted external_share_outbox row. Provider workers then own their own
-- delivery state without coupling retries or risking a duplicate Discord send.

-- This is a stop-and-drain consumer floor. A pre-0108 scheduled worker would
-- treat a provider-targeted push as Discord-only, so refuse installation while
-- any online node lacks the new contract and fence every later claim with a
-- transaction-local capability declaration.
LOCK TABLE worker_nodes, scheduled_messages IN SHARE ROW EXCLUSIVE MODE;

CREATE OR REPLACE FUNCTION agentdesk_assert_scheduled_external_delivery_consumer_v1_ready()
RETURNS VOID AS $$
DECLARE
    legacy_nodes TEXT;
BEGIN
    SELECT string_agg(instance_id, ', ' ORDER BY instance_id)
      INTO legacy_nodes
      FROM worker_nodes
     WHERE status = 'online'
       AND COALESCE(
           capabilities #>> '{scheduled_messages,external_delivery_consumer_v1}',
           'false'
       ) <> 'true';

    IF legacy_nodes IS NOT NULL THEN
        RAISE EXCEPTION
            'scheduled external delivery consumer floor requires a stopped and upgraded fleet'
            USING ERRCODE = '55000',
                  DETAIL = format(
                      'online nodes missing scheduled_messages.external_delivery_consumer_v1: %s',
                      legacy_nodes
                  ),
                  HINT = 'Stop and drain the fleet, mark verified stopped worker_nodes offline, then apply migration 0108 before restarting only floor-capable binaries.';
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT agentdesk_assert_scheduled_external_delivery_consumer_v1_ready();

ALTER TABLE scheduled_messages
    ADD COLUMN external_delivery_plan_id UUID,
    ADD COLUMN external_delivery_plan_ciphertext BYTEA,
    ADD COLUMN external_delivery_plan_nonce BYTEA,
    ADD COLUMN external_delivery_plan_key_version SMALLINT,
    ADD COLUMN external_delivery_summary JSONB,
    ADD COLUMN external_delivery_plan_scrubbed_at TIMESTAMPTZ;

ALTER TABLE scheduled_messages
    ADD CONSTRAINT chk_smsg_external_delivery_plan_shape CHECK (
        (
            external_delivery_plan_id IS NULL
            AND external_delivery_plan_ciphertext IS NULL
            AND external_delivery_plan_nonce IS NULL
            AND external_delivery_plan_key_version IS NULL
            AND external_delivery_summary IS NULL
            AND external_delivery_plan_scrubbed_at IS NULL
        ) OR (
            external_delivery_plan_id IS NOT NULL
            AND external_delivery_summary IS NOT NULL
            AND delivery_kind = 'push'
            AND (
                (
                    external_delivery_plan_ciphertext IS NOT NULL
                    AND external_delivery_plan_nonce IS NOT NULL
                    AND octet_length(external_delivery_plan_nonce) = 24
                    AND external_delivery_plan_key_version > 0
                    AND external_delivery_plan_scrubbed_at IS NULL
                ) OR (
                    external_delivery_plan_ciphertext IS NULL
                    AND external_delivery_plan_nonce IS NULL
                    AND external_delivery_plan_key_version IS NULL
                    AND external_delivery_plan_scrubbed_at IS NOT NULL
                )
            )
        )
    );

COMMENT ON COLUMN scheduled_messages.external_delivery_plan_ciphertext IS
    'Encrypted provider target plan. Raw recipient identifiers never enter plaintext storage.';
COMMENT ON COLUMN scheduled_messages.external_delivery_summary IS
    'PII-free API summary keyed by provider target.';

CREATE TABLE external_share_outbox (
    id UUID PRIMARY KEY,
    provider TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    account_key TEXT NOT NULL,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    scheduled_delivery_id TEXT NOT NULL REFERENCES scheduled_message_deliveries(id),
    requested_count SMALLINT NOT NULL,
    payload_ciphertext BYTEA,
    payload_nonce BYTEA,
    payload_key_version SMALLINT,
    status TEXT NOT NULL DEFAULT 'pending',
    claim_owner TEXT,
    claim_token UUID,
    claimed_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count SMALLINT NOT NULL DEFAULT 0,
    deliver_before TIMESTAMPTZ,
    operation_id UUID REFERENCES external_share_operations(operation_id),
    safe_summary JSONB,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    CONSTRAINT uq_external_share_outbox_source
        UNIQUE (provider, channel_id, account_key, source, source_key),
    CONSTRAINT chk_external_share_outbox_status CHECK (
        status IN ('pending', 'processing', 'success', 'partial_success', 'failed', 'unknown')
    ),
    CONSTRAINT chk_external_share_outbox_requested_count
        CHECK (requested_count BETWEEN 1 AND 1000),
    CONSTRAINT chk_external_share_outbox_retry_count
        CHECK (retry_count BETWEEN 0 AND 100),
    CONSTRAINT chk_external_share_outbox_claim_shape CHECK (
        (status = 'processing' AND claim_owner IS NOT NULL AND claim_token IS NOT NULL
            AND claimed_at IS NOT NULL)
        OR
        (status <> 'processing' AND claim_owner IS NULL AND claim_token IS NULL
            AND claimed_at IS NULL)
    ),
    CONSTRAINT chk_external_share_outbox_payload_shape CHECK (
        (
            status IN ('pending', 'processing')
            AND payload_ciphertext IS NOT NULL
            AND payload_nonce IS NOT NULL
            AND octet_length(payload_nonce) = 24
            AND payload_key_version > 0
            AND safe_summary IS NULL
            AND finished_at IS NULL
        ) OR (
            status IN ('success', 'partial_success', 'failed', 'unknown')
            AND payload_ciphertext IS NULL
            AND payload_nonce IS NULL
            AND payload_key_version IS NULL
            AND safe_summary IS NOT NULL
            AND finished_at IS NOT NULL
        )
    )
);

CREATE INDEX idx_external_share_outbox_claim
    ON external_share_outbox (next_attempt_at, created_at)
    WHERE status IN ('pending', 'processing');

CREATE INDEX idx_external_share_outbox_scheduled_delivery
    ON external_share_outbox (scheduled_delivery_id, created_at);

COMMENT ON TABLE external_share_outbox IS
    'Durable encrypted handoff queue for scheduled external-provider deliveries.';

-- Active recurring definitions need their encrypted target plan for the next
-- occurrence. Terminal definitions retain only the safe provider/count summary.
CREATE OR REPLACE FUNCTION agentdesk_scrub_terminal_scheduled_external_delivery_plan()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status IN ('sent', 'failed', 'canceled', 'expired')
       AND NEW.external_delivery_plan_id IS NOT NULL
       AND NEW.external_delivery_plan_ciphertext IS NOT NULL THEN
        NEW.external_delivery_plan_ciphertext := NULL;
        NEW.external_delivery_plan_nonce := NULL;
        NEW.external_delivery_plan_key_version := NULL;
        NEW.external_delivery_plan_scrubbed_at := NOW();
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_scrub_terminal_scheduled_external_delivery_plan
    ON scheduled_messages;
CREATE TRIGGER trg_scrub_terminal_scheduled_external_delivery_plan
BEFORE INSERT OR UPDATE OF status ON scheduled_messages
FOR EACH ROW
EXECUTE FUNCTION agentdesk_scrub_terminal_scheduled_external_delivery_plan();

CREATE OR REPLACE FUNCTION agentdesk_require_scheduled_external_delivery_consumer_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'firing'
       AND OLD.status IS DISTINCT FROM 'firing'
       AND NEW.external_delivery_plan_id IS NOT NULL
       AND COALESCE(
           current_setting(
               'agentdesk.scheduled_external_delivery_consumer_v1',
               true
           ),
           ''
       ) <> 'enabled' THEN
        RAISE EXCEPTION
            'scheduled external delivery claim requires consumer capability v1'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_scheduled_external_delivery_consumer_v1
    ON scheduled_messages;
CREATE TRIGGER trg_scheduled_external_delivery_consumer_v1
BEFORE UPDATE ON scheduled_messages
FOR EACH ROW
EXECUTE FUNCTION agentdesk_require_scheduled_external_delivery_consumer_v1();
