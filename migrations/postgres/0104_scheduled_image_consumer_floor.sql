-- Durable consumer floor for scheduled-message image attachments.
--
-- The API's worker capability gate prevents new image reservations while a
-- known online worker is old, but it cannot fence a pre-0103 process that
-- registers or resumes after that check. Image-bearing definitions and outbox
-- rows may therefore move into a delivery-owned state only from a transaction
-- that explicitly declares the v1 consumer capability. Older binaries cannot
-- make that declaration and fail closed at the database boundary.
--
-- This is deliberately a stop-and-drain binary floor, not a rolling migration.
-- A legacy consumer can claim several text and image rows in one transaction;
-- installing a row trigger while that consumer is live would either allow the
-- image through or abort the mixed batch. Lock the claim tables and refuse the
-- migration until every online node advertises the floor protocol and no image
-- claim is already in flight. See docs/agent-maintenance/multinode-transition.md.

LOCK TABLE worker_nodes, scheduled_messages, message_outbox
    IN SHARE ROW EXCLUSIVE MODE;

CREATE OR REPLACE FUNCTION agentdesk_assert_scheduled_image_consumer_floor_ready()
RETURNS VOID AS $$
DECLARE
    legacy_nodes TEXT;
    scheduled_claims BIGINT;
    outbox_claims BIGINT;
BEGIN
    SELECT string_agg(instance_id, ', ' ORDER BY instance_id)
      INTO legacy_nodes
      FROM worker_nodes
     WHERE status = 'online'
       AND COALESCE(
           capabilities #>> '{scheduled_messages,consumer_floor_v1}',
           'false'
       ) <> 'true';

    IF legacy_nodes IS NOT NULL THEN
        RAISE EXCEPTION
            'scheduled image consumer floor requires a stopped and upgraded fleet'
            USING ERRCODE = '55000',
                  DETAIL = format(
                      'online nodes missing scheduled_messages.consumer_floor_v1: %s',
                      legacy_nodes
                  ),
                  HINT = 'Stop and drain the fleet, mark verified stopped worker_nodes offline, then apply migration 0104 before restarting only floor-capable binaries.';
    END IF;

    SELECT COUNT(*)
      INTO scheduled_claims
      FROM scheduled_messages
     WHERE status = 'firing'
       AND image_data IS NOT NULL;

    SELECT COUNT(*)
      INTO outbox_claims
      FROM message_outbox
     WHERE status = 'processing'
       AND attachment_data IS NOT NULL;

    IF scheduled_claims > 0 OR outbox_claims > 0 THEN
        RAISE EXCEPTION
            'scheduled image consumer floor requires all image claims to be drained'
            USING ERRCODE = '55000',
                  DETAIL = format(
                      'image claims still in flight: scheduled_messages=%s, message_outbox=%s',
                      scheduled_claims,
                      outbox_claims
                  ),
                  HINT = 'Let the pre-0104 image-capable fleet finish or recover these claims before stopping it and applying migration 0104.';
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT agentdesk_assert_scheduled_image_consumer_floor_ready();

CREATE OR REPLACE FUNCTION agentdesk_require_scheduled_image_consumer_v1()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'firing'
       AND OLD.status IS DISTINCT FROM 'firing'
       AND NEW.image_data IS NOT NULL
       AND COALESCE(
           current_setting('agentdesk.scheduled_image_consumer_v1', true),
           ''
       ) <> 'enabled'
    THEN
        RAISE EXCEPTION
            'scheduled image attachment claim requires consumer capability v1'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_scheduled_image_consumer_v1 ON scheduled_messages;
CREATE TRIGGER trg_scheduled_image_consumer_v1
    BEFORE UPDATE ON scheduled_messages
    FOR EACH ROW
    EXECUTE FUNCTION agentdesk_require_scheduled_image_consumer_v1();

CREATE OR REPLACE FUNCTION agentdesk_require_outbox_image_consumer_v1()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'processing'
       AND NEW.attachment_data IS NOT NULL
       AND (
           OLD.status IS DISTINCT FROM 'processing'
           OR OLD.claimed_at IS DISTINCT FROM NEW.claimed_at
       )
       AND COALESCE(
           current_setting('agentdesk.scheduled_image_consumer_v1', true),
           ''
       ) <> 'enabled'
    THEN
        RAISE EXCEPTION
            'message outbox image attachment claim requires consumer capability v1'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_outbox_image_consumer_v1 ON message_outbox;
CREATE TRIGGER trg_outbox_image_consumer_v1
    BEFORE UPDATE ON message_outbox
    FOR EACH ROW
    EXECUTE FUNCTION agentdesk_require_outbox_image_consumer_v1();
