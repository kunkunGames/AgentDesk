-- Durable consumer floor for scheduled-message image attachments.
--
-- The API's worker capability gate prevents new image reservations while a
-- known online worker is old, but it cannot fence a pre-0103 process that
-- registers or resumes after that check. Image-bearing definitions and outbox
-- rows may therefore move into a delivery-owned state only from a transaction
-- that explicitly declares the v1 consumer capability. Older binaries cannot
-- make that declaration and fail closed at the database boundary.

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
