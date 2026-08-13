-- Kakao multi-account ownership. Keep legacy primary ciphertext and existing
-- encrypted schedules untouched; only add non-secret identity/selection metadata.
ALTER TABLE oauth_connection_accounts
    ADD COLUMN subject_hash BYTEA;

ALTER TABLE oauth_connection_accounts
    ADD CONSTRAINT oauth_connection_accounts_subject_hash_size
    CHECK (subject_hash IS NULL OR octet_length(subject_hash) = 32);

CREATE UNIQUE INDEX oauth_connection_accounts_provider_subject_hash_uq
    ON oauth_connection_accounts (provider, subject_hash)
    WHERE subject_hash IS NOT NULL;

ALTER TABLE scheduled_messages
    ADD COLUMN external_delivery_account_key TEXT;

-- v1 plans were always sent by primary. This makes that legacy ownership
-- queryable for delete guards without decrypting or rewriting the plan.
UPDATE scheduled_messages
SET external_delivery_account_key = 'primary'
WHERE external_delivery_plan_id IS NOT NULL
  AND external_delivery_account_key IS NULL;

ALTER TABLE scheduled_messages
    ADD CONSTRAINT scheduled_messages_external_delivery_account_shape CHECK (
        (external_delivery_plan_id IS NULL AND external_delivery_account_key IS NULL)
        OR
        (external_delivery_plan_id IS NOT NULL AND external_delivery_account_key IS NOT NULL)
    );

CREATE INDEX scheduled_messages_external_delivery_account_active_idx
    ON scheduled_messages (external_delivery_account_key)
    WHERE external_delivery_account_key IS NOT NULL
      AND status IN ('scheduled', 'firing');

-- Account deletion and every new durable reference serialize on the account
-- row. A normal FK would prevent local disconnect forever because terminal
-- at-most-once tombstones intentionally outlive credentials; this trigger
-- takes only the row lock needed to close the active-reference race.
CREATE OR REPLACE FUNCTION agentdesk_lock_kakao_oauth_account_reference()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.provider = 'kakao' AND NOT EXISTS (
        SELECT 1
        FROM oauth_connection_accounts
        WHERE provider = NEW.provider
          AND account_key = NEW.account_key
        FOR KEY SHARE
    ) THEN
        RAISE EXCEPTION 'referenced Kakao account is not connected'
            USING ERRCODE = '23503';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_external_share_operation_kakao_account
BEFORE INSERT ON external_share_operations
FOR EACH ROW
EXECUTE FUNCTION agentdesk_lock_kakao_oauth_account_reference();

CREATE TRIGGER trg_external_share_outbox_kakao_account
BEFORE INSERT ON external_share_outbox
FOR EACH ROW
EXECUTE FUNCTION agentdesk_lock_kakao_oauth_account_reference();

CREATE OR REPLACE FUNCTION agentdesk_lock_scheduled_kakao_account_reference()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.external_delivery_account_key IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM oauth_connection_accounts
        WHERE provider = 'kakao'
          AND account_key = NEW.external_delivery_account_key
        FOR KEY SHARE
    ) THEN
        RAISE EXCEPTION 'referenced Kakao account is not connected'
            USING ERRCODE = '23503';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_scheduled_message_kakao_account
BEFORE INSERT OR UPDATE OF external_delivery_account_key ON scheduled_messages
FOR EACH ROW
EXECUTE FUNCTION agentdesk_lock_scheduled_kakao_account_reference();
