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

CREATE INDEX scheduled_messages_external_delivery_account_active_idx
    ON scheduled_messages (external_delivery_account_key)
    WHERE external_delivery_account_key IS NOT NULL
      AND status IN ('scheduled', 'firing');
