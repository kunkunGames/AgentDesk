-- Kakao friend share is an operator-owned, optional OAuth integration. The
-- tables deliberately persist only encrypted credentials, hashed browser/
-- idempotency values, and aggregate delivery outcomes. Friend UUIDs, nicknames,
-- message text, authorization codes, and raw OAuth state never enter storage.

CREATE TABLE oauth_connection_sessions (
    id UUID PRIMARY KEY,
    provider TEXT NOT NULL,
    state_hash BYTEA NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oauth_connection_sessions_provider_state_unique
        UNIQUE (provider, state_hash),
    CONSTRAINT oauth_connection_sessions_state_hash_size
        CHECK (octet_length(state_hash) = 32),
    CONSTRAINT oauth_connection_sessions_provider_nonempty
        CHECK (length(provider) BETWEEN 1 AND 64)
);

CREATE INDEX oauth_connection_sessions_expiry_idx
    ON oauth_connection_sessions (expires_at);

CREATE TABLE oauth_connection_accounts (
    provider TEXT NOT NULL,
    account_key TEXT NOT NULL,
    token_ciphertext BYTEA NOT NULL,
    token_nonce BYTEA NOT NULL,
    key_version SMALLINT NOT NULL DEFAULT 1,
    scopes TEXT[] NOT NULL DEFAULT '{}',
    access_expires_at TIMESTAMPTZ,
    refresh_expires_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'active',
    refresh_lease_id UUID,
    refresh_lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, account_key),
    CONSTRAINT oauth_connection_accounts_status_check
        CHECK (status IN ('active', 'consent_incomplete', 'reauth_required')),
    CONSTRAINT oauth_connection_accounts_nonce_size
        CHECK (octet_length(token_nonce) = 24),
    CONSTRAINT oauth_connection_accounts_key_version_check
        CHECK (key_version > 0),
    CONSTRAINT oauth_connection_accounts_refresh_lease_pair CHECK (
        (refresh_lease_id IS NULL AND refresh_lease_expires_at IS NULL) OR
        (refresh_lease_id IS NOT NULL AND refresh_lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX oauth_connection_accounts_refresh_lease_idx
    ON oauth_connection_accounts (refresh_lease_expires_at)
    WHERE refresh_lease_id IS NOT NULL;

CREATE TABLE external_share_operations (
    operation_id UUID PRIMARY KEY,
    provider TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    account_key TEXT NOT NULL,
    idempotency_key_hash BYTEA NOT NULL,
    request_fingerprint BYTEA NOT NULL,
    state TEXT NOT NULL,
    safe_summary JSONB,
    dispatch_deadline TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT external_share_operations_key_unique
        UNIQUE (provider, channel_id, account_key, idempotency_key_hash),
    CONSTRAINT external_share_operations_state_check
        CHECK (state IN ('dispatching', 'success', 'partial_success', 'failed', 'unknown')),
    CONSTRAINT external_share_operations_idempotency_hash_size
        CHECK (octet_length(idempotency_key_hash) = 32),
    CONSTRAINT external_share_operations_fingerprint_size
        CHECK (octet_length(request_fingerprint) = 32),
    CONSTRAINT external_share_operations_terminal_summary_check CHECK (
        (state = 'dispatching' AND safe_summary IS NULL) OR
        (state <> 'dispatching' AND safe_summary IS NOT NULL)
    )
);

CREATE INDEX external_share_operations_rate_window_idx
    ON external_share_operations (provider, channel_id, account_key, created_at DESC);

COMMENT ON TABLE external_share_operations IS
    'Non-reclaiming at-most-once fence for non-idempotent external provider POSTs.';
