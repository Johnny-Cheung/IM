ALTER TABLE sessions
    ADD COLUMN active_user_id BIGINT UNSIGNED
        GENERATED ALWAYS AS (IF(revoked_at IS NULL, user_id, NULL)) STORED,
    ADD UNIQUE KEY uk_sessions_active_user (active_user_id);
