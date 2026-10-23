ALTER TABLE friend_requests
    ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'pending' AFTER to_user_id,
    ADD COLUMN handled_at DATETIME(3) NULL AFTER created_at,
    ADD KEY idx_friend_requests_from (from_user_id, created_at),
    ADD CONSTRAINT chk_friend_requests_status
        CHECK (status IN ('pending', 'accepted', 'rejected'));
