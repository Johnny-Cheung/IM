ALTER TABLE friend_requests
    ADD COLUMN pending_from_user_id BIGINT UNSIGNED NULL AFTER status,
    ADD COLUMN pending_to_user_id BIGINT UNSIGNED NULL AFTER pending_from_user_id,
    ADD UNIQUE KEY uk_friend_requests_pending (pending_from_user_id, pending_to_user_id);
