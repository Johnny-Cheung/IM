CREATE TABLE IF NOT EXISTS friend_requests (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    from_user_id BIGINT UNSIGNED NOT NULL,
    to_user_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_friend_requests_direction (from_user_id, to_user_id),
    KEY idx_friend_requests_to (to_user_id, created_at),
    CONSTRAINT fk_friend_requests_from FOREIGN KEY (from_user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_friend_requests_to FOREIGN KEY (to_user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_friend_requests_not_self CHECK (from_user_id <> to_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
