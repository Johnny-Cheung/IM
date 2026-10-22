CREATE TABLE IF NOT EXISTS friendships (
    user_id BIGINT UNSIGNED NOT NULL,
    friend_user_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (user_id, friend_user_id),
    KEY idx_friendships_friend (friend_user_id),
    CONSTRAINT fk_friendships_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_friendships_friend FOREIGN KEY (friend_user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_friendships_not_self CHECK (user_id <> friend_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
