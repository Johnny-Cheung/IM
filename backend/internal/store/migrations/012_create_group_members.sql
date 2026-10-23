CREATE TABLE IF NOT EXISTS group_members (
    group_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    joined_at DATETIME(3) NOT NULL,
    last_read_message_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, user_id),
    KEY idx_group_members_user (user_id, group_id),
    CONSTRAINT fk_group_members_group FOREIGN KEY (group_id) REFERENCES chat_groups (id) ON DELETE CASCADE,
    CONSTRAINT fk_group_members_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
