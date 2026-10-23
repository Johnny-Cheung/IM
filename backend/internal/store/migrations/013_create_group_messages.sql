CREATE TABLE IF NOT EXISTS group_messages (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    group_id BIGINT UNSIGNED NOT NULL,
    sender_id BIGINT UNSIGNED NOT NULL,
    client_message_id VARCHAR(64) NOT NULL,
    body TEXT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_group_messages_client (group_id, sender_id, client_message_id),
    KEY idx_group_messages_group (group_id, id),
    KEY idx_group_messages_sender (sender_id, id),
    CONSTRAINT fk_group_messages_group FOREIGN KEY (group_id) REFERENCES chat_groups (id) ON DELETE CASCADE,
    CONSTRAINT fk_group_messages_sender FOREIGN KEY (sender_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
