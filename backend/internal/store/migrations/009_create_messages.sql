CREATE TABLE IF NOT EXISTS messages (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sender_id BIGINT UNSIGNED NOT NULL,
    recipient_id BIGINT UNSIGNED NOT NULL,
    client_message_id VARCHAR(64) NOT NULL,
    body TEXT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_messages_client (sender_id, recipient_id, client_message_id),
    KEY idx_messages_pair_forward (sender_id, recipient_id, id),
    KEY idx_messages_pair_reverse (recipient_id, sender_id, id),
    KEY idx_messages_sender (sender_id, id),
    KEY idx_messages_recipient (recipient_id, id),
    CONSTRAINT fk_messages_sender FOREIGN KEY (sender_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_recipient FOREIGN KEY (recipient_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_messages_not_self CHECK (sender_id <> recipient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
