CREATE TABLE IF NOT EXISTS conversation_reads (
    user_id BIGINT UNSIGNED NOT NULL,
    peer_user_id BIGINT UNSIGNED NOT NULL,
    last_read_message_id BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    PRIMARY KEY (user_id, peer_user_id),
    KEY idx_conversation_reads_peer (peer_user_id, user_id),
    CONSTRAINT fk_conversation_reads_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_conversation_reads_peer FOREIGN KEY (peer_user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_conversation_reads_not_self CHECK (user_id <> peer_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
