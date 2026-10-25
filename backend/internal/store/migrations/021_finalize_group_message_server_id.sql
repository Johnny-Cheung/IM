ALTER TABLE group_messages
    MODIFY COLUMN server_message_id BIGINT UNSIGNED NOT NULL,
    ADD UNIQUE KEY uk_group_messages_server_id (server_message_id),
    ADD KEY idx_group_messages_group_server (group_id, server_message_id),
    ADD KEY idx_group_messages_sender_server (sender_id, server_message_id);