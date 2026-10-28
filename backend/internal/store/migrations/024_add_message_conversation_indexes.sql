ALTER TABLE messages
    ADD UNIQUE KEY uk_messages_conversation_seq (conversation_low_id, conversation_high_id, seq),
    ADD KEY idx_messages_conversation_server (conversation_low_id, conversation_high_id, server_message_id);
