ALTER TABLE messages
    ADD COLUMN conversation_low_id BIGINT UNSIGNED NULL AFTER recipient_id,
    ADD COLUMN conversation_high_id BIGINT UNSIGNED NULL AFTER conversation_low_id,
    ADD COLUMN seq BIGINT UNSIGNED NULL AFTER server_message_id;
