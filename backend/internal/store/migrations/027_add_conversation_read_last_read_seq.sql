ALTER TABLE conversation_reads ADD COLUMN last_read_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER last_read_message_id;
