ALTER TABLE group_messages ADD UNIQUE KEY uk_group_messages_group_seq (group_id, seq);
