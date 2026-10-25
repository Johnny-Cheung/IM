UPDATE message_id_sequences
SET last_message_id = GREATEST(
    last_message_id,
    COALESCE((SELECT MAX(server_message_id) FROM messages), 0),
    COALESCE((SELECT MAX(server_message_id) FROM group_messages), 0)
)
WHERE scope = 'direct';