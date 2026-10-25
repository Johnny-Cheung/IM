INSERT INTO message_id_sequences (scope, last_message_id)
SELECT 'direct', COALESCE(MAX(server_message_id), 0)
FROM messages;