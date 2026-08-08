ALTER TABLE private_messages ADD COLUMN client_msg_id VARCHAR(128) NULL;
ALTER TABLE group_messages ADD COLUMN client_msg_id VARCHAR(128) NULL;
CREATE INDEX idx_private_client_msg ON private_messages (sender_id, client_msg_id);
CREATE INDEX idx_group_client_msg ON group_messages (sender_id, client_msg_id);
