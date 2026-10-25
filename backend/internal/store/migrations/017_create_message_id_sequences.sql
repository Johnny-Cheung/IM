CREATE TABLE IF NOT EXISTS message_id_sequences (
    scope VARCHAR(32) NOT NULL,
    last_value BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (scope)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;