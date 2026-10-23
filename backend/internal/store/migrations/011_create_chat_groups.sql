CREATE TABLE IF NOT EXISTS chat_groups (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(30) NOT NULL,
    owner_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL,
    dissolved_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    KEY idx_chat_groups_owner (owner_id),
    CONSTRAINT fk_chat_groups_owner FOREIGN KEY (owner_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_chat_groups_name CHECK (CHAR_LENGTH(TRIM(name)) BETWEEN 1 AND 30)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
