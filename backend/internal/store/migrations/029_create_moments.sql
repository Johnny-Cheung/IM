CREATE TABLE IF NOT EXISTS moments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    author_id BIGINT UNSIGNED NOT NULL,
    body TEXT NOT NULL,
    visibility VARCHAR(16) NOT NULL DEFAULT 'friends',
    like_count INT UNSIGNED NOT NULL DEFAULT 0,
    comment_count INT UNSIGNED NOT NULL DEFAULT 0,
    created_at DATETIME(3) NOT NULL,
    deleted_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    KEY idx_moments_author (author_id, id),
    CONSTRAINT fk_moments_author FOREIGN KEY (author_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_moments_visibility CHECK (visibility IN ('friends', 'private')),
    CONSTRAINT chk_moments_body CHECK (CHAR_LENGTH(TRIM(body)) BETWEEN 1 AND 1000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
