CREATE TABLE IF NOT EXISTS moment_comments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    moment_id BIGINT UNSIGNED NOT NULL,
    author_id BIGINT UNSIGNED NOT NULL,
    body VARCHAR(500) NOT NULL,
    created_at DATETIME(3) NOT NULL,
    deleted_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    KEY idx_moment_comments_moment (moment_id, id),
    CONSTRAINT fk_moment_comments_moment FOREIGN KEY (moment_id) REFERENCES moments (id) ON DELETE CASCADE,
    CONSTRAINT fk_moment_comments_author FOREIGN KEY (author_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT chk_moment_comments_body CHECK (CHAR_LENGTH(TRIM(body)) BETWEEN 1 AND 500)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
