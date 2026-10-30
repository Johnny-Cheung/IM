CREATE TABLE IF NOT EXISTS moment_likes (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    moment_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_moment_likes_member (moment_id, user_id),
    KEY idx_moment_likes_moment (moment_id, id),
    KEY idx_moment_likes_user (user_id, moment_id),
    CONSTRAINT fk_moment_likes_moment FOREIGN KEY (moment_id) REFERENCES moments (id) ON DELETE CASCADE,
    CONSTRAINT fk_moment_likes_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
