CREATE TABLE IF NOT EXISTS `notes` (
    `id`         INT           NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `title`      VARCHAR(255)  NOT NULL,
    `body`       TEXT,
    `pinned`     TINYINT(1)    NOT NULL DEFAULT 0,
    `created_at` TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
