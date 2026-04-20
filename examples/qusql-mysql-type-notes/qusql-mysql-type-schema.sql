-- Notes schema for qusql-mysql-type-notes (qusql-mysql-type-schema.sql).
--
-- Uses a single CREATE TABLE IF NOT EXISTS for a plain idempotent bootstrap.
-- Re-running this file against a live database is safe: the table is
-- created when absent and left untouched when it already exists.
--
-- For a more complete migration pattern that tracks applied revisions and
-- supports incremental schema evolution without touching existing data, see
-- examples/qusql-mysql-type-books/qusql-mysql-type-schema.sql.

CREATE TABLE IF NOT EXISTS `notes` (
    `id`         INT           NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `title`      VARCHAR(255)  NOT NULL,
    `body`       TEXT,
    `pinned`     TINYINT(1)    NOT NULL DEFAULT FALSE,
    `created_at` TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
