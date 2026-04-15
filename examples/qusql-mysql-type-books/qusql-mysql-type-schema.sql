-- qusql-mysql-type-schema.sql -- Library catalog schema for qusql-mysql-type-books.
--
-- This file is idempotent: running it against a live database skips every
-- revision that has already been applied and leaves existing data untouched.
-- Running it against a fresh, empty database bootstraps the full schema.
--
-- Migration pattern
-- -----------------
-- Each revision is a stored procedure whose body starts with an IF NOT EXISTS
-- guard against schema_revisions.  CALL-ing the procedure on an already-
-- migrated database is a no-op.  The schema evaluator in qusql-type executes
-- the procedure body the same way it would against an empty database, so the
-- compile-time type checker always sees the fully-migrated schema.

CREATE TABLE IF NOT EXISTS `schema_revisions` (
    `id`             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name`           VARCHAR(255) NOT NULL UNIQUE,
    `sequence_index` INT          NOT NULL UNIQUE,
    `applied_at`     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Revision 0 - initial schema: authors, books, loans
-- ---------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `apply_revision_0`;
DELIMITER $$
CREATE PROCEDURE `apply_revision_0`()
MODIFIES SQL DATA
BEGIN
    IF NOT EXISTS (SELECT 1 FROM `schema_revisions` WHERE `name` = 'init') THEN
        INSERT INTO `schema_revisions` (`name`, `sequence_index`) VALUES ('init', 0);

        CREATE TABLE IF NOT EXISTS `authors` (
            `id`         INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `name`       VARCHAR(255) NOT NULL,
            `email`      VARCHAR(255) NOT NULL UNIQUE,
            `bio`        TEXT,
            `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE TABLE IF NOT EXISTS `books` (
            `id`           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `author_id`    INT          NOT NULL,
            `title`        VARCHAR(255) NOT NULL,
            `isbn`         VARCHAR(20)  NOT NULL UNIQUE,
            `published_on` DATE         NOT NULL,
            `genre`        ENUM('Fiction','NonFiction','Science','History','Biography','Children') NOT NULL,
            `total_copies` INT          NOT NULL DEFAULT 1,
            CONSTRAINT `fk_books_author`
                FOREIGN KEY (`author_id`) REFERENCES `authors` (`id`)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE INDEX IF NOT EXISTS `books_author_idx` ON `books` (`author_id`);
        CREATE INDEX IF NOT EXISTS `books_genre_idx`  ON `books` (`genre`);

        -- A loan tracks a single copy of a book checked out to a borrower.
        -- returned_at is NULL while the copy is still on loan.
        CREATE TABLE IF NOT EXISTS `loans` (
            `id`            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `book_id`       INT          NOT NULL,
            `borrower_name` VARCHAR(255) NOT NULL,
            `borrowed_at`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `due_date`      DATE         NOT NULL,
            `returned_at`   TIMESTAMP    NULL,
            CONSTRAINT `fk_loans_book`
                FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE INDEX IF NOT EXISTS `loans_book_idx`   ON `loans` (`book_id`);
        CREATE INDEX IF NOT EXISTS `loans_active_idx` ON `loans` (`book_id`, `returned_at`);
    END IF;
END $$
DELIMITER ;

CALL `apply_revision_0`();
DROP PROCEDURE IF EXISTS `apply_revision_0`;


-- ---------------------------------------------------------------------------
-- Revision 1 - add reviews
-- ---------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `apply_revision_1`;
DELIMITER $$
CREATE PROCEDURE `apply_revision_1`()
MODIFIES SQL DATA
BEGIN
    IF NOT EXISTS (SELECT 1 FROM `schema_revisions` WHERE `name` = 'add_reviews') THEN
        INSERT INTO `schema_revisions` (`name`, `sequence_index`) VALUES ('add_reviews', 1);

        CREATE TABLE IF NOT EXISTS `reviews` (
            `id`            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `book_id`       INT          NOT NULL,
            `reviewer_name` VARCHAR(255) NOT NULL,
            -- 1 = poor ... 5 = excellent
            `rating`        TINYINT      NOT NULL,
            `body`          TEXT,
            `reviewed_at`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT `fk_reviews_book`
                FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE INDEX IF NOT EXISTS `reviews_book_idx` ON `reviews` (`book_id`);
    END IF;
END $$
DELIMITER ;

CALL `apply_revision_1`();
DROP PROCEDURE IF EXISTS `apply_revision_1`;
