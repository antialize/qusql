-- qusql-py-mysql-type-books: library catalog schema.
--
-- Migration pattern: each revision is a stored procedure with an IF NOT EXISTS
-- guard against schema_revisions.  CALL-ing it on an already-migrated database
-- is a no-op.  The qusql-type schema evaluator executes every procedure body
-- when it processes the CALL, so the mypy plugin always sees the fully-migrated
-- schema regardless of the runtime guards.

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

        CREATE TABLE IF NOT EXISTS `loans` (
            `id`            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `book_id`       INT          NOT NULL,
            `borrower_name` VARCHAR(255) NOT NULL,
            `due_date`      DATE         NOT NULL,
            `returned_at`   TIMESTAMP,
            CONSTRAINT `fk_loans_book`
                FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    END IF;
END $$
DELIMITER ;

CALL `apply_revision_0`();
DROP PROCEDURE IF EXISTS `apply_revision_0`;

-- ---------------------------------------------------------------------------
-- Revision 1 - add reviews table
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
            `rating`        TINYINT      NOT NULL,
            `body`          TEXT,
            `reviewed_at`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT `fk_reviews_book`
                FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    END IF;
END $$
DELIMITER ;

CALL `apply_revision_1`();
DROP PROCEDURE IF EXISTS `apply_revision_1`;
