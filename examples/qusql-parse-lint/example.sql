-- example.sql -- library catalog schema for qusql-parse-lint.
--
-- All keywords are ALL CAPS and every identifier is backtick-quoted, so
-- running the linter produces no warnings.  Try removing quotes or lowercasing
-- keywords to see the linter flag them.

CREATE TABLE IF NOT EXISTS `authors` (
    `id`         INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name`       VARCHAR(255) NOT NULL,
    `email`      VARCHAR(255) NOT NULL UNIQUE,
    `bio`        TEXT,
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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
);

CREATE TABLE IF NOT EXISTS `loans` (
    `id`            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `book_id`       INT          NOT NULL,
    `borrower_name` VARCHAR(255) NOT NULL,
    `due_date`      DATE         NOT NULL,
    `returned_at`   TIMESTAMP,
    CONSTRAINT `fk_loans_book`
        FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Typical read queries.
SELECT `b`.`id`, `b`.`title`, `b`.`isbn`, `b`.`genre`, `b`.`total_copies`
FROM   `books` `b`
JOIN   `authors` `a` ON `a`.`id` = `b`.`author_id`
WHERE  `a`.`id` = ?
ORDER  BY `b`.`published_on`;

SELECT `l`.`id`, `b`.`title`, `l`.`borrower_name`, `l`.`due_date`
FROM   `loans` `l`
JOIN   `books` `b` ON `b`.`id` = `l`.`book_id`
WHERE  `l`.`returned_at` IS NULL
ORDER  BY `l`.`due_date`;
