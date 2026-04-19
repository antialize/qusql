-- MariaDB/MySQL notes schema for the qusql-sqlx-type-mariadb-notes example.
-- No sql-product comment = MariaDB/MySQL mode (? argument placeholders).
CREATE TABLE IF NOT EXISTS notes (
    id    INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    body  TEXT
);
