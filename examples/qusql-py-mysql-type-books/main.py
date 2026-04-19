"""
qusql-py-mysql-type-books -- library catalog demonstrating the migration pattern.

Every execute() call is checked by the mypy plugin against mysql-type-schema.sql
at static-analysis time.  No running database is needed for the type check.

The schema uses stored procedures as idempotent revision guards so that running
the file against an existing database is safe.  The qusql-type evaluator
executes each procedure body when it processes the CALL, so mypy always sees
the fully-migrated schema regardless of the runtime guards.

Run:
    cd examples/qusql-py-mysql-type-books
    mypy main.py                    # static type check
    DATABASE_URL=mysql://books:books@127.0.0.1:3306/books_example python main.py
"""

import datetime
import os

import MySQLdb  # type: ignore[import-untyped]
import MySQLdb.cursors  # type: ignore[import-untyped]
from typing import cast
from qusql_mysql_type import execute


def connect() -> MySQLdb.Connection:
    url = os.environ.get(
        "DATABASE_URL", "mysql://books:books@127.0.0.1:3306/books_example"
    )
    rest = url.removeprefix("mysql://")
    userinfo, hostdb = rest.split("@", 1)
    user, passwd = userinfo.split(":", 1)
    hostport, db = hostdb.split("/", 1)
    host, port_str = hostport.split(":", 1) if ":" in hostport else (hostport, "3306")
    return MySQLdb.connect(
        host=host,
        port=int(port_str),
        user=user,
        passwd=passwd,
        db=db,
        use_unicode=True,
        autocommit=True,
    )


def _split_sql(sql: str) -> list[str]:
    """Split a SQL script into statements, honouring DELIMITER directives."""
    delimiter = ";"
    pending = ""
    statements = []
    for line in sql.splitlines():
        if line.strip().upper().startswith("DELIMITER "):
            if pending.strip():
                statements.append(pending.strip())
                pending = ""
            delimiter = line.strip()[len("DELIMITER ") :].strip()
            continue
        pending += line + "\n"
        while delimiter in pending:
            head, pending = pending.split(delimiter, 1)
            if head.strip():
                statements.append(head.strip())
    if pending.strip():
        statements.append(pending.strip())
    return statements


def bootstrap(conn: MySQLdb.Connection) -> None:
    """Apply pending migrations.  Each statement is separated by ';'.
    On an empty database all revisions run; on an existing one the
    IF NOT EXISTS guards inside each procedure skip already-applied revisions."""
    with open(os.path.join(os.path.dirname(__file__), "mysql-type-schema.sql")) as f:
        schema = f.read()
    c = conn.cursor()
    for stmt in _split_sql(schema):
        c.execute(stmt)


def main() -> None:
    conn = connect()
    bootstrap(conn)
    c = cast(MySQLdb.cursors.Cursor, conn.cursor())

    # ------------------------------------------------------------------
    # Authors
    # ------------------------------------------------------------------

    # mypy checks that (str, str, str | None) matches (VARCHAR, VARCHAR, TEXT)
    author_res = execute(
        c,
        "INSERT INTO `authors` (`name`, `email`, `bio`) VALUES (%s, %s, %s)",
        "Ada Lovelace",
        "ada@lovelace.example",
        "English mathematician and the first computer programmer.",
    )
    author_id: int = author_res.lastrowid
    print(f"Created author #{author_id}")

    # ------------------------------------------------------------------
    # Books
    # ------------------------------------------------------------------

    # genre is ENUM -- the plugin accepts str for enum columns.
    # published_on is DATE -- datetime.date is the correct Python type.
    book1_res = execute(
        c,
        "INSERT INTO `books`"
        " (`author_id`, `title`, `isbn`, `published_on`, `genre`, `total_copies`)"
        " VALUES (%s, %s, %s, %s, %s, %s)",
        author_id,
        "Notes on the Analytical Engine",
        "978-0-000-00001-1",
        datetime.date(1843, 7, 10),
        "Science",
        3,
    )
    book1_id: int = book1_res.lastrowid

    book2_res = execute(
        c,
        "INSERT INTO `books`"
        " (`author_id`, `title`, `isbn`, `published_on`, `genre`, `total_copies`)"
        " VALUES (%s, %s, %s, %s, %s, %s)",
        author_id,
        "Sketch of the Analytical Engine",
        "978-0-000-00002-8",
        datetime.date(1843, 9, 1),
        "Science",
        2,
    )
    book2_id: int = book2_res.lastrowid

    # SELECT with JOIN.
    # mypy infers the row type as tuple[int, str, str, str, int]
    books = execute(
        c,
        "SELECT b.`id`, b.`title`, b.`isbn`, b.`genre`, b.`total_copies`"
        " FROM `books` b"
        " JOIN `authors` a ON a.`id` = b.`author_id`"
        " WHERE a.`id` = %s"
        " ORDER BY b.`published_on`",
        author_id,
    ).fetchall()

    print("\nBooks by Ada Lovelace:")
    for book_id, title, isbn, genre, copies in books:
        print(f"  [{book_id}] {title} (ISBN: {isbn}, genre: {genre}, {copies} copies)")

    # ------------------------------------------------------------------
    # Loans
    # ------------------------------------------------------------------

    # due_date is DATE NOT NULL -> datetime.date for both input and output.
    loan_res = execute(
        c,
        "INSERT INTO `loans` (`book_id`, `borrower_name`, `due_date`)"
        " VALUES (%s, %s, %s)",
        book1_id,
        "Charles Babbage",
        datetime.date(2026, 5, 15),
    )
    loan_id: int = loan_res.lastrowid
    print(
        f"\nLoan #{loan_id}: 'Notes on the Analytical Engine' issued to Charles Babbage"
    )

    # returned_at is TIMESTAMP NULL -- it does not appear in this SELECT.
    # mypy infers: list[tuple[int, str, str, datetime.date]]
    active = execute(
        c,
        "SELECT l.`id`, b.`title`, l.`borrower_name`, l.`due_date`"
        " FROM `loans` l"
        " JOIN `books` b ON b.`id` = l.`book_id`"
        " WHERE l.`returned_at` IS NULL"
        " ORDER BY l.`due_date`",
    ).fetchall()

    print(f"\nActive loans ({len(active)} total):")
    for lid, title, borrower, due in active:
        print(f"  [{lid}] '{title}' -> {borrower} (due {due})")

    execute(
        c,
        "UPDATE `loans` SET `returned_at` = NOW() WHERE `id` = %s",
        loan_id,
    )
    print(f"  Loan #{loan_id} returned.")

    # ------------------------------------------------------------------
    # Reviews
    # ------------------------------------------------------------------

    # body is TEXT (nullable) -- str | None accepted.
    # rating is TINYINT NOT NULL -- int accepted.
    execute(
        c,
        "INSERT INTO `reviews` (`book_id`, `reviewer_name`, `rating`, `body`)"
        " VALUES (%s, %s, %s, %s)",
        book1_id,
        "Charles Babbage",
        5,
        "An indispensable companion to the engine itself.",
    )
    execute(
        c,
        "INSERT INTO `reviews` (`book_id`, `reviewer_name`, `rating`, `body`)"
        " VALUES (%s, %s, %s, %s)",
        book2_id,
        "Charles Babbage",
        4,
        "A clear and faithful translation of Menabrea's memoir.",
    )

    # body is nullable TEXT -> str | None in the row tuple.
    # mypy infers: list[tuple[str, int, str | None]]
    reviews = execute(
        c,
        "SELECT r.`reviewer_name`, r.`rating`, r.`body`"
        " FROM `reviews` r"
        " WHERE r.`book_id` = %s"
        " ORDER BY r.`reviewed_at`",
        book1_id,
    ).fetchall()

    print("\nReviews of 'Notes on the Analytical Engine':")
    for reviewer, rating, body in reviews:
        comment = body or "(no comment)"
        print(f"  {rating} star(s) - {reviewer}: {comment}")

    # ------------------------------------------------------------------
    # Clean up
    # ------------------------------------------------------------------

    execute(c, "DELETE FROM `reviews` WHERE `book_id` = %s", book1_id)
    execute(c, "DELETE FROM `reviews` WHERE `book_id` = %s", book2_id)
    execute(
        c,
        "DELETE FROM `loans` WHERE `book_id` = %s OR `book_id` = %s",
        book1_id,
        book2_id,
    )
    execute(c, "DELETE FROM `books` WHERE `author_id` = %s", author_id)
    execute(c, "DELETE FROM `authors` WHERE `id` = %s", author_id)

    print("\nDone - demo data cleaned up.")


if __name__ == "__main__":
    main()
