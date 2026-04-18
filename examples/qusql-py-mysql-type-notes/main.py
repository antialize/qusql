"""
qusql-py-mysql-type-notes -- a tiny note-taking CLI.

Every execute() call is checked by the mypy plugin at static-analysis time
against mysql-type-schema.sql in this directory.  Wrong column names, wrong
argument types, or wrong result-row types all become mypy errors; no running
database is required for the static check.

Usage (after following the README setup steps):

    python main.py add "Buy milk"
    python main.py add "Read the docs" "Start with the README"
    python main.py list
    python main.py pin 1
    python main.py delete 2
    python main.py list
"""

import os
import sys

import MySQLdb  # type: ignore[import-untyped]
import MySQLdb.cursors  # type: ignore[import-untyped]
from typing import cast
from qusql_mysql_type import execute


def connect() -> MySQLdb.Connection:
    url = os.environ.get(
        "DATABASE_URL", "mysql://notes:notes@127.0.0.1:3306/notes_example"
    )
    # Parse mysql://user:pass@host:port/db
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


def bootstrap(conn: MySQLdb.Connection) -> None:
    """Apply the schema (idempotent via IF NOT EXISTS)."""
    with open(os.path.join(os.path.dirname(__file__), "mysql-type-schema.sql")) as f:
        schema = f.read()
    c = conn.cursor()
    for stmt in schema.split(";"):
        stmt = stmt.strip()
        if stmt:
            c.execute(stmt)


def cmd_add(conn: MySQLdb.Connection, title: str, body: str | None) -> None:
    c = cast(MySQLdb.cursors.Cursor, conn.cursor())
    result = execute(
        c,
        "INSERT INTO `notes` (`title`, `body`) VALUES (%s, %s)",
        title,
        body,
    )
    print(f"Created note #{result.lastrowid}: {title!r}")


def cmd_list(conn: MySQLdb.Connection) -> None:
    c = cast(MySQLdb.cursors.Cursor, conn.cursor())
    rows = execute(
        c,
        "SELECT `id`, `title`, `body`, `pinned` FROM `notes`"
        " ORDER BY `pinned` DESC, `created_at` DESC",
    ).fetchall()
    # mypy infers: list[tuple[int, str, str | None, int]]
    if not rows:
        print("No notes.")
        return
    for note_id, title, body, pinned in rows:
        pin_marker = "*" if pinned else " "
        body_preview = f" -- {body[:40]}" if body else ""
        print(f"  [{pin_marker}] #{note_id}: {title}{body_preview}")


def cmd_pin(conn: MySQLdb.Connection, note_id: int) -> None:
    c = cast(MySQLdb.cursors.Cursor, conn.cursor())
    result = execute(
        c,
        "UPDATE `notes` SET `pinned` = NOT `pinned` WHERE `id` = %s",
        note_id,
    )
    if result.rowcount == 0:
        print(f"Note #{note_id} not found.", file=sys.stderr)
        sys.exit(1)
    print(f"Toggled pin on note #{note_id}.")


def cmd_delete(conn: MySQLdb.Connection, note_id: int) -> None:
    c = cast(MySQLdb.cursors.Cursor, conn.cursor())
    result = execute(
        c,
        "DELETE FROM `notes` WHERE `id` = %s",
        note_id,
    )
    if result.rowcount == 0:
        print(f"Note #{note_id} not found.", file=sys.stderr)
        sys.exit(1)
    print(f"Deleted note #{note_id}.")


def usage() -> None:
    print(
        "Usage:\n"
        "  python main.py add <title> [body]\n"
        "  python main.py list\n"
        "  python main.py pin <id>\n"
        "  python main.py delete <id>",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        usage()

    conn = connect()
    bootstrap(conn)

    match args[0]:
        case "add":
            if len(args) < 2:
                usage()
            cmd_add(conn, args[1], args[2] if len(args) > 2 else None)
        case "list":
            cmd_list(conn)
        case "pin":
            if len(args) < 2:
                usage()
            cmd_pin(conn, int(args[1]))
        case "delete":
            if len(args) < 2:
                usage()
            cmd_delete(conn, int(args[1]))
        case _:
            usage()
