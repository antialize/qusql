"""
Integration test for qusql-mysql-type + qusql-mysql-type-plugin.

Run from this directory so the plugin can find mysql-type-schema.sql:

    cd qusql-py-mysql-type-test
    mypy test.py          # static type check
    python test.py        # runtime check (needs a MariaDB instance)

The MariaDB connection parameters can be overridden with environment variables:
    DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
"""

import os
import sys
from typing import assert_type

import MySQLdb  # type: ignore[import-untyped]
import MySQLdb.cursors  # type: ignore[import-untyped]
from qusql_mysql_type import execute


def connect() -> MySQLdb.Connection:
    return MySQLdb.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", "1235")),
        user=os.environ.get("DB_USER", "root"),
        passwd=os.environ.get("DB_PASS", "test"),
        db=os.environ.get("DB_NAME", "test"),
        use_unicode=True,
        autocommit=True,
    )


def setup(connection: MySQLdb.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS `py_test`")
    cursor.execute(
        """
        CREATE TABLE `py_test` (
            `id`            int(11) NOT NULL AUTO_INCREMENT,
            `name`          varchar(100) NOT NULL,
            `value`         int NOT NULL,
            `optional_text` text,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
    )


def test_insert_returns_lastrowid(connection: MySQLdb.Connection) -> None:
    c: MySQLdb.cursors.Cursor = connection.cursor()
    result = execute(
        c,
        "INSERT INTO `py_test` (`name`, `value`) VALUES (%s, %s)",
        "hello",
        42,
    )
    from qusql_mysql_type import InsertWithLastRowIdResult

    assert_type(result, InsertWithLastRowIdResult)
    row_id: int = result.lastrowid
    assert row_id > 0, f"Expected a positive lastrowid, got {row_id}"


def test_select_tuple_types(connection: MySQLdb.Connection) -> None:
    c: MySQLdb.cursors.Cursor = connection.cursor()
    execute(
        c,
        "INSERT INTO `py_test` (`name`, `value`, `optional_text`) VALUES (%s, %s, %s)",
        "world",
        99,
        "some text",
    )

    row = execute(
        c,
        "SELECT `name`, `value`, `optional_text` FROM `py_test` WHERE `name` = %s",
        "world",
    ).fetchone()

    # mypy knows: tuple[str, int, str | None]
    assert_type(row, tuple[str, int, str | None] | None)
    assert row is not None
    name, value, optional_text = row
    assert name == "world"
    assert value == 99
    assert optional_text == "some text"


def test_select_nullable(connection: MySQLdb.Connection) -> None:
    c: MySQLdb.cursors.Cursor = connection.cursor()
    execute(
        c,
        "INSERT INTO `py_test` (`name`, `value`) VALUES (%s, %s)",
        "nullable",
        0,
    )

    row = execute(
        c,
        "SELECT `optional_text` FROM `py_test` WHERE `name` = %s",
        "nullable",
    ).fetchone()

    assert_type(row, tuple[str | None] | None)
    assert row is not None
    (optional_text,) = row
    # mypy knows optional_text is str | None
    assert optional_text is None


def test_fetchall(connection: MySQLdb.Connection) -> None:
    c: MySQLdb.cursors.Cursor = connection.cursor()
    rows = execute(
        c,
        "SELECT `name`, `value` FROM `py_test`",
    ).fetchall()

    assert_type(rows, list[tuple[str, int]])
    assert len(rows) >= 3
    for name, value in rows:
        assert isinstance(name, str)
        assert isinstance(value, int)


def test_list_hack(connection: MySQLdb.Connection) -> None:
    c: MySQLdb.cursors.Cursor = connection.cursor()
    rows = execute(
        c,
        "SELECT `name` FROM `py_test` WHERE `value` IN (_LIST_)",
        [0, 42, 99],
    ).fetchall()

    # assert_type errors at mypy-time if the plugin returns Any or a wrong type
    assert_type(rows, list[tuple[str]])
    assert len(rows) == 3


def test_list_hack_scalar_before_list(connection: MySQLdb.Connection) -> None:
    # Scalar arg appearing before the _LIST_ arg must end up in the right position.
    c: MySQLdb.cursors.Cursor = connection.cursor()
    rows = execute(
        c,
        "SELECT `name` FROM `py_test` WHERE `name` != %s AND `value` IN (_LIST_)",
        "hello",
        [0, 42, 99],
    ).fetchall()

    # "hello" (value=42) is excluded by the name filter; "world" (99) and "nullable" (0) remain
    assert len(rows) == 2


def test_list_hack_scalar_after_list(connection: MySQLdb.Connection) -> None:
    # Scalar arg appearing after the _LIST_ arg must end up in the right position.
    c: MySQLdb.cursors.Cursor = connection.cursor()
    rows = execute(
        c,
        "SELECT `name` FROM `py_test` WHERE `value` IN (_LIST_) AND `name` != %s",
        [0, 42, 99],
        "hello",
    ).fetchall()

    # Same filter as above, different arg order
    assert len(rows) == 2


def test_list_hack_empty_list(connection: MySQLdb.Connection) -> None:
    # An empty list produces IN (NULL), which matches no rows.
    c: MySQLdb.cursors.Cursor = connection.cursor()
    rows = execute(
        c,
        "SELECT `name` FROM `py_test` WHERE `value` IN (_LIST_)",
        [],
    ).fetchall()

    assert len(rows) == 0


def test_insert_returning(connection: MySQLdb.Connection) -> None:
    c: MySQLdb.cursors.Cursor = connection.cursor()
    row = execute(
        c,
        "INSERT INTO `py_test` (`name`, `value`) VALUES (%s, %s) RETURNING `id`, `name`",
        "returning_insert",
        7,
    ).fetchone()

    # mypy knows: tuple[int, str]
    assert_type(row, tuple[int, str] | None)
    assert row is not None
    inserted_id, inserted_name = row
    assert inserted_id > 0
    assert inserted_name == "returning_insert"


if __name__ == "__main__":
    conn = connect()
    setup(conn)

    tests = [
        test_insert_returns_lastrowid,
        test_select_tuple_types,
        test_select_nullable,
        test_fetchall,
        test_list_hack,
        test_list_hack_scalar_before_list,
        test_list_hack_scalar_after_list,
        test_list_hack_empty_list,
        test_insert_returning,
    ]

    failed = 0
    for test in tests:
        try:
            test(conn)
            print(f"  PASS  {test.__name__}")
        except Exception as e:
            print(f"  FAIL  {test.__name__}: {e}")
            failed += 1

    if failed:
        print(f"\n{failed}/{len(tests)} tests failed")
        sys.exit(1)
    else:
        print(f"\nAll {len(tests)} tests passed")
