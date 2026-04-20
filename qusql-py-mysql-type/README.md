# qusql-py-mysql-type

Facilitate MySQL typed queries that can be checked using mypy.

A schema definition must be placed in `mysql-type-schema.sql` in the working
directory when mypy runs:

```sql
DROP TABLE IF EXISTS `t1`;
CREATE TABLE `t1` (
    `id` int(11) NOT NULL,
    `cbool` tinyint(1) NOT NULL,
    `cu8` tinyint UNSIGNED NOT NULL,
    `cu16` smallint UNSIGNED NOT NULL,
    `cu32` int UNSIGNED NOT NULL,
    `cu64` bigint UNSIGNED NOT NULL,
    `ci8` tinyint,
    `ci16` smallint,
    `ci32` int,
    `ci64` bigint,
    `ctext` varchar(100) NOT NULL,
    `cbytes` blob,
    `cf32` float,
    `cf64` double
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `t1`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
```

See [qusql_type::schema](https://docs.rs/qusql-type/latest/qusql_type/schema/index.html)
for a detailed description of supported schema syntax.

This schema can then be used to type queries.  Pass a cursor to `execute()` - use
`cast()` if your type stubs do not annotate the return type of `cursor()`:

```py
import MySQLdb as mdb
from qusql_mysql_type import execute

connection = mdb.connect(
    host="127.0.0.1",
    user="test",
    passwd="test",
    db="test",
    port=3306,
    use_unicode=True,
    autocommit=True,
)

id = execute(
    connection,
    "INSERT INTO `t1` (`cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ctext`)"
    "VALUES (%s, %s, %s, %s, %s, %s)",
    True, 8, 1243, 42, 42, "Hello world"
).lastrowid


(cu16, ctext, ci32) = execute(
    connection,
    "SELECT `cu16`, `ctext`, `ci32` FROM `t1` WHERE `id`=%s",
    id
).fetchone()

assert cu16 == 1243
assert ctext == "Hello world"
```

Enable the plugin in your `pyproject.toml` so that mypy can type-check the queries:

```toml
[tool.mypy]
plugins = ["qusql_mysql_type_plugin"]
```

## List hack

Support for lists as arguments to queries is provided by automatically expanding
`_LIST_` into the correct number of `%s` placeholders:

```python
rows = execute(
    connection,
    "SELECT `v`, `t` FROM `table` WHERE `v` IN (_LIST_)",
    [42, 43, 45],
).fetchall()
```

If the list is empty, `_LIST_` is replaced by `NULL`.

See also the examples:

- [`examples/qusql-py-mysql-type-notes`](../examples/qusql-py-mysql-type-notes) -
  simple introductory CLI with uv setup
- [`examples/qusql-py-mysql-type-books`](../examples/qusql-py-mysql-type-books) -
  library catalog with JOINs, enums, dates, and an idempotent migration pattern
