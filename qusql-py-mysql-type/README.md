# qusql-py-mysql-type

Facilitate mysql typed queries that can be checked using mypy.


A schema definition must be placed in "mysql-type-schema.sql" in the root of the project:

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
See [sql_type::schema] for a detailed description.

[sql_type::schema]: https://docs.rs/sql-type/latest/sql_type/schema/index.html

This schema can then be used to type queries:


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


(cu16, ctext, ci31) = execute(
    connection,
    "SELECT `cu16`, `ctext`, `ci32` FROM `t1` WHERE `id`=%s",
    id
).fetchone()

assert row.cu16 == 1234
assert cu16 == 1234
assert ctext == "Hello would"
```

These queries can then be checked by mypy assuming the "qusql_mysql_type_plugin" is enabled in tho pyproject.toml

```toml
[mypy]
plugins = qusql_mysql_type_plugin
```

List hack
----------
Support for lists as arguments to queries is added
this is done done by adding a sufficient number of `%s`'s to the query.

```python
rows = execute(
    connection,
    "SELECT `v`, `t` FROM `table` WHERE `v` IN (_LIST_)",
    [42, 43, 45],
).fetchall()
```
Here the `_LIST_` is replaced with `%s,%s,..,%s` where the number of `%s`'s depend on the length
of the list. If the list is empty `_LIST_` is replaced by `NULL`
