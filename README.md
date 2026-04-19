Qusql
=====
[![License](https://img.shields.io/crates/l/sql-type.svg)](https://github.com/antialize/qusql)
[![Rust](https://github.com/antialize/qusql/actions/workflows/rust.yml/badge.svg)](https://github.com/antialize/qusql/actions/workflows/rust.yml)

This project contains several sub-crates that each try to make it quicker and
easier to interact with SQL databases.  MariaDB/MySQL and PostgreSQL are both
first-class citizens.

Qusql-parse
-----------
[Qusql-parse](qusql-parse/README.md) is a fast Rust SQL parser.  It supports
MySQL/MariaDB, PostgreSQL/PostGIS, and SQLite, and produces a unified abstract
syntax tree (AST) across all three dialects.

Example code:
```rust
use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statement, Issues};

let options = ParseOptions::new()
    .dialect(SQLDialect::MariaDB)
    .arguments(SQLArguments::QuestionMark)
    .warn_unquoted_identifiers(true);

let sql = "SELECT `monkey`,
           FROM `t1` LEFT JOIN `t2` ON `t2`.`id` = `t1.two`
           WHERE `t1`.`id` = ?";
let mut issues = Issues::new(sql);
let ast = parse_statement(sql, &mut issues, &options);

println!("{}", issues);
println!("AST: {:#?}", ast);
```

See also: [`examples/qusql-parse-lint`](examples/qusql-parse-lint) - a
command-line SQL linter built on this crate.

Qusql-type
----------
[Qusql-type](qusql-type/README.md) is a SQL type-inference engine in Rust.  It
parses a schema definition (CREATE TABLE / ALTER TABLE / stored procedures /
PL/pgSQL functions, ...) and uses it to validate SQL statements and infer the
types of result columns and query arguments.  Both MariaDB/MySQL and PostgreSQL
are well supported.

Example code:
```rust
use qusql_type::{schema::parse_schemas, type_statement, TypeOptions,
    SQLDialect, SQLArguments, StatementType, Issues};

let schemas = "
    CREATE TABLE `events` (
      `id` bigint(20) NOT NULL,
      `user` int(11) NOT NULL,
      `message` text NOT NULL
    );";

let mut issues = Issues::new(schemas);
let schemas = parse_schemas(schemas,
    &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB));
assert!(issues.is_ok());

let sql = "SELECT `id`, `user`, `message` FROM `events` WHERE `id` = ?";
let mut issues = Issues::new(sql);
let stmt = type_statement(&schemas, sql, &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB).arguments(SQLArguments::QuestionMark));
assert!(issues.is_ok());

match stmt {
    StatementType::Select{columns, arguments} => {
        assert_eq!(columns.len(), 3);
        assert_eq!(arguments.len(), 1);
    }
    _ => panic!("Expected select statement")
};
```

See also: [`examples/qusql-type-check`](examples/qusql-type-check) - a
command-line tool that prints the inferred types for a set of queries against a
given schema.

Qusql-mysql
-----------
[Qusql-mysql](qusql-mysql/README.md) is an async Rust database connector for
MariaDB/MySQL.  It is designed to be quick and efficient, and supports
cancelling queries when a connection is dropped.

Example code:
```rust
use qusql_mysql::{Pool, ConnectionOptions, PoolOptions, ConnectionError, ExecutorExt, Executor};

async fn test() -> Result<(), ConnectionError> {
    let pool = Pool::connect(
        ConnectionOptions::from_url("mysql://user:pw@127.0.0.1:3307/db").unwrap(),
        PoolOptions::new().max_connections(10)
    ).await?;

    let mut conn = pool.acquire().await?;

    let mut tr = conn.begin().await?;
    tr.execute("INSERT INTO `table` (`v`, `t`) VALUES (?)", (42, "test_string")).await?;
    tr.commit().await?;

    let rows: Vec<(i64, &str)> = conn.fetch_all("SELECT `v`, `t` FROM `table`", ()).await?;
    Ok(())
}
```

Qusql-mysql-type
----------------
[Qusql-mysql-type](qusql-mysql-type/README.md) adds compile-time type checking
on top of [qusql-mysql](qusql-mysql/README.md) using
[qusql-type](qusql-type/README.md).  SQL queries are validated against a schema
file at compile time - no running database is needed.

The schema must be placed in `qusql-mysql-type-schema.sql` in the root of the
using crate:
```sql
DROP TABLE IF EXISTS `t1`;
CREATE TABLE `t1` (
    `id` int(11) NOT NULL,
    `cbool` tinyint(1) NOT NULL DEFAULT false,
    `cu8` tinyint UNSIGNED NOT NULL DEFAULT 0,
    `cu16` smallint UNSIGNED NOT NULL DEFAULT 1,
    `cu32` int UNSIGNED NOT NULL DEFAULT 2,
    `cu64` bigint UNSIGNED NOT NULL DEFAULT 3,
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

This schema is then used to type queries:
```rust
use qusql_mysql::connection::{ConnectionOptions, ConnectionError, ExecutorExt};
use qusql_mysql::pool::{Pool, PoolOptions};
use qusql_mysql_type::{execute, fetch_one};

async fn test() -> Result<(), ConnectionError> {
    let pool = Pool::connect(
        ConnectionOptions::from_url("mysql://user:pw@127.0.0.1:3307/db").unwrap(),
        PoolOptions::new().max_connections(10)
    ).await?;

    let mut conn = pool.acquire().await?;

    let id = execute!(&mut conn, "INSERT INTO `t1` (
       `cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ctext`)
        VALUES (?, ?, ?, ?, ?, ?)",
        true, 8, 1243, 42, 42, "Hello world").await?.last_insert_id();

    let row = fetch_one!(&mut conn,
        "SELECT `cu16`, `ctext`, `ci32` FROM `t1` WHERE `id`=?", id).await?;

    assert_eq!(row.cu16, 1234);
    assert_eq!(row.ctext, "Hello would");
    assert!(row.ci32.is_none());
    Ok(())
}
```

See also the examples:

- [`examples/qusql-mysql-type-notes`](examples/qusql-mysql-type-notes) -
  simple introductory CLI (single-table schema)
- [`examples/qusql-mysql-type-books`](examples/qusql-mysql-type-books) -
  library catalog with JOINs, enums, dates, and an idempotent migration pattern

Qusql-sqlx-type
---------------
[Qusql-sqlx-type](qusql-sqlx-type/README.md) provides `query!` proc-macros for
PostgreSQL via [sqlx](https://github.com/launchbadge/sqlx), with compile-time
type checking driven by a local schema file.  No `cargo sqlx prepare` step and
no running database are needed during `cargo check`.

The schema must be placed in `sqlx-type-schema.sql` in the root of the using
crate:
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

This schema is then used to type queries:
```rust
use {std::env, sqlx::MySqlPool, qusql_sqlx_type::query};

async fn test() -> Result<(), sqlx::Error> {
    let pool = MySqlPool::connect(&env::var("DATABASE_URL").unwrap()).await?;

    let id = query!("INSERT INTO `t1` (`cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ctext`)
        VALUES (?, ?, ?, ?, ?, ?)", true, 8, 1243, 42, 42, "Hello world")
        .execute(&pool).await?.last_insert_id();

    let row = query!("SELECT `cu16`, `ctext`, `ci32` FROM `t1` WHERE `id`=?", id)
        .fetch_one(&pool).await?;

    assert_eq!(row.cu16, 1234);
    assert_eq!(row.ctext, "Hello would");
    assert!(row.ci32.is_none());
    Ok(())
}
```

See also the examples:

- [`examples/qusql-sqlx-type-notes`](examples/qusql-sqlx-type-notes) -
  simple introductory CLI (single-table schema)
- [`examples/qusql-sqlx-type-books`](examples/qusql-sqlx-type-books) -
  library catalog with JOINs, enums, UUIDs, dates, and an idempotent migration
  pattern

qusql-py-mysql-type
--------------------
The [`qusql-mysql-type`](https://pypi.org/project/qusql-mysql-type/) and
[`qusql-mysql-type-plugin`](https://pypi.org/project/qusql-mysql-type-plugin/)
PyPI packages enable mypy to type-check MySQL/MariaDB queries in Python at
static-analysis time.

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

Enable the plugin in `pyproject.toml`:
```toml
[tool.mypy]
plugins = ["qusql_mysql_type_plugin"]
```

Queries can then be type-checked by mypy:
```python
from typing import cast
import MySQLdb
import MySQLdb.cursors
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

See also the examples:

- [`examples/qusql-py-mysql-type-notes`](examples/qusql-py-mysql-type-notes) -
  simple introductory CLI with uv setup
- [`examples/qusql-py-mysql-type-books`](examples/qusql-py-mysql-type-books) -
  library catalog with JOINs, enums, dates, and an idempotent migration pattern
