Qusql
=====
[![License](https://img.shields.io/crates/l/sql-type.svg)](https://github.com/antialize/qusql)
[![Rust](https://github.com/antialize/qusql/actions/workflows/rust.yml/badge.svg)](https://github.com/antialize/qusql/actions/workflows/rust.yml)

This projects contains serval subprocess that each try to make it quicker and easier to interact with SQL servers.

Qusql-parse
-----------
[Qusql-parse](/qusql-parse/README.md) is a fast rust based parser for SQL. It supports different SQL dialects (mysql, postgresql, sqlite), and parses this sql into a unified abstract syntax tree.

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

Qusql-type
----------
[Qusql-type](/qusql-type/README.md) is a SQL typing engine in rust. It can parse a SQL schema definition consisting of among other things `CREATE TABLE` statements. Given this schema SQL statements can then be validated and typed, such that the type of returned columns and supplied arguments can be inferred. The type system for Mariadb/Mysql is modelled fairly well, and most queries and functions are supported. While support for Postgres and sqlite is much less developed.


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

// Compute terse representation of the schemas
let schemas = parse_schemas(schemas,
    &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB));
assert!(issues.is_ok());

let sql = "SELECT `id`, `user`, `message` FROM `events` WHERE `id` = ?";
let mut issues = Issues::new(sql);
let stmt = type_statement(&schemas, sql, &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB).arguments(SQLArguments::QuestionMark));
assert!(issues.is_ok());

let stmt = match stmt {
    StatementType::Select{columns, arguments} => {
        assert_eq!(columns.len(), 3);
        assert_eq!(arguments.len(), 1);
    }
    _ => panic!("Expected select statement")
};
```

Qusql-mysql
-----------
[Qusql-mysql](/qusql-mysql/README.md) is an async rust database connector for mysql/mariadb. It is designed to be quick and efficient. And to allow cancelling queries when a connection is dropped.

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
[Qusql-mysql-type](/qusql-mysql-type/README.md) adds support for typed queries on on top of [qusql-mysql](/qusql-mysql/README.md) using [qusql-type](/qusql-type/README.md).

The queries are typed based on a schema definition, that must be placed in "qusql-mysql-type-schema.sql"
in the root of a using crate:
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

This schema can then be used to type queries:
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