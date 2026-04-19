# qusql-sqlx-type
[![crates.io](https://img.shields.io/crates/v/qusql-sqlx-type.svg)](https://crates.io/crates/qusql-sqlx-type)
[![crates.io](https://docs.rs/qusql-sqlx-type/badge.svg)](https://docs.rs/qusql-sqlx-type)
[![License](https://img.shields.io/crates/l/qusql-sqlx-type.svg)](https://github.com/antialize/qusql)

Proc macros to perform typed SQL queries on top of [sqlx](https://github.com/launchbadge/sqlx)
for PostgreSQL, MySQL/MariaDB, and (to a lesser extent) SQLite, without the need to run
`cargo sqlx prepare` or have a running database during `cargo check`.

A schema definition must be placed in `sqlx-type-schema.sql` in the root of a using crate:

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

This schema can then be used to type queries:

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

- [`examples/qusql-sqlx-type-notes`](../examples/qusql-sqlx-type-notes) -
  simple introductory CLI (single-table schema)
- [`examples/qusql-sqlx-type-books`](../examples/qusql-sqlx-type-books) -
  library catalog with JOINs, enums, UUIDs, dates, and an idempotent migration
  pattern
