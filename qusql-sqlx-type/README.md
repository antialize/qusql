# sqlx-type
[![crates.io](https://img.shields.io/crates/v/sqlx-type.svg)](https://crates.io/crates/sqlx-type)
[![crates.io](https://docs.rs/sqlx-type/badge.svg)](https://docs.rs/sqlx-type)
[![License](https://img.shields.io/crates/l/sqlx-type.svg)](https://github.com/antialize/sqlx-type)
[![actions-badge](https://github.com/antialize/sqlx-type/workflows/Rust/badge.svg?branch=main)](https://github.com/antialize/sqlx-type/actions)

Proc macros to perform type sql queries similarly to sqlx::query, but without the need
to run `cargo sqlx prepare`

A schema definition must be placed in "sqlx-type-schema.sql" in the root of a using crate:

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

```rust
use std::env, sqlx::MySqlPool, sqlx_type::query;

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