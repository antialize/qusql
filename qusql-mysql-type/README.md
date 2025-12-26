# qusql-mysql-type
[![crates.io](https://img.shields.io/crates/v/qusql-mysql-type.svg)](https://crates.io/crates/qusql-mysql-type)
[![crates.io](https://docs.rs/qusql-mysql-type/badge.svg)](https://docs.rs/qusql-mysql-type)
[![License](https://img.shields.io/crates/l/qusql-mysql-type.svg)](https://github.com/antialize/qusql)


Proc macros to perform type typed mysql queries on top of [qusql-mysql](../qusql-mysql/README.md).

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
See [qusql_type::schema](../qusql-type/src/schema.rs) for a detailed description.

[qusql_type::schema]: https://docs.rs/qusql-type/latest/qusql_type/schema/index.html

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