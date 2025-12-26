# qusql-mysql
[![crates.io](https://img.shields.io/crates/v/qusql-mysql.svg)](https://crates.io/crates/qusql-mysql)
[![crates.io](https://docs.rs/qusql-mysql/badge.svg)](https://docs.rs/qusql-mysql)
[![License](https://img.shields.io/crates/l/qusql-mysql.svg)](https://github.com/antialize/qusql)

This crate allow efficient async communication with Mariadb/Mysql

You can either establish a raw connection using connection or
use a pool of connections.

Drop handling
------------

When dropping/cancelling any future or struct returned by the library. The connections
will keep working. This is done by having the Connection have an internal state and
finish up any partially executed queries when the next query is executed or when Connection::cleanup
is called.

When dropping a PoolConnection, any partial queries or transactions are finished up in
a spawned task. If this task takes too long to execute the connection is closed an a new
connection may be established.

This means that if the qusql-mysql is used in a backend to handle web requests, and the
web request is cancelled while performing a long running query. Then the long running
query will be killed shortly after.

Efficiency
-----------
We spawn very few tasks.  Currently the only task spawned is
when a PoolConnection is dropped while there is a ongoing query or transaction.

We have few memory allocations. In the cause of a normal
query, even one returning a string, no memory will be allocated

The error types returned are all 8 bytes.

The [benchmark](../benchmark) folder contains a benchmark that compares sqlx to qusql-mysql.
When run it shows the we are significantly more efficient than sqlx

| Test          |   Qusql time |    Sqlx time |
|---------------|--------------|--------------|
| Setup         |     0.921 ms |     1.189 ms |
| Insert        | 14218.778 ms | 15499.612 ms |
| Select all    | 10968.823 ms | 15860.648 ms |
| Select stream |  9991.353 ms | 13215.973 ms |
| Select one    | 19085.157 ms | 34728.834 ms |

Feature flags
--------------
* stats: Add query count and timing statistics to the Connection
* chrono: Add bind and decode support for chrono DateTime and Time
* list_hack: Add support lists as arguments to queries

Example
--------
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

List hack
----------
If the list `list_hack` feature is enabled. Support for lists as arguments to queries is added
this is done done by adding a sufficient number of `?`'s to the query.

``` rust
use qusql_mysql::{Connection, ExecutorExt, Executor, list, ConnectionError};

async fn test(conn: &mut Connection, vs: &[u32]) -> Result<(), ConnectionError> {
    let rows: Vec<(i64, &str)> = conn.fetch_all(
        "SELECT `v`, `t` FROM `table` WHERE `v` IN (_LIST_)", (list(vs),)).await?;
    Ok(())
}
```
Here the `_LIST_` is replaced with `?,?,..,?` where the number of `?`'s depend on the length
of the list. If the list is empty `_LIST_` is replaced by `NULL`