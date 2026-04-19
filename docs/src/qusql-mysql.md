# qusql-mysql and qusql-mysql-type

This chapter covers the two MySQL/MariaDB crates in the qusql family:

- **`qusql-mysql`**: an async MySQL/MariaDB driver focused on low overhead and
  correct cancellation behaviour.
- **`qusql-mysql-type`**: a thin proc-macro layer on top that gives you
  compile-time type checking of SQL queries against a schema file.

## qusql-mysql

`qusql-mysql` is a lightweight async MySQL/MariaDB driver.  It deliberately
prioritizes efficiency: a normal query returning a string allocates no extra
memory, error types are 8 bytes, and very few tasks are spawned.  A benchmark
against sqlx shows it is significantly faster:

| Test | qusql-mysql | sqlx |
|---|---|---|
| Insert (400 k rows) | 14 219 ms | 15 500 ms |
| Select all (100 x) | 10 969 ms | 15 861 ms |
| Select stream (100 x) | 9 991 ms | 13 216 ms |
| Select one (400 k) | 19 085 ms | 34 729 ms |

### Cancellation safety

Dropping or cancelling any future or struct returned by the library does not
corrupt the connection.  The connection has internal state that finishes any
partially executed query the next time it is used, or when
`Connection::cleanup()` is called.  When a `PoolConnection` is dropped mid-query
the cleanup runs in a spawned task; if that takes too long the connection is
closed and a new one is established on the next request.

This means `qusql-mysql` is safe to use in a web server where requests can be
cancelled at any time: a long-running query will be killed shortly after its
request is dropped.

### Feature flags

| Flag | Effect |
|---|---|
| `chrono` | Bind and decode support for `chrono::DateTime` and `chrono::NaiveTime` |
| `list_hack` | Support for passing a `List(&slice)` as a dynamically-sized `IN (?)` argument |
| `stats` | Add query count and timing statistics to `Connection` |

### Basic usage

```rust
use qusql_mysql::{
    ConnectionError, ConnectionOptions, Executor, ExecutorExt, Pool, PoolOptions,
};

async fn example() -> Result<(), ConnectionError> {
    let pool = Pool::connect(
        ConnectionOptions::from_url("mysql://user:pw@127.0.0.1:3306/db").unwrap(),
        PoolOptions::new().max_connections(10),
    )
    .await?;

    let mut conn = pool.acquire().await?;

    // Execute a statement
    let mut tr = conn.begin().await?;
    tr.execute("INSERT INTO notes (title) VALUES (?)", ("Hello",))
        .await?;
    tr.commit().await?;

    // Fetch rows as tuples: no schema knowledge required
    let _rows: Vec<(i64, String)> = conn.fetch_all("SELECT id, title FROM notes", ()).await?;
    Ok(())
}
```

### Links

- [crates.io](https://crates.io/crates/qusql-mysql)
- [docs.rs](https://docs.rs/qusql-mysql)
- [Benchmark](https://github.com/antialize/qusql/tree/main/benchmark)

## qusql-mysql-type

`qusql-mysql-type` wraps `qusql-mysql` with a set of proc-macros that
type-check your SQL queries at `cargo check` time using a plain SQL schema
file.  The type-checking model is the same as described in the
[qusql-sqlx-type](qusql-sqlx-type.md) chapter (compile-time schema evaluation,
nullability inference, argument type checking, "did you mean" hints, and so on)
and the schema file format is identical to what is described in the
[qusql-type](qusql-type.md) chapter.

### Setup

Add to `Cargo.toml`:

```toml
[dependencies]
qusql-mysql-type = "*"
```

Place your schema in `qusql-mysql-type-schema.sql` next to the crate's
`Cargo.toml`:

```sql
CREATE TABLE notes (
    id     INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title  VARCHAR(255) NOT NULL,
    body   TEXT,
    pinned TINYINT(1)   NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

For guidance on how the schema file is evaluated, how to export one from an
existing database, and how to handle migrations, see the
[qusql-type schema evaluation model](qusql-type.md#schema-evaluation-model).

### Macros

All macros take `(&mut conn, "SQL", args...)`.  `execute!` has no variants;
every fetch macro comes in four forms combining two independent axes:

**Base operations:**

| Macro | Description |
|---|---|
| `execute!` | Run a statement; returns affected row count |
| `fetch_one!` | Fetch exactly one row (error if zero or more than one) |
| `fetch_optional!` | Fetch zero or one rows |
| `fetch_all!` | Fetch all rows into a `Vec` |
| `fetch!` | Fetch rows lazily as an async stream |

**Variant suffixes** (apply to all fetch macros):

| Suffix | Effect |
|---|---|
| *(none)* | Rows returned as tuples with borrowed values where possible (e.g. `&str` for `VARCHAR`) |
| `_owned` | Like the base, but all values are owned (e.g. `String` instead of `&str`) |
| `_as` | Maps each row into an explicit struct: `fetch_all_as!(MyRow, &mut conn, "SQL", args...)` |
| `_as_owned` | `_as` with owned values |

So the full set of fetch macros is `fetch_one!`, `fetch_one_owned!`, `fetch_one_as!`,
`fetch_one_as_owned!`, and the same pattern for `fetch_optional`, `fetch_all`, and `fetch`.

### Example

```rust
use qusql_mysql_type::{execute, fetch_all};

// Argument types are checked at compile time
execute!(
    &mut conn,
    "INSERT INTO notes (title, pinned) VALUES (?, ?)",
    "Hello",
    false,
)
.await?;

// Return types are inferred from the schema:
// (i32, String, Option<&str>)
let notes = fetch_all!(&mut conn, "SELECT id, title, body FROM notes ORDER BY id",).await?;

for n in &notes {
    println!("{}: {}", n.title, n.body.unwrap_or(""));
}
```

### Links

- [crates.io](https://crates.io/crates/qusql-mysql-type)
- [docs.rs](https://docs.rs/qusql-mysql-type)
- [Example: qusql-mysql-type-notes](https://github.com/antialize/qusql/tree/main/examples/qusql-mysql-type-notes)
- [Example: qusql-mysql-type-books](https://github.com/antialize/qusql/tree/main/examples/qusql-mysql-type-books)
