# qusql-mysql-type-books

A small library catalog application that demonstrates compile-time SQL type
checking for MariaDB/MySQL using `qusql-mysql-type`.

Every `execute!` and `fetch_all!` call in `src/main.rs` is validated against
`qusql-mysql-type-schema.sql` at **compile time**.  Type mismatches between
Rust code and the schema appear as ordinary Rust compiler errors with no need
for a running database during `cargo check`.

## What the example covers

| Feature | Where |
|---|---|
| `INSERT` and `last_insert_id()` for auto-increment IDs | author and book inserts |
| `SELECT` with `JOIN` and typed columns | book listing |
| Enum columns passed as `&str` | book insert |
| `DATE` columns as `chrono::NaiveDate` | `published_on`, `due_date` |
| Nullable columns decoded as `Option<T>` | `reviews.body` |
| `UPDATE` | marking a loan returned |
| `DELETE` in foreign-key order | cleanup at the end |
| Idempotent migration pattern via stored procedures | `qusql-mysql-type-schema.sql` |

The schema is split into numbered revisions, each implemented as a stored
procedure with an `IF NOT EXISTS` guard that checks `schema_revisions`.
Running the schema file against an empty database bootstraps everything.
Running it again later is a no-op for revisions that have already been applied,
so existing data is never touched.

The schema evaluator in `qusql-type` executes the body of each `CREATE
PROCEDURE` and each `CALL` statement the same way it would against an empty
database, so the compile-time type checker always sees the fully-migrated
schema regardless of the runtime guards.

## Running the example

### 1. Start a MariaDB container

```bash
podman run --rm --detach \
    --name library-mariadb \
    -e MARIADB_DATABASE=books_example \
    -e MARIADB_USER=books \
    -e MARIADB_PASSWORD=books \
    -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 \
    -p 3306:3306 \
    docker.io/library/mariadb:11
```

Wait a moment for the server to finish starting:

```bash
podman exec library-mariadb mariadb-admin ping -u books -pbooks
```

### 2. Run the example

```bash
DATABASE_URL=mysql://books:books@127.0.0.1:3306/books_example \
    cargo run -p qusql-mysql-type-books
```

The first run bootstraps the schema and inserts some demo data, then cleans it
up before exiting.  Subsequent runs skip revisions that have already been
applied.

### 3. Stop the container

The container was started with `--rm`, so stopping it is enough:

```bash
podman stop library-mariadb
```

## Project layout

```
examples/qusql-mysql-type-books/
    Cargo.toml                    -- crate manifest
    qusql-mysql-type-schema.sql   -- schema + migrations (read by the macros
                                     at compile time and by include_str! at runtime)
    src/
        main.rs                   -- example code
    README.md                     -- this file
```
