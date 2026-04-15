# qusql-sqlx-type-books

A small library catalog application that demonstrates compile-time SQL type
checking for PostgreSQL using `qusql-sqlx-type`.

Every `query!` call in `src/main.rs` is validated against
`sqlx-type-schema.sql` at **compile time**.  Type mismatches between Rust
code and the schema appear as ordinary Rust compiler errors with no need for
`cargo sqlx prepare` or a running database during `cargo check`.

## What the example covers

| Feature | Where |
|---|---|
| `INSERT ... RETURNING` with UUID and integer results | author and book inserts |
| `SELECT` with `JOIN` and typed columns | book listing |
| Enum columns (`Genre`) passed as `&str` | book insert |
| `date` columns as `chrono::NaiveDate` | `published_on`, `due_date` |
| Nullable columns decoded as `Option<T>` | `reviews.body` |
| `UPDATE` | marking a loan returned |
| `DELETE` in foreign-key order | cleanup at the end |
| Idempotent migration pattern (`schema_revisions`) | `sqlx-type-schema.sql` |

The schema is split into numbered revisions managed by a small
`apply_revision()` PL/pgSQL helper.  Running the migration file against an
empty database bootstraps everything in one transaction.  Running it again
later only applies revisions that have not been recorded yet, so existing data
is never touched.

## Running the example

### 1. Start a PostgreSQL container

```bash
podman run --rm --detach \
    --name books-pg \
    -e POSTGRES_DB=books_example \
    -e POSTGRES_USER=books \
    -e POSTGRES_PASSWORD=books \
    -p 5432:5432 \
    docker.io/library/postgres:16
```

Wait a moment for the server to finish starting:

```bash
podman exec books-pg pg_isready -U books
```

### 2. Run the example

```bash
DATABASE_URL=postgres://books:books@localhost/books_example \
    cargo run -p qusql-sqlx-type-books
```

The first run bootstraps the schema and inserts some demo data, then cleans it
up before exiting.  Subsequent runs skip revisions that have already been
applied.

### 3. Stop the container

The container was started with `--rm`, so stopping it is enough:

```bash
podman stop books-pg
```

## Project layout

```
examples/qusql-sqlx-type-books/
    Cargo.toml              -- crate manifest
    sqlx-type-schema.sql    -- schema + migrations (read by the query! macros
                               at compile time and by include_str! at runtime)
    src/
        main.rs             -- example code
    README.md               -- this file
```
