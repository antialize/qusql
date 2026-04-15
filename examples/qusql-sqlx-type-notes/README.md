# qusql-sqlx-type-notes

A minimal note-taking CLI that demonstrates compile-time SQL type checking for
PostgreSQL using `qusql-sqlx-type`.

This is the introductory example for the library.  The schema is a single
`CREATE TABLE IF NOT EXISTS` statement, which keeps the focus on the
type-checking macros rather than on migration infrastructure.  If you want to
see a production-style idempotent migration pattern that can evolve your schema
without touching existing data, look at
[`examples/qusql-sqlx-type-books`](../qusql-sqlx-type-books/README.md).

## The key idea

Every `query!` call is checked against `sqlx-type-schema.sql` **at compile
time**.  The schema file is read by the proc-macro; no running database and no
`cargo sqlx prepare` step are needed.

```rust
// Passing a string where an integer is expected is a compiler error:
query!("UPDATE notes SET pinned = NOT pinned WHERE id = $1", "oops")
//                                                            ^^^^^^
// error: expected i32, found &str
```

Column types in result rows are also inferred.  Because `body text` is
nullable, `row.body` has type `Option<String>` automatically:

```rust
let notes = query!("SELECT id, title, body FROM notes ORDER BY id")
    .fetch_all(&pool).await?;
for n in &notes {
    println!("{}: {}", n.title, n.body.as_deref().unwrap_or(""));
    //                          ^^^^^^^ Option<String>, no annotation needed
}
```

## Running the example

### 1. Start a PostgreSQL container

```bash
podman run --rm --detach \
    --name notes-pg \
    -e POSTGRES_DB=notes_example \
    -e POSTGRES_USER=notes \
    -e POSTGRES_PASSWORD=notes \
    -p 5432:5432 \
    docker.io/library/postgres:16
```

Wait for the server to be ready:

```bash
podman exec notes-pg pg_isready -U notes
```

### 2. Use the CLI

```bash
export DATABASE_URL=postgres://notes:notes@localhost/notes_example

# schema is created automatically on first run
cargo run -p qusql-sqlx-type-notes -- add "Buy milk"
cargo run -p qusql-sqlx-type-notes -- add "Read the docs" "Start with the README"
cargo run -p qusql-sqlx-type-notes -- list
cargo run -p qusql-sqlx-type-notes -- pin 1
cargo run -p qusql-sqlx-type-notes -- delete 2
cargo run -p qusql-sqlx-type-notes -- list
```

### 3. Stop the container

```bash
podman stop notes-pg
```

## Project layout

```
examples/qusql-sqlx-type-notes/
    Cargo.toml              -- crate manifest
    sqlx-type-schema.sql    -- schema (read by query! macros at compile time
                               and by include_str! at runtime)
    src/
        main.rs             -- CLI implementation
    README.md               -- this file
```
