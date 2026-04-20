# qusql-mysql-type-notes

A minimal note-taking CLI that demonstrates compile-time SQL type checking for
MariaDB/MySQL using `qusql-mysql-type`.

This is the introductory example for the library.  The schema is a single
`CREATE TABLE IF NOT EXISTS` statement, which keeps the focus on the
type-checking macros rather than on migration infrastructure.  If you want to
see a production-style idempotent migration pattern that can evolve your schema
without touching existing data, look at
[`examples/qusql-mysql-type-books`](../qusql-mysql-type-books/README.md).

## The key idea

Every `execute!` and `fetch_all!` call is checked against
`qusql-mysql-type-schema.sql` **at compile time**.  The schema file is read by
the proc-macro; no running database and no code-generation step are needed.

```rust
// Passing a string where an integer is expected is a compiler error:
execute!(&mut conn, "UPDATE notes SET pinned = NOT pinned WHERE id = ?", "oops")
//                                                                         ^^^^^^
// error: expected i32, found &str
```

Column types in result rows are also inferred.  Because `body text` is
nullable, `row.body` has type `Option<&str>` automatically:

```rust
let notes = fetch_all!(
    &mut conn,
    "SELECT id, title, body FROM notes ORDER BY id",
)
.await?;
for n in &notes {
    println!("{}: {}", n.title, n.body.unwrap_or(""));
    //                          ^^^^^^^ Option<&str>, no annotation needed
}
```

## Running the example

### 1. Start a MariaDB container

```bash
podman run --rm --detach \
    --name notes-mariadb \
    -e MARIADB_DATABASE=notes_example \
    -e MARIADB_USER=notes \
    -e MARIADB_PASSWORD=notes \
    -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 \
    -p 3306:3306 \
    docker.io/library/mariadb:11
```

Wait for the server to be ready:

```bash
podman exec notes-mariadb mariadb-admin ping -u notes -pnotes
```

### 2. Use the CLI

```bash
export DATABASE_URL=mysql://notes:notes@127.0.0.1:3306/notes_example

# schema is created automatically on first run
cargo run -p qusql-mysql-type-notes -- add "Buy milk"
cargo run -p qusql-mysql-type-notes -- add "Read the docs" "Start with the README"
cargo run -p qusql-mysql-type-notes -- list
cargo run -p qusql-mysql-type-notes -- pin 1
cargo run -p qusql-mysql-type-notes -- delete 2
cargo run -p qusql-mysql-type-notes -- list
```

### 3. Stop the container

```bash
podman stop notes-mariadb
```

## Project layout

```
examples/qusql-mysql-type-notes/
    Cargo.toml                    -- crate manifest
    qusql-mysql-type-schema.sql   -- schema (read by macros at compile time
                                     and by include_str! at runtime)
    src/
        main.rs                   -- CLI implementation
    README.md                     -- this file
```
