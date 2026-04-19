# qusql-py-mysql-type-notes

A minimal note-taking CLI that demonstrates compile-time SQL type checking for
MariaDB/MySQL using `qusql-mysql-type` and the `qusql-mysql-type-plugin` mypy
plugin.

This is the introductory Python example for the library.  The schema is a
single `CREATE TABLE IF NOT EXISTS` statement, which keeps the focus on the
type-checking rather than on migration infrastructure.  If you want to see a
production-style idempotent migration pattern, look at
[`examples/qusql-py-mysql-type-books`](../qusql-py-mysql-type-books/README.md).

## The key idea

Every `execute()` call is checked against `mysql-type-schema.sql` **by mypy**
at static-analysis time.  No running database is needed for the type check.

```python
# Passing a string where an integer is expected is a mypy error:
execute(c, "UPDATE notes SET pinned = NOT pinned WHERE id = %s", "oops")
#                                                                  ^^^^
# error: Argument 1 has incompatible type "str"; expected "int"
```

The result type is also inferred.  Because `body text` is nullable, mypy knows
its column type is `str | None` without any annotation:

```python
rows = execute(
    c,
    "SELECT id, title, body FROM notes ORDER BY id",
).fetchall()
# mypy infers: list[tuple[int, str, str | None]]
for note_id, title, body in rows:
    print(title, body or "")
    #                ^^^ str | None -- no cast needed
```

## Setup

### 1. Create a virtual environment with uv

[uv](https://docs.astral.sh/uv/) reads `pyproject.toml` and installs all
dependencies (including mypy and the type-checking plugin) into a local venv:

```bash
cd examples/qusql-py-mysql-type-notes
uv sync
```

That is all that is needed; no separate `pip install` steps.

### 2. Start a MariaDB container

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

### 3. Run the static type check

The plugin reads `mysql-type-schema.sql` from the **current directory**, so
run mypy from inside the example directory:

```bash
uv run mypy main.py
```

### 4. Use the CLI

```bash
export DATABASE_URL=mysql://notes:notes@127.0.0.1:3306/notes_example

# schema is created automatically on first run
uv run python main.py add "Buy milk"
uv run python main.py add "Read the docs" "Start with the README"
uv run python main.py list
uv run python main.py pin 1
uv run python main.py delete 2
uv run python main.py list
```

### 5. Stop the container

```bash
podman stop notes-mariadb
```

## Project layout

```
examples/qusql-py-mysql-type-notes/
    mysql-type-schema.sql   -- schema (read by the mypy plugin for type
                               checking and by open() at runtime)
    main.py                 -- CLI implementation
    pyproject.toml          -- dependencies + mypy plugin configuration
    README.md               -- this file
```
