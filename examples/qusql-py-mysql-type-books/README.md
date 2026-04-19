# qusql-py-mysql-type-books

A small library catalog application that demonstrates static SQL type checking
for MariaDB/MySQL using `qusql-mysql-type` and the `qusql-mysql-type-plugin`
mypy plugin.

Every `execute()` call in `main.py` is validated against
`mysql-type-schema.sql` by **mypy** at static-analysis time.  Type mismatches
between Python code and the schema appear as ordinary mypy errors with no need
for a running database.

## What the example covers

| Feature | Where |
|---|---|
| `INSERT` and `lastrowid` for auto-increment IDs | author and book inserts |
| `SELECT` with `JOIN` and typed result tuples | book listing |
| `ENUM` columns accepted as `str` | book insert |
| `DATE` columns as `datetime.date` | `published_on`, `due_date` |
| Nullable `TEXT` column as `str \| None` | `reviews.body` |
| `UPDATE` | marking a loan returned |
| `DELETE` in foreign-key order | cleanup at the end |
| Idempotent migration pattern via stored procedures | `mysql-type-schema.sql` |

The schema uses stored procedures as idempotent revision guards.  The
`qusql-type` schema evaluator executes every procedure body when it processes
the `CALL`, so mypy always sees the fully-migrated schema regardless of the
`IF NOT EXISTS` guards.

## Setup

### 1. Install the packages from PyPI

[uv](https://docs.astral.sh/uv/) reads `pyproject.toml` and installs all
dependencies (including mypy and the type-checking plugin) into a local venv:

```bash
cd examples/qusql-py-mysql-type-books
uv sync
```

That is all that is needed - no separate `pip install` steps.

### 2. Start a MariaDB container

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

### 3. Run the static type check

The plugin reads `mysql-type-schema.sql` from the **current working
directory**, so run mypy from inside the example directory:

```bash
uv run mypy main.py
```

### 4. Run the example

```bash
DATABASE_URL=mysql://books:books@127.0.0.1:3306/books_example \
    uv run python main.py
```

The first run bootstraps the schema and inserts some demo data, then cleans it
up before exiting.  Subsequent runs skip revisions that have already been
applied.

### 5. Stop the container

```bash
podman stop library-mariadb
```

## Project layout

```
examples/qusql-py-mysql-type-books/
    mysql-type-schema.sql   -- schema + migrations (read by the mypy plugin for
                               type checking and by open() at runtime)
    main.py                 -- example code
    pyproject.toml          -- dependencies + mypy plugin configuration
    README.md               -- this file
```
