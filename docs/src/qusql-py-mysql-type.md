# qusql-py-mysql-type

`qusql-py-mysql-type` and `qusql-mysql-type-plugin` let you write MySQL queries
in Python that are **type-checked by mypy** at static analysis time, with no runtime
surprises.

The plugin is implemented as a native extension (compiled Rust via PyO3) that
mypy loads to evaluate your schema and annotate every `execute()` call.

## Setup

Install the packages (e.g. with uv):

```toml
# pyproject.toml
[project]
dependencies = ["qusql-mysql-type"]

[dependency-groups]
dev = ["qusql-mysql-type-plugin", "mypy", "types-mysqlclient"]

[tool.mypy]
plugins = ["qusql_mysql_type_plugin"]
```

Place your schema in `mysql-type-schema.sql` in the **working directory where
mypy runs** (usually the project root):

```sql
CREATE TABLE IF NOT EXISTS notes (
    id     INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title  VARCHAR(255) NOT NULL,
    body   TEXT,
    pinned TINYINT(1)   NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Usage

```python
from typing import cast
import MySQLdb
import MySQLdb.cursors
from qusql_mysql_type import execute

conn = MySQLdb.connect(host="127.0.0.1", user="test", passwd="test", db="test")

# cast() is required because MySQLdb stubs leave cursor() return type as Any
c = cast(MySQLdb.cursors.Cursor, conn.cursor())

# mypy infers: list[tuple[int, str, str | None]]
rows = execute(c, "SELECT id, title, body FROM notes ORDER BY id").fetchall()
for note_id, title, body in rows:
    print(title, body or "")
```

## What mypy checks

- Column names referenced in `SELECT` exist in the table
- `%s` argument count matches the query
- `%s` argument types match the expected SQL column types
- Inferred return type flows into the rest of your code; wrong destructuring
  patterns are caught at analysis time
- Invalid SQL is a mypy error

## The `_LIST_` expansion

For `IN (...)` queries with a variable-length list, use `_LIST_`:

```python
ids = [1, 2, 3]
rows = execute(c, "SELECT id, title FROM notes WHERE id IN (_LIST_)", ids).fetchall()
```

`_LIST_` is expanded at runtime to the correct number of `%s` placeholders.  If
the list is empty it becomes `NULL`.

## Links

- [qusql-mysql-type on PyPI](https://pypi.org/project/qusql-mysql-type/)
- [qusql-mysql-type-plugin on PyPI](https://pypi.org/project/qusql-mysql-type-plugin/)
- [Example: qusql-py-mysql-type-notes](https://github.com/antialize/qusql/tree/main/examples/qusql-py-mysql-type-notes)
- [Example: qusql-py-mysql-type-books](https://github.com/antialize/qusql/tree/main/examples/qusql-py-mysql-type-books)
