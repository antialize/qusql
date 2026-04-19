# qusql-sqlx-type: SQL queries that are checked at `cargo check`

If you have used [sqlx](https://github.com/launchbadge/sqlx) you almost
certainly know its `query!` macro.  It gives you compile-time checked SQL by
connecting to a real database during the build, verifying the query against the
live schema, and recording the result in a `sqlx-data.json` file that is checked
into source control for CI.  The feature is genuinely useful, but the workflow
has some friction:

- You need a running database when you run `cargo check` (or `cargo build`
  locally), unless the project has an up-to-date `sqlx-data.json`.
- Every schema change means re-running `cargo sqlx prepare` to regenerate that
  file and commit it alongside the code.
- In CI, you need to spin up a database (or carefully maintain `sqlx-data.json`)
  to get the compile step to succeed.

`qusql-sqlx-type` takes a different approach: **your schema lives in a plain SQL
file right next to your `Cargo.toml`**, and the proc-macro reads and parses it
at compile time without touching a database at all.  The tradeoff is that the
crate's schema parser has to understand your schema definition rather than asking the database;
in practice that covers the vast majority of real-world schemas.

## Quick start

We will walk through how to use the library, starting with adding the dependency
and writing a schema file, then showing how to write type-checked queries against
it.

Add the dependencies to your `Cargo.toml`

```toml
[dependencies]
qusql-sqlx-type = "*"
sqlx = { version = "*", features = ["postgres", "runtime-tokio"] }
tokio = { version = "*", features = ["full"] }
```

Place your schema in `sqlx-type-schema.sql` in the root of the crate
(alongside `Cargo.toml`):

```sql
-- -*- sql-product: postgres -*-
CREATE TABLE IF NOT EXISTS notes (
    id         integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    title      text        NOT NULL,
    body       text,
    pinned     boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

Now write queries exactly as you would with `sqlx::query!`, but import the macro
from `qusql_sqlx_type` instead:

```rust
use qusql_sqlx_type::query;

// Argument types checked: $1 must be compatible with `text NOT NULL`,
// $2 must be compatible with `text` (nullable; Option<...> is fine).
query!(
    "INSERT INTO notes (title, body) VALUES ($1, $2)",
    title,
    body
)
.execute(&pool)
.await?;

// Return types inferred from the schema, no annotations needed:
//   row.id         : i32                         (integer NOT NULL)
//   row.title      : String                      (text NOT NULL)
//   row.body       : Option<String>              (text, nullable)
//   row.created_at : chrono::DateTime<chrono::Utc> (timestamptz NOT NULL)
let notes = query!("SELECT id, title, body, created_at FROM notes ORDER BY id")
    .fetch_all(&pool)
    .await?;
```

If you write the wrong column name, pass the wrong argument type, or try to
treat a nullable column as if it were non-null, you get a **Rust compiler error**,
not a runtime panic.

## Error messages

One area where `qusql-sqlx-type` aims to be noticeably better than working
directly with a database driver is the quality of error messages.

When you run a query with a typo against PostgreSQL the error you get back at
runtime is:

```
Error: error returned from database: column "titl" does not exist
```

MySQL gives a different format but is equally a runtime error:

```
Error: error returned from database: 1054 (42S22): Unknown column 'titl' in 'field list'
```

With `qusql-sqlx-type` the error is caught at compile time and rendered using
[ariadne](https://github.com/zesterer/ariadne)-style diagnostics.  The message
is attached to the `query!` call as a Rust compiler error, with a labelled span
pointing to the exact character inside the SQL string literal:

```
error:    ╭─[ query:1:8 ]
          │
        1 │ SELECT titl, body FROM notes WHERE id = $1
          │        ──┬─  
          │          ╰─── Unknown identifier
          │ 
          │ Help: did you mean `title`?
       ───╯
 --> src/main.rs:7:24
  |
7 |     let _rows = query!("SELECT titl, body FROM notes WHERE id = $1", id)
  |                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
```

The same mechanism applies to type errors inside the SQL itself: mismatched
types in a comparison, an argument bound to the wrong placeholder, or a nullable
column used where a non-null value is required all produce labelled compile
errors that point you straight to the problem.

## How it differs from `sqlx::query!` in practice

With `sqlx::query!` the schema truth lives in the running database.  The macro
connects, asks the database to describe the query, and records the answers.
That's powerful (it handles every database feature automatically), but it means
your build is always one step behind: you change the schema, run a migration, run
`cargo sqlx prepare`, commit `sqlx-data.json`, and only then do other developers
get accurate type information.

With `qusql-sqlx-type` the schema truth lives in `sqlx-type-schema.sql`, the
same file your application reads to bootstrap the database on first run.  There
is no side-channel file, no prepare step, and no running database needed.  When
you add a column, you update the SQL file; `cargo check` picks up the change
instantly on the next invocation.

This also makes the crate pleasant in monorepos where you might not want every
crate to have its own database connection string in the environment, and in
early-stage projects where the schema is still changing fast.

## The mental model: start with an empty database

The way to think about `sqlx-type-schema.sql` is: **this is exactly the SQL
you would run against a fresh, empty database to create every object the
application needs**.  The schema evaluator processes it top-to-bottom, executing
`CREATE TABLE`, `ALTER TABLE`, `CREATE TYPE`, `CREATE INDEX`, stored procedures,
PL/pgSQL functions, and so on, and builds an in-memory representation of the
resulting database state.  Every `query!` invocation is then type-checked against
that in-memory state.

### Dialect detection

Like sqlx, `qusql-sqlx-type` supports multiple database backends.  You specify
which database your schema targets by adding a short comment to the first line of
`sqlx-type-schema.sql`.  The macro reads this comment at compile time and selects
the right SQL dialect, argument placeholder style, and type mappings automatically:

```sql
-- -*- sql-product: postgres -*-   -> PostgreSQL mode ($1/$2/... arguments)
-- -*- sql-product: postgis -*-    -> PostGIS mode (same + PostGIS extensions)
-- (no comment, or any other)      -> MariaDB/MySQL mode (? arguments)
```

## Migrations in production

Because the schema file describes the final desired state, bootstrapping a fresh
database is trivial: just `sqlx::raw_sql(include_str!("../sqlx-type-schema.sql")).execute(&pool).await?`.

For migrating an *existing* database there are two common approaches:

**1. A separate migrations folder** (e.g. with
[sqlx-migrate](https://github.com/launchbadge/sqlx) or any other migration tool).
You write your full schema in `sqlx-type-schema.sql` for type-checking
purposes, and maintain an independent set of incremental migration files for
production.  The two are manually kept in sync: a migration adds a column; you
also add it to the schema file.

**2. An idempotent schema with revision tracking**, the pattern shown in the
[`qusql-sqlx-type-books` example](https://github.com/antialize/qusql/tree/main/examples/qusql-sqlx-type-books).
The schema file itself contains a small `schema_revisions` table and a PL/pgSQL
`apply_revision()` function.  Each logical migration is a `SELECT apply_revision(...)`
call that wraps a block of schema statements; the function skips it if the revision name has
already been recorded.  Running the schema file against a live database is
therefore idempotent: it applies only the new revisions, in order, in a single
transaction:

```sql
BEGIN;

CREATE TABLE IF NOT EXISTS schema_revisions (
    id             integer  PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name           text     NOT NULL UNIQUE,
    sequence_index integer  NOT NULL UNIQUE,
    applied_at     timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION apply_revision(rev_name text, description text,
    seq_idx integer, command text) RETURNS BOOLEAN AS $$ ... $$ LANGUAGE plpgsql;

-- Revision 0
SELECT apply_revision('init', 'Initial schema', 0, $rev$
  CREATE TABLE IF NOT EXISTS notes ( ... );
$rev$);

-- Revision 1: added pinned column
SELECT apply_revision('add_pinned', 'Add pinned flag', 1, $rev$
  ALTER TABLE notes ADD COLUMN IF NOT EXISTS pinned boolean NOT NULL DEFAULT false;
$rev$);

COMMIT;
```

The schema evaluator is smart enough to process `DO`, `CREATE OR REPLACE
FUNCTION`, `SELECT apply_revision(..., $rev$ ... $rev$)` revision blocks, and bare `BEGIN`/`COMMIT`
blocks, so the `query!` macros see the fully-evaluated state regardless of which
pattern you choose.

## The type system

### Nullability is first-class

`sqlx::query!` does track nullability, using the database engine to determine
whether a column can be null.  For a plain column reference like `SELECT x FROM t`
this works well, and it is often preserved through simple expressions like `x + x`.
However it is frequently lost through function calls, and the database engine has
no way to narrow nullability based on your `WHERE` clause.

`qusql-sqlx-type` goes further in two ways.  First, it analyses `WHERE` clauses
to narrow nullability: if your query is

```sql
SELECT x FROM t WHERE x IS NOT NULL
```

or

```sql
SELECT x FROM t WHERE x = $1
```

then `x` is known to be non-null in the result and the macro gives it type `T`
rather than `Option<T>`.

### Argument type checking is stricter than sqlx

`sqlx::query!` generally accepts any type that implements the right `sqlx::Encode`
trait for the target column type.  `qusql-sqlx-type` has a curated set of
allowed conversions per SQL type.  For example, you cannot accidentally bind a
`f64` where a column expects `integer`: only `i8` through `i64` (and their
unsigned counterparts) are accepted for integer columns.  This catches a real
class of bugs where a loosely-typed intermediate value gets passed to the wrong
placeholder.

Type checking also applies *inside* the SQL query itself.  Most databases allow
implicit coercions that are technically valid but hide likely mistakes: using an
integer where a boolean is expected, or comparing an integer column to a
floating-point argument.  `qusql-sqlx-type` rejects these.  If you genuinely
need a cross-type operation you can make the intent explicit with a SQL cast:

```sql
SELECT * FROM t WHERE active = $1::integer   -- explicit cast, accepted
SELECT * FROM t WHERE score > $1::float8     -- explicit cast, accepted
```

## MySQL/MariaDB support and the `_LIST_` hack

For MySQL and MariaDB that do not support lists as arguments we have added a **`_LIST_` hack**.

Queries with `IN (...)` clauses over a runtime-determined list of values are
notoriously awkward in sqlx because sqlx does not support dynamically-sized
parameter lists.  `qusql-sqlx-type` solves this with a special placeholder:

```rust
// Pass a slice; the macro expands _LIST_ to the correct number of ? at runtime.
let ids: Vec<i32> = vec![1, 2, 3];
let rows = query!("SELECT id, title FROM notes WHERE id IN (_LIST_)", &ids,)
    .fetch_all(&pool)
    .await?;
```

`_LIST_` is replaced with the right number of `?` placeholders based on the
slice length.  If the slice is empty it is replaced with `NULL` so that `IN
(NULL)` is always a valid SQL expression.  Note that `x IN (NULL)` evaluates
to `UNKNOWN` (never `TRUE`) for any value of `x`, so a query with an empty
list will return no rows, which is the expected behaviour.

## What is supported; what is not

The schema evaluator understands the most common schema and data manipulation statements used in real-world
applications:

- `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE`
- `CREATE INDEX` / `DROP INDEX`
- `CREATE TYPE ... AS ENUM` (PostgreSQL)
- `CREATE VIEW` / `CREATE MATERIALIZED VIEW`
- `CREATE FUNCTION` / `CREATE OR REPLACE FUNCTION` (PL/pgSQL)
- `CREATE PROCEDURE` / `CALL` (MySQL stored procedures)
- `DO $$ ... $$` anonymous blocks
- `INSERT`, `UPDATE`, `DELETE`, `SELECT` including `JOIN`, subqueries,
  `WINDOW` functions, `CTEs` (`WITH`), `RETURNING`, `ON CONFLICT`

Many (most) of the SQL functions and operators are understood: aggregates, string
functions, date arithmetic, JSON operators, and so on.  That said, the parser is
not a full PostgreSQL or MySQL engine.  Some less common functions or vendor
extensions may not yet be recognized, in which case the macro will emit a warning
(not a hard error) and fall back to a permissive unknown type for that expression.

**Pull requests and issues are very welcome** for anything you find missing.  The
parser is written in pure Rust (no unsafe code) and is straightforward to extend.
The project is at [github.com/antialize/qusql](https://github.com/antialize/qusql).

## Examples

To make the concepts above concrete, the repository includes two fully-working
example programs that you can clone and run against a local PostgreSQL instance.
They are intentionally small enough to read in a few minutes, and together they
cover the full range from a single-table beginner setup to a multi-table schema
with enums, UUIDs, and migrations.

- [`qusql-sqlx-type-notes`](https://github.com/antialize/qusql/tree/main/examples/qusql-sqlx-type-notes):
  a small CLI that adds, lists, pins, and deletes notes from a single-table
  PostgreSQL schema.  Good for getting started.

- [`qusql-sqlx-type-books`](https://github.com/antialize/qusql/tree/main/examples/qusql-sqlx-type-books):
  a library catalog with authors, books, loans, and reviews; demonstrates UUIDs,
  user-defined `ENUM` types, `JOIN` queries, `RETURNING`, `ON CONFLICT`, the
  idempotent migration pattern, and `chrono` date types.

Both compile without a running database (`cargo check` and the `query!` macros
work offline) and can be run against a local PostgreSQL instance with a single
`DATABASE_URL` environment variable.

## Links

- [crates.io](https://crates.io/crates/qusql-sqlx-type)
- [docs.rs](https://docs.rs/qusql-sqlx-type)
- [GitHub](https://github.com/antialize/qusql)
