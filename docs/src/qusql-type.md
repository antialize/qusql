# qusql-type

`qusql-type` is the type-inference engine at the heart of qusql.  You give it a
schema (CREATE TABLE, ALTER TABLE, stored procedures, PL/pgSQL functions, and so on) and
it tells you the **types of result columns and query arguments** for any SQL
statement, without needing a running database.

Both MariaDB/MySQL and PostgreSQL are well supported.  The parser covers
essentially the full grammar of both databases, and the type checker
understands almost all built-in functions and operators: aggregates, window
functions, string and date arithmetic, JSON operators, PostGIS geometry
functions, type casts, and more.  If a function is not yet recognised the
macro emits a warning and falls back to an unknown type rather than a hard
error, so new or obscure functions degrade gracefully rather than blocking
compilation.

## How it works

1. **Parse the schema** with `parse_schemas()`.  This evaluates the schema definition statements
   (CREATE TABLE, ALTER TABLE, CREATE VIEW, and so on) and builds an internal model of every
   table, view, procedure and function.
2. **Type a statement** with `type_statement()`.  This returns a `StatementType`
   that describes what the query produces and what arguments it expects.

```rust
use qusql_type::{
    schema::parse_schemas, type_statement, TypeOptions,
    SQLDialect, SQLArguments, StatementType, Issues,
};

let schema_sql = "
    CREATE TABLE notes (
        id    integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        title text    NOT NULL,
        body  text
    );";

let opts = TypeOptions::new().dialect(SQLDialect::PostgreSQL);

let mut issues = Issues::new(schema_sql);
let schemas = parse_schemas(schema_sql, &mut issues, &opts);
assert!(issues.is_ok());

let query = "SELECT id, title, body FROM notes WHERE id = $1";
let mut issues = Issues::new(query);
let stmt = type_statement(
    &schemas, query, &mut issues,
    &TypeOptions::new()
        .dialect(SQLDialect::PostgreSQL)
        .arguments(SQLArguments::Dollar),
);
assert!(issues.is_ok());

match stmt {
    StatementType::Select { columns, arguments } => {
        // columns[0] -> id   : i32,  not-null
        // columns[1] -> title: String, not-null
        // columns[2] -> body : Option<String>
        // arguments[0] -> i32 (the type of `id`)
        println!("{} columns, {} arguments", columns.len(), arguments.len());
    }
    _ => panic!("expected SELECT"),
}
```

## `StatementType` variants

| Variant | Produced by |
|---|---|
| `Select { columns, arguments }` | `SELECT` |
| `Insert { arguments }` | `INSERT` |
| `Update { arguments }` | `UPDATE` |
| `Delete { arguments }` | `DELETE` |
| `Call { arguments }` | `CALL` |
| `Invalid` | Statement that type-checks to a type error |
| `AlterTable` / `CreateTable` / ... | Schema definition statements |

## Schema evaluation model

`parse_schemas()` processes the schema string the same way a database would
bootstrap from an empty state: it reads statements top to bottom, executing
each one in order.  After the last statement the resulting in-memory model
reflects the fully-constructed database state, which `type_statement()` then
queries.

This means the schema string should contain **everything needed to build the
database from scratch**: `CREATE TABLE`, `ALTER TABLE`, `CREATE VIEW`,
`CREATE TYPE ... AS ENUM`, `CREATE FUNCTION`, `CREATE PROCEDURE`, `DO` blocks,
`BEGIN`/`COMMIT`, `INSERT` (for seed data used by `apply_revision`-style
migration helpers), and so on.

The evaluator understands `IF` / `ELSE` blocks and can follow a PL/pgSQL
`apply_revision()`-style pattern where each migration step is wrapped in a
function call that skips already-applied revisions.  `SELECT apply_revision(...,
$rev$ ... $rev$)` blocks are interpreted, so the schema sees the final
accumulated state regardless of which revision style you use.

### What is supported

| Statement | Notes |
|---|---|
| `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE` | Full column and constraint handling |
| `CREATE VIEW` / `CREATE MATERIALIZED VIEW` | |
| `CREATE TYPE ... AS ENUM` | PostgreSQL enum types |
| `CREATE FUNCTION` / `CREATE OR REPLACE FUNCTION` | PL/pgSQL body evaluated |
| `CREATE PROCEDURE` / `DROP PROCEDURE` | MySQL stored procedures |
| `CREATE INDEX` / `DROP INDEX` | Tracked but not type-checked |
| `CREATE TRIGGER` / `DROP TRIGGER` | Accepted, ignored |
| `DROP TABLE` / `DROP VIEW` / `DROP FUNCTION` | Remove object from model |
| `DO $$ ... $$` | Anonymous blocks |
| `BEGIN` / `COMMIT` | Transaction wrappers (assumed to commit) |
| `SELECT` (at schema level) | Evaluated for side effects (e.g. calling migration helpers) |
| `INSERT` (at schema level) | Evaluated for seed data side effects |
| `GRANT` / `COMMENT ON` / `ANALYZE` | Accepted, ignored |
| `IF` / `ELSE` | Conditional schema evolution |

### Obtaining a schema for an existing database

If you have an existing database rather than a hand-written schema file, most
database tools can export the schema without data:

- **MySQL / MariaDB via phpMyAdmin**: select the database, go to *Export*,
  choose *Custom*, tick *Structure only* (untick *Data*), and export as SQL.
  The resulting file can be used directly as your schema file.
- **MySQL / MariaDB via the command line**: `mysqldump --no-data mydb > schema.sql`
- **PostgreSQL via `pg_dump`**: `pg_dump --schema-only mydb > schema.sql`

You may want to lightly edit the exported file: remove
`SET` statements and `/*!...*/` MySQL version guards that are not needed for
type-checking, and add the dialect comment on the first line if you are using
the `qusql-sqlx-type` or `qusql-mysql-type` macros.

### Migration support

Because `parse_schemas()` processes schema statements sequentially from an
empty state, it naturally supports incremental migrations represented as
ordered schema statements.  There are two common patterns:

**Separate migrations folder.** Keep a canonical `schema.sql` that describes
the final desired state for type-checking purposes, and maintain a separate set
of incremental migration files for production use (e.g. with any migration
tool).  The two are kept in sync manually: when a migration adds a column, you
also add it to `schema.sql`.

**Idempotent schema with revision tracking.** The schema file itself contains a
`schema_revisions` table and a helper function (e.g. `apply_revision()`) that
skips a migration block if its revision name has already been recorded.  Each
logical migration is a `SELECT apply_revision(..., $rev$ ... $rev$)` call; running
the file against a live database is therefore idempotent.  `parse_schemas()`
evaluates the revision blocks and sees the full accumulated schema state, so
type-checking always reflects all revisions.  This pattern is shown in the
[`qusql-sqlx-type-books`
example](https://github.com/antialize/qusql/tree/main/examples/qusql-sqlx-type-books).

## Column types

Each column in `columns` is a `FullType`:

- `FullType.type_`: the base SQL type (e.g. `BaseType::String`, `BaseType::I64`)
- `FullType.not_null`: `true` if the column is `NOT NULL`; maps to `T` vs `Option<T>` in generated Rust

## Links

- [crates.io](https://crates.io/crates/qusql-type)
- [docs.rs](https://docs.rs/qusql-type)
- [Example: qusql-type-check](https://github.com/antialize/qusql/tree/main/examples/qusql-type-check): a CLI tool that prints inferred types for queries against a schema
