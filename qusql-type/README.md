# qusql-type
[![crates.io](https://img.shields.io/crates/v/qusql-type.svg)](https://crates.io/crates/qusql-type)
[![crates.io](https://docs.rs/qusql-type/badge.svg)](https://docs.rs/qusql-type)
[![License](https://img.shields.io/crates/l/qusql-type.svg)](https://github.com/antialize/qusql)

Type sql statements

This crate provides a facility to process a SQL schema definition, and
then use this definition to type the argument and return value
of SQL statements.

Currently primarily focused on MariaDB/MySQL, with partial support for PostgreSQL and SQLite.

## How the type system works

Typing a statement is a two-phase process: first the schema is parsed into an internal
representation, and then individual SQL statements are typed against that representation.

### Phase 1 — Schema parsing (`schema::parse_schemas`)

`parse_schemas` accepts a string of SQL DDL statements (e.g. an `mysqldump` export) and
builds a `Schemas` value that compactly describes every table, view, procedure, function,
and index that was defined. The supported DDL statements are: `CREATE TABLE`, `CREATE VIEW`,
`CREATE FUNCTION`, `CREATE PROCEDURE`, `DROP TABLE/VIEW/FUNCTION/PROCEDURE`, and
`ALTER TABLE` (to add/modify/drop columns and add indices).

Each table or view becomes a `Schema` with an ordered list of `Column`s. Each column carries:
- its name (`Identifier`)
- its `FullType` (see below)
- flags: `auto_increment`, `default`, `generated`

SQL column types are mapped to the internal type representation during this phase.
`TINYINT(1)` without `UNSIGNED` becomes `Bool`, signed integer types become `I8`/`I16`/
`I24`/`I32`/`I64`, unsigned integers become `U8`/`U16`/`U24`/`U32`/`U64`, `FLOAT`/
`DOUBLE` become `F32`/`F64`, text types and `VARCHAR`/`CHAR` become `String`, `BLOB`/
`BINARY` variants become `Bytes`, `ENUM` and `SET` retain their variant lists verbatim,
and temporal columns map to `Date`, `DateTime`, `Time`, `TimeStamp`, or `TimeInterval`.
The `NOT NULL` property is preserved in `FullType::not_null`.

### Phase 2 — Statement typing (`type_statement`)

`type_statement` parses a single SQL statement and walks the parse tree with a `Typer`
context that holds:
- a reference to the `Schemas` built in phase 1
- a stack of *reference types* — the columns currently visible from the `FROM`/`JOIN`
  clauses of the statement being typed
- a list of *argument types* — the inferred types of query placeholders (`?`, `$1`, or
  named arguments)

The result is a `StatementType` enum with one variant per DML kind:

| Variant | Payload |
|---------|---------|
| `Select` | `columns: Vec<SelectTypeColumn>`, `arguments` |
| `Insert` | `yield_autoincrement`, `arguments`, optional `returning` columns |
| `Update` | `arguments`, optional `returning` columns |
| `Delete` | `arguments`, optional `returning` columns |
| `Replace` | `arguments`, optional `returning` columns |
| `Invalid` | (errors were added to `Issues`) |

### Type representation

There are three layers:

- **`BaseType`** — a coarse, canonical kind: `Any`, `Bool`, `Bytes`, `Date`, `DateTime`,
  `Float`, `Integer`, `String`, `Time`, `TimeStamp`, `TimeInterval`. Used for type
  compatibility checks and argument inference.
- **`Type<'a>`** — a fine-grained type that can be one of the concrete integer/float widths
  (`I8`/`U8`…`I64`/`U64`, `F32`, `F64`), `Enum(variants)`, `Set(variants)`, `JSON`,
  `Base(BaseType)` for abstract/unresolved types, `Null` (the SQL `NULL` literal), or
  `Invalid` (type error propagation). The internal `Args` variant carries argument index
  constraints and is never exposed to callers.
- **`FullType<'a>`** — wraps a `Type` with a `not_null: bool` flag and a `list_hack: bool`
  flag (for the `_LIST_` extension, see below).

Every `Type` can report its `BaseType` via `.base()`, which is used when performing
compatibility checks (e.g. adding two integer columns of different widths is still valid
because both have `BaseType::Integer`).

### Argument type inference

Query placeholders are typed lazily. When the typer encounters a placeholder in a position
where the expected type is known (e.g. `WHERE id = ?` where `id` is `I64 NOT NULL`), it
calls `constrain_arg`, which records that type for the placeholder. If the placeholder is
later found in a context with a different type, the more specific type wins. Arguments are
keyed by `ArgumentKey::Index(usize)` for positional placeholders or
`ArgumentKey::Identifier(&str)` for named ones.

### Handling of NULL and outer joins

`FullType::not_null` is set to `true` for `NOT NULL` columns and is propagated through
expressions. For `LEFT`/`RIGHT`/`FULL` outer joins, all columns from the optionally-null
side have `not_null` forced to `false`, ensuring callers know those values may be `NULL`
at runtime.

### CTEs (WITH queries)

`WITH` blocks are typed first and their resulting columns are injected into a temporary
virtual `Schema` that is made available to the rest of the query. This allows subsequent
`FROM` clauses to reference the CTE by name just like a real table.

### `_LIST_` hack

When the `list_hack` option is enabled, the special placeholder `_LIST_` can be used
inside an `IN (…)` clause to represent a dynamically-sized list of values. The
corresponding argument will have `FullType::list_hack = true` set, which signals to
callers that they need to expand the placeholder at runtime.

## Example code
```rust
use qusql_type::{schema::parse_schemas, type_statement, TypeOptions,
    SQLDialect, SQLArguments, StatementType, Issues};
let schemas = "
    CREATE TABLE `events` (
      `id` bigint(20) NOT NULL,
      `user` int(11) NOT NULL,
      `message` text NOT NULL
    );";

let mut issues = Issues::new(schemas);

// Compute terse representation of the schemas
let schemas = parse_schemas(schemas,
    &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB));
assert!(issues.is_ok());

let sql = "SELECT `id`, `user`, `message` FROM `events` WHERE `id` = ?";
let mut issues = Issues::new(sql);
let stmt = type_statement(&schemas, sql, &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB).arguments(SQLArguments::QuestionMark));
assert!(issues.is_ok());

let stmt = match stmt {
    StatementType::Select{columns, arguments} => {
        assert_eq!(columns.len(), 3);
        assert_eq!(arguments.len(), 1);
    }
    _ => panic!("Expected select statement")
};
```
