# qusql-parse

`qusql-parse` is a fast, zero-dependency SQL parser for Rust.  It turns a SQL
string into an Abstract Syntax Tree (AST) and emits structured diagnostics
(errors and warnings) with **byte-accurate source spans** so you can display
them to users.

## Supported dialects

Pass a `SQLDialect` to `ParseOptions` to select the right keyword set, identifier quoting rules, and type names for your database:

| Dialect | `SQLDialect` value |
|---|---|
| MariaDB / MySQL | `SQLDialect::MariaDB` |
| PostgreSQL | `SQLDialect::PostgreSQL` |
| PostGIS | `SQLDialect::PostGIS` |
| SQLite | `SQLDialect::Sqlite` |

## Argument placeholder styles

The parser needs to know which placeholder syntax your driver uses so it can parse arguments correctly and assign them indices:

| Style | `SQLArguments` value | Example |
|---|---|---|
| `?` | `SQLArguments::QuestionMark` | MariaDB / MySQL |
| `$1`, `$2`, ... | `SQLArguments::Dollar` | PostgreSQL |
| `%s` | `SQLArguments::Percent` | Python drivers |

## Basic usage

```rust
use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statement, Issues};

let options = ParseOptions::new()
    .dialect(SQLDialect::MariaDB)
    .arguments(SQLArguments::QuestionMark)
    .warn_unquoted_identifiers(true);

let sql = "SELECT `id`, `title` FROM `notes` WHERE `id` = ?";
let mut issues = Issues::new(sql);
let ast = parse_statement(sql, &mut issues, &options);

// Issues implements Display - prints a plain-text summary of all diagnostics.
println!("{}", issues);
println!("AST: {:#?}", ast);
```

## Error recovery

The parser is deliberately **error-tolerant**: it keeps going after a syntax
error and returns the best AST it can produce.  All problems are collected in
`Issues` rather than returned as a `Result`.  This means you can highlight
multiple errors in one pass, which is important for editor tooling and linters.

## Source spans

Every AST node implements `Spanned`, which returns a `Range<usize>` of **byte**
offsets into the original source string.  If you need character offsets (e.g.
for [ariadne](https://github.com/zesterer/ariadne) diagnostics), convert with
`ByteToChar`:

```rust
use qusql_parse::ByteToChar;

let b2c = ByteToChar::new(sql.as_bytes());
let char_span = b2c.map_span(byte_span.start..byte_span.end);
```

## Links

- [crates.io](https://crates.io/crates/qusql-parse)
- [docs.rs](https://docs.rs/qusql-parse)
- [Example: qusql-parse-lint](https://github.com/antialize/qusql/tree/main/examples/qusql-parse-lint) - a command-line SQL linter built on this crate
