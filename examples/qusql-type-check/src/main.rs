//! qusql-type-check -- inspect the inferred SQL types for a schema.
//!
//! Parses a schema file, then type-checks each SQL query passed as a
//! command-line argument and prints the inferred column/argument types.
//! Exits non-zero if any query is invalid according to the schema.
//!
//! # Usage
//!
//! ```text
//! qusql-type-check <schema.sql> <query> [<query> ...]
//! ```
//!
//! # Example
//!
//! ```text
//! qusql-type-check schema.sql \
//!     'SELECT `id`, `username`, `score` FROM `users` WHERE `id` = ?' \
//!     'INSERT INTO `posts` (`user_id`, `title`, `body`) VALUES (?, ?, ?)'
//! ```

use qusql_type::{
    ArgumentKey, Issues, Level, SQLArguments, SQLDialect, StatementType, TypeOptions,
    schema::parse_schemas, type_statement,
};
use std::process;

// Print the argument list that a statement expects.
// `ArgumentKey::Index` comes from positional placeholders (`?`, `$1`).
// `ArgumentKey::Identifier` comes from named placeholders (`:name`).
// FullType carries both the concrete type (e.g. `i32`, `string`) and a
// `not_null` flag derived from the column's schema definition.
fn print_arguments(arguments: &[(ArgumentKey<'_>, qusql_type::FullType<'_>)]) {
    if arguments.is_empty() {
        return;
    }
    println!("  arguments ({}):", arguments.len());
    for (key, ft) in arguments {
        let k = match key {
            ArgumentKey::Index(idx) => format!("${}", idx + 1),
            ArgumentKey::Identifier(n) => format!(":{n}"),
        };
        let null = if ft.not_null { "NOT NULL" } else { "NULL" };
        println!("    {k}: {t} {null}", t = ft.t);
    }
}

fn main() {
    let mut args_iter = std::env::args().skip(1);
    let schema_path = match args_iter.next() {
        Some(p) => p,
        None => {
            eprintln!("Usage: qusql-type-check <schema.sql> <query> [<query> ...]");
            process::exit(2);
        }
    };
    let queries: Vec<String> = args_iter.collect();
    if queries.is_empty() {
        eprintln!("Usage: qusql-type-check <schema.sql> <query> [<query> ...]");
        process::exit(2);
    }

    let schema_src = std::fs::read_to_string(&schema_path).unwrap_or_else(|e| {
        eprintln!("error: cannot read '{schema_path}': {e}");
        process::exit(2);
    });

    // TypeOptions threads dialect and argument style through both schema
    // parsing and statement typing.  We use the same options for both so that
    // column types inferred from the schema and argument types inferred from
    // the query are consistent.
    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark);

    // parse_schemas evaluates the DDL (CREATE TABLE, ALTER TABLE, stored
    // procedures, ...) and builds an in-memory schema representation.  Issues
    // such as duplicate table definitions or unknown types are collected here.
    let mut schema_issues = Issues::new(&schema_src);
    let schemas = parse_schemas(&schema_src, &mut schema_issues, &options);
    if !schema_issues.is_ok() {
        print!("{schema_issues}");
        // Warnings in the schema are non-fatal; errors mean the schema is
        // incomplete and any subsequent type-checking would be unreliable.
        if schema_issues.get().iter().any(|i| i.level == Level::Error) {
            process::exit(1);
        }
    }

    let mut all_ok = true;
    for (i, sql) in queries.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("Query {}: {sql}", i + 1);

        // type_statement parses the query, resolves every table/column
        // reference against `schemas`, and returns a StatementType describing
        // the result columns and the expected argument types.
        let mut issues = Issues::new(sql.as_str());
        let stmt = type_statement(&schemas, sql.as_str(), &mut issues, &options);

        if !issues.is_ok() {
            print!("{issues}");
            if issues.get().iter().any(|i| i.level == Level::Error) {
                all_ok = false;
                println!("  INVALID");
                continue;
            }
        }

        // StatementType is an enum: the variant tells us what kind of
        // statement it is, and each variant carries the type information that
        // is relevant for that kind (columns for SELECT, autoincrement flag
        // for INSERT, RETURNING columns for DML with RETURNING, etc.).
        match stmt {
            StatementType::Select { columns, arguments } => {
                println!("  type: SELECT");
                // Each SelectTypeColumn has an optional name (None for
                // expressions like `1 + 1` with no alias), a FullType, and a
                // span pointing at the expression in the source.
                println!("  columns ({}):", columns.len());
                for col in &columns {
                    let name = col.name.as_ref().map(|n| n.value).unwrap_or("<unnamed>");
                    let null = if col.type_.not_null {
                        "NOT NULL"
                    } else {
                        "NULL"
                    };
                    println!("    {name}: {t} {null}", t = col.type_.t);
                }
                print_arguments(&arguments);
            }
            StatementType::Insert {
                yield_autoincrement,
                arguments,
                returning,
            } => {
                // yield_autoincrement tells callers whether they can expect a
                // useful last-insert-id after executing this statement.
                println!("  type: INSERT  (autoincrement: {yield_autoincrement:?})");
                print_arguments(&arguments);
                if let Some(cols) = returning {
                    println!("  returning ({}):", cols.len());
                    for col in &cols {
                        let name = col.name.as_ref().map(|n| n.value).unwrap_or("<unnamed>");
                        let null = if col.type_.not_null {
                            "NOT NULL"
                        } else {
                            "NULL"
                        };
                        println!("    {name}: {t} {null}", t = col.type_.t);
                    }
                }
            }
            StatementType::Update {
                arguments,
                returning,
            } => {
                println!("  type: UPDATE");
                print_arguments(&arguments);
                if let Some(cols) = returning {
                    println!("  returning ({}):", cols.len());
                    for col in &cols {
                        let name = col.name.as_ref().map(|n| n.value).unwrap_or("<unnamed>");
                        let null = if col.type_.not_null {
                            "NOT NULL"
                        } else {
                            "NULL"
                        };
                        println!("    {name}: {t} {null}", t = col.type_.t);
                    }
                }
            }
            StatementType::Delete {
                arguments,
                returning,
            } => {
                println!("  type: DELETE");
                print_arguments(&arguments);
                if let Some(cols) = returning {
                    println!("  returning ({}):", cols.len());
                    for col in &cols {
                        let name = col.name.as_ref().map(|n| n.value).unwrap_or("<unnamed>");
                        let null = if col.type_.not_null {
                            "NOT NULL"
                        } else {
                            "NULL"
                        };
                        println!("    {name}: {t} {null}", t = col.type_.t);
                    }
                }
            }
            // StatementType::Invalid is returned when parsing succeeds but
            // type-checking fails (e.g. unknown table).  Errors are already
            // printed above via `issues`.
            StatementType::Invalid => {
                println!("  type: INVALID");
                all_ok = false;
            }
            // Transaction control (BEGIN/COMMIT), SET, LOCK, CALL, etc. carry
            // no column or argument type information worth printing.
            other => {
                println!("  type: {other:?}");
            }
        }
    }

    if !all_ok {
        process::exit(1);
    }
}
