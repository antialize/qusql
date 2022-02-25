# sql-type
[![crates.io](https://img.shields.io/crates/v/sql-type.svg)](https://crates.io/crates/sql-type)
[![crates.io](https://docs.rs/sql-type/badge.svg)](https://docs.rs/sql-type)
[![License](https://img.shields.io/crates/l/sql-type.svg)](https://github.com/antialize/sql-type)
[![actions-badge](https://github.com/antialize/sql-type/workflows/Rust/badge.svg?branch=main)](https://github.com/antialize/sql-type/actions)

Type sql statements

This crate provides a facility to process a sql schema definition, and
then use this definition to type the argument and return value
of sql statements.

Currently primarily focused on MariaDB/Mysql.

Example code:
```rust
use sql_type::{schema::parse_schemas, type_statement, TypeOptions,
    SQLDialect, SQLArguments, StatementType};
let schemas = "
    CREATE TABLE `events` (
      `id` bigint(20) NOT NULL,
      `user` int(11) NOT NULL,
      `message` text NOT NULL
    );";

let mut issues = Vec::new();

// Compute terse representation of the schemas
let schemas = parse_schemas(schemas,
    &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB));
assert!(issues.is_empty());

let sql = "SELECT `id`, `user`, `message` FROM `events` WHERE `id` = ?";
let stmt = type_statement(&schemas, sql, &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB).arguments(SQLArguments::QuestionMark));
assert!(issues.is_empty());

let stmt = match stmt {
    StatementType::Select{columns, arguments} => {
        assert_eq!(columns.len(), 3);
        assert_eq!(arguments.len(), 1);
    }
    _ => panic!("Expected select statement")
};
```
