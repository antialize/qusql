# qusql-type
[![crates.io](https://img.shields.io/crates/v/qusql-type.svg)](https://crates.io/crates/qusql-type)
[![crates.io](https://docs.rs/qusql-type/badge.svg)](https://docs.rs/qusql-type)
[![License](https://img.shields.io/crates/l/qusql-type.svg)](https://github.com/antialize/qusql)

Type SQL statements

This crate provides a facility to process a SQL schema definition, and
then use that definition to type the argument and return value
of SQL statements.  Both MariaDB/MySQL and PostgreSQL are well supported.

Example code:
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
let schemas = parse_schemas(schemas,
    &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB));
assert!(issues.is_ok());

let sql = "SELECT `id`, `user`, `message` FROM `events` WHERE `id` = ?";
let mut issues = Issues::new(sql);
let stmt = type_statement(&schemas, sql, &mut issues,
    &TypeOptions::new().dialect(SQLDialect::MariaDB).arguments(SQLArguments::QuestionMark));
assert!(issues.is_ok());

match stmt {
    StatementType::Select{columns, arguments} => {
        assert_eq!(columns.len(), 3);
        assert_eq!(arguments.len(), 1);
    }
    _ => panic!("Expected select statement")
};
```

See also: [`examples/qusql-type-check`](../examples/qusql-type-check) - a
command-line tool that prints the inferred types for a set of queries against a
given schema.
