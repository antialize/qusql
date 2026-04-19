# Introduction

Qusql is a collection of crates that make it quicker and easier to write
**correct, type-safe SQL** in Rust and Python.  MariaDB/MySQL and PostgreSQL are
both first-class citizens.

## The problem

SQL queries are usually just strings at compile time.  Mistakes like wrong column
names, wrong argument counts, and mismatched types only surface at runtime, often
in production.

Qusql solves this by **parsing your schema at compile time** and checking every
query against it before your binary is built.

## Crates at a glance

| Crate | What it does |
|---|---|
| [qusql-parse](qusql-parse.md) | SQL lexer and parser; produces an AST |
| [qusql-type](qusql-type.md) | Type-inference engine; checks queries against a schema |
| [qusql-mysql-type](qusql-mysql-type.md) | Compile-time typed MySQL/MariaDB queries in Rust (via `qusql-mysql`) |
| [qusql-sqlx-type](qusql-sqlx-type.md) | Compile-time typed PostgreSQL queries in Rust (via sqlx) |
| [qusql-py-mysql-type](qusql-py-mysql-type.md) | mypy-checked MySQL queries in Python |

## Source code

Everything lives in one repository:
<https://github.com/antialize/qusql>

Each crate also has a worked example in the
[`examples/`](https://github.com/antialize/qusql/tree/main/examples) directory.
