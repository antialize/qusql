# Development Guide

This document contains useful commands and tips for developing and debugging qusql.

## Testing the Parser

### Quick SQL Parsing Test

To quickly test a SQL statement and see the parsed output or error messages:

```bash
echo "SQL STATEMENT HERE" | cargo run -- --dialect postgresql
```

Examples:
```bash
echo "CREATE MATERIALIZED VIEW IF NOT EXISTS stats AS SELECT * FROM data" | cargo run -- --dialect postgresql
echo "TRUNCATE users, orders RESTART IDENTITY" | cargo run -- --dialect postgresql
```

### Running Test Suite

From the `parse-test` directory:

```bash
# Run all PostgreSQL tests
python3 test.py test-postgresql

# Run tests matching a filter
python3 test.py test-postgresql --filter "CREATE MATERIALIZED"

# Update test expectations
python3 test.py test-postgresql --update-output --filter "CREATE MATERIALIZED"
```

## Building and Checking Code

```bash
# Build specific package
cargo build -p qusql-parse

# Format code
cargo fmt

# Run clippy
cargo clippy --all

# Both formatting and linting
cargo fmt && cargo clippy --all
```

## Parser Coding Conventions

### Consuming Multiple Keywords

Prefer using `consume_keywords(&[])` over repeated `consume_keyword()` invocations:

```rust
// Preferred
let span = parser.consume_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])?;

// Avoid
let if_span = parser.consume_keyword(Keyword::IF)?;
let not_span = parser.consume_keyword(Keyword::NOT)?;
let exists_span = parser.consume_keyword(Keyword::EXISTS)?;
let span = if_span.join_span(&not_span).join_span(&exists_span);
```

This is more concise and returns a single span covering all the keywords.

### Implementing Spanned Trait

When implementing the `Spanned` trait, start with a non-optional member and then chain calls to `join_span`, which accepts optional arguments:

```rust
// Preferred
impl Spanned for MyStruct {
    fn span(&self) -> Span {
        self.create_span               // Start with a required span
            .join_span(&self.options)  // join_span accepts Option<&impl Spanned>
            .join_span(&self.name)     // and Vec<impl Spanned>
            .join_span(&self.if_not_exists)  // chains naturally
    }
}

// Avoid starting with an optional field
impl Spanned for MyStruct {
    fn span(&self) -> Span {
        self.if_not_exists  // This is Option<Span> - awkward to start with
            .unwrap_or_else(|| self.create_span)
            .join_span(&self.name)
    }
}
```

This pattern ensures you always have a valid span to start with and can cleanly chain optional spans.

## Git Workflow

When making changes, prefer multiple focused commits over one large commit:

```bash
# Make focused commits
git add -p  # Add specific changes
git commit -m "descriptive message"
```
