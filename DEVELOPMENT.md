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

### Dialect-Specific Features

Parse syntax unconditionally, then emit errors conditionally based on dialect:

```rust
// Preferred - parse first, validate later
let truncate_span = parser.consume_keyword(Keyword::TRUNCATE)?;
let restart_span = parser.consume_keywords(&[Keyword::RESTART, Keyword::IDENTITY])?;
parser.postgres_only(&restart_span);  // Error only if not PostgreSQL

// Avoid - checking dialect before parsing
if parser.options.dialect.is_postgresql() {
    // Parse RESTART IDENTITY
}
```

This approach:
- Produces better error messages showing what was parsed
- Keeps parsing logic simpler and more uniform
- Makes it easier to test syntax without dialect restrictions

### Enum Changes and Pattern Matching

When changing enum variants (e.g., tuple to struct variant), update ALL pattern matches across the codebase:

```rust
// Changed enum
pub enum CreateOption {
    Temporary { local_span: Option<Span>, temporary_span: Span },  // Was: Temporary(Span)
}

// Must update all matches, including in other crates:
// - qusql-parse/src/create.rs
// - qusql-type/src/schema.rs
// - Any other files that match on this enum
```

Use `cargo build --all` to find all compilation errors after enum changes.

### Exporting New Types

When adding new public types, remember to export them in `lib.rs`:

```rust
// In qusql-parse/src/lib.rs
pub use truncate::{CascadeOption, IdentityOption, TruncateTable, TruncateTableSpec};
pub use create::CreateOption;  // If variants changed
```

### Error Recovery

Use `parser.recovered()` to parse within expected boundaries and provide better error messages:

```rust
parser.recovered(
    "')' or ','",  // What we're looking for
    &|t| matches!(t, Token::RParen | Token::Comma),  // Stop conditions
    |parser| {
        // Parse content
        let value = parser.consume_string()?;
        values.push(value);
        Ok(())
    },
)?;
```

This allows the parser to skip malformed input and continue parsing.

## Common Patterns

### Adding Keywords

1. Add to `qusql-parse/src/keywords.rs` in alphabetical order
2. Use in parser with `Token::Ident(_, Keyword::YOUR_KEYWORD)`
3. Consume with `parser.consume_keyword(Keyword::YOUR_KEYWORD)?`

### Adding New Statement Types

1. Define struct in appropriate file (e.g., `create.rs`, `select.rs`)
2. Add variant to `Statement` enum in `statement.rs`
3. Implement `Spanned` trait
4. Add parsing function `parse_your_statement()`
5. Export types in `lib.rs`
6. Add tests to `parse-test/postgres-tests.json` or `mysql-tests.json`

## Troubleshooting

### "Expected tuple struct or tuple variant" Error

This means an enum variant changed from tuple to struct form. Update the pattern match:

```rust
// Old: Temporary(span)
// New: Temporary { local_span, temporary_span }
```

### Tests Showing "failure: true"

Run with `--update-output` to see the actual output, then decide if it's correct:

```bash
python3 test.py test-postgresql --update-output --filter "YOUR SQL"
```

### Clippy Warnings

Fix before committing:
- Unused imports: Remove them
- Collapsible if statements: Use `if let` chains or `&&`
- Empty line after doc comment: Remove the empty line

## Git Workflow

When making changes, prefer multiple focused commits over one large commit:

```bash
# Make focused commits
git add -p  # Add specific changes
git commit -m "descriptive message"
```
