use std::process::exit;

use qusql_type::{Issues, Level, SQLArguments, SQLDialect, TypeOptions, schema::parse_schemas};

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: schema-parse-test <mysql_schema.sql>");
        exit(2);
    });

    let schema = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("Error reading {path}: {e}");
        exit(2);
    });

    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark);

    let mut issues = Issues::new(&schema);
    let _schemas = parse_schemas(&schema, &mut issues, &options);

    if !issues.is_ok() {
        eprintln!("{issues}");
    }

    if issues.get().iter().any(|i| matches!(i.level, Level::Error)) {
        exit(1);
    }
}
