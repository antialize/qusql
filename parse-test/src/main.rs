//! Small CLI to parse a source file using `qusql-parse` and emit JSON results.

use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::fs;
use std::io::Read;
use std::path::PathBuf;

/// JSON output structure containing the parsed value and any issues.
#[derive(Serialize)]
struct ResultOut {
    /// The pretty-printed parsed AST (if parsing succeeded).
    value: Option<String>,
    /// Collected parse/analysis issues as strings.
    issues: Vec<String>,
    /// Whether parsing succeeded without issues.
    success: bool,
}

/// Command-line arguments for the parser tool.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the Rust source file to read SQL from. If omitted, read from stdin.
    #[arg(short, long, value_name = "FILE")]
    file: Option<PathBuf>,

    /// SQL dialect to use when parsing: maria|postgresql|sqlite
    #[arg(short, long, default_value = "maria")]
    dialect: DialectArg,
}

/// Supported SQL dialects for parsing.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum DialectArg {
    /// MariaDB / MySQL dialect
    Maria,
    /// PostgreSQL dialect
    Postgresql,
    /// SQLite dialect
    Sqlite,
}

/// Map CLI dialect argument to `qusql_parse::SQLDialect`.
fn map_dialect(d: DialectArg) -> qusql_parse::SQLDialect {
    match d {
        DialectArg::Maria => qusql_parse::SQLDialect::MariaDB,
        DialectArg::Postgresql => qusql_parse::SQLDialect::PostgreSQL,
        DialectArg::Sqlite => qusql_parse::SQLDialect::Sqlite,
    }
}

/// Entry point: parse the file given by `--file` and print JSON.
fn main() {
    let args = Args::parse();

    let src = match args.file {
        Some(path) => match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to read {}: {}", path.display(), e);
                std::process::exit(2);
            }
        },
        None => {
            let mut s = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut s) {
                eprintln!("Failed to read from stdin: {}", e);
                std::process::exit(2);
            }
            s
        }
    };

    let options = qusql_parse::ParseOptions::new()
        .dialect(map_dialect(args.dialect))
        .arguments(qusql_parse::SQLArguments::QuestionMark);
    let mut issues = qusql_parse::Issues::new(&src);

    let value =
        qusql_parse::parse_statement(&src, &mut issues, &options).map(|v| format!("{:#?}", v));

    let success = issues
        .issues
        .iter()
        .all(|issue| issue.level != qusql_parse::Level::Error);
    let issues = issues
        .issues
        .iter()
        .map(|issue| format!("{:#?}", issue))
        .collect();

    let result = ResultOut {
        value,
        issues,
        success,
    };
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
