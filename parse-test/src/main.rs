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

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormatArg {
    /// Output the parsed AST as pretty-printed JSON.
    Json,
    /// Output the parsed AST as a debug string.
    PrettyJson,
    /// Output the parsed AST as a debug string.
    Pretty,
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

    /// Whether to run a benchmark (1000 iterations) instead of normal parsing.
    #[arg(short, long, default_value_t = false)]
    benchmark: bool,

    /// Output format: json|pretty-json|pretty
    #[arg(short, long, default_value = "json")]
    output_format: OutputFormatArg,

    /// Whether to parse multiple statements
    #[arg(short, long, default_value_t = false)]
    multiple: bool,
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
        .arguments(if matches!(args.dialect, DialectArg::Postgresql) {
            qusql_parse::SQLArguments::Dollar
        } else {
            qusql_parse::SQLArguments::QuestionMark
        });
    let mut issues = qusql_parse::Issues::new(&src);

    if args.benchmark {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            if args.multiple {
                qusql_parse::parse_statements(&src, &mut issues, &options);
            } else {
                qusql_parse::parse_statement(&src, &mut issues, &options);
            }
        }
        let duration = start.elapsed();
        println!("Benchmark: 1000 iterations took {:.2?}", duration);
    } else {
        let value = if args.multiple {
            let stms = qusql_parse::parse_statements(&src, &mut issues, &options);
            Some(format!("{:#?}", stms))
        } else {
            qusql_parse::parse_statement(&src, &mut issues, &options).map(|v| format!("{:#?}", v))
        };
        let success = issues
            .issues
            .iter()
            .all(|issue| issue.level != qusql_parse::Level::Error);

        match args.output_format {
            OutputFormatArg::Json => {
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
            OutputFormatArg::PrettyJson => {
                use ariadne::{Color, Label, Report, ReportKind, Source};
                let mut pretty_issues = Vec::new();
                for issue in issues.get() {
                    let mut w = Vec::new();
                    Report::build(ReportKind::Error, issue.span.clone())
                        .with_message(&issue.message)
                        .with_label(
                            Label::new(issue.span.clone())
                                .with_message("Issue here")
                                .with_color(Color::Red),
                        )
                        .finish()
                        .write(Source::from(&src), &mut w)
                        .unwrap();
                    pretty_issues.push(String::from_utf8(w).unwrap());
                }

                let result = ResultOut {
                    value,
                    issues: pretty_issues,
                    success,
                };
                println!("{}", serde_json::to_string_pretty(&result).unwrap());
            }
            OutputFormatArg::Pretty => {
                if let Some(value) = &value {
                    println!("Parsed AST:\n{}", value);
                } else {
                    println!()
                }
                println!("Issues:");
                for issue in issues.get() {
                    use ariadne::{Color, Label, Report, ReportKind, Source};
                    Report::build(ReportKind::Error, issue.span.clone())
                        .with_message(&issue.message)
                        .with_label(
                            Label::new(issue.span.clone())
                                .with_message("Issue here")
                                .with_color(Color::Red),
                        )
                        .finish()
                        .print(Source::from(&src))
                        .unwrap();
                }
                println!("Success: {}", success);
            }
        }

    }
}
