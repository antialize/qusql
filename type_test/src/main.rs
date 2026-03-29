use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::{Parser, ValueEnum};
use qusql_type::{
    Issues, Level, SQLDialect, TypeOptions,
    schema::parse_schemas,
};
use serde::Serialize;
use std::{fs, path::PathBuf, process};

#[derive(Clone, ValueEnum, Debug)]
enum Dialect {
    MariaDB,
    MySQL,
    PostgreSQL,
    Sqlite,
}

impl From<Dialect> for SQLDialect {
    fn from(d: Dialect) -> Self {
        match d {
            Dialect::MariaDB | Dialect::MySQL => SQLDialect::MariaDB,
            Dialect::PostgreSQL => SQLDialect::PostgreSQL,
            Dialect::Sqlite => SQLDialect::Sqlite,
        }
    }
}

#[derive(Clone, ValueEnum, Debug, Default)]
enum ErrorFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Parser, Debug)]
#[command(about = "Parse a SQL schema file and output the computed schema as JSON")]
struct Args {
    /// Path to the SQL schema file
    sql_file: PathBuf,

    /// SQL dialect to use when parsing
    #[arg(long, short, value_enum, default_value = "maria-db")]
    dialect: Dialect,

    /// How to display issues: pretty (ariadne) or json
    #[arg(long, short, value_enum, default_value = "pretty")]
    error_format: ErrorFormat,
}

// ---- Serializable schema types ----

#[derive(Serialize)]
struct JsonColumn {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    not_null: bool,
    auto_increment: bool,
    has_default: bool,
    generated: bool,
}

#[derive(Serialize)]
struct JsonTable {
    name: String,
    view: bool,
    columns: Vec<JsonColumn>,
}

#[derive(Serialize)]
struct JsonSchema {
    tables: Vec<JsonTable>,
}

// ---- Issue output helpers ----

#[derive(Serialize)]
struct JsonFragment {
    message: String,
    start: usize,
    end: usize,
    sql_segment: String,
}

#[derive(Serialize)]
struct JsonIssue {
    level: String,
    message: String,
    start: usize,
    end: usize,
    sql_segment: String,
    fragments: Vec<JsonFragment>,
}

fn print_issues_pretty(src: &str, filename: &str, issues: &Issues) {
    for issue in issues.get() {
        let kind = match issue.level {
            Level::Error => ReportKind::Error,
            Level::Warning => ReportKind::Warning,
        };
        let span = (filename, issue.span.start..issue.span.end);
        let issue_color = match issue.level {
            Level::Error => Color::Red,
            Level::Warning => Color::Yellow,
        };
        let mut report = Report::build(kind, span.clone())
            .with_message(&*issue.message)
            .with_label(
                Label::new(span)
                    .with_message(&*issue.message)
                    .with_color(issue_color),
            );
        for frag in &issue.fragments {
            report = report.with_label(
                Label::new((filename, frag.span.start..frag.span.end))
                    .with_message(&*frag.message)
                    .with_color(Color::Cyan),
            );
        }
        report
            .finish()
            .print((filename, Source::from(src)))
            .expect("ariadne print failed");
    }
}

fn issues_to_json(issues: &Issues) -> Vec<JsonIssue> {
    issues
        .get()
        .iter()
        .map(|issue| JsonIssue {
            level: match issue.level {
                Level::Error => "error".into(),
                Level::Warning => "warning".into(),
            },
            message: issue.message.to_string(),
            start: issue.span.start,
            end: issue.span.end,
            sql_segment: issue.sql_segment.to_string(),
            fragments: issue
                .fragments
                .iter()
                .map(|f| JsonFragment {
                    message: f.message.to_string(),
                    start: f.span.start,
                    end: f.span.end,
                    sql_segment: f.sql_segment.to_string(),
                })
                .collect(),
        })
        .collect()
}

fn main() {
    let args = Args::parse();

    let sql = fs::read_to_string(&args.sql_file).unwrap_or_else(|e| {
        eprintln!("Failed to read {:?}: {e}", args.sql_file);
        process::exit(1);
    });

    let filename = args
        .sql_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("<sql>");

    let options = TypeOptions::new().dialect(SQLDialect::from(args.dialect));
    let mut issues = Issues::new(&sql);
    let schemas = parse_schemas(&sql, &mut issues, &options);

    let has_errors = issues.get().iter().any(|i| i.level == Level::Error);

    if !issues.get().is_empty() {
        match args.error_format {
            ErrorFormat::Pretty => print_issues_pretty(&sql, filename, &issues),
            ErrorFormat::Json => {
                let json_issues = issues_to_json(&issues);
                println!("{}", serde_json::to_string_pretty(&json_issues).unwrap());
            }
        }
    }

    if has_errors {
        process::exit(1);
    }

    // Build serializable schema
    let mut tables: Vec<JsonTable> = schemas
        .schemas
        .into_iter()
        .map(|(name, schema)| JsonTable {
            name: name.value.to_string(),
            view: schema.view,
            columns: schema
                .columns
                .into_iter()
                .map(|col| JsonColumn {
                    name: col.identifier.value.to_string(),
                    type_: col.type_.t.to_string(),
                    not_null: col.type_.not_null,
                    auto_increment: col.auto_increment,
                    has_default: col.default,
                    generated: col.generated,
                })
                .collect(),
        })
        .collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    let schema = JsonSchema { tables };
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
