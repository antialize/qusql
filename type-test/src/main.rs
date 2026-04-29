use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::{Args, Parser, Subcommand, ValueEnum};
use qusql_type::{
    ArgumentKey, ByteToChar, FullType, Issues, Level, SQLArguments, SQLDialect, SelectTypeColumn,
    StatementType, TypeOptions, schema::parse_schemas, type_statement,
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

fn default_arguments(dialect: &SQLDialect) -> SQLArguments {
    match dialect {
        SQLDialect::PostgreSQL => SQLArguments::Dollar,
        _ => SQLArguments::QuestionMark,
    }
}

#[derive(Clone, ValueEnum, Debug, Default)]
enum ErrorFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Parser, Debug)]
#[command(about = "SQL schema type checker and query typer")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Parse a SQL schema file and output the computed column types as JSON
    Schema(SchemaArgs),
    /// Type SQL queries against a schema and output argument/result-column types as JSON
    Queries(QueriesArgs),
}

#[derive(Args, Debug)]
struct SchemaArgs {
    /// Path to the SQL schema file
    sql_file: PathBuf,

    /// SQL dialect to use when parsing
    #[arg(long, short, value_enum, default_value = "maria-db")]
    dialect: Dialect,

    /// How to display issues: pretty (ariadne) or json
    #[arg(long, short, value_enum, default_value = "pretty")]
    error_format: ErrorFormat,
}

#[derive(Args, Debug)]
struct QueriesArgs {
    /// Path to the SQL schema file
    schema_file: PathBuf,

    /// Path to the queries file (SQL with `-- query: <name>` separators)
    queries_file: PathBuf,

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
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    not_null: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    auto_increment: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    has_default: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    generated: bool,
}

#[derive(Serialize)]
struct JsonTable {
    name: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    view: bool,
    columns: Vec<JsonColumn>,
}

#[derive(Serialize)]
struct JsonSchema {
    tables: Vec<JsonTable>,
}

// ---- Serializable query types ----

#[derive(Serialize)]
struct JsonResultColumn {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(rename = "type")]
    type_: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    not_null: bool,
}

#[derive(Serialize)]
struct JsonArgument {
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(rename = "type")]
    type_: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    not_null: bool,
}

#[derive(Serialize)]
struct JsonQueryResult {
    query: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<JsonResultColumn>>,
    arguments: Vec<JsonArgument>,
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
    let byte_to_char = ByteToChar::new(src.as_bytes());
    for issue in issues.get() {
        let kind = match issue.level {
            Level::Error => ReportKind::Error,
            Level::Warning => ReportKind::Warning,
        };
        let char_span = byte_to_char.map_span(issue.span.start..issue.span.end);
        let span = (filename, char_span);
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
            let frag_char_span = byte_to_char.map_span(frag.span.start..frag.span.end);
            report = report.with_label(
                Label::new((filename, frag_char_span))
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

fn serialize_columns(cols: &[SelectTypeColumn<'_>]) -> Vec<JsonResultColumn> {
    cols.iter()
        .map(|col| JsonResultColumn {
            name: col.name.as_ref().map(|n| n.value.to_string()),
            type_: col.type_.t.to_string(),
            not_null: col.type_.not_null,
        })
        .collect()
}

fn serialize_arguments(args: &[(ArgumentKey<'_>, FullType<'_>)]) -> Vec<JsonArgument> {
    args.iter()
        .map(|(key, ft)| JsonArgument {
            index: match key {
                ArgumentKey::Index(i) => Some(*i),
                ArgumentKey::Identifier(_) => None,
            },
            name: match key {
                ArgumentKey::Index(_) => None,
                ArgumentKey::Identifier(s) => Some(s.to_string()),
            },
            type_: ft.t.to_string(),
            not_null: ft.not_null,
        })
        .collect()
}

/// Split a queries SQL file on `-- query: <name>` comment markers.
/// Returns a list of (name, sql_text) pairs.
fn split_queries(src: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_lines: Vec<&str> = Vec::new();

    for line in src.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("-- query:") {
            // Flush the previous query if any
            if let Some(name) = current_name.take() {
                let sql = current_lines.join("\n").trim().to_string();
                if !sql.is_empty() {
                    result.push((name, sql));
                }
            }
            current_name = Some(rest.trim().to_string());
            current_lines.clear();
        } else if current_name.is_some() {
            current_lines.push(line);
        }
    }
    // Flush the last query
    if let Some(name) = current_name {
        let sql = current_lines.join("\n").trim().to_string();
        if !sql.is_empty() {
            result.push((name, sql));
        }
    }
    result
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Schema(args) => run_schema(args),
        Command::Queries(args) => run_queries(args),
    }
}

fn run_schema(args: SchemaArgs) {
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
            name: name.table_name().value.to_string(),
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

fn run_queries(args: QueriesArgs) {
    let schema_sql = fs::read_to_string(&args.schema_file).unwrap_or_else(|e| {
        eprintln!("Failed to read {:?}: {e}", args.schema_file);
        process::exit(1);
    });
    let queries_src = fs::read_to_string(&args.queries_file).unwrap_or_else(|e| {
        eprintln!("Failed to read {:?}: {e}", args.queries_file);
        process::exit(1);
    });

    let schema_filename = args
        .schema_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("<schema>");
    let queries_filename = args
        .queries_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("<queries>");

    let dialect = SQLDialect::from(args.dialect);
    let schema_opts = TypeOptions::new().dialect(dialect.clone());
    let mut schema_issues = Issues::new(&schema_sql);
    let schemas = parse_schemas(&schema_sql, &mut schema_issues, &schema_opts);

    if schema_issues.get().iter().any(|i| i.level == Level::Error) {
        match args.error_format {
            ErrorFormat::Pretty => {
                print_issues_pretty(&schema_sql, schema_filename, &schema_issues)
            }
            ErrorFormat::Json => {
                let json_issues = issues_to_json(&schema_issues);
                eprintln!("{}", serde_json::to_string_pretty(&json_issues).unwrap());
            }
        }
        process::exit(1);
    }

    let arguments = default_arguments(&dialect);
    let query_opts = TypeOptions::new().dialect(dialect).arguments(arguments);

    let named_queries = split_queries(&queries_src);
    let mut results: Vec<JsonQueryResult> = Vec::new();
    let mut any_error = false;

    for (name, query_sql) in &named_queries {
        let mut q_issues = Issues::new(query_sql.as_str());
        let stmt_type = type_statement(&schemas, query_sql.as_str(), &mut q_issues, &query_opts);

        let has_errors = q_issues.get().iter().any(|i| i.level == Level::Error);
        if has_errors {
            any_error = true;
            match args.error_format {
                ErrorFormat::Pretty => {
                    eprintln!("-- query: {name}");
                    print_issues_pretty(query_sql.as_str(), queries_filename, &q_issues);
                }
                ErrorFormat::Json => {
                    let json_issues = issues_to_json(&q_issues);
                    eprintln!("{}", serde_json::to_string_pretty(&json_issues).unwrap());
                }
            }
        }

        let (kind, columns, arguments) = match stmt_type {
            StatementType::Select { columns, arguments } => (
                "select",
                Some(serialize_columns(&columns)),
                serialize_arguments(&arguments),
            ),
            StatementType::Insert {
                arguments,
                returning,
                ..
            } => (
                "insert",
                returning.as_deref().map(serialize_columns),
                serialize_arguments(&arguments),
            ),
            StatementType::Update {
                arguments,
                returning,
            } => (
                "update",
                returning.as_deref().map(serialize_columns),
                serialize_arguments(&arguments),
            ),
            StatementType::Delete {
                arguments,
                returning,
            } => (
                "delete",
                returning.as_deref().map(serialize_columns),
                serialize_arguments(&arguments),
            ),
            StatementType::Replace {
                arguments,
                returning,
            } => (
                "replace",
                returning.as_deref().map(serialize_columns),
                serialize_arguments(&arguments),
            ),
            StatementType::Call { arguments } => ("call", None, serialize_arguments(&arguments)),
            StatementType::Truncate => ("truncate", None, Vec::new()),
            StatementType::Transaction => ("transaction", None, Vec::new()),
            StatementType::Set => ("set", None, Vec::new()),
            StatementType::Lock => ("lock", None, Vec::new()),
            StatementType::Invalid => ("invalid", None, Vec::new()),
        };

        results.push(JsonQueryResult {
            query: name.clone(),
            kind: kind.to_string(),
            columns,
            arguments,
        });
    }

    println!("{}", serde_json::to_string_pretty(&results).unwrap());

    if any_error {
        process::exit(1);
    }
}
