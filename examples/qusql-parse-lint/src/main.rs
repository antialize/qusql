//! qusql-parse-lint -- a command-line SQL syntax checker powered by qusql-parse.
//!
//! Parses one or more SQL files and reports warnings and errors with source
//! location information using pretty ariadne diagnostics.  Exits non-zero when
//! any file contains a parse error.  Warnings are printed but do not affect the
//! exit code.
//!
//! # Usage
//!
//! ```text
//! qusql-parse-lint [OPTIONS] <file.sql> [<file.sql> ...]
//! ```

use ariadne::{Color, Label, Report, ReportKind, Source};
use clap::{Parser, ValueEnum};
use qusql_parse::{
    ByteToChar, Issue, Issues, Level, ParseOptions, SQLArguments, SQLDialect, parse_statements,
};
use std::process;

// ---------------------------------------------------------------------------
// CLI argument types
//
// clap's `ValueEnum` derive generates the --dialect / --arguments flag values
// from these enums automatically.  We keep them separate from qusql-parse's
// own enums so clap can produce clean lowercase flag values (e.g. "mariadb"
// rather than "MariaDB").
// ---------------------------------------------------------------------------

#[derive(Clone, ValueEnum)]
enum Dialect {
    Mariadb,
    Postgresql,
    Sqlite,
}

// Map our CLI enum to qusql-parse's SQLDialect.
impl From<Dialect> for SQLDialect {
    fn from(d: Dialect) -> Self {
        match d {
            Dialect::Mariadb => SQLDialect::MariaDB,
            Dialect::Postgresql => SQLDialect::PostgreSQL,
            Dialect::Sqlite => SQLDialect::Sqlite,
        }
    }
}

#[derive(Clone, ValueEnum)]
enum ArgStyle {
    /// `?` positional placeholders (MySQL / MariaDB style)
    QuestionMark,
    /// `$1`, `$2`, ... dollar-number placeholders (PostgreSQL style)
    Dollar,
    /// `%s` / `%d` percent-style placeholders
    Percent,
}

// Map our CLI enum to qusql-parse's SQLArguments.
impl From<ArgStyle> for SQLArguments {
    fn from(a: ArgStyle) -> Self {
        match a {
            ArgStyle::QuestionMark => SQLArguments::QuestionMark,
            ArgStyle::Dollar => SQLArguments::Dollar,
            ArgStyle::Percent => SQLArguments::Percent,
        }
    }
}

#[derive(Parser)]
#[command(
    name = "qusql-parse-lint",
    about = "Lint SQL files using qusql-parse",
    long_about = "Parses SQL files and reports parse errors and optional style \
                  warnings (unquoted identifiers, non-uppercase keywords).\n\n\
                  Exits 0 when all files parse without errors, 1 when any errors \
                  are found, and 2 on usage/IO problems."
)]
struct Cli {
    /// SQL files to lint
    #[arg(required = true)]
    files: Vec<String>,

    /// SQL dialect to use when parsing
    #[arg(long, value_enum, default_value = "mariadb")]
    dialect: Dialect,

    /// Placeholder style used for query arguments
    #[arg(long, value_enum, default_value = "question-mark")]
    arguments: ArgStyle,

    /// Warn when identifiers are not backtick- or double-quote-quoted
    #[arg(long, default_value_t = false)]
    warn_unquoted_identifiers: bool,

    /// Warn when SQL keywords are not written in ALL CAPS
    #[arg(long, default_value_t = false)]
    warn_none_capital_keywords: bool,
}

// ---------------------------------------------------------------------------
// Ariadne diagnostic rendering
//
// qusql-parse works with byte offsets internally.  Ariadne expects character
// (Unicode scalar) offsets, so we use ByteToChar to convert before handing
// spans to ariadne.
//
// Each Issue has:
//   - a primary span + message (the main error/warning site)
//   - zero or more Fragment labels that point at related locations (e.g.
//     "defined here" notes pointing at an earlier declaration)
// ---------------------------------------------------------------------------

fn print_issues(path: &str, src: &str, issues: &[Issue<'_>]) {
    // ByteToChar builds an index that translates byte ranges to char ranges.
    let b2c = ByteToChar::new(src.as_bytes());
    for issue in issues {
        let span = b2c.map_span(issue.span.start..issue.span.end);
        let kind = match issue.level {
            Level::Error => ReportKind::Error,
            Level::Warning => ReportKind::Warning,
        };
        let label_color = match issue.level {
            Level::Error => Color::Red,
            Level::Warning => Color::Yellow,
        };
        // The tuple (path, span) tells ariadne which file a span belongs to.
        // We use the file path as the cache key since we only ever have one
        // source file per Report.
        let mut builder = Report::build(kind, (path, span.clone()))
            .with_message(&*issue.message)
            .with_label(
                Label::new((path, span))
                    .with_message(&*issue.message)
                    .with_color(label_color),
            );
        // Attach any secondary labels (fragments) in cyan so they're visually
        // distinct from the primary error/warning label.
        for frag in &issue.fragments {
            let fspan = b2c.map_span(frag.span.start..frag.span.end);
            builder = builder.with_label(
                Label::new((path, fspan))
                    .with_message(&*frag.message)
                    .with_color(Color::Cyan),
            );
        }
        builder.finish().eprint((path, Source::from(src))).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Core lint logic
// ---------------------------------------------------------------------------

fn lint_file(path: &str, options: &ParseOptions) -> bool {
    let src = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("error: cannot read '{path}': {e}");
        process::exit(2);
    });

    // parse_statements returns an AST even when there are errors; the errors
    // are collected into `issues` rather than being returned as a Result.
    let mut issues = Issues::new(&src);
    parse_statements(&src, &mut issues, options);

    let has_errors = issues.get().iter().any(|i| i.level == Level::Error);
    print_issues(path, &src, issues.get());
    // Return true (success) only when there are no errors; warnings are fine.
    !has_errors
}

fn main() {
    let cli = Cli::parse();

    // Build ParseOptions from the CLI flags.  All options are forwarded
    // directly to qusql-parse; we do not hard-code any defaults here so that
    // the binary's behaviour matches exactly what the flags say.
    let options = ParseOptions::new()
        .dialect(SQLDialect::from(cli.dialect))
        .arguments(SQLArguments::from(cli.arguments))
        .warn_unquoted_identifiers(cli.warn_unquoted_identifiers)
        .warn_none_capital_keywords(cli.warn_none_capital_keywords);

    let mut all_ok = true;
    for path in &cli.files {
        if !lint_file(path, &options) {
            all_ok = false;
        }
    }

    if !all_ok {
        process::exit(1);
    }
}
