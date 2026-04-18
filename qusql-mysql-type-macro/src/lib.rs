//! Implementation of typed db query macros
//!
//! Used the exposed macros from the qusql-mysql-type crate
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::hash::Hash;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use ariadne::{Color, Label, Report, ReportKind, Source};
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned};
use qusql_type::schema::{parse_schemas, Schemas};
use qusql_type::{
    type_statement, ByteToChar, FullType, Issue, SQLArguments, SQLDialect, SelectTypeColumn,
    TypeOptions,
};
use syn::spanned::Spanned;
use syn::{parse::Parse, punctuated::Punctuated, Expr, Ident, LitStr, Token};
use yoke::{Yoke, Yokeable};

/// Do we support _LIST_ in queries
#[cfg(feature = "list_hack")]
const LIST_HACK: bool = true;
/// Do we support _LIST_ in queries
#[cfg(not(feature = "list_hack"))]
const LIST_HACK: bool = false;

/// Cache of resolved schema paths, keyed by `CARGO_MANIFEST_DIR`.
///
/// Path resolution is cheap when the schema sits next to `Cargo.toml`, but
/// falls back to `cargo metadata` (a subprocess) when it doesn't.  Either
/// way the result is stable for the lifetime of the proc-macro server process,
/// so we cache it per manifest-dir..
static RESOLVED_SCHEMA_PATHS: Mutex<Option<HashMap<PathBuf, PathBuf>>> = Mutex::new(None);

/// Resolve the path to `qusql-mysql-type-schema.sql` for the crate currently
/// being compiled.
///
/// The result is cached per `CARGO_MANIFEST_DIR`, so the (potentially
/// expensive) `cargo metadata` fallback runs at most once per crate per
/// proc-macro server process.
///
/// If the file does not exist next to the crate's own `Cargo.toml`, fall back
/// to the workspace root (the `CARGO_MANIFEST_DIR` workaround described in
/// https://github.com/rust-lang/cargo/issues/3946).
fn resolve_schema_path() -> PathBuf {
    let manifest_dir: PathBuf = std::env::var("CARGO_MANIFEST_DIR")
        .expect("`CARGO_MANIFEST_DIR` must be set")
        .into();

    let mut cache_guard = RESOLVED_SCHEMA_PATHS
        .lock()
        .expect("resolved schema paths lock poisoned");
    let cache = cache_guard.get_or_insert_with(HashMap::new);

    if let Some(cached) = cache.get(&manifest_dir) {
        return cached.clone();
    }

    // Not cached yet — do the (potentially expensive) resolution.
    let mut schema_path = manifest_dir.join("qusql-mysql-type-schema.sql");

    if !schema_path.exists() {
        use serde::Deserialize;
        use std::process::Command;

        let cargo = std::env::var("CARGO").expect("`CARGO` must be set");

        let output = Command::new(cargo)
            .args(["metadata", "--format-version=1"])
            .current_dir(&manifest_dir)
            .env_remove("__CARGO_FIX_PLZ")
            .output()
            .expect("Could not fetch metadata");

        /// Representation of `cargo metadata`
        #[derive(Deserialize)]
        struct CargoMetadata {
            /// The path where the workspace root is
            workspace_root: PathBuf,
        }

        let metadata: CargoMetadata =
            serde_json::from_slice(&output.stdout).expect("Invalid `cargo metadata` output");

        schema_path = metadata.workspace_root.join("qusql-mysql-type-schema.sql");
    }
    if !schema_path.exists() {
        panic!("Unable to locate qusql-mysql-type-schema.sql");
    }

    cache.insert(manifest_dir, schema_path.clone());
    schema_path
}

/// Wrapper so we can derive [Yokeable] for [Schemas] without modifying the
/// qusql-type crate.
#[derive(Yokeable)]
struct SchemasYoke<'a>(Schemas<'a>);

/// Parsed schema and its source string, kept alive together.
struct SchemaCacheEntry {
    /// The parsed schema yoked to the source it borrows from.
    schemas: Yoke<SchemasYoke<'static>, String>,
    /// File byte-length at the time of parsing, used for cache invalidation.
    file_len: u64,
    /// File modification time at the time of parsing, used for cache invalidation.
    modified: SystemTime,
    /// Hash of the schema content
    hash: u64,
}

/// Per-path cache of parsed schemas, invalidated by file mtime/size.
///
/// Keyed by resolved schema path so that multiple crates compiled in the same
/// proc-macro server process (e.g. rust-analyzer) each get their own entry.
static SCHEMA_CACHE: Mutex<Option<HashMap<PathBuf, Arc<SchemaCacheEntry>>>> = Mutex::new(None);

/// Return the parsed schema for the crate currently being compiled, reparsing
/// from disk only when the file's mtime or size has changed.
fn get_schemas() -> Arc<SchemaCacheEntry> {
    let path = resolve_schema_path();
    let meta = std::fs::metadata(&path).unwrap_or_else(|e| panic!("Cannot stat {path:?}: {e}"));
    let file_len = meta.len();
    let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);

    let mut cache_guard = SCHEMA_CACHE.lock().expect("schema cache lock poisoned");
    let cache = cache_guard.get_or_insert_with(HashMap::new);
    if let Some(entry) = cache.get(&path) {
        if entry.file_len == file_len && entry.modified == modified {
            return Arc::clone(entry);
        }
    }

    // Cache miss or stale — read and reparse.
    let src_string = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Unable to read schema from {path:?}: {e}"));

    // Compute the schema hash from the fresh source before we move it into the cart.
    let schema_hash = {
        use std::hash::{DefaultHasher, Hasher};
        let mut hasher = DefaultHasher::new();
        src_string.hash(&mut hasher);
        hasher.finish()
    };

    let schemas = Yoke::attach_to_cart(src_string, |src| {
        let options = TypeOptions::new();
        let mut issues = qusql_type::Issues::new(src);
        let parsed = parse_schemas(src, &mut issues, &options);
        if !issues.is_ok() {
            let b2c = ByteToChar::new(src.as_bytes());
            let source = NamedSource("qusql-mysql-type-schema.sql", Source::from(src));
            let mut err = false;
            for issue in issues.into_vec() {
                if issue.level == qusql_type::Level::Error {
                    err = true;
                }
                issue_to_report_color(issue, &b2c).eprint(&source).unwrap();
            }
            if err {
                panic!("Errors processing qusql-mysql-type-schema.sql");
            }
        }
        SchemasYoke(parsed)
    });

    let entry = Arc::new(SchemaCacheEntry {
        schemas,
        file_len,
        modified,
        hash: schema_hash,
    });
    cache.insert(path, Arc::clone(&entry));
    entry
}

/// Convert an [Issue] to a [Report]
fn issue_to_report(issue: Issue, b2c: &ByteToChar) -> Report<'static, std::ops::Range<usize>> {
    let span = b2c.map_span(issue.span);
    let mut builder: ariadne::ReportBuilder<'_, std::ops::Range<usize>> = Report::build(
        match issue.level {
            qusql_type::Level::Warning => ReportKind::Warning,
            qusql_type::Level::Error => ReportKind::Error,
        },
        span.clone(),
    )
    .with_config(ariadne::Config::default().with_color(false))
    .with_label(
        Label::new(span)
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message),
    );
    for frag in issue.fragments {
        builder =
            builder.with_label(Label::new(b2c.map_span(frag.span)).with_message(frag.message));
    }
    builder.finish()
}

/// Convert an [Issue] to a [Report] with colours
fn issue_to_report_color(
    issue: Issue,
    b2c: &ByteToChar,
) -> Report<'static, std::ops::Range<usize>> {
    let span = b2c.map_span(issue.span);
    let mut builder = Report::build(
        match issue.level {
            qusql_type::Level::Warning => ReportKind::Warning,
            qusql_type::Level::Error => ReportKind::Error,
        },
        span.clone(),
    )
    .with_label(
        Label::new(span)
            .with_color(match issue.level {
                qusql_type::Level::Warning => Color::Yellow,
                qusql_type::Level::Error => Color::Red,
            })
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message),
    );
    for frag in issue.fragments {
        builder = builder.with_label(
            Label::new(b2c.map_span(frag.span))
                .with_color(Color::Blue)
                .with_message(frag.message),
        );
    }
    builder.finish()
}

/// Source code wit attached name for [ariadne::Cache]
struct NamedSource<'a>(&'a str, Source<&'a str>);

impl<'a> ariadne::Cache<()> for &NamedSource<'a> {
    type Storage = &'a str;

    fn display<'b>(&self, _: &'b ()) -> Option<impl std::fmt::Display + 'b> {
        Some(self.0.to_string())
    }

    fn fetch(&mut self, _: &()) -> Result<&Source<Self::Storage>, impl std::fmt::Debug> {
        Ok::<_, ()>(&self.1)
    }
}

/// Map a [FullType] to a build in type or a tag type in [qusql_mysql_type]
fn map_type(ta: &FullType<'_>) -> proc_macro2::TokenStream {
    let t = match ta.t {
        qusql_type::Type::U8 => quote! {u8},
        qusql_type::Type::I8 => quote! {i8},
        qusql_type::Type::U16 => quote! {u16},
        qusql_type::Type::I16 => quote! {i16},
        qusql_type::Type::U24 => quote! {u32},
        qusql_type::Type::I24 => quote! {i32},
        qusql_type::Type::U32 => quote! {u32},
        qusql_type::Type::I32 => quote! {i32},
        qusql_type::Type::U64 => quote! {u64},
        qusql_type::Type::I64 => quote! {i64},
        qusql_type::Type::Base(qusql_type::BaseType::Any) => quote! {qusql_mysql_type::Any},
        qusql_type::Type::Base(qusql_type::BaseType::Bool) => quote! {bool},
        qusql_type::Type::Base(qusql_type::BaseType::Bytes) => quote! {&[u8]},
        qusql_type::Type::Base(qusql_type::BaseType::Date) => quote! {qusql_mysql_type::Date},
        qusql_type::Type::Base(qusql_type::BaseType::DateTime) => {
            quote! {qusql_mysql_type::DateTime}
        }
        qusql_type::Type::Base(qusql_type::BaseType::Float) => quote! {qusql_mysql_type::Float},
        qusql_type::Type::Base(qusql_type::BaseType::Integer) => quote! {qusql_mysql_type::Integer},
        qusql_type::Type::Base(qusql_type::BaseType::String) => quote! {&str},
        qusql_type::Type::Base(qusql_type::BaseType::Time) => quote! {qusql_mysql_type::Time},
        qusql_type::Type::Base(qusql_type::BaseType::TimeInterval) => todo!("time_interval"),
        qusql_type::Type::Base(qusql_type::BaseType::TimeStamp) => {
            quote! {qusql_mysql_type::Timestamp}
        }
        qusql_type::Type::Base(qusql_type::BaseType::Uuid) => quote! {&str},
        qusql_type::Type::Null => todo!("null"),
        qusql_type::Type::Invalid => quote! {std::convert::Infallible},
        qusql_type::Type::Enum(_) => quote! {&str},
        qusql_type::Type::Set(_) => quote! {&str},
        qusql_type::Type::Args(_, _) => todo!("args"),
        qusql_type::Type::F32 => quote! {f32},
        qusql_type::Type::F64 => quote! {f64},
        qusql_type::Type::JSON => quote! {qusql_mysql_type::Any},
        qusql_type::Type::Geometry => quote! {qusql_mysql_type::Any},
        qusql_type::Type::Array(_) => quote! {qusql_mysql_type::Any},
        qusql_type::Type::Range(_) => todo!(),
    };
    if !ta.not_null {
        quote! {Option<#t>}
    } else {
        t
    }
}

/// Generate code to validate and build arguments
fn handle_argumens(
    errors: &mut Vec<proc_macro2::TokenStream>,
    last_span: Span,
    args: &[Expr],
    arguments: &[(qusql_type::ArgumentKey<'_>, qusql_type::FullType)],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let mut at = Vec::new();
    let inv = qusql_type::FullType::invalid();
    for (k, v) in arguments {
        match k {
            qusql_type::ArgumentKey::Index(i) => {
                while at.len() <= *i {
                    at.push(&inv);
                }
                at[*i] = v;
            }
            qusql_type::ArgumentKey::Identifier(_) => {
                errors.push(
                    syn::Error::new(last_span.span(), "Named arguments not supported")
                        .to_compile_error(),
                );
            }
        }
    }

    if at.len() > args.len() {
        errors.push(
            syn::Error::new(
                last_span,
                format!("Expected {} additional arguments", at.len() - args.len()),
            )
            .to_compile_error(),
        );
    }

    if let Some(args) = args.get(at.len()..) {
        for arg in args {
            errors.push(syn::Error::new(arg.span(), "unexpected argument").to_compile_error());
        }
    }

    let mut arg_bindings = Vec::new();

    for (qa, ta) in args.iter().zip(at) {
        let t = map_type(ta);
        let span = qa.span();
        if ta.list_hack {
            arg_bindings.push(quote_spanned! {span=>
                qusql_mysql_type::check_arg_list_hack::<#t, _>(&#qa);
            });
        } else {
            arg_bindings.push(quote_spanned! {span=>
                qusql_mysql_type::check_arg::<#t, _>(&#qa);
            });
        }
    }

    let at: Vec<_> = args
        .iter()
        .map(|qa| {
            let span = qa.span();
            quote_spanned! {span=>
                (&#qa),
            }
        })
        .collect();

    (
        quote! {
            if false {
                #(#arg_bindings)*
                ::std::panic!();
            }
        },
        quote! {
            ( #(#at)* )
        },
    )
}

/// Output code to display issues to users
fn issues_to_errors(issues: Vec<Issue>, source: &str, span: Span) -> Vec<proc_macro2::TokenStream> {
    if !issues.is_empty() {
        let b2c = ByteToChar::new(source.as_bytes());
        let source = NamedSource("", Source::from(source));
        let mut err = false;
        let mut out = Vec::new();
        for issue in issues {
            if issue.level == qusql_type::Level::Error {
                err = true;
            }
            let r = issue_to_report(issue, &b2c);
            r.write(&source, &mut out).unwrap();
        }
        if err {
            return vec![syn::Error::new(span, String::from_utf8(out).unwrap()).to_compile_error()];
        }
    }
    Vec::new()
}

/// Construct row handling bits
fn construct_row(
    columns: &[SelectTypeColumn],
    owned: bool,
) -> (
    Vec<proc_macro2::TokenStream>,
    Vec<proc_macro2::TokenStream>,
    Vec<proc_macro2::TokenStream>,
    bool,
) {
    let mut row_members = Vec::new();
    let mut row_construct = Vec::new();
    let mut validate = Vec::new();
    let mut has_borrowed = false;
    for c in columns.iter() {
        let mut t = match c.type_.t {
            qusql_type::Type::U8 => quote! {u8},
            qusql_type::Type::I8 => quote! {i8},
            qusql_type::Type::U16 => quote! {u16},
            qusql_type::Type::I16 => quote! {i16},
            qusql_type::Type::I24 => quote! {i32},
            qusql_type::Type::U24 => quote! {u32},
            qusql_type::Type::U32 => quote! {u32},
            qusql_type::Type::I32 => quote! {i32},
            qusql_type::Type::U64 => quote! {u64},
            qusql_type::Type::I64 => quote! {i64},
            qusql_type::Type::Base(qusql_type::BaseType::Any) => todo!("from_any"),
            qusql_type::Type::Base(qusql_type::BaseType::Bool) => quote! {bool},
            qusql_type::Type::Base(qusql_type::BaseType::Bytes) if owned => quote! {Vec<u8>},
            qusql_type::Type::Base(qusql_type::BaseType::Bytes) => {
                has_borrowed = true;
                quote! {&'a [u8]}
            }
            qusql_type::Type::Base(qusql_type::BaseType::Date) => quote! {chrono::NaiveDate},
            qusql_type::Type::Base(qusql_type::BaseType::DateTime) => {
                quote! {chrono::NaiveDateTime}
            }
            qusql_type::Type::Base(qusql_type::BaseType::Float) => quote! {f64},
            qusql_type::Type::Base(qusql_type::BaseType::Integer) => quote! {i64},
            qusql_type::Type::Base(qusql_type::BaseType::String) if owned => quote! {String},
            qusql_type::Type::Base(qusql_type::BaseType::String) => {
                has_borrowed = true;
                quote! {&'a str}
            }
            qusql_type::Type::Base(qusql_type::BaseType::Time) => todo!("from_time"),
            qusql_type::Type::Base(qusql_type::BaseType::TimeInterval) => {
                todo!("from_time_interval")
            }
            qusql_type::Type::Base(qusql_type::BaseType::TimeStamp) => {
                quote! {chrono::DateTime<chrono::Utc>}
            }
            qusql_type::Type::Base(qusql_type::BaseType::Uuid) if owned => quote! {String},
            qusql_type::Type::Base(qusql_type::BaseType::Uuid) => {
                has_borrowed = true;
                quote! {&'a str}
            }
            qusql_type::Type::Null => todo!("from_null"),
            qusql_type::Type::Invalid => quote! {i64},
            qusql_type::Type::Enum(_) => quote! {String},
            qusql_type::Type::Set(_) => quote! {String},
            qusql_type::Type::Args(_, _) => todo!("from_args"),
            qusql_type::Type::F32 => quote! {f32},
            qusql_type::Type::F64 => quote! {f64},
            qusql_type::Type::JSON => quote! {String},
            qusql_type::Type::Geometry => quote! {Vec<u8>},
            qusql_type::Type::Array(_) => quote! {qusql_mysql_type::Any},
            qusql_type::Type::Range(_) => todo!(),
        };
        let name = match &c.name {
            Some(v) => v,
            None => continue,
        };

        let ident = format!("r#{}", name);
        let ident: Ident = if let Ok(ident) = syn::parse_str(&ident) {
            ident
        } else {
            // TODO error
            //errors.push(syn::Error::new(span, String::from_utf8(out).unwrap()).to_compile_error().into());
            continue;
        };

        let tident = format!("col_{}", name);
        let tident: Ident = if let Ok(tident) = syn::parse_str(&tident) {
            tident
        } else {
            // TODO error
            //errors.push(syn::Error::new(span, String::from_utf8(out).unwrap()).to_compile_error().into());
            continue;
        };

        if !c.type_.not_null {
            t = quote! {Option<#t>};
        }
        row_members.push(quote! {
            #ident : #t
        });

        let ct = map_type(&c.type_);
        validate.push(quote! {
            struct #tident;
            qusql_mysql_type::check_arg_out::<#tident, #ct, _>(&v.#ident);
        });

        let loc_str = format!("Column {}", name.value);
        row_construct.push(quote! {
            #ident: parser.next().loc(#loc_str)?
        });
    }

    (row_members, row_construct, validate, has_borrowed)
}

/// Parsed arguments for all the proc maracos in this cate
struct QueryInner {
    /// The as type if it is an as type query
    as_: Option<Ident>,
    /// The executor to run the query on
    executor: Expr,
    /// The query to run
    query: String,
    /// The span of the query to run
    query_span: Span,
    /// The arguments to the query
    args: Vec<Expr>,
    /// The span of the large thing
    last_span: Span,
}

/// Parsed arguments for an as query
struct AsQuery(QueryInner);

/// Parsed argument of a query without as
struct Query(QueryInner);

impl QueryInner {
    /// Parse proc macro arguments
    fn parse(input: syn::parse::ParseStream, with_as: bool) -> syn::Result<Self> {
        let as_ = if with_as {
            let as_ = input.parse::<Ident>()?;
            let _ = input.parse::<syn::token::Comma>()?;
            Some(as_)
        } else {
            None
        };

        let executor = input.parse::<Expr>()?;
        let _ = input.parse::<syn::token::Comma>()?;

        let query_ = Punctuated::<LitStr, Token![+]>::parse_separated_nonempty(input)?;
        let query: String = query_.iter().map(LitStr::value).collect();
        let query_span = query_.span();
        let mut last_span = query_span;
        let mut args = Vec::new();
        while !input.is_empty() {
            let _ = input.parse::<syn::token::Comma>()?;
            if input.is_empty() {
                break;
            }
            let arg = input.parse::<Expr>()?;
            last_span = arg.span();
            args.push(arg);
        }
        Ok(Self {
            as_,
            executor,
            query,
            query_span,
            args,
            last_span,
        })
    }
}

impl Parse for AsQuery {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(AsQuery(QueryInner::parse(input, true)?))
    }
}

impl Parse for Query {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Query(QueryInner::parse(input, false)?))
    }
}

/// How does the query fetch rows
enum FetchMode {
    /// Fetch all the rows into a vector
    All,
    /// Fetch Option row
    Optional,
    /// Fetch exactly one row
    One,
    /// Stream the rows one at a time
    Stream,
}

/// How is the data in a row stored
enum FetchType {
    /// We construct a row type that borrows strings and blobs
    Borrowed,
    /// We construct a row type that owns strings and blobs
    Owned,
    /// User supplied a row (as) type that borrows strings and blobs
    AsBorrowed,
    /// The user supplied a row (as) type that owns string and blobs
    AsOwned,
}

/// Statically typed execute query
#[doc(hidden)]
#[proc_macro]
pub fn execute_impl(input: TokenStream) -> TokenStream {
    let query = syn::parse_macro_input!(input as Query).0;
    let cache = get_schemas();
    let schemas = &cache.schemas.get().0;

    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .list_hack(LIST_HACK);

    let mut issues = qusql_type::Issues::new(&query.query);
    let stmt = type_statement(schemas, &query.query, &mut issues, &options);
    let mut errors = issues_to_errors(issues.into_vec(), &query.query, query.query_span);

    let schema_hash = cache.hash;

    let arguments: Result<&[_], _> = match &stmt {
        qusql_type::StatementType::Select { .. } => Err("SELECT"),
        qusql_type::StatementType::Delete {
            arguments,
            returning: None,
        } => Ok(arguments),
        qusql_type::StatementType::Delete {
            returning: Some(_), ..
        } => Err("DELETE with RETURNING"),
        qusql_type::StatementType::Insert {
            arguments,
            returning: None,
            ..
        } => Ok(arguments),
        qusql_type::StatementType::Insert {
            returning: Some(_), ..
        } => Err("INSERT with RETURNING"),
        qusql_type::StatementType::Update {
            arguments,
            returning: None,
        } => Ok(arguments),
        qusql_type::StatementType::Update {
            returning: Some(_), ..
        } => Err("UPDATE with RETURNING"),
        qusql_type::StatementType::Replace {
            arguments,
            returning: None,
        } => Ok(arguments),
        qusql_type::StatementType::Replace {
            returning: Some(_), ..
        } => Err("REPLACE with RETURNING"),
        qusql_type::StatementType::Truncate => Ok(&[]),
        qusql_type::StatementType::Call { arguments } => Ok(arguments),
        qusql_type::StatementType::Transaction => Ok(&[]),
        qusql_type::StatementType::Set => Ok(&[]),
        qusql_type::StatementType::Lock => Ok(&[]),
        qusql_type::StatementType::Invalid => {
            let s = quote! { {
                #(#errors; )*;
                todo!("Invalid")
            }};
            return s.into();
        }
    };
    let arguments = match arguments {
        Ok(v) => v,
        Err(_) => {
            let s = quote! { {
                #(#errors; )*;
                todo!("Invalid")
            }};
            return s.into();
        }
    };

    let (arg_check, arg_gen) =
        handle_argumens(&mut errors, query.last_span, &query.args, arguments);

    let e = query.executor;
    let q = query.query;
    quote! { {
        use qusql_mysql::connection::{WithLoc, ExecutorExt};

        const _SCHEMA_HASH: u64 = #schema_hash;

        #(#errors; )*
        #arg_check
        (#e).execute(#q, #arg_gen)
    }}
    .into()
}

/// Implementation of statically checked fetch queries
fn build_fetch_impl(input: TokenStream, mode: FetchMode, t: FetchType) -> TokenStream {
    let query = match t {
        FetchType::Borrowed | FetchType::Owned => syn::parse_macro_input!(input as Query).0,
        FetchType::AsBorrowed | FetchType::AsOwned => syn::parse_macro_input!(input as AsQuery).0,
    };

    let cache = get_schemas();
    let schemas = &cache.schemas.get().0;
    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .list_hack(LIST_HACK);
    let mut issues = qusql_type::Issues::new(&query.query);
    let stmt = type_statement(schemas, &query.query, &mut issues, &options);

    let mut errors = issues_to_errors(issues.into_vec(), &query.query, query.query_span);

    let schema_hash = cache.hash;

    let args = match &stmt {
        qusql_type::StatementType::Select { columns, arguments } => Ok((columns, arguments)),
        qusql_type::StatementType::Delete {
            returning: None, ..
        } => Err("DELETE"),
        qusql_type::StatementType::Delete {
            arguments,
            returning: Some(columns),
            ..
        } => Ok((columns, arguments)),
        qusql_type::StatementType::Insert {
            returning: None, ..
        } => Err("INSERT"),
        qusql_type::StatementType::Insert {
            arguments,
            returning: Some(columns),
            ..
        } => Ok((columns, arguments)),
        qusql_type::StatementType::Update {
            returning: None, ..
        } => Err("UPDATE"),
        qusql_type::StatementType::Update {
            arguments,
            returning: Some(columns),
            ..
        } => Ok((columns, arguments)),
        qusql_type::StatementType::Replace {
            returning: None, ..
        } => Err("REPLACE"),
        qusql_type::StatementType::Replace {
            arguments,
            returning: Some(columns),
            ..
        } => Ok((columns, arguments)),
        qusql_type::StatementType::Truncate => Err("TRUNCATE"),
        qusql_type::StatementType::Call { .. } => Err("CALL"),
        qusql_type::StatementType::Transaction => Err("Transaction control"),
        qusql_type::StatementType::Set => Err("SET"),
        qusql_type::StatementType::Lock => Err("LOCK"),
        qusql_type::StatementType::Invalid => {
            let s = quote! { {
                #(#errors; )*;
                todo!("Invalid")
            }};
            return s.into();
        }
    };

    let (columns, arguments) = match args {
        Ok(v) => v,
        Err(_) => {
            let s = quote! { {
                #(#errors; )*;
                todo!("Invalid")
            }};
            return s.into();
        }
    };

    let (arg_check, arg_gen) =
        handle_argumens(&mut errors, query.last_span, &query.args, arguments);

    let e = query.executor;
    let q = &query.query;

    let (row_construct, row_name, full_row_name, s) = match t {
        FetchType::Borrowed => {
            let (row_members, row_construct, _, has_borrowed) = construct_row(columns, false);

            if has_borrowed {
                let s = quote! {
                    #[derive(Debug)]
                    struct Row<'a> {
                        #(#row_members),*
                    }
                };
                (row_construct, quote! {Row}, quote! {Row<'a>}, s)
            } else {
                let s = quote! {
                    #[derive(Debug)]
                    struct Row {
                        #(#row_members),*
                    }
                };
                (row_construct, quote! {Row}, quote! {Row}, s)
            }
        }
        FetchType::Owned => {
            let (row_members, row_construct, _, _) = construct_row(columns, true);
            let s = quote! {
                #[derive(Debug)]
                struct Row {
                    #(#row_members),*
                }
            };
            (row_construct, quote! {Row}, quote! {Row}, s)
        }
        FetchType::AsBorrowed => {
            let (_, row_construct, validate, _) = construct_row(columns, false);
            let as_ = query.as_.unwrap();
            let s = quote! {
                fn validate(v: &#as_) -> () {
                    #(#validate)*
                }
            };
            (row_construct, quote! {#as_}, quote! {#as_<'a>}, s)
        }
        FetchType::AsOwned => {
            let (_, row_construct, validate, _) = construct_row(columns, true);
            let as_ = query.as_.unwrap();
            let s = quote! {
                fn validate(v: &#as_) -> () {
                    #(#validate)*
                }
            };
            (row_construct, quote! {#as_}, quote! {#as_}, s)
        }
    };

    let qm = match mode {
        FetchMode::All => {
            quote! {fetch_all_map}
        }
        FetchMode::Optional => {
            quote! {fetch_optional_map}
        }
        FetchMode::One => {
            quote! {fetch_one_map}
        }
        FetchMode::Stream => {
            quote! {fetch_map}
        }
    };

    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/qlog")
        .unwrap();
    let b = format!("{}\n", s);
    f.write_all(b.as_bytes()).unwrap();

    quote! { {
        use qusql_mysql::connection::{WithLoc, ExecutorExt};

        const _SCHEMA_HASH: u64 = #schema_hash;

        #(#errors; )*
        #arg_check

        #s

        struct M;

        impl<'a> qusql_mysql::RowMap<'a> for M {
            type T = #full_row_name;
            type E = qusql_mysql::ConnectionError;

            fn map(row: qusql_mysql::Row<'a>) -> Result<Self::T, Self::E> {
                let mut parser = row.parse();
                Ok(#row_name{
                    #(#row_construct, )*
                })
            }
        }

        #e.#qm::<M>(#q, #arg_gen)
    }}
    .into()
}

/// Statically checked fetch_one with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_one_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::One, FetchType::Borrowed)
}

/// Statically checked fetch_one with owned values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_one_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::One, FetchType::Owned)
}

/// Statically checked fetch_one returning into a given type with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_one_as_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::One, FetchType::AsBorrowed)
}

/// Statically checked fetch_one returning into a given type with owned values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_one_as_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::One, FetchType::AsOwned)
}

/// Statically checked fetch_optional with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_optional_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Optional, FetchType::Borrowed)
}

/// Statically checked fetch_optional with owned values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_optional_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Optional, FetchType::Owned)
}

/// Statically checked fetch_optional returning into a given type with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_optional_as_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Optional, FetchType::AsBorrowed)
}

/// Statically checked fetch_optional returning into a given type with owned values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_optional_as_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Optional, FetchType::AsOwned)
}

/// Statically checked fetch_all with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_all_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::All, FetchType::Borrowed)
}

/// Statically checked fetch_all with owned values.
#[doc(hidden)]
#[proc_macro]
pub fn fetch_all_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::All, FetchType::Owned)
}

/// Statically checked fetch_all returning into a given type with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_all_as_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::All, FetchType::AsBorrowed)
}

/// Statically checked fetch_all returning into a given type with owned values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_all_as_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::All, FetchType::AsOwned)
}

/// Statically checked fetch with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Stream, FetchType::Borrowed)
}

/// Statically checked fetch with owned values.e.
#[doc(hidden)]
#[proc_macro]
pub fn fetch_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Stream, FetchType::Owned)
}

/// Statically checked fetch returning into a given type with borrowed values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_as_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Stream, FetchType::AsBorrowed)
}

/// Statically checked fetch returning into a given type with owned values
#[doc(hidden)]
#[proc_macro]
pub fn fetch_as_owned_impl(input: TokenStream) -> TokenStream {
    build_fetch_impl(input, FetchMode::Stream, FetchType::AsOwned)
}
