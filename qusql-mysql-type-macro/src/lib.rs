//! Implementation of typed db query macros
//!
//! Used the exposed macros from the qusql-mysql-type crate
#![forbid(unsafe_code)]

use std::hash::Hash;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;

use ariadne::{Color, Label, Report, ReportKind, Source};
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote, quote_spanned};
use qusql_type::schema::{parse_schemas, Schemas};
use qusql_type::{
    type_statement, FullType, Issue, SQLArguments, SQLDialect, SelectTypeColumn, TypeOptions,
};
use std::sync::LazyLock;
use syn::spanned::Spanned;
use syn::{parse::Parse, punctuated::Punctuated, Expr, Ident, LitStr, Token};

/// Path of where the qusql-mysql-type-schema.sql file can be found
///
/// If we are in a workspace, lookup `workspace_root` since `CARGO_MANIFEST_DIR` won't
/// reflect the workspace dir: https://github.com/rust-lang/cargo/issues/3946
static SCHEMA_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    let mut schema_path: PathBuf = std::env::var("CARGO_MANIFEST_DIR")
        .expect("`CARGO_schema_path` must be set")
        .into();

    schema_path.push("qusql-mysql-type-schema.sql");

    if !schema_path.exists() {
        use serde::Deserialize;
        use std::process::Command;

        let cargo = std::env::var("CARGO").expect("`CARGO` must be set");
        schema_path.pop();

        let output = Command::new(cargo)
            .args(["metadata", "--format-version=1"])
            .current_dir(&schema_path)
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

        schema_path = metadata.workspace_root;
        schema_path.push("qusql-mysql-type-schema.sql");
    }
    if !schema_path.exists() {
        panic!("Unable to locate qusql-mysql-type-schema.sql");
    }
    schema_path
});

/// The content of the qusql-mysql-type-schema.sql file
static SCHEMA_SRC: LazyLock<(String, u64)> =
    LazyLock::new(|| match std::fs::read_to_string(SCHEMA_PATH.as_path()) {
        Ok(v) => {
            use std::hash::{DefaultHasher, Hasher};
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            let h = hasher.finish();
            (v, h)
        }
        Err(e) => panic!(
            "Unable to read schema from {:?}: {}",
            SCHEMA_PATH.as_path(),
            e
        ),
    });

/// Convert an [Issue] to a [Report]
fn issue_to_report(issue: Issue) -> Report<'static, std::ops::Range<usize>> {
    let mut builder: ariadne::ReportBuilder<'_, std::ops::Range<usize>> = Report::build(
        match issue.level {
            qusql_type::Level::Warning => ReportKind::Warning,
            qusql_type::Level::Error => ReportKind::Error,
        },
        issue.span.clone(),
    )
    .with_config(ariadne::Config::default().with_color(false))
    .with_label(
        Label::new(issue.span)
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message),
    );
    for frag in issue.fragments {
        builder = builder.with_label(Label::new(frag.span).with_message(frag.message));
    }
    builder.finish()
}

/// Convert an [Issue] to a [Report] with colours
fn issue_to_report_color(issue: Issue) -> Report<'static, std::ops::Range<usize>> {
    let mut builder = Report::build(
        match issue.level {
            qusql_type::Level::Warning => ReportKind::Warning,
            qusql_type::Level::Error => ReportKind::Error,
        },
        issue.span.clone(),
    )
    .with_label(
        Label::new(issue.span)
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
            Label::new(frag.span)
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

/// Parsed content of qusql-mysql-type-schema.sql
static SCHEMAS: LazyLock<Schemas> = LazyLock::new(|| {
    let schema_src = SCHEMA_SRC.0.as_str();

    let options = TypeOptions::new();
    let mut issues = qusql_type::Issues::new(schema_src);
    let schemas = parse_schemas(schema_src, &mut issues, &options);
    if !issues.is_ok() {
        let source = NamedSource("qusql-mysql-type-schema.sql", Source::from(schema_src));
        let mut err = false;
        for issue in issues.into_vec() {
            if issue.level == qusql_type::Level::Error {
                err = true;
            }
            let r = issue_to_report_color(issue);
            r.eprint(&source).unwrap();
        }
        if err {
            panic!("Errors processing qusql-mysql-type-schema.sql");
        }
    }
    schemas
});

/// Map a [FullType] to a build in type or a tag type in [qusql_mysql_type]
fn map_type(ta: &FullType<'_>) -> proc_macro2::TokenStream {
    let t = match ta.t {
        qusql_type::Type::U8 => quote! {u8},
        qusql_type::Type::I8 => quote! {i8},
        qusql_type::Type::U16 => quote! {u16},
        qusql_type::Type::I16 => quote! {i16},
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
        qusql_type::Type::Null => todo!("null"),
        qusql_type::Type::Invalid => quote! {std::convert::Infallible},
        qusql_type::Type::Enum(_) => quote! {&str},
        qusql_type::Type::Set(_) => quote! {&str},
        qusql_type::Type::Args(_, _) => todo!("args"),
        qusql_type::Type::F32 => quote! {f32},
        qusql_type::Type::F64 => quote! {f64},
        qusql_type::Type::JSON => quote! {qusql_mysql_type::Any},
    };
    if !ta.not_null {
        quote! {Option<#t>}
    } else {
        t
    }
}

/// Generate code to validate and build arguments
///
/// Handels list hack
fn handle_argumens(
    errors: &mut Vec<proc_macro2::TokenStream>,
    query: &str,
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

    let arg_names = (0..args.len())
        .map(|i| format_ident!("arg{}", i))
        .collect::<Vec<_>>();

    let mut arg_bindings = Vec::new();
    let mut arg_add = Vec::new();

    let mut list_lengths = Vec::new();

    for ((qa, ta), name) in args.iter().zip(at).zip(&arg_names) {
        let t = map_type(ta);
        let span = qa.span();
        if ta.list_hack {
            list_lengths.push(quote!(#name.len()));
            arg_bindings.push(quote_spanned! {span=>
                let #name = &(#qa);
                if false {
                    sqlx_type::check_arg_list_hack::<#t, _>(#name);
                    ::std::panic!();
                }
            });
            arg_add.push(quote!(
                for v in #name.iter() {
                    e = e.and_then(|()| query_args.add(v));
                }
            ));
        } else {
            arg_bindings.push(quote_spanned! {span=>
                qusql_mysql_type::check_arg::<#t, _>(&#qa);
            });
            arg_add.push(quote!(#qa, ));
        }
    }

    let query = if list_lengths.is_empty() {
        quote!(#query)
    } else {
        quote!(
            &sqlx_type::convert_list_query(#query, &[#(#list_lengths),*])
        )
    };

    (
        quote! {
            if false {
                #(#arg_bindings)*
                ::std::panic!();
            }
            let query_args = (#(#arg_add)*);
        },
        query,
    )
}

/// Output code to display issues to users
fn issues_to_errors(issues: Vec<Issue>, source: &str, span: Span) -> Vec<proc_macro2::TokenStream> {
    if !issues.is_empty() {
        let source = NamedSource("", Source::from(source));
        let mut err = false;
        let mut out = Vec::new();
        for issue in issues {
            if issue.level == qusql_type::Level::Error {
                err = true;
            }
            let r = issue_to_report(issue);
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
            qusql_type::Type::Null => todo!("from_null"),
            qusql_type::Type::Invalid => quote! {i64},
            qusql_type::Type::Enum(_) => quote! {String},
            qusql_type::Type::Set(_) => quote! {String},
            qusql_type::Type::Args(_, _) => todo!("from_args"),
            qusql_type::Type::F32 => quote! {f32},
            qusql_type::Type::F64 => quote! {f64},
            qusql_type::Type::JSON => quote! {String},
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
    let schemas = SCHEMAS.deref();
    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .list_hack(true);
    let mut issues = qusql_type::Issues::new(&query.query);
    let stmt = type_statement(schemas, &query.query, &mut issues, &options);
    let mut errors = issues_to_errors(issues.into_vec(), &query.query, query.query_span);

    let schema_hash = SCHEMA_SRC.1;

    let arguments = match &stmt {
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

    let (args_tokens, q) = handle_argumens(
        &mut errors,
        &query.query,
        query.last_span,
        &query.args,
        arguments,
    );

    let e = query.executor;

    quote! { {
        use qusql_mysql::connection::{WithLoc, ExecutorExt};

        const _SCHEMA_HASH: u64 = #schema_hash;


        #(#errors; )*
        #args_tokens
        (#e).execute(#q, query_args)
    }}
    .into()
}

/// Implementation of statically checked fetch queries
fn build_fetch_impl(input: TokenStream, mode: FetchMode, t: FetchType) -> TokenStream {
    let query = match t {
        FetchType::Borrowed | FetchType::Owned => syn::parse_macro_input!(input as Query).0,
        FetchType::AsBorrowed | FetchType::AsOwned => syn::parse_macro_input!(input as AsQuery).0,
    };

    let schemas = SCHEMAS.deref();
    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .list_hack(true);
    let mut issues = qusql_type::Issues::new(&query.query);
    let stmt = type_statement(schemas, &query.query, &mut issues, &options);

    let mut errors = issues_to_errors(issues.into_vec(), &query.query, query.query_span);

    let schema_hash = SCHEMA_SRC.1;

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

    let (args_tokens, q) = handle_argumens(
        &mut errors,
        &query.query,
        query.last_span,
        &query.args,
        arguments,
    );

    let e = query.executor;

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
        #args_tokens

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

        #e.#qm::<M>(#q, query_args)
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
