#![forbid(unsafe_code)]

use std::ops::Deref;
use std::path::PathBuf;

use ariadne::{Color, Label, Report, ReportKind, Source};
use once_cell::sync::Lazy;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote, quote_spanned};
use sql_type::schema::{parse_schemas, Schemas};
use sql_type::{type_statement, Issue, SQLArguments, SQLDialect, SelectTypeColumn, TypeOptions};
use syn::spanned::Spanned;
use syn::{parse::Parse, punctuated::Punctuated, Expr, Ident, LitStr, Token};

static SCHEMA_PATH: Lazy<PathBuf> = Lazy::new(|| {
    let mut schema_path: PathBuf = std::env::var("CARGO_MANIFEST_DIR")
        .expect("`CARGO_schema_path` must be set")
        .into();

    schema_path.push("sqlx-type-schema.sql");

    if !schema_path.exists() {
        use serde::Deserialize;
        use std::process::Command;

        let cargo = std::env::var("CARGO").expect("`CARGO` must be set");
        schema_path.pop();

        let output = Command::new(&cargo)
            .args(&["metadata", "--format-version=1"])
            .current_dir(&schema_path)
            .env_remove("__CARGO_FIX_PLZ")
            .output()
            .expect("Could not fetch metadata");

        #[derive(Deserialize)]
        struct CargoMetadata {
            workspace_root: PathBuf,
        }

        let metadata: CargoMetadata =
            serde_json::from_slice(&output.stdout).expect("Invalid `cargo metadata` output");

        schema_path = metadata.workspace_root;
        schema_path.push("sqlx-type-schema.sql");
    }
    if !schema_path.exists() {
        panic!("Unable to locate sqlx-type-schema.sql");
    }
    schema_path
});

// If we are in a workspace, lookup `workspace_root` since `CARGO_MANIFEST_DIR` won't
// reflect the workspace dir: https://github.com/rust-lang/cargo/issues/3946
static SCHEMA_SRC: Lazy<String> =
    Lazy::new(|| match std::fs::read_to_string(SCHEMA_PATH.as_path()) {
        Ok(v) => v,
        Err(e) => panic!(
            "Unable to read schema from {:?}: {}",
            SCHEMA_PATH.as_path(),
            e
        ),
    });

fn issue_to_report(issue: Issue) -> Report<'static, std::ops::Range<usize>> {
    let mut builder = Report::build(
        match issue.level {
            sql_type::Level::Warning => ReportKind::Warning,
            sql_type::Level::Error => ReportKind::Error,
        },
        (),
        issue.span.start,
    )
    .with_config(ariadne::Config::default().with_color(false))
    .with_label(
        Label::new(issue.span)
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message),
    );
    for frag in issue.fragments {
        builder = builder.with_label(Label::new(frag.1).with_message(frag.0));
    }
    builder.finish()
}

fn issue_to_report_color(issue: Issue) -> Report<'static, std::ops::Range<usize>> {
    let mut builder = Report::build(
        match issue.level {
            sql_type::Level::Warning => ReportKind::Warning,
            sql_type::Level::Error => ReportKind::Error,
        },
        (),
        issue.span.start,
    )
    .with_label(
        Label::new(issue.span)
            .with_color(match issue.level {
                sql_type::Level::Warning => Color::Yellow,
                sql_type::Level::Error => Color::Red,
            })
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message),
    );
    for frag in issue.fragments {
        builder = builder.with_label(
            Label::new(frag.1)
                .with_color(Color::Blue)
                .with_message(frag.0),
        );
    }
    builder.finish()
}

struct NamedSource<'a>(&'a str, Source);

impl<'a> ariadne::Cache<()> for &NamedSource<'a> {
    fn fetch(&mut self, _: &()) -> Result<&Source, Box<dyn std::fmt::Debug + '_>> {
        Ok(&self.1)
    }

    fn display<'b>(&self, _: &'b ()) -> Option<Box<dyn std::fmt::Display + 'b>> {
        Some(Box::new(self.0.to_string()))
    }
}

static SCHEMAS: Lazy<(Schemas, SQLDialect)> = Lazy::new(|| {
    let dialect = if let Some(first_line) = SCHEMA_SRC.as_str().lines().next() {
        if first_line.contains("sql-product: postgres") {
            SQLDialect::PostgreSQL
        } else {
            SQLDialect::MariaDB
        }
    } else {
        SQLDialect::MariaDB
    };

    let options = TypeOptions::new().dialect(dialect.clone());
    let mut issues = Vec::new();
    let schemas = parse_schemas(SCHEMA_SRC.as_str(), &mut issues, &options);
    if !issues.is_empty() {
        let source = NamedSource("sqlx-type-schema.sql", Source::from(SCHEMA_SRC.as_str()));
        let mut err = false;
        for issue in issues {
            if issue.level == sql_type::Level::Error {
                err = true;
            }
            let r = issue_to_report_color(issue);
            r.eprint(&source).unwrap();
        }
        if err {
            panic!("Errors processing sqlx-type-schema.sql");
        }
    }
    (schemas, dialect)
});

fn quote_args(
    errors: &mut Vec<proc_macro2::TokenStream>,
    query: &str,
    last_span: Span,
    args: &[Expr],
    arguments: &[(sql_type::ArgumentKey<'_>, sql_type::FullType)],
    dialect: &SQLDialect,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let cls = match dialect {
        SQLDialect::MariaDB => quote!(sqlx::mysql::MySql),
        SQLDialect::PostgreSQL => quote!(sqlx::postgres::Postgres),
    };

    let mut at = Vec::new();
    let inv = sql_type::FullType::invalid();
    for (k, v) in arguments {
        match k {
            sql_type::ArgumentKey::Index(i) => {
                while at.len() <= *i {
                    at.push(&inv);
                }
                at[*i] = v;
            }
            sql_type::ArgumentKey::Identifier(_) => {
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
        let mut t = match ta.t {
            sql_type::Type::U8 => quote! {u8},
            sql_type::Type::I8 => quote! {i8},
            sql_type::Type::U16 => quote! {u16},
            sql_type::Type::I16 => quote! {i16},
            sql_type::Type::U32 => quote! {u32},
            sql_type::Type::I32 => quote! {i32},
            sql_type::Type::U64 => quote! {u64},
            sql_type::Type::I64 => quote! {i64},
            sql_type::Type::Base(sql_type::BaseType::Any) => quote! {sqlx_type::Any},
            sql_type::Type::Base(sql_type::BaseType::Bool) => quote! {bool},
            sql_type::Type::Base(sql_type::BaseType::Bytes) => quote! {&[u8]},
            sql_type::Type::Base(sql_type::BaseType::Date) => quote! {sqlx_type::Date},
            sql_type::Type::Base(sql_type::BaseType::DateTime) => quote! {sqlx_type::DateTime},
            sql_type::Type::Base(sql_type::BaseType::Float) => quote! {sqlx_type::Float},
            sql_type::Type::Base(sql_type::BaseType::Integer) => quote! {sqlx_type::Integer},
            sql_type::Type::Base(sql_type::BaseType::String) => quote! {&str},
            sql_type::Type::Base(sql_type::BaseType::Time) => todo!("time"),
            sql_type::Type::Base(sql_type::BaseType::TimeStamp) => quote! {sqlx_type::Timestamp},
            sql_type::Type::Null => todo!("null"),
            sql_type::Type::Invalid => todo!("invalid"),
            sql_type::Type::Enum(_) => quote! {&str},
            sql_type::Type::Set(_) => quote! {&str},
            sql_type::Type::Args(_, _) => todo!("args"),
            sql_type::Type::F32 => quote! {f32},
            sql_type::Type::F64 => quote! {f64},
            sql_type::Type::JSON => quote! {sqlx_type::Any},
        };
        if !ta.not_null {
            t = quote! {Option<#t>}
        }
        let span = qa.span();
        if ta.list_hack {
            list_lengths.push(quote!(#name.len()));
            arg_bindings.push(quote_spanned! {span=>
                let #name = &(#qa);
                args_count += #name.len();
                for v in #name.iter() {
                    size_hints += ::sqlx::encode::Encode::<#cls>::size_hint(v);
                }
                if false {
                    sqlx_type::check_arg_list_hack::<#t, _>(#name);
                    ::std::panic!();
                }
            });
            arg_add.push(quote!(
                for v in #name.iter() {
                    query_args.add(v);
                }
            ));
        } else {
            arg_bindings.push(quote_spanned! {span=>
                let #name = &(#qa);
                args_count += 1;
                size_hints += ::sqlx::encode::Encode::<#cls>::size_hint(#name);
                if false {
                    sqlx_type::check_arg::<#t, _>(#name);
                    ::std::panic!();
                }
            });
            arg_add.push(quote!(query_args.add(#name);));
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
            let mut size_hints = 0;
            let mut args_count = 0;
            #(#arg_bindings)*

            let mut query_args = <#cls as ::sqlx::database::HasArguments>::Arguments::default();
            query_args.reserve(args_count, size_hints);

            #(#arg_add)*
        },
        query,
    )
}

fn issues_to_errors(issues: Vec<Issue>, source: &str, span: Span) -> Vec<proc_macro2::TokenStream> {
    if !issues.is_empty() {
        let source = NamedSource("", Source::from(source));
        let mut err = false;
        let mut out = Vec::new();
        for issue in issues {
            if issue.level == sql_type::Level::Error {
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

fn construct_row(
    _errors: &mut Vec<proc_macro2::TokenStream>,
    columns: &[SelectTypeColumn],
) -> (Vec<proc_macro2::TokenStream>, Vec<proc_macro2::TokenStream>) {
    let mut row_members = Vec::new();
    let mut row_construct = Vec::new();
    for (i, c) in columns.iter().enumerate() {
        let mut t = match c.type_.t {
            sql_type::Type::U8 => quote! {u8},
            sql_type::Type::I8 => quote! {i8},
            sql_type::Type::U16 => quote! {u16},
            sql_type::Type::I16 => quote! {i16},
            sql_type::Type::U32 => quote! {u32},
            sql_type::Type::I32 => quote! {i32},
            sql_type::Type::U64 => quote! {u64},
            sql_type::Type::I64 => quote! {i64},
            sql_type::Type::Base(sql_type::BaseType::Any) => todo!("from_any"),
            sql_type::Type::Base(sql_type::BaseType::Bool) => quote! {bool},
            sql_type::Type::Base(sql_type::BaseType::Bytes) => quote! {Vec<u8>},
            sql_type::Type::Base(sql_type::BaseType::Date) => quote! {chrono::NaiveDate},
            sql_type::Type::Base(sql_type::BaseType::DateTime) => quote! {chrono::NaiveDateTime},
            sql_type::Type::Base(sql_type::BaseType::Float) => quote! {f64},
            sql_type::Type::Base(sql_type::BaseType::Integer) => quote! {i64},
            sql_type::Type::Base(sql_type::BaseType::String) => quote! {String},
            sql_type::Type::Base(sql_type::BaseType::Time) => todo!("from_time"),
            sql_type::Type::Base(sql_type::BaseType::TimeStamp) => {
                quote! {sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>}
            }
            sql_type::Type::Null => todo!("from_null"),
            sql_type::Type::Invalid => quote! {i64},
            sql_type::Type::Enum(_) => quote! {String},
            sql_type::Type::Set(_) => quote! {String},
            sql_type::Type::Args(_, _) => todo!("from_args"),
            sql_type::Type::F32 => quote! {f32},
            sql_type::Type::F64 => quote! {f64},
            sql_type::Type::JSON => quote! {String},
        };
        let name = match c.name {
            Some(v) => v,
            None => continue,
        };

        let ident = String::from("r#") + name;
        let ident: Ident = if let Ok(ident) = syn::parse_str(&ident) {
            ident
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
        row_construct.push(quote! {
            #ident: sqlx::Row::get(&row, #i)
        });
    }
    (row_members, row_construct)
}

struct Query {
    query: String,
    query_span: Span,
    args: Vec<Expr>,
    last_span: Span,
}

impl Parse for Query {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
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
            query,
            query_span,
            args,
            last_span,
        })
    }
}

/// Statically checked SQL query, similarly to sqlx::query!.
///
/// This expands to an instance of query::Map that outputs an ad-hoc anonymous struct type.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let query = syn::parse_macro_input!(input as Query);
    let (schemas, dialect) = SCHEMAS.deref();
    let options = TypeOptions::new()
        .dialect(dialect.clone())
        .arguments(match &dialect {
            SQLDialect::MariaDB => SQLArguments::QuestionMark,
            SQLDialect::PostgreSQL => SQLArguments::Dollar,
        })
        .list_hack(true);
    let mut issues = Vec::new();
    let stmt = type_statement(schemas, &query.query, &mut issues, &options);
    let sp = SCHEMA_PATH.as_path().to_str().unwrap();

    let mut errors = issues_to_errors(issues, &query.query, query.query_span);
    match &stmt {
        sql_type::StatementType::Select { columns, arguments } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query.query,
                query.last_span,
                &query.args,
                arguments,
                dialect,
            );
            let (row_members, row_construct) = construct_row(&mut errors, columns);
            let s = quote! { {
                use ::sqlx::Arguments as _;
                let _ = std::include_bytes!(#sp);
                #(#errors; )*
                #args_tokens

                struct Row {
                    #(#row_members),*
                };
                sqlx::query_with(#q, query_args).map(|row|
                    Row{
                        #(#row_construct),*
                    }
                )
            }};
            s.into()
        }
        sql_type::StatementType::Delete { arguments } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query.query,
                query.last_span,
                &query.args,
                arguments,
                dialect,
            );
            let s = quote! { {
                use ::sqlx::Arguments as _;
                #(#errors; )*
                #args_tokens
                sqlx::query_with(#q, query_args)
            }
            };
            s.into()
        }
        sql_type::StatementType::Insert {
            arguments,
            returning,
            ..
        } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query.query,
                query.last_span,
                &query.args,
                arguments,
                dialect,
            );
            let s = match returning.as_ref() {
                Some(returning) => {
                    let (row_members, row_construct) = construct_row(&mut errors, returning);
                    quote! { {
                        use ::sqlx::Arguments as _;
                        let _ = std::include_bytes!(#sp);
                        #(#errors; )*
                        #args_tokens

                        struct Row {
                            #(#row_members),*
                        };
                        sqlx::query_with(#q, query_args).map(|row|
                            Row{
                                #(#row_construct),*
                            }
                        )
                    }}
                }
                None => quote! { {
                    use ::sqlx::Arguments as _;
                    #(#errors; )*
                    #args_tokens
                    sqlx::query_with(#q, query_args)
                }
                },
            };
            s.into()
        }
        sql_type::StatementType::Update { arguments } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query.query,
                query.last_span,
                &query.args,
                arguments,
                dialect,
            );
            let s = quote! { {
                use ::sqlx::Arguments as _;
                #(#errors; )*
                #args_tokens
                sqlx::query_with(#q, query_args)
            }
            };
            s.into()
        }
        sql_type::StatementType::Replace {
            arguments,
            returning,
        } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query.query,
                query.last_span,
                &query.args,
                arguments,
                dialect,
            );
            let s = match returning.as_ref() {
                Some(returning) => {
                    let (row_members, row_construct) = construct_row(&mut errors, returning);
                    quote! { {
                        use ::sqlx::Arguments as _;
                        let _ = std::include_bytes!(#sp);
                        #(#errors; )*
                        #args_tokens

                        struct Row {
                            #(#row_members),*
                        };
                        sqlx::query_with(#q, query_args).map(|row|
                            Row{
                                #(#row_construct),*
                            }
                        )
                    }}
                }
                None => quote! { {
                    use ::sqlx::Arguments as _;
                    #(#errors; )*
                    #args_tokens
                    sqlx::query_with(#q, query_args)
                }
                },
            };
            s.into()
        }
        sql_type::StatementType::Invalid => {
            let s = quote! { {
                #(#errors; )*;
                todo!("Invalid")
            }};
            s.into()
        }
    }
}

fn construct_row2(
    _errors: &mut Vec<proc_macro2::TokenStream>,
    columns: &[SelectTypeColumn],
) -> Vec<proc_macro2::TokenStream> {
    let mut row_construct = Vec::new();
    for (i, c) in columns.iter().enumerate() {
        let mut t = match c.type_.t {
            sql_type::Type::U8 => quote! {u8},
            sql_type::Type::I8 => quote! {i8},
            sql_type::Type::U16 => quote! {u16},
            sql_type::Type::I16 => quote! {i16},
            sql_type::Type::U32 => quote! {u32},
            sql_type::Type::I32 => quote! {i32},
            sql_type::Type::U64 => quote! {u64},
            sql_type::Type::I64 => quote! {i64},
            sql_type::Type::Base(sql_type::BaseType::Any) => todo!("from_any"),
            sql_type::Type::Base(sql_type::BaseType::Bool) => quote! {bool},
            sql_type::Type::Base(sql_type::BaseType::Bytes) => quote! {Vec<u8>},
            sql_type::Type::Base(sql_type::BaseType::Date) => quote! {chrono::NaiveDate},
            sql_type::Type::Base(sql_type::BaseType::DateTime) => quote! {chrono::NaiveDateTime},
            sql_type::Type::Base(sql_type::BaseType::Float) => quote! {f64},
            sql_type::Type::Base(sql_type::BaseType::Integer) => quote! {i64},
            sql_type::Type::Base(sql_type::BaseType::String) => quote! {String},
            sql_type::Type::Base(sql_type::BaseType::Time) => todo!("from_time"),
            sql_type::Type::Base(sql_type::BaseType::TimeStamp) => {
                quote! {sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>}
            }
            sql_type::Type::Null => todo!("from_null"),
            sql_type::Type::Invalid => quote! {i64},
            sql_type::Type::Enum(_) => quote! {String},
            sql_type::Type::Set(_) => quote! {String},
            sql_type::Type::Args(_, _) => todo!("from_args"),
            sql_type::Type::F32 => quote! {f32},
            sql_type::Type::F64 => quote! {f64},
            sql_type::Type::JSON => quote! {String},
        };
        let name = match c.name {
            Some(v) => v,
            None => continue,
        };

        let ident = String::from("r#") + name;
        let ident: Ident = if let Ok(ident) = syn::parse_str(&ident) {
            ident
        } else {
            // TODO error
            //errors.push(syn::Error::new(span, String::from_utf8(out).unwrap()).to_compile_error().into());
            continue;
        };

        if !c.type_.not_null {
            t = quote! {Option<#t>};
        }
        row_construct.push(quote! {
            #ident: sqlx_type::arg_out::<#t, _, #i>(sqlx::Row::get(&row, #i))
        });
    }
    row_construct
}

struct QueryAs {
    as_: Ident,
    query: String,
    query_span: Span,
    args: Vec<Expr>,
    last_span: Span,
}

impl Parse for QueryAs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let as_ = input.parse::<Ident>()?;
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
            query,
            query_span,
            args,
            last_span,
        })
    }
}

/// A variant of query! which takes a path to an explicitly defined struct as the output type.
///
/// This lets you return the struct from a function or add your own trait implementations.
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let query_as = syn::parse_macro_input!(input as QueryAs);
    let (schemas, dialect) = SCHEMAS.deref();
    let options = TypeOptions::new()
        .dialect(dialect.clone())
        .arguments(match &dialect {
            SQLDialect::MariaDB => SQLArguments::QuestionMark,
            SQLDialect::PostgreSQL => SQLArguments::Dollar,
        })
        .list_hack(true);
    let mut issues = Vec::new();
    let stmt = type_statement(schemas, &query_as.query, &mut issues, &options);

    let mut errors = issues_to_errors(issues, &query_as.query, query_as.query_span);
    match &stmt {
        sql_type::StatementType::Select { columns, arguments } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query_as.query,
                query_as.last_span,
                &query_as.args,
                arguments,
                dialect,
            );

            let row_construct = construct_row2(&mut errors, columns);
            let row = query_as.as_;
            let s = quote! { {
                use ::sqlx::Arguments as _;
                #(#errors; )*
                #args_tokens
                sqlx::query_with(#q, query_args).map(|row|
                    #row{
                        #(#row_construct),*
                    }
                )
            }};
            //println!("TOKENS: {}", s);
            s.into()
        }
        sql_type::StatementType::Delete { .. } => {
            errors.push(
                syn::Error::new(query_as.query_span, "DELETE not support in query_as")
                    .to_compile_error(),
            );
            quote! { {
                #(#errors; )*
                todo!("delete")
            }}
            .into()
        }
        sql_type::StatementType::Insert {
            returning: None, ..
        } => {
            errors.push(
                syn::Error::new(
                    query_as.query_span,
                    "INSERT without RETURNING not support in query_as",
                )
                .to_compile_error(),
            );
            quote! { {
                #(#errors; )*
                todo!("insert")
            }}
            .into()
        }
        sql_type::StatementType::Insert {
            arguments,
            returning: Some(returning),
            ..
        } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query_as.query,
                query_as.last_span,
                &query_as.args,
                arguments,
                dialect,
            );

            let row_construct = construct_row2(&mut errors, returning);
            let row = query_as.as_;
            let s = quote! { {
                use ::sqlx::Arguments as _;
                #(#errors; )*
                #args_tokens
                sqlx::query_with(#q, query_args).map(|row|
                    #row{
                        #(#row_construct),*
                    }
                )
            }};
            s.into()
        }
        sql_type::StatementType::Update { .. } => {
            errors.push(
                syn::Error::new(query_as.query_span, "UPDATE not support in query_as")
                    .to_compile_error(),
            );
            quote! { {
                #(#errors; )*
                todo!("update")
            }}
            .into()
        }
        sql_type::StatementType::Replace {
            returning: None, ..
        } => {
            errors.push(
                syn::Error::new(
                    query_as.query_span,
                    "REPLACE without RETURNING not support in query_as",
                )
                .to_compile_error(),
            );
            quote! { {
                #(#errors; )*
                todo!("replace")
            }}
            .into()
        }
        sql_type::StatementType::Replace {
            arguments,
            returning: Some(returning),
            ..
        } => {
            let (args_tokens, q) = quote_args(
                &mut errors,
                &query_as.query,
                query_as.last_span,
                &query_as.args,
                arguments,
                dialect,
            );

            let row_construct = construct_row2(&mut errors, returning);
            let row = query_as.as_;
            let s = quote! { {
                use ::sqlx::Arguments as _;
                #(#errors; )*
                #args_tokens
                sqlx::query_with(#q, query_args).map(|row|
                    #row{
                        #(#row_construct),*
                    }
                )
            }};
            s.into()
        }
        sql_type::StatementType::Invalid => quote! { {
            #(#errors; )*;
            todo!("invalid")
        }}
        .into(),
    }
}
