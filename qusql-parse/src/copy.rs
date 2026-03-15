// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PostgreSQL `COPY` statement AST and parser.
//!
//! Implements the full COPY syntax including:
//! - Table or subquery as source
//! - FROM/TO direction with file, PROGRAM, STDIN, or STDOUT
//! - Modern `WITH ( options )` syntax
//! - Legacy bare-keyword options (pre-PostgreSQL 9.0 syntax)
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-copy.html>
//!
//! ```text
//! COPY table_name [ ( column_name [, ...] ) ]
//!     FROM { 'filename' | PROGRAM 'command' | STDIN }
//!     [ [ WITH ] ( option [, ...] ) ]
//!     [ WHERE condition ]
//!
//! COPY { table_name [ ( column_name [, ...] ) ] | ( query ) }
//!     TO { 'filename' | PROGRAM 'command' | STDOUT }
//!     [ [ WITH ] ( option [, ...] ) ]
//! ```

use alloc::{boxed::Box, vec::Vec};

use crate::{
    Identifier, QualifiedName, SString, Span, Spanned, Statement,
    expression::{Expression, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
    statement::parse_statement,
};

/// A column list in a COPY option: either `*` (all columns) or `( col, ... )`.
///
/// Used by `FORCE_QUOTE`, `FORCE_NOT_NULL`, and `FORCE_NULL` options.
#[derive(Clone, Debug)]
pub enum CopyColumnList<'a> {
    /// `*` — applies to all columns
    All(Span),
    /// `( col1, col2, ... )` — applies to named columns
    Columns {
        lparen_span: Span,
        columns: Vec<Identifier<'a>>,
        rparen_span: Span,
    },
}

impl<'a> Spanned for CopyColumnList<'a> {
    fn span(&self) -> Span {
        match self {
            CopyColumnList::All(s) => s.clone(),
            CopyColumnList::Columns {
                lparen_span,
                rparen_span,
                ..
            } => lparen_span.join_span(rparen_span),
        }
    }
}

fn parse_copy_column_list<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<CopyColumnList<'a>, ParseError> {
    if let Some(star) = parser.skip_token(Token::Mul) {
        return Ok(CopyColumnList::All(star));
    }
    let lparen_span = parser.consume_token(Token::LParen)?;
    let mut columns = Vec::new();
    if !matches!(parser.token, Token::RParen) {
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    columns.push(parser.consume_plain_identifier_unreserved()?);
                    Ok(())
                },
            )?;
            if matches!(parser.token, Token::RParen) {
                break;
            }
            parser.consume_token(Token::Comma)?;
        }
    }
    let rparen_span = parser.consume_token(Token::RParen)?;
    Ok(CopyColumnList::Columns {
        lparen_span,
        columns,
        rparen_span,
    })
}

/// Value for the `HEADER` option: a boolean or `MATCH`.
#[derive(Clone, Debug)]
pub enum CopyHeaderValue {
    /// `TRUE`, `ON`, or `1` — include the header line
    True(Span),
    /// `FALSE`, `OFF`, or `0` — no header line
    False(Span),
    /// `MATCH` — validate that the header matches the table columns (FROM only)
    Match(Span),
}

impl Spanned for CopyHeaderValue {
    fn span(&self) -> Span {
        match self {
            CopyHeaderValue::True(s) | CopyHeaderValue::False(s) | CopyHeaderValue::Match(s) => {
                s.clone()
            }
        }
    }
}

/// A single `WITH ( ... )` option in a COPY statement.
#[derive(Clone, Debug)]
pub enum CopyOption<'a> {
    /// `FORMAT format_name` — data format: `text`, `csv`, or `binary`
    Format { span: Span, name: Identifier<'a> },
    /// `FREEZE [ boolean ]` — copy with rows already frozen
    Freeze { span: Span, value: Option<bool> },
    /// `DELIMITER 'delimiter_character'` — column separator
    Delimiter { span: Span, value: SString<'a> },
    /// `NULL 'null_string'` — string representing NULL
    Null { span: Span, value: SString<'a> },
    /// `DEFAULT 'default_string'` — string representing DEFAULT (FROM only)
    Default { span: Span, value: SString<'a> },
    /// `HEADER [ boolean | MATCH ]` — header line handling
    Header {
        span: Span,
        value: Option<CopyHeaderValue>,
    },
    /// `QUOTE 'quote_character'` — CSV quoting character
    Quote { span: Span, value: SString<'a> },
    /// `ESCAPE 'escape_character'` — CSV escape character
    Escape { span: Span, value: SString<'a> },
    /// `FORCE_QUOTE { ( col, ... ) | * }` — force quoting (TO CSV only)
    ForceQuote {
        span: Span,
        columns: CopyColumnList<'a>,
    },
    /// `FORCE_NOT_NULL { ( col, ... ) | * }` — never match null string (FROM CSV only)
    ForceNotNull {
        span: Span,
        columns: CopyColumnList<'a>,
    },
    /// `FORCE_NULL { ( col, ... ) | * }` — match null string even when quoted (FROM CSV only)
    ForceNull {
        span: Span,
        columns: CopyColumnList<'a>,
    },
    /// `ON_ERROR error_action` — behaviour when input value conversion fails (FROM only)
    OnError { span: Span, action: Identifier<'a> },
    /// `REJECT_LIMIT maxerror` — maximum conversion errors before failing (FROM only)
    RejectLimit { span: Span, limit: Expression<'a> },
    /// `ENCODING 'encoding_name'` — file encoding
    Encoding { span: Span, value: SString<'a> },
    /// `LOG_VERBOSITY verbosity` — message verbosity: `default`, `verbose`, or `silent`
    LogVerbosity {
        span: Span,
        verbosity: Identifier<'a>,
    },
}

impl<'a> Spanned for CopyOption<'a> {
    fn span(&self) -> Span {
        match self {
            CopyOption::Format { span, name } => span.join_span(name),
            CopyOption::Freeze { span, .. } => span.clone(),
            CopyOption::Delimiter { span, value } => span.join_span(value),
            CopyOption::Null { span, value } => span.join_span(value),
            CopyOption::Default { span, value } => span.join_span(value),
            CopyOption::Header { span, value } => span.join_span(value),
            CopyOption::Quote { span, value } => span.join_span(value),
            CopyOption::Escape { span, value } => span.join_span(value),
            CopyOption::ForceQuote { span, columns } => span.join_span(columns),
            CopyOption::ForceNotNull { span, columns } => span.join_span(columns),
            CopyOption::ForceNull { span, columns } => span.join_span(columns),
            CopyOption::OnError { span, action } => span.join_span(action),
            CopyOption::RejectLimit { span, limit } => span.join_span(limit),
            CopyOption::Encoding { span, value } => span.join_span(value),
            CopyOption::LogVerbosity { span, verbosity } => span.join_span(verbosity),
        }
    }
}

/// The source / destination of a COPY: either a table (with optional column list)
/// or a parenthesised query (only valid with `TO`).
#[derive(Clone, Debug)]
pub enum CopySource<'a> {
    /// Plain table reference, e.g. `public.actor (col1, col2)`
    Table {
        name: QualifiedName<'a>,
        columns: Vec<Identifier<'a>>,
    },
    /// Subquery source, e.g. `(SELECT * FROM t)` — only valid with `TO`
    Query {
        lparen_span: Span,
        query: Box<Statement<'a>>,
        rparen_span: Span,
    },
}

impl<'a> Spanned for CopySource<'a> {
    fn span(&self) -> Span {
        match self {
            CopySource::Table { name, columns } => name.span().join_span(columns),
            CopySource::Query {
                lparen_span,
                query,
                rparen_span,
            } => lparen_span.join_span(query.as_ref()).join_span(rparen_span),
        }
    }
}

/// Where to read from / write to in a COPY statement.
#[derive(Clone, Debug)]
pub enum CopyLocation<'a> {
    /// A file path: `'path/to/file'`
    Filename(SString<'a>),
    /// A shell command: `PROGRAM 'command'`
    Program {
        program_span: Span,
        command: SString<'a>,
    },
    /// Standard input (only valid with `FROM`)
    Stdin(Span),
    /// Standard output (only valid with `TO`)
    Stdout(Span),
}

impl<'a> Spanned for CopyLocation<'a> {
    fn span(&self) -> Span {
        match self {
            CopyLocation::Filename(s) => s.span(),
            CopyLocation::Program {
                program_span,
                command,
            } => program_span.join_span(command),
            CopyLocation::Stdin(s) | CopyLocation::Stdout(s) => s.clone(),
        }
    }
}

/// A PostgreSQL `COPY ... FROM` statement.
///
/// ```sql
/// COPY table_name FROM STDIN;
/// COPY country FROM '/tmp/data.csv' WITH (FORMAT csv, HEADER);
/// ```
#[derive(Clone, Debug)]
pub struct CopyFrom<'a> {
    /// Span of `COPY`
    pub copy_span: Span,
    /// Table source (a subquery is invalid for FROM)
    pub source: CopySource<'a>,
    /// Span of `FROM`
    pub from_span: Span,
    /// Location: file, PROGRAM, or STDIN
    pub location: CopyLocation<'a>,
    /// Span of `WITH` keyword if present
    pub with_span: Option<Span>,
    /// Options specified after `WITH ( ... )` or as legacy bare keywords
    pub options: Vec<CopyOption<'a>>,
    /// Optional `WHERE condition`
    pub where_: Option<(Span, Expression<'a>)>,
}

impl<'a> Spanned for CopyFrom<'a> {
    fn span(&self) -> Span {
        self.copy_span
            .join_span(&self.source)
            .join_span(&self.from_span)
            .join_span(&self.location)
            .join_span(&self.with_span)
            .join_span(&self.options)
            .join_span(&self.where_)
    }
}

impl<'a> CopyFrom<'a> {
    /// Returns `true` when this COPY reads its data from stdin, signalling the
    /// parser to consume the following inline data block up to `\.`.
    pub(crate) fn reads_from_stdin(&self) -> bool {
        matches!(&self.location, CopyLocation::Stdin(_))
    }
}

/// A PostgreSQL `COPY ... TO` statement.
///
/// ```sql
/// COPY (SELECT * FROM t) TO '/tmp/out.csv' WITH (FORMAT csv, HEADER);
/// COPY country TO PROGRAM 'gzip > /tmp/country.gz';
/// ```
#[derive(Clone, Debug)]
pub struct CopyTo<'a> {
    /// Span of `COPY`
    pub copy_span: Span,
    /// Table or subquery source
    pub source: CopySource<'a>,
    /// Span of `TO`
    pub to_span: Span,
    /// Location: file, PROGRAM, or STDOUT
    pub location: CopyLocation<'a>,
    /// Span of `WITH` keyword if present
    pub with_span: Option<Span>,
    /// Options specified after `WITH ( ... )` or as legacy bare keywords
    pub options: Vec<CopyOption<'a>>,
}

impl<'a> Spanned for CopyTo<'a> {
    fn span(&self) -> Span {
        self.copy_span
            .join_span(&self.source)
            .join_span(&self.to_span)
            .join_span(&self.location)
            .join_span(&self.with_span)
            .join_span(&self.options)
    }
}

/// Parse the location (`'file'`, `PROGRAM 'cmd'`, `STDIN`, or `STDOUT`)
/// that follows `FROM` or `TO`.
fn parse_copy_location<'a>(parser: &mut Parser<'a, '_>) -> Result<CopyLocation<'a>, ParseError> {
    match &parser.token {
        Token::String(_, _) => Ok(CopyLocation::Filename(parser.consume_string()?)),
        Token::Ident(_, Keyword::PROGRAM) => {
            let program_span = parser.consume_keyword(Keyword::PROGRAM)?;
            let command = parser.consume_string()?;
            Ok(CopyLocation::Program {
                program_span,
                command,
            })
        }
        Token::Ident(_, Keyword::STDIN) => {
            Ok(CopyLocation::Stdin(parser.consume_keyword(Keyword::STDIN)?))
        }
        Token::Ident(_, Keyword::STDOUT) => Ok(CopyLocation::Stdout(
            parser.consume_keyword(Keyword::STDOUT)?,
        )),
        _ => parser.expected_failure("'filename', PROGRAM, STDIN, or STDOUT"),
    }
}

/// Parse a single option from a modern `WITH ( option, ... )` option list.
fn parse_copy_option_modern<'a>(parser: &mut Parser<'a, '_>) -> Result<CopyOption<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::FORMAT) => {
            let span = parser.consume_keyword(Keyword::FORMAT)?;
            let name = parser.consume_plain_identifier_unreserved()?;
            Ok(CopyOption::Format { span, name })
        }
        Token::Ident(_, Keyword::FREEZE) => {
            let span = parser.consume_keyword(Keyword::FREEZE)?;
            let value = parser.try_parse_bool().map(|(b, _)| b);
            Ok(CopyOption::Freeze { span, value })
        }
        Token::Ident(_, Keyword::DELIMITER) => {
            let span = parser.consume_keyword(Keyword::DELIMITER)?;
            let value = parser.consume_string()?;
            Ok(CopyOption::Delimiter { span, value })
        }
        Token::Ident(_, Keyword::NULL) => {
            let span = parser.consume_keyword(Keyword::NULL)?;
            let value = parser.consume_string()?;
            Ok(CopyOption::Null { span, value })
        }
        Token::Ident(_, Keyword::DEFAULT) => {
            let span = parser.consume_keyword(Keyword::DEFAULT)?;
            let value = parser.consume_string()?;
            Ok(CopyOption::Default { span, value })
        }
        Token::Ident(_, Keyword::HEADER) => {
            let span = parser.consume_keyword(Keyword::HEADER)?;
            let value = match &parser.token {
                Token::Ident(_, Keyword::MATCH) => Some(CopyHeaderValue::Match(
                    parser.consume_keyword(Keyword::MATCH)?,
                )),
                _ => parser.try_parse_bool().map(|(b, s)| {
                    if b {
                        CopyHeaderValue::True(s)
                    } else {
                        CopyHeaderValue::False(s)
                    }
                }),
            };
            Ok(CopyOption::Header { span, value })
        }
        Token::Ident(_, Keyword::QUOTE) => {
            let span = parser.consume_keyword(Keyword::QUOTE)?;
            let value = parser.consume_string()?;
            Ok(CopyOption::Quote { span, value })
        }
        Token::Ident(_, Keyword::ESCAPE) => {
            let span = parser.consume_keyword(Keyword::ESCAPE)?;
            let value = parser.consume_string()?;
            Ok(CopyOption::Escape { span, value })
        }
        Token::Ident(_, Keyword::FORCE_QUOTE) => {
            let span = parser.consume_keyword(Keyword::FORCE_QUOTE)?;
            let columns = parse_copy_column_list(parser)?;
            Ok(CopyOption::ForceQuote { span, columns })
        }
        Token::Ident(_, Keyword::FORCE_NOT_NULL) => {
            let span = parser.consume_keyword(Keyword::FORCE_NOT_NULL)?;
            let columns = parse_copy_column_list(parser)?;
            Ok(CopyOption::ForceNotNull { span, columns })
        }
        Token::Ident(_, Keyword::FORCE_NULL) => {
            let span = parser.consume_keyword(Keyword::FORCE_NULL)?;
            let columns = parse_copy_column_list(parser)?;
            Ok(CopyOption::ForceNull { span, columns })
        }
        Token::Ident(_, Keyword::ON_ERROR) => {
            let span = parser.consume_keyword(Keyword::ON_ERROR)?;
            let action = parser.consume_plain_identifier_unreserved()?;
            Ok(CopyOption::OnError { span, action })
        }
        Token::Ident(_, Keyword::REJECT_LIMIT) => {
            let span = parser.consume_keyword(Keyword::REJECT_LIMIT)?;
            let limit = parse_expression_unreserved(parser, false)?;
            Ok(CopyOption::RejectLimit { span, limit })
        }
        Token::Ident(_, Keyword::ENCODING) => {
            let span = parser.consume_keyword(Keyword::ENCODING)?;
            let value = parser.consume_string()?;
            Ok(CopyOption::Encoding { span, value })
        }
        Token::Ident(_, Keyword::LOG_VERBOSITY) => {
            let span = parser.consume_keyword(Keyword::LOG_VERBOSITY)?;
            let verbosity = parser.consume_plain_identifier_unreserved()?;
            Ok(CopyOption::LogVerbosity { span, verbosity })
        }
        _ => parser.expected_failure(
            "COPY option (FORMAT, FREEZE, DELIMITER, NULL, DEFAULT, HEADER, QUOTE, ESCAPE, \
             FORCE_QUOTE, FORCE_NOT_NULL, FORCE_NULL, ON_ERROR, REJECT_LIMIT, ENCODING, \
             LOG_VERBOSITY)",
        ),
    }
}

/// Parse a comma-separated list of options inside `( ... )`.
fn parse_copy_options_modern<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<(Span, Vec<CopyOption<'a>>), ParseError> {
    let lparen = parser.consume_token(Token::LParen)?;
    let mut options = Vec::new();
    if !matches!(parser.token, Token::RParen) {
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    options.push(parse_copy_option_modern(parser)?);
                    Ok(())
                },
            )?;
            if matches!(parser.token, Token::RParen) {
                break;
            }
            parser.consume_token(Token::Comma)?;
        }
    }
    let rparen = parser.consume_token(Token::RParen)?;
    Ok((lparen.join_span(&rparen), options))
}

/// Parse legacy bare-keyword options (pre-PostgreSQL 9.0 syntax):
///
/// ```text
/// [ [ WITH ]
///     [ BINARY ]
///     [ DELIMITER [ AS ] 'char' ]
///     [ NULL [ AS ] 'string' ]
///     [ CSV
///         [ HEADER ]
///         [ QUOTE [ AS ] 'char' ]
///         [ ESCAPE [ AS ] 'char' ]
///         [ FORCE NOT NULL col, ...   (FROM only) ]
///         [ FORCE QUOTE { col, ... | * }  (TO only) ]
///     ]
/// ]
/// ```
fn parse_copy_options_legacy<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<CopyOption<'a>>, ParseError> {
    let mut options = Vec::new();

    // BINARY — sets FORMAT to binary
    if let Some(span) = parser.skip_keyword(Keyword::BINARY) {
        options.push(CopyOption::Format {
            span: span.clone(),
            name: Identifier::new("binary", span),
        });
    }

    // DELIMITER [ AS ] 'char'
    if let Some(span) = parser.skip_keyword(Keyword::DELIMITER) {
        parser.skip_keyword(Keyword::AS);
        let value = parser.consume_string()?;
        options.push(CopyOption::Delimiter { span, value });
    }

    // NULL [ AS ] 'string'
    if let Some(span) = parser.skip_keyword(Keyword::NULL) {
        parser.skip_keyword(Keyword::AS);
        let value = parser.consume_string()?;
        options.push(CopyOption::Null { span, value });
    }

    // CSV [ HEADER ] [ QUOTE [ AS ] 'char' ] [ ESCAPE [ AS ] 'char' ]
    //     [ FORCE NOT NULL col, ... | FORCE QUOTE { col, ... | * } ]
    if let Some(csv_span) = parser.skip_keyword(Keyword::CSV) {
        // Record FORMAT csv
        options.push(CopyOption::Format {
            span: csv_span.clone(),
            name: Identifier::new("csv", csv_span),
        });

        // HEADER
        if let Some(span) = parser.skip_keyword(Keyword::HEADER) {
            options.push(CopyOption::Header { span, value: None });
        }

        // QUOTE [ AS ] 'char'
        if let Some(span) = parser.skip_keyword(Keyword::QUOTE) {
            parser.skip_keyword(Keyword::AS);
            let value = parser.consume_string()?;
            options.push(CopyOption::Quote { span, value });
        }

        // ESCAPE [ AS ] 'char'
        if let Some(span) = parser.skip_keyword(Keyword::ESCAPE) {
            parser.skip_keyword(Keyword::AS);
            let value = parser.consume_string()?;
            options.push(CopyOption::Escape { span, value });
        }

        // FORCE NOT NULL col1, col2, ...   (FROM CSV)
        // FORCE QUOTE { col1, col2, ... | * }   (TO CSV)
        if let Some(force_span) = parser.skip_keyword(Keyword::FORCE) {
            match &parser.token {
                Token::Ident(_, Keyword::NOT) => {
                    // FORCE NOT NULL col, ...
                    let span = force_span
                        .join_span(&parser.consume_keyword(Keyword::NOT)?)
                        .join_span(&parser.consume_keyword(Keyword::NULL)?);
                    let mut cols = Vec::new();
                    loop {
                        cols.push(parser.consume_plain_identifier_unreserved()?);
                        if parser.skip_token(Token::Comma).is_none() {
                            break;
                        }
                    }
                    let col_span = if let Some(last) = cols.last() {
                        span.clone().join_span(last)
                    } else {
                        span.clone()
                    };
                    options.push(CopyOption::ForceNotNull {
                        span,
                        columns: CopyColumnList::Columns {
                            lparen_span: col_span.clone(),
                            columns: cols,
                            rparen_span: col_span,
                        },
                    });
                }
                Token::Ident(_, Keyword::QUOTE) => {
                    // FORCE QUOTE { * | col, ... }
                    let span = force_span.join_span(&parser.consume_keyword(Keyword::QUOTE)?);
                    let columns = if let Some(star) = parser.skip_token(Token::Mul) {
                        CopyColumnList::All(star)
                    } else {
                        let mut cols = Vec::new();
                        loop {
                            cols.push(parser.consume_plain_identifier_unreserved()?);
                            if parser.skip_token(Token::Comma).is_none() {
                                break;
                            }
                        }
                        let col_span = if let Some(last) = cols.last() {
                            span.clone().join_span(last)
                        } else {
                            span.clone()
                        };
                        CopyColumnList::Columns {
                            lparen_span: col_span.clone(),
                            columns: cols,
                            rparen_span: col_span,
                        }
                    };
                    options.push(CopyOption::ForceQuote { span, columns });
                }
                _ => {
                    parser
                        .expected_failure("NOT NULL or QUOTE after FORCE in COPY legacy options")?;
                }
            }
        }
    }

    Ok(options)
}

/// Shared helper: parse the optional `[ WITH ] ( options )` or legacy bare
/// options that follow the location in both COPY FROM and COPY TO.
fn parse_copy_options_clause<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<(Option<Span>, Vec<CopyOption<'a>>), ParseError> {
    if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
        if matches!(parser.token, Token::LParen) {
            let (_, opts) = parse_copy_options_modern(parser)?;
            Ok((Some(with_span), opts))
        } else {
            let opts = parse_copy_options_legacy(parser)?;
            Ok((Some(with_span), opts))
        }
    } else if matches!(parser.token, Token::LParen) {
        let (_, opts) = parse_copy_options_modern(parser)?;
        Ok((None, opts))
    } else if {
        let parser: &Parser<'_, '_> = parser;
        matches!(
            &parser.token,
            Token::Ident(
                _,
                Keyword::BINARY | Keyword::DELIMITER | Keyword::NULL | Keyword::CSV
            )
        )
    } {
        let opts = parse_copy_options_legacy(parser)?;
        Ok((None, opts))
    } else {
        Ok((None, Vec::new()))
    }
}

/// Parse the source (table or parenthesised query) that immediately follows
/// the `COPY` keyword.
fn parse_copy_source<'a>(parser: &mut Parser<'a, '_>) -> Result<CopySource<'a>, ParseError> {
    if matches!(parser.token, Token::LParen) {
        let lparen_span = parser.consume_token(Token::LParen)?;
        let query =
            parser.recovered(
                "')'",
                &|t| t == &Token::RParen,
                |parser| match parse_statement(parser)? {
                    Some(s) => Ok(Some(s)),
                    None => {
                        parser.expected_error("query");
                        Ok(None)
                    }
                },
            )?;
        let rparen_span = parser.consume_token(Token::RParen)?;
        let query = match query {
            Some(s) => Box::new(s),
            None => return Err(crate::parser::ParseError::Unrecovered),
        };
        Ok(CopySource::Query {
            lparen_span,
            query,
            rparen_span,
        })
    } else {
        let name = parse_qualified_name_unreserved(parser)?;
        let columns = if matches!(parser.token, Token::LParen) {
            parser.consume_token(Token::LParen)?;
            let mut cols = Vec::new();
            if !matches!(parser.token, Token::RParen) {
                loop {
                    parser.recovered(
                        "')' or ','",
                        &|t| matches!(t, Token::RParen | Token::Comma),
                        |parser| {
                            cols.push(parser.consume_plain_identifier_unreserved()?);
                            Ok(())
                        },
                    )?;
                    if matches!(parser.token, Token::RParen) {
                        break;
                    }
                    parser.consume_token(Token::Comma)?;
                }
            }
            parser.consume_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };
        Ok(CopySource::Table { name, columns })
    }
}

/// Parse `COPY ... FROM location [options] [WHERE]`.
fn parse_copy_from<'a>(
    parser: &mut Parser<'a, '_>,
    copy_span: Span,
    source: CopySource<'a>,
) -> Result<CopyFrom<'a>, ParseError> {
    let from_span = parser.consume_keyword(Keyword::FROM)?;
    let location = parse_copy_location(parser)?;

    if matches!(source, CopySource::Query { .. }) {
        parser.err(
            "Subquery source is only valid with COPY ... TO, not FROM",
            &from_span,
        );
    }

    let (with_span, options) = parse_copy_options_clause(parser)?;

    let where_ = if let Some(where_span) = parser.skip_keyword(Keyword::WHERE) {
        let cond = parse_expression_unreserved(parser, false)?;
        Some((where_span, cond))
    } else {
        None
    };

    Ok(CopyFrom {
        copy_span,
        source,
        from_span,
        location,
        with_span,
        options,
        where_,
    })
}

/// Parse `COPY ... TO location [options]`.
fn parse_copy_to<'a>(
    parser: &mut Parser<'a, '_>,
    copy_span: Span,
    source: CopySource<'a>,
) -> Result<CopyTo<'a>, ParseError> {
    let to_span = parser.consume_keyword(Keyword::TO)?;
    let location = parse_copy_location(parser)?;
    let (with_span, options) = parse_copy_options_clause(parser)?;

    Ok(CopyTo {
        copy_span,
        source,
        to_span,
        location,
        with_span,
        options,
    })
}

/// Parse a PostgreSQL `COPY` statement, returning either a
/// [`Statement::CopyFrom`] or [`Statement::CopyTo`].
pub(crate) fn parse_copy_statement<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Statement<'a>, ParseError> {
    let copy_span = parser.consume_keyword(Keyword::COPY)?;
    let source = parse_copy_source(parser)?;

    match &parser.token {
        Token::Ident(_, Keyword::FROM) => Ok(Statement::CopyFrom(Box::new(parse_copy_from(
            parser, copy_span, source,
        )?))),
        Token::Ident(_, Keyword::TO) => Ok(Statement::CopyTo(Box::new(parse_copy_to(
            parser, copy_span, source,
        )?))),
        _ => parser.expected_failure("FROM or TO"),
    }
}
