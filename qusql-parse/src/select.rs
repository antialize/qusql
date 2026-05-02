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
use alloc::{borrow::Cow, boxed::Box, vec::Vec};

use crate::QualifiedName;
use crate::qualified_name::parse_qualified_name_unreserved;
use crate::{
    DataType, Identifier, SString, Span, Spanned, Statement,
    data_type::{DataTypeContext, parse_data_type},
    expression::{Expression, PRIORITY_MAX, parse_expression_unreserved},
    keywords::{Keyword, Restrict},
    lexer::{StringType, Token},
    parser::{ParseError, Parser},
    span::OptSpanned,
    statement::parse_compound_query,
};

/// Value in select
#[derive(Debug, Clone)]
pub struct SelectExpr<'a> {
    /// Value to select
    pub expr: Expression<'a>,
    /// Optional name to give value if specified
    pub as_: Option<Identifier<'a>>,
}

impl<'a> Spanned for SelectExpr<'a> {
    fn span(&self) -> Span {
        self.expr.join_span(&self.as_)
    }
}

pub(crate) fn parse_select_expr<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<SelectExpr<'a>, ParseError> {
    let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
    let reserved = parser.reserved();
    let as_ = if parser.skip_keyword(Keyword::AS).is_some() {
        Some(parser.consume_plain_identifier_unreserved()?)
    } else {
        let is_implicit_alias = match &parser.token {
            Token::Ident(_, kw) => !kw.restricted(reserved),
            Token::String(_, StringType::DoubleQuoted) => parser.options.dialect.is_postgresql(),
            _ => false,
        };
        if is_implicit_alias {
            Some(parser.consume_plain_identifier_unreserved()?)
        } else {
            None
        }
    };
    Ok(SelectExpr { expr, as_ })
}

/// Specification for join
#[derive(Debug, Clone)]
pub enum JoinSpecification<'a> {
    /// On specification expression and span of "ON"
    On(Expression<'a>, Span),
    /// List of columns to joint using, and span of "USING"
    Using(Vec<Identifier<'a>>, Span),
}

impl<'a> Spanned for JoinSpecification<'a> {
    fn span(&self) -> Span {
        match &self {
            JoinSpecification::On(v, s) => s.join_span(v),
            JoinSpecification::Using(v, s) => s.join_span(v),
        }
    }
}

/// Type of join
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner(Span),
    Cross(Span),
    Normal(Span),
    Straight(Span),
    Left(Span),
    Right(Span),
    FullOuter(Span),
    Natural(Span),
    NaturalInner(Span),
    NaturalLeft(Span),
    NaturalRight(Span),
}
impl Spanned for JoinType {
    fn span(&self) -> Span {
        match &self {
            JoinType::Inner(v) => v.span(),
            JoinType::Cross(v) => v.span(),
            JoinType::Normal(v) => v.span(),
            JoinType::Straight(v) => v.span(),
            JoinType::Left(v) => v.span(),
            JoinType::Right(v) => v.span(),
            JoinType::FullOuter(v) => v.span(),
            JoinType::Natural(v) => v.span(),
            JoinType::NaturalInner(v) => v.span(),
            JoinType::NaturalLeft(v) => v.span(),
            JoinType::NaturalRight(v) => v.span(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum IndexHintUse {
    Use(Span),
    Ignore(Span),
    Force(Span),
}
impl Spanned for IndexHintUse {
    fn span(&self) -> Span {
        match &self {
            IndexHintUse::Use(v) => v.span(),
            IndexHintUse::Ignore(v) => v.span(),
            IndexHintUse::Force(v) => v.span(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum IndexHintType {
    Index(Span),
    Key(Span),
}
impl Spanned for IndexHintType {
    fn span(&self) -> Span {
        match &self {
            IndexHintType::Index(v) => v.span(),
            IndexHintType::Key(v) => v.span(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum IndexHintFor {
    Join(Span),
    OrderBy(Span),
    GroupBy(Span),
}
impl Spanned for IndexHintFor {
    fn span(&self) -> Span {
        match &self {
            IndexHintFor::Join(v) => v.span(),
            IndexHintFor::OrderBy(v) => v.span(),
            IndexHintFor::GroupBy(v) => v.span(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexHint<'a> {
    pub use_: IndexHintUse,
    pub type_: IndexHintType,
    pub for_: Option<(Span, IndexHintFor)>,
    pub lparen: Span,
    pub index_list: Vec<Identifier<'a>>,
    pub rparen: Span,
}

impl<'a> Spanned for IndexHint<'a> {
    fn span(&self) -> Span {
        self.use_
            .span()
            .join_span(&self.type_)
            .join_span(&self.for_)
            .join_span(&self.lparen)
            .join_span(&self.index_list)
            .join_span(&self.rparen)
    }
}

/// JSON_TABLE ON EMPTY/ERROR behavior
#[derive(Debug, Clone)]
pub enum JsonTableOnErrorEmpty<'a> {
    /// DEFAULT value
    Default(Expression<'a>),
    /// ERROR
    Error(Span),
    /// NULL
    Null(Span),
}

impl<'a> Spanned for JsonTableOnErrorEmpty<'a> {
    fn span(&self) -> Span {
        match self {
            JsonTableOnErrorEmpty::Default(expr) => expr.span(),
            JsonTableOnErrorEmpty::Error(s) => s.clone(),
            JsonTableOnErrorEmpty::Null(s) => s.clone(),
        }
    }
}

/// JSON_TABLE column definition
#[derive(Debug, Clone)]
pub enum JsonTableColumn<'a> {
    /// Regular column: name data_type PATH 'path' [options]
    Column {
        name: Identifier<'a>,
        data_type: DataType<'a>,
        path_span: Span,
        path: Expression<'a>,
        /// ON EMPTY clause
        on_empty: Option<(JsonTableOnErrorEmpty<'a>, Span)>,
        /// ON ERROR clause
        on_error: Option<(JsonTableOnErrorEmpty<'a>, Span)>,
    },
    /// Ordinality column: name FOR ORDINALITY
    Ordinality {
        name: Identifier<'a>,
        for_ordinality_span: Span,
    },
    /// Nested path: NESTED PATH 'path' COLUMNS (...)
    Nested {
        nested_span: Span,
        path_span: Span,
        path: Expression<'a>,
        columns_span: Span,
        columns: Vec<JsonTableColumn<'a>>,
    },
}

impl<'a> Spanned for JsonTableColumn<'a> {
    fn span(&self) -> Span {
        match self {
            JsonTableColumn::Column {
                name,
                data_type,
                path_span,
                path,
                on_empty,
                on_error,
            } => name
                .span()
                .join_span(data_type)
                .join_span(path_span)
                .join_span(path)
                .join_span(on_empty)
                .join_span(on_error),
            JsonTableColumn::Ordinality {
                name,
                for_ordinality_span,
            } => name.span().join_span(for_ordinality_span),
            JsonTableColumn::Nested {
                nested_span,
                path_span,
                path,
                columns_span,
                columns,
            } => nested_span
                .join_span(path_span)
                .join_span(path)
                .join_span(columns_span)
                .join_span(columns),
        }
    }
}

/// The name of a table-valued function in a FROM clause.
///
/// Well-known set-returning functions get their own variant so consumers can
/// match on them without string comparison. Any other function name falls into
/// `Other`.
#[derive(Debug, Clone)]
pub enum TableFunctionName<'a> {
    /// `UNNEST(array, ...)` — expand one or more arrays into a set of rows
    Unnest(Span),
    /// `GENERATE_SERIES(start, stop [, step])` — generate a series of values
    GenerateSeries(Span),
    /// `STRING_TO_TABLE(str, delimiter)` — split a string into rows
    StringToTable(Span),
    /// Any other set-returning function (user-defined or less common builtins)
    Other(QualifiedName<'a>),
}

impl<'a> Spanned for TableFunctionName<'a> {
    fn span(&self) -> Span {
        match self {
            TableFunctionName::Unnest(s)
            | TableFunctionName::GenerateSeries(s)
            | TableFunctionName::StringToTable(s) => s.clone(),
            TableFunctionName::Other(n) => n.span(),
        }
    }
}

/// Reference to table in select
#[derive(Debug, Clone)]
pub enum TableReference<'a> {
    /// Reference to a table or view
    Table {
        /// Name of table to to select from
        identifier: QualifiedName<'a>,
        /// Span of "AS" if specified
        as_span: Option<Span>,
        /// Alias for table if specified
        as_: Option<Identifier<'a>>,
        /// Index hints
        index_hints: Vec<IndexHint<'a>>,
    },
    /// Subquery
    Query {
        /// Query yielding table
        query: Box<Statement<'a>>,
        /// Span of "AS" if specified
        as_span: Option<Span>,
        /// Alias for table if specified
        as_: Option<Identifier<'a>>,
        /// Optional column list `(col1, col2, ...)` after the alias
        col_list: Vec<Identifier<'a>>,
    },
    /// JSON_TABLE function
    JsonTable {
        /// Span of "JSON_TABLE"
        json_table_span: Span,
        /// JSON data expression
        json_expr: Expression<'a>,
        /// JSON path expression
        path: Expression<'a>,
        /// COLUMNS keyword span
        columns_keyword_span: Span,
        /// Column definitions
        columns: Vec<JsonTableColumn<'a>>,
        /// Span of "AS" if specified
        as_span: Option<Span>,
        /// Alias for table if specified
        as_: Option<Identifier<'a>>,
    },
    /// Table-valued function call, e.g. `generate_series(1, 10)` or `unnest(arr) WITH ORDINALITY`
    Function {
        /// The function name
        name: TableFunctionName<'a>,
        /// Arguments passed to the function
        args: Vec<Expression<'a>>,
        /// Span of `WITH ORDINALITY` if present
        with_ordinality: Option<Span>,
        /// Span of "AS" if specified
        as_span: Option<Span>,
        /// Alias for the result set if specified
        as_: Option<Identifier<'a>>,
        /// Optional column alias list `(col1, col2, ...)` after the alias
        col_list: Vec<Identifier<'a>>,
    },
    /// Join
    Join {
        /// What type of join is it
        join: JoinType,
        /// Left hand side of join
        left: Box<TableReference<'a>>,
        /// Right hand side of join
        right: Box<TableReference<'a>>,
        /// How to do the join if specified
        specification: Option<JoinSpecification<'a>>,
    },
}

impl<'a> Spanned for TableReference<'a> {
    fn span(&self) -> Span {
        match &self {
            TableReference::Table {
                identifier,
                as_span,
                as_,
                index_hints,
            } => identifier
                .opt_join_span(as_span)
                .opt_join_span(as_)
                .opt_join_span(index_hints)
                .expect("span of table"),
            TableReference::Query {
                query,
                as_span,
                as_,
                col_list,
            } => query.join_span(as_span).join_span(as_).join_span(col_list),
            TableReference::JsonTable {
                json_table_span,
                json_expr,
                path,
                columns_keyword_span,
                columns,
                as_span,
                as_,
            } => json_table_span
                .join_span(json_expr)
                .join_span(path)
                .join_span(columns_keyword_span)
                .join_span(columns)
                .join_span(as_span)
                .join_span(as_),
            TableReference::Function {
                name,
                args,
                with_ordinality,
                as_span,
                as_,
                col_list,
            } => name
                .span()
                .join_span(args)
                .join_span(with_ordinality)
                .join_span(as_span)
                .join_span(as_)
                .join_span(col_list),
            TableReference::Join {
                join,
                left,
                right,
                specification,
            } => join
                .join_span(left)
                .join_span(right)
                .join_span(specification),
        }
    }
}

pub(crate) fn parse_table_reference_inner<'a>(
    parser: &mut Parser<'a, '_>,
    additional_restrict: crate::keywords::Restrict,
) -> Result<TableReference<'a>, ParseError> {
    // TODO [LATERAL] table_subquery [AS] alias [(col_list)]
    // if parser.skip_token(Token::LParen).is_some() {
    //     let a = parse_table_reference(parser)?;
    //     parser.consume_token(Token::RParen)?;
    //     return Ok(a);
    // }

    match &parser.token {
        Token::Ident(_, Keyword::LATERAL) => {
            // LATERAL subquery/function
            let lateral_span = parser.consume_keyword(Keyword::LATERAL)?;
            parser.postgres_only(&lateral_span);
            let query = parse_compound_query(parser)?;
            let as_span = parser.skip_keyword(Keyword::AS);
            let as_ = if as_span.is_some()
                || (matches!(&parser.token, Token::Ident(_, k) if !k.restricted(parser.reserved() | additional_restrict)))
            {
                Some(
                    parser.consume_plain_identifier_restrict(
                        parser.reserved() | additional_restrict,
                    )?,
                )
            } else {
                None
            };
            Ok(TableReference::Query {
                query: Box::new(query),
                as_span: as_span
                    .map(|s| lateral_span.join_span(&s))
                    .or(Some(lateral_span)),
                as_,
                col_list: Vec::new(),
            })
        }
        Token::Ident(_, Keyword::SELECT) | Token::LParen => {
            // Consume the outer '(' if present. By doing this first we only need
            // one level of peek to distinguish a parenthesised join group from a
            // parenthesised subquery when the contents themselves start with '(':
            //   ((t1 JOIN t2) JOIN t3)  – join group  (peek after inner ( is an ident)
            //   ((SELECT …) UNION …)    – subquery    (peek after inner ( is SELECT)
            let outer_paren = if matches!(parser.token, Token::LParen) {
                Some(parser.consume_token(Token::LParen)?)
            } else {
                None
            };

            // A join group is only possible when we consumed an outer '(' and
            // what follows is NOT a subquery start (SELECT/VALUES/WITH directly,
            // or '(' whose own first token is SELECT/VALUES/WITH).
            let is_join_group = outer_paren.is_some()
                && !matches!(
                    parser.token,
                    Token::Ident(_, Keyword::SELECT | Keyword::VALUES | Keyword::WITH)
                )
                && !(matches!(parser.token, Token::LParen)
                    && matches!(
                        parser.peek(),
                        Token::Ident(_, Keyword::SELECT | Keyword::VALUES | Keyword::WITH)
                    ));

            if is_join_group {
                let inner = parse_table_reference(parser, additional_restrict)?;
                parser.consume_token(Token::RParen)?;
                return Ok(inner);
            }

            // Subquery path. parse_compound_query sees either SELECT (when
            // outer_paren is None) or '(' / SELECT (when outer_paren is Some).
            // Either way it returns with the matching closing ')' already consumed.
            // We then consume the outer ')' we pre-consumed above (if any), and
            // only after that do we look for the alias.
            let query = parse_compound_query(parser)?;

            if outer_paren.is_some() {
                parser.consume_token(Token::RParen)?;
            }

            let as_span = parser.skip_keyword(Keyword::AS);
            let as_ = if as_span.is_some()
                || (matches!(&parser.token, Token::Ident(_, k) if !k.restricted(parser.reserved() | additional_restrict)))
            {
                Some(
                    parser.consume_plain_identifier_restrict(
                        parser.reserved() | additional_restrict,
                    )?,
                )
            } else {
                None
            };
            // Optional column list: AS alias (col1, col2, ...)
            let mut col_list = Vec::new();
            if as_.is_some() && matches!(parser.token, Token::LParen) {
                parser.consume_token(Token::LParen)?;
                loop {
                    parser.recovered(
                        "')' or ','",
                        &|t| matches!(t, Token::RParen | Token::Comma),
                        |parser| {
                            col_list
                                .push(parser.consume_plain_identifier_restrict(Restrict::EMPTY)?);
                            Ok(())
                        },
                    )?;
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                parser.consume_token(Token::RParen)?;
            };
            Ok(TableReference::Query {
                query: Box::new(query),
                as_span,
                as_,
                col_list,
            })
        }
        Token::Ident(_, _) => parse_table_reference_named(parser, additional_restrict),
        Token::String(_, StringType::DoubleQuoted) if parser.options.dialect.is_postgresql() => {
            parse_table_reference_named(parser, additional_restrict)
        }
        _ => parser.expected_failure("subquery or identifier"),
    }
}

fn parse_table_reference_named<'a>(
    parser: &mut Parser<'a, '_>,
    additional_restrict: Restrict,
) -> Result<TableReference<'a>, ParseError> {
    if matches!(parser.token, Token::Ident(_, Keyword::JSON_TABLE))
        && matches!(parser.peek(), Token::LParen)
    {
        let json_table_span = parser.consume_keyword(Keyword::JSON_TABLE)?;

        parser.consume_token(Token::LParen)?;

        // Parse JSON data expression (first argument)
        let json_expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;

        // Expect comma
        parser.consume_token(Token::Comma)?;

        // Parse JSON path - just a simple string for now
        let path = match &parser.token {
            Token::String(s, _) => {
                let val = *s;
                let span = parser.consume();
                Expression::String(Box::new(SString::new(Cow::Borrowed(val), span)))
            }
            _ => {
                // Fall back to expression parsing
                parse_expression_unreserved(parser, PRIORITY_MAX)?
            }
        };

        // Parse COLUMNS clause (no comma before COLUMNS)
        let columns_keyword_span = parser.consume_keyword(Keyword::COLUMNS)?;
        parser.consume_token(Token::LParen)?;

        let columns = parser.recovered(")", &|t| t == &Token::RParen, |parser| {
            // Parse column definitions
            parse_json_table_columns(parser)
        })?;

        parser.consume_token(Token::RParen)?;

        // Closing parenthesis of JSON_TABLE
        parser.consume_token(Token::RParen)?;

        // Parse AS and alias
        let as_span = parser.skip_keyword(Keyword::AS);
        let as_ = if as_span.is_some()
            || (matches!(&parser.token, Token::Ident(_, k) if !k.restricted(parser.reserved() | additional_restrict)))
        {
            Some(parser.consume_plain_identifier_unreserved()?)
        } else {
            None
        };

        return Ok(TableReference::JsonTable {
            json_table_span,
            json_expr,
            path,
            columns_keyword_span,
            columns,
            as_span,
            as_,
        });
    }

    // Snapshot the first token's keyword (if any) before consuming the qualified name,
    // so we can build the correct TableFunctionName variant if this turns out to be a
    // table-valued function call.
    let first_kw = match &parser.token {
        Token::Ident(_, kw) => Some(*kw),
        _ => None,
    };
    let identifier = parse_qualified_name_unreserved(parser)?;

    if matches!(parser.token, Token::LParen) {
        // Table-valued function call (any set-returning function)
        let func_name = if identifier.prefix.is_empty() {
            match first_kw {
                Some(Keyword::UNNEST) => TableFunctionName::Unnest(identifier.identifier.span),
                Some(Keyword::GENERATE_SERIES) => {
                    TableFunctionName::GenerateSeries(identifier.identifier.span)
                }
                Some(Keyword::STRING_TO_TABLE) => {
                    TableFunctionName::StringToTable(identifier.identifier.span)
                }
                _ => TableFunctionName::Other(identifier),
            }
        } else {
            TableFunctionName::Other(identifier)
        };
        parser.consume_token(Token::LParen)?;
        let mut args = Vec::new();
        if !matches!(parser.token, Token::RParen) {
            loop {
                parser.recovered(
                    "')' or ','",
                    &|t| matches!(t, Token::RParen | Token::Comma),
                    |parser| {
                        args.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
                        Ok(())
                    },
                )?;
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
        }
        parser.consume_token(Token::RParen)?;
        // WITH ORDINALITY is a general FROM-clause modifier supported by all SRFs
        let with_ordinality = if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
            let ord_span = parser.consume_keyword(Keyword::ORDINALITY)?;
            Some(with_span.join_span(&ord_span))
        } else {
            None
        };
        let as_span = parser.skip_keyword(Keyword::AS);
        let as_ = if as_span.is_some()
            || (matches!(&parser.token, Token::Ident(_, k) if !k.restricted(parser.reserved() | additional_restrict)))
        {
            Some(parser.consume_plain_identifier_unreserved()?)
        } else {
            None
        };
        // Parse optional column alias list `alias(col1, col2, ...)` (PostgreSQL table-function syntax)
        let col_list = if as_.is_some() && matches!(parser.token, Token::LParen) {
            parser.consume_token(Token::LParen)?;
            let mut cols = Vec::new();
            loop {
                parser.recovered(
                    "')' or ','",
                    &|t| matches!(t, Token::RParen | Token::Comma),
                    |parser| {
                        cols.push(parser.consume_plain_identifier_unreserved()?);
                        Ok(())
                    },
                )?;
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            parser.consume_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };
        return Ok(TableReference::Function {
            name: func_name,
            args,
            with_ordinality,
            as_span,
            as_,
            col_list,
        });
    }

    // TODO [PARTITION (partition_names)] [[AS] alias]
    let as_span = parser.skip_keyword(Keyword::AS);
    let base_restrict = parser.reserved() | additional_restrict;
    let as_ = if as_span.is_some()
        || (matches!(&parser.token, Token::Ident(_, k) if !k.restricted(base_restrict | Restrict::FROM_ALIAS)))
    {
        Some(parser.consume_plain_identifier_restrict(base_restrict)?)
    } else {
        None
    };

    let mut index_hints = Vec::new();
    loop {
        let use_ = match parser.token {
            Token::Ident(_, Keyword::USE) => IndexHintUse::Use(parser.consume()),
            Token::Ident(_, Keyword::IGNORE) => IndexHintUse::Ignore(parser.consume()),
            Token::Ident(_, Keyword::FORCE) => IndexHintUse::Force(parser.consume()),
            _ => break,
        };
        let type_ = match parser.token {
            Token::Ident(_, Keyword::INDEX) => IndexHintType::Index(parser.consume()),
            Token::Ident(_, Keyword::KEY) => IndexHintType::Key(parser.consume()),
            _ => parser.expected_failure("'INDEX' or 'KEY'")?,
        };
        let for_ = if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
            let v = match parser.token {
                Token::Ident(_, Keyword::JOIN) => IndexHintFor::Join(parser.consume()),
                Token::Ident(_, Keyword::GROUP) => {
                    IndexHintFor::GroupBy(parser.consume_keywords(&[Keyword::GROUP, Keyword::BY])?)
                }
                Token::Ident(_, Keyword::ORDER) => {
                    IndexHintFor::OrderBy(parser.consume_keywords(&[Keyword::ORDER, Keyword::BY])?)
                }
                _ => parser.expected_failure("'JOIN', 'GROUP BY' or 'ORDER BY'")?,
            };
            Some((for_span, v))
        } else {
            None
        };
        let lparen = parser.consume_token(Token::LParen)?;
        let mut index_list = Vec::new();
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    index_list.push(parser.consume_plain_identifier_unreserved()?);
                    Ok(())
                },
            )?;
            if matches!(parser.token, Token::RParen) {
                break;
            }
            parser.consume_token(Token::Comma)?;
        }
        let rparen = parser.consume_token(Token::RParen)?;
        index_hints.push(IndexHint {
            use_,
            type_,
            for_,
            lparen,
            index_list,
            rparen,
        })
    }

    if !index_hints.is_empty() && !parser.options.dialect.is_maria() {
        parser.err(
            "Index hints only supported by MariaDb",
            &index_hints.opt_span().unwrap(),
        );
    }

    Ok(TableReference::Table {
        identifier,
        as_span,
        as_,
        index_hints,
    })
}

fn parse_json_table_columns<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<JsonTableColumn<'a>>, ParseError> {
    let mut columns = Vec::new();

    loop {
        // Check for NESTED PATH
        if let Some(nested_span) = parser.skip_keyword(Keyword::NESTED) {
            let path_span = parser.consume_keyword(Keyword::PATH)?;
            let path = match &parser.token {
                Token::String(s, _) => {
                    let val = *s;
                    let span = parser.consume();
                    Expression::String(Box::new(SString::new(Cow::Borrowed(val), span)))
                }
                _ => parse_expression_unreserved(parser, PRIORITY_MAX)?,
            };
            let columns_span = parser.consume_keyword(Keyword::COLUMNS)?;
            parser.consume_token(Token::LParen)?;
            let nested_columns = parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                parse_json_table_columns(parser)
            })?;
            parser.consume_token(Token::RParen)?;

            columns.push(JsonTableColumn::Nested {
                nested_span,
                path_span,
                path,
                columns_span,
                columns: nested_columns,
            });
        } else {
            // Parse column name
            let name = parser.consume_plain_identifier_unreserved()?;

            // Check if this is FOR ORDINALITY
            if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
                let ordinality_span = parser.consume_keyword(Keyword::ORDINALITY)?;
                let for_ordinality_span = for_span.join_span(&ordinality_span);

                columns.push(JsonTableColumn::Ordinality {
                    name,
                    for_ordinality_span,
                });
            } else {
                // Parse data type
                let data_type = parse_data_type(parser, DataTypeContext::FunctionReturn)?;

                // Check for EXISTS before PATH
                let _ = parser.skip_keyword(Keyword::EXISTS);

                // Parse PATH keyword and path expression
                let path_span = parser.consume_keyword(Keyword::PATH)?;
                let path = match &parser.token {
                    Token::String(s, _) => {
                        let val = *s;
                        let span = parser.consume();
                        Expression::String(Box::new(SString::new(Cow::Borrowed(val), span)))
                    }
                    _ => parse_expression_unreserved(parser, PRIORITY_MAX)?,
                };

                // Parse ON EMPTY and ON ERROR clauses
                // These can appear in various orders: DEFAULT '...' ON EMPTY, ERROR ON ERROR, etc.
                let mut on_empty = None;
                let mut on_error = None;

                loop {
                    let behavior_start = parser.span.span().start;
                    let behavior = match &parser.token {
                        Token::Ident(_, Keyword::DEFAULT) => {
                            parser.consume();
                            // Parse the default value
                            let default_val = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                            Some(JsonTableOnErrorEmpty::Default(default_val))
                        }
                        Token::Ident(_, Keyword::ERROR) => {
                            let error_span = parser.consume();
                            Some(JsonTableOnErrorEmpty::Error(error_span))
                        }
                        Token::Ident(_, Keyword::NULL) => {
                            let null_span = parser.consume();
                            Some(JsonTableOnErrorEmpty::Null(null_span))
                        }
                        _ => None,
                    };

                    if let Some(behavior) = behavior {
                        parser.consume_keyword(Keyword::ON)?;
                        let clause_end = parser.span.span().end;
                        match &parser.token {
                            Token::Ident(_, Keyword::EMPTY) => {
                                parser.consume();
                                let span = behavior_start..clause_end;
                                on_empty = Some((behavior, span));
                            }
                            Token::Ident(_, Keyword::ERROR) => {
                                parser.consume();
                                let span = behavior_start..clause_end;
                                on_error = Some((behavior, span));
                            }
                            _ => {
                                // Not EMPTY or ERROR, emit an error
                                parser.expected_failure("EMPTY or ERROR")?;
                            }
                        }
                    } else {
                        // No behavior keyword, we're done
                        break;
                    }
                }

                columns.push(JsonTableColumn::Column {
                    name,
                    data_type,
                    path_span,
                    path,
                    on_empty,
                    on_error,
                });
            }
        }

        // Check if there are more columns
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }

        // Check if we've reached the end of the column list
        if matches!(parser.token, Token::RParen) {
            break;
        }
    }

    Ok(columns)
}

pub(crate) fn parse_table_reference<'a>(
    parser: &mut Parser<'a, '_>,
    additional_restrict: crate::keywords::Restrict,
) -> Result<TableReference<'a>, ParseError> {
    let mut ans = parse_table_reference_inner(parser, additional_restrict)?;
    loop {
        let join = match parser.token {
            Token::Ident(_, Keyword::FULL) => {
                let full = parser.consume_keyword(Keyword::FULL)?;
                parser.postgres_only(&full);
                if let Some(outer) = parser.skip_keyword(Keyword::OUTER) {
                    JoinType::FullOuter(
                        full.join_span(&outer)
                            .join_span(&parser.consume_keyword(Keyword::JOIN)?),
                    )
                } else {
                    JoinType::FullOuter(full.join_span(&parser.consume_keyword(Keyword::JOIN)?))
                }
            }
            Token::Ident(_, Keyword::INNER) => JoinType::Inner(
                parser
                    .consume_keyword(Keyword::INNER)?
                    .join_span(&parser.consume_keyword(Keyword::JOIN)?),
            ),
            Token::Ident(_, Keyword::CROSS) => JoinType::Cross(
                parser
                    .consume_keyword(Keyword::CROSS)?
                    .join_span(&parser.consume_keyword(Keyword::JOIN)?),
            ),
            Token::Ident(_, Keyword::JOIN) => {
                JoinType::Normal(parser.consume_keyword(Keyword::JOIN)?)
            }
            Token::Ident(_, Keyword::STRAIGHT_JOIN) => {
                JoinType::Straight(parser.consume_keyword(Keyword::STRAIGHT_JOIN)?)
            }
            Token::Ident(_, Keyword::LEFT) => {
                let left = parser.consume_keyword(Keyword::LEFT)?;
                if let Some(outer) = parser.skip_keyword(Keyword::OUTER) {
                    JoinType::Left(
                        left.join_span(&outer)
                            .join_span(&parser.consume_keyword(Keyword::JOIN)?),
                    )
                } else {
                    JoinType::Left(left.join_span(&parser.consume_keyword(Keyword::JOIN)?))
                }
            }
            Token::Ident(_, Keyword::RIGHT) => {
                let right = parser.consume_keyword(Keyword::RIGHT)?;
                if let Some(outer) = parser.skip_keyword(Keyword::OUTER) {
                    JoinType::Right(
                        right
                            .join_span(&outer)
                            .join_span(&parser.consume_keyword(Keyword::JOIN)?),
                    )
                } else {
                    JoinType::Right(right.join_span(&parser.consume_keyword(Keyword::JOIN)?))
                }
            }
            Token::Ident(_, Keyword::NATURAL) => {
                let natural = parser.consume_keyword(Keyword::NATURAL)?;
                match &parser.token {
                    Token::Ident(_, Keyword::INNER) => JoinType::NaturalInner(
                        natural
                            .join_span(&parser.consume_keywords(&[Keyword::INNER, Keyword::JOIN])?),
                    ),
                    Token::Ident(_, Keyword::LEFT) => {
                        let left = parser.consume_keyword(Keyword::LEFT)?;
                        if let Some(outer) = parser.skip_keyword(Keyword::OUTER) {
                            JoinType::NaturalLeft(
                                left.join_span(&outer)
                                    .join_span(&parser.consume_keyword(Keyword::JOIN)?),
                            )
                        } else {
                            JoinType::NaturalLeft(
                                left.join_span(&parser.consume_keyword(Keyword::JOIN)?),
                            )
                        }
                    }
                    Token::Ident(_, Keyword::RIGHT) => {
                        let right = parser.consume_keyword(Keyword::RIGHT)?;
                        if let Some(outer) = parser.skip_keyword(Keyword::OUTER) {
                            JoinType::NaturalRight(
                                right
                                    .join_span(&outer)
                                    .join_span(&parser.consume_keyword(Keyword::JOIN)?),
                            )
                        } else {
                            JoinType::NaturalRight(
                                right.join_span(&parser.consume_keyword(Keyword::JOIN)?),
                            )
                        }
                    }
                    Token::Ident(_, Keyword::JOIN) => JoinType::Natural(
                        natural.join_span(&parser.consume_keyword(Keyword::JOIN)?),
                    ),
                    _ => parser.expected_failure("'INNER', 'LEFT', 'RIGHT' or 'JOIN'")?,
                }
            }
            _ => break,
        };

        let right = parse_table_reference_inner(parser, additional_restrict)?;

        let specification = match &parser.token {
            Token::Ident(_, Keyword::ON) => {
                let on = parser.consume_keyword(Keyword::ON)?;
                let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                Some(JoinSpecification::On(expr, on))
            }
            Token::Ident(_, Keyword::USING) => {
                let using = parser.consume_keyword(Keyword::USING)?;
                let mut join_column_list = Vec::new();
                loop {
                    join_column_list.push(parser.consume_plain_identifier_unreserved()?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                Some(JoinSpecification::Using(join_column_list, using))
            }
            _ => None,
        };

        ans = TableReference::Join {
            join,
            left: Box::new(ans),
            right: Box::new(right),
            specification,
        };
    }
    Ok(ans)
}

/// Flags specified after SELECT
#[derive(Debug, Clone)]
pub enum SelectFlag {
    All(Span),
    Distinct(Span),
    DistinctOn(Span),
    DistinctRow(Span),
    HighPriority(Span),
    StraightJoin(Span),
    SqlSmallResult(Span),
    SqlBigResult(Span),
    SqlBufferResult(Span),
    SqlNoCache(Span),
    SqlCalcFoundRows(Span),
}

impl Spanned for SelectFlag {
    fn span(&self) -> Span {
        match &self {
            SelectFlag::All(v) => v.span(),
            SelectFlag::Distinct(v) => v.span(),
            SelectFlag::DistinctOn(v) => v.span(),
            SelectFlag::DistinctRow(v) => v.span(),
            SelectFlag::HighPriority(v) => v.span(),
            SelectFlag::StraightJoin(v) => v.span(),
            SelectFlag::SqlSmallResult(v) => v.span(),
            SelectFlag::SqlBigResult(v) => v.span(),
            SelectFlag::SqlBufferResult(v) => v.span(),
            SelectFlag::SqlNoCache(v) => v.span(),
            SelectFlag::SqlCalcFoundRows(v) => v.span(),
        }
    }
}

/// Ordering direction
#[derive(Debug, Clone)]
pub enum OrderFlag {
    Asc(Span),
    Desc(Span),
    AscNullsFirst(Span),
    AscNullsLast(Span),
    DescNullsFirst(Span),
    DescNullsLast(Span),
    NullsFirst(Span),
    NullsLast(Span),
    None,
}
impl OptSpanned for OrderFlag {
    fn opt_span(&self) -> Option<Span> {
        match &self {
            OrderFlag::Asc(v) => v.opt_span(),
            OrderFlag::Desc(v) => v.opt_span(),
            OrderFlag::AscNullsFirst(v) => v.opt_span(),
            OrderFlag::AscNullsLast(v) => v.opt_span(),
            OrderFlag::DescNullsFirst(v) => v.opt_span(),
            OrderFlag::DescNullsLast(v) => v.opt_span(),
            OrderFlag::NullsFirst(v) => v.opt_span(),
            OrderFlag::NullsLast(v) => v.opt_span(),
            OrderFlag::None => None,
        }
    }
}

/// Lock strength for locking
#[derive(Debug, Clone)]
pub enum LockStrength {
    Update(Span),
    Share(Span),
    NoKeyUpdate(Span),
    KeyShare(Span),
}
impl Spanned for LockStrength {
    fn span(&self) -> Span {
        match &self {
            LockStrength::Update(v) => v.span(),
            LockStrength::Share(v) => v.span(),
            LockStrength::NoKeyUpdate(v) => v.span(),
            LockStrength::KeyShare(v) => v.span(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum LockWait {
    NoWait(Span),
    SkipLocket(Span),
    Default,
}
impl OptSpanned for LockWait {
    fn opt_span(&self) -> Option<Span> {
        match &self {
            LockWait::NoWait(v) => v.opt_span(),
            LockWait::SkipLocket(v) => v.opt_span(),
            LockWait::Default => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Locking<'a> {
    /// Span of "FOR"
    pub for_span: Span,
    pub strength: LockStrength,
    pub of: Option<(Span, Vec<Identifier<'a>>)>,
    pub wait: LockWait,
}
impl<'a> Spanned for Locking<'a> {
    fn span(&self) -> Span {
        self.for_span
            .join_span(&self.strength)
            .join_span(&self.of)
            .join_span(&self.wait)
    }
}

/// Representation of select Statement
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statement, Select, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "SELECT f1,f2 FROM t1 WHERE f3<=10 AND f4='y'";
/// let mut issues = Issues::new(sql);
/// let stmt = parse_statement(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: Select = match stmt {
///     Some(Statement::Select(s)) => *s,
///     _ => panic!("We should get an select statement")
/// };
///
/// println!("{:#?}", s.where_);
///
/// let sql = "SELECT CAST(NULL AS CHAR)";
/// let stmt = parse_statement(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: Select = match stmt {
///     Some(Statement::Select(s)) => *s,
///     _ => panic!("We should get an select statement")
/// };
///
/// println!("{:#?}", s.where_);
///
/// let sql = "SELECT * FROM t1, d2.t2 FOR SHARE OF t1, t2 NOWAIT";
/// let stmt = parse_statement(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: Select = match stmt {
///     Some(Statement::Select(s)) => *s,
///     _ => panic!("We should get an select statement")
/// };
///
/// assert!(s.locking.is_some());
/// println!("{:#?}", s.locking);
/// ```
#[derive(Debug, Clone)]
pub struct Select<'a> {
    /// Span of "SELECT"
    pub select_span: Span,
    /// Flags specified after "SELECT"
    pub flags: Vec<SelectFlag>,
    /// List of values to select
    pub select_exprs: Vec<SelectExpr<'a>>,
    /// Span of "FROM"
    pub from_span: Option<Span>,
    /// List of tables to select from
    pub table_references: Option<Vec<TableReference<'a>>>,
    /// Where expression and span of "WHERE" if specified
    pub where_: Option<(Expression<'a>, Span)>,
    /// Span of "GROUP_BY" and group expression if specified
    pub group_by: Option<(Span, Vec<Expression<'a>>)>,
    /// Having expression and span of "HAVING" if specified
    pub having: Option<(Expression<'a>, Span)>,
    /// Span of window if specified
    pub window_span: Option<Span>,
    /// Span of "ORDER BY" and list of order expression and directions, if specified
    pub order_by: Option<(Span, Vec<(Expression<'a>, OrderFlag)>)>,
    /// Span of "LIMIT", offset and count expressions if specified
    pub limit: Option<(Span, Option<Expression<'a>>, Expression<'a>)>,
    /// PostgreSQL: DISTINCT ON (expr, ...) list
    pub distinct_on: Option<(Span, Vec<Expression<'a>>)>,
    /// PostgreSQL: OFFSET n [ROWS] (when no LIMIT is present)
    pub offset: Option<(Span, Expression<'a>)>,
    /// PostgreSQL: FETCH {FIRST|NEXT} n {ROW|ROWS} ONLY
    pub fetch: Option<(Span, Expression<'a>)>,
    /// Row locking clause
    pub locking: Option<Locking<'a>>,
}

impl<'a> Spanned for Select<'a> {
    fn span(&self) -> Span {
        self.select_span
            .join_span(&self.flags)
            .join_span(&self.select_exprs)
            .join_span(&self.from_span)
            .join_span(&self.table_references)
            .join_span(&self.where_)
            .join_span(&self.group_by)
            .join_span(&self.having)
            .join_span(&self.window_span)
            .join_span(&self.order_by)
            .join_span(&self.limit)
            .join_span(&self.distinct_on)
            .join_span(&self.offset)
            .join_span(&self.fetch)
            .join_span(&self.locking)
    }
}

/// Parse a SELECT body starting after the `SELECT` keyword has already been consumed.
/// `select_span` is the span of the keyword (or a synthetic span standing in for it).
pub(crate) fn parse_select_body<'a>(
    parser: &mut Parser<'a, '_>,
    select_span: Span,
) -> Result<Select<'a>, ParseError> {
    let mut flags = Vec::new();
    let mut select_exprs = Vec::new();

    loop {
        match &parser.token {
            Token::Ident(_, Keyword::ALL) => {
                flags.push(SelectFlag::All(parser.consume_keyword(Keyword::ALL)?))
            }
            Token::Ident(_, Keyword::DISTINCT) => {
                let distinct_span = parser.consume_keyword(Keyword::DISTINCT)?;
                // PostgreSQL: DISTINCT ON (expr, ...)
                if matches!(parser.token, Token::Ident(_, Keyword::ON)) {
                    flags.push(SelectFlag::DistinctOn(distinct_span));
                } else {
                    flags.push(SelectFlag::Distinct(distinct_span));
                }
            }
            Token::Ident(_, Keyword::DISTINCTROW) => flags.push(SelectFlag::DistinctRow(
                parser.consume_keyword(Keyword::DISTINCTROW)?,
            )),
            Token::Ident(_, Keyword::HIGH_PRIORITY) => flags.push(SelectFlag::HighPriority(
                parser.consume_keyword(Keyword::HIGH_PRIORITY)?,
            )),
            Token::Ident(_, Keyword::STRAIGHT_JOIN) => flags.push(SelectFlag::StraightJoin(
                parser.consume_keyword(Keyword::STRAIGHT_JOIN)?,
            )),
            Token::Ident(_, Keyword::SQL_SMALL_RESULT) => flags.push(SelectFlag::SqlSmallResult(
                parser.consume_keyword(Keyword::SQL_SMALL_RESULT)?,
            )),
            Token::Ident(_, Keyword::SQL_BIG_RESULT) => flags.push(SelectFlag::SqlBigResult(
                parser.consume_keyword(Keyword::SQL_BIG_RESULT)?,
            )),
            Token::Ident(_, Keyword::SQL_BUFFER_RESULT) => flags.push(SelectFlag::SqlBufferResult(
                parser.consume_keyword(Keyword::SQL_BUFFER_RESULT)?,
            )),
            Token::Ident(_, Keyword::SQL_NO_CACHE) => flags.push(SelectFlag::SqlNoCache(
                parser.consume_keyword(Keyword::SQL_NO_CACHE)?,
            )),
            Token::Ident(_, Keyword::SQL_CALC_FOUND_ROWS) => flags.push(
                SelectFlag::SqlCalcFoundRows(parser.consume_keyword(Keyword::SQL_CALC_FOUND_ROWS)?),
            ),
            _ => break,
        }
    }

    // PostgreSQL: parse DISTINCT ON (expr, ...) if signaled by DistinctOn flag
    let distinct_on = if flags
        .last()
        .is_some_and(|f| matches!(f, SelectFlag::DistinctOn(_)))
    {
        let on_span = parser.consume_keyword(Keyword::ON)?;
        parser.postgres_only(&on_span);
        parser.consume_token(Token::LParen)?;
        let mut exprs = Vec::new();
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    exprs.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
                    Ok(())
                },
            )?;
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        parser.consume_token(Token::RParen)?;
        Some((on_span, exprs))
    } else {
        None
    };

    loop {
        select_exprs.push(parse_select_expr(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }

    // TODO [into_option]

    let from_span = parser.skip_keyword(Keyword::FROM);

    let table_references = if from_span.is_some() {
        let mut table_references = Vec::new();
        loop {
            table_references.push(parse_table_reference(parser, Restrict::EMPTY)?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Some(table_references)
    } else {
        None
    };

    // TODO PARTITION partition_list;
    let where_ = if let Some(span) = parser.skip_keyword(Keyword::WHERE) {
        Some((parse_expression_unreserved(parser, PRIORITY_MAX)?, span))
    } else {
        None
    };

    let group_by = if let Some(group_span) = parser.skip_keyword(Keyword::GROUP) {
        let span = parser.consume_keyword(Keyword::BY)?.join_span(&group_span);
        let mut groups = Vec::new();
        loop {
            groups.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        // TODO [WITH ROLLUP]]
        Some((span, groups))
    } else {
        None
    };

    let having = if let Some(span) = parser.skip_keyword(Keyword::HAVING) {
        Some((parse_expression_unreserved(parser, PRIORITY_MAX)?, span))
    } else {
        None
    };

    let window_span = parser.skip_keyword(Keyword::WINDOW);
    if window_span.is_some() {
        //TODO window_name AS (window_spec) [, window_name AS (window_spec)] ...]
    }

    let order_by = if let Some(span) = parser.skip_keyword(Keyword::ORDER) {
        let span = parser.consume_keyword(Keyword::BY)?.join_span(&span);
        let mut order = Vec::new();
        loop {
            let e = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            let dir_span_opt = match &parser.token {
                Token::Ident(_, Keyword::ASC) => Some((true, parser.consume())),
                Token::Ident(_, Keyword::DESC) => Some((false, parser.consume())),
                _ => None,
            };
            let f = if let Some(nulls_span) = parser.skip_keyword(Keyword::NULLS) {
                match &parser.token {
                    Token::Ident(_, Keyword::FIRST) => {
                        let s = parser.consume().join_span(&nulls_span);
                        parser.postgres_only(&s);
                        match dir_span_opt {
                            Some((true, _)) => OrderFlag::AscNullsFirst(s),
                            Some((false, _)) => OrderFlag::DescNullsFirst(s),
                            None => OrderFlag::NullsFirst(s),
                        }
                    }
                    Token::Ident(_, Keyword::LAST) => {
                        let s = parser.consume().join_span(&nulls_span);
                        parser.postgres_only(&s);
                        match dir_span_opt {
                            Some((true, _)) => OrderFlag::AscNullsLast(s),
                            Some((false, _)) => OrderFlag::DescNullsLast(s),
                            None => OrderFlag::NullsLast(s),
                        }
                    }
                    _ => parser.expected_failure("FIRST or LAST")?,
                }
            } else {
                match dir_span_opt {
                    Some((true, s)) => OrderFlag::Asc(s),
                    Some((false, s)) => OrderFlag::Desc(s),
                    None => OrderFlag::None,
                }
            };
            order.push((e, f));
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Some((span, order))
    } else {
        None
    };

    let limit = if let Some(span) = parser.skip_keyword(Keyword::LIMIT) {
        let n = parse_expression_unreserved(parser, PRIORITY_MAX)?;
        match parser.token {
            Token::Comma => {
                parser.consume();
                Some((
                    span,
                    Some(n),
                    parse_expression_unreserved(parser, PRIORITY_MAX)?,
                ))
            }
            Token::Ident(_, Keyword::OFFSET) => {
                parser.consume();
                Some((
                    span,
                    Some(parse_expression_unreserved(parser, PRIORITY_MAX)?),
                    n,
                ))
            }
            _ => Some((span, None, n)),
        }
    } else {
        None
    };

    // PostgreSQL: standalone OFFSET n [ROWS] (only when no LIMIT already consumed offset)
    let offset = if limit.is_none() {
        if let Some(offset_span) = parser.skip_keyword(Keyword::OFFSET) {
            parser.postgres_only(&offset_span);
            let n = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            // skip optional ROWS or ROW
            if matches!(parser.token, Token::Ident(_, Keyword::ROWS | Keyword::ROW)) {
                parser.consume();
            }
            Some((offset_span, n))
        } else {
            None
        }
    } else {
        None
    };

    // PostgreSQL: FETCH {FIRST|NEXT} n {ROW|ROWS} ONLY
    let fetch = if let Some(fetch_span) = parser.skip_keyword(Keyword::FETCH) {
        parser.postgres_only(&fetch_span);
        match &parser.token {
            Token::Ident(_, Keyword::FIRST | Keyword::NEXT) => {
                parser.consume();
            }
            _ => parser.expected_failure("FIRST or NEXT")?,
        }
        let n = parse_expression_unreserved(parser, PRIORITY_MAX)?;
        if matches!(parser.token, Token::Ident(_, Keyword::ROWS | Keyword::ROW)) {
            parser.consume();
        }
        parser.consume_keyword(Keyword::ONLY)?;
        Some((fetch_span, n))
    } else {
        None
    };

    let locking = if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
        let strength = match &parser.token {
            Token::Ident(_, Keyword::UPDATE) => {
                LockStrength::Update(parser.consume_keyword(Keyword::UPDATE)?)
            }
            Token::Ident(_, Keyword::SHARE) => {
                LockStrength::Share(parser.consume_keyword(Keyword::SHARE)?)
            }
            Token::Ident(_, Keyword::NO) => {
                LockStrength::NoKeyUpdate(parser.consume_keywords(&[
                    Keyword::NO,
                    Keyword::KEY,
                    Keyword::UPDATE,
                ])?)
            }
            Token::Ident(_, Keyword::KEY) => {
                LockStrength::KeyShare(parser.consume_keywords(&[Keyword::KEY, Keyword::SHARE])?)
            }
            _ => parser.expected_failure("UPDATE, SHARE, NO KEY UPDATE or KEY SHARE here")?,
        };

        if let LockStrength::NoKeyUpdate(s) | LockStrength::KeyShare(s) = &strength
            && !parser.options.dialect.is_postgresql()
        {
            parser.err("Only support by PostgreSQL", s);
        }

        let of = if let Some(of_span) = parser.skip_keyword(Keyword::OF) {
            let mut table_references = Vec::new();
            loop {
                table_references.push(parser.consume_plain_identifier_unreserved()?);
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            Some((of_span, table_references))
        } else {
            None
        };

        let wait = match &parser.token {
            Token::Ident(_, Keyword::NOWAIT) => {
                LockWait::NoWait(parser.consume_keyword(Keyword::NOWAIT)?)
            }
            Token::Ident(_, Keyword::SKIP) => {
                LockWait::SkipLocket(parser.consume_keywords(&[Keyword::SKIP, Keyword::LOCKED])?)
            }
            _ => LockWait::Default,
        };
        Some(Locking {
            for_span,
            strength,
            of,
            wait,
        })
    } else {
        None
    };

    // TODO [into_option]
    // [into_option]

    // into_option: {
    // INTO OUTFILE 'file_name'
    //     [CHARACTER SET charset_name]
    //     export_options
    // | INTO DUMPFILE 'file_name'
    // | INTO var_name [, var_name] ...
    // }

    Ok(Select {
        select_span,
        flags,
        select_exprs,
        from_span,
        table_references,
        where_,
        group_by,
        having,
        window_span,
        order_by,
        limit,
        distinct_on,
        offset,
        fetch,
        locking,
    })
}

pub(crate) fn parse_select<'a>(parser: &mut Parser<'a, '_>) -> Result<Select<'a>, ParseError> {
    let select_span = parser.consume_keyword(Keyword::SELECT)?;
    parse_select_body(parser, select_span)
}
