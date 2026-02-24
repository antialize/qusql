//! PostgreSQL VALUES statement parsing and AST definitions.
//!
//! See https://www.postgresql.org/docs/current/sql-values.html
// Licensed under the Apache License, Version 2.0
// See LICENSE.TXT for details.

use crate::keywords::Keyword;
use crate::lexer::Token;
use crate::parser::{ParseError, Parser};
use crate::{Expression, OptSpanned, Span, Spanned};
use alloc::format;
use alloc::vec::Vec;

/// Direction for FETCH clause (FIRST or NEXT)
#[derive(Debug, Clone)]
pub enum FetchDirection {
    /// FETCH FIRST ...
    First(Span),
    /// FETCH NEXT ...
    Next(Span),
}

impl Spanned for FetchDirection {
    fn span(&self) -> Span {
        match self {
            FetchDirection::First(span) => span.clone(),
            FetchDirection::Next(span) => span.clone(),
        }
    }
}

/// Representation of a FETCH clause in VALUES
#[derive(Debug, Clone)]
pub struct Fetch<'a> {
    /// Span of the FETCH keyword
    pub fetch_span: Span,
    /// Direction (FIRST/NEXT) if present
    pub direction: Option<FetchDirection>,
    /// Row count expression if present
    pub count: Option<Expression<'a>>,
    /// Span of ROW/ROWS ONLY
    pub row_span: Span,
}

/// Compute the span for a FETCH clause
impl<'a> Spanned for Fetch<'a> {
    fn span(&self) -> Span {
        self.fetch_span
            .join_span(&self.direction)
            .join_span(&self.count)
            .join_span(&self.row_span)
    }
}

/// Parse a FETCH clause (assumes FETCH keyword already consumed)
fn parse_fetch<'a>(parser: &mut Parser<'a, '_>, fetch_span: Span) -> Result<Fetch<'a>, ParseError> {
    let direction = match &parser.token {
        Token::Ident(_, Keyword::FIRST) => Some(FetchDirection::First(parser.consume())),
        Token::Ident(_, Keyword::NEXT) => Some(FetchDirection::Next(parser.consume())),
        _ => None,
    };
    let count = if !matches!(
        &parser.token,
        Token::Ident(_, Keyword::ROW | Keyword::ROWS | Keyword::ONLY)
    ) {
        Some(crate::expression::parse_expression(parser, true)?)
    } else {
        None
    };
    // Capture the span of ROW/ROWS ONLY
    let row_span = match &parser.token {
        Token::Ident(_, Keyword::ROW | Keyword::ROWS) => Some(parser.consume()),
        _ => None,
    };
    let row_span = parser.consume_keyword(Keyword::ONLY)?.join_span(&row_span);
    Ok(Fetch {
        fetch_span,
        direction,
        count,
        row_span,
    })
}

/// Representation of a VALUES statement
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statement, Values, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "VALUES (1, 'one'), (2, 'two')";
/// let mut issues = Issues::new(sql);
/// let stmt = parse_statement(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let v: Values = match stmt {
///     Some(Statement::Values(v)) => v,
///     _ => panic!("We should get a VALUES statement")
/// };
///
/// println!("{:#?}", v.rows);
/// ```
#[derive(Debug, Clone)]
pub struct Values<'a> {
    pub values_span: Span,
    pub rows: Vec<Vec<Expression<'a>>>,
    pub order_by: Option<(Span, Vec<(Expression<'a>, crate::select::OrderFlag)>)>, // (ORDER BY span, list of (expr, OrderFlag))
    pub limit: Option<(Span, Expression<'a>)>,
    pub offset: Option<(Span, Expression<'a>)>,
    pub fetch: Option<Fetch<'a>>,
}

impl<'a> Spanned for Values<'a> {
    fn span(&self) -> Span {
        self.values_span
            .join_span(&self.rows)
            .join_span(&self.order_by)
            .join_span(&self.limit)
            .join_span(&self.offset)
            .join_span(&self.fetch)
    }
}

/// Parse a VALUES statement (PostgreSQL style)
pub(crate) fn parse_values<'a>(parser: &mut Parser<'a, '_>) -> Result<Values<'a>, ParseError> {
    let values_span = parser.consume_keyword(Keyword::VALUES)?;
    parser.postgres_only(&values_span);

    let mut rows = Vec::new();
    loop {
        parser.consume_token(Token::LParen)?;
        let mut row = Vec::new();
        parser.recovered(
            "')' or ','",
            &|t| matches!(t, Token::RParen | Token::Comma),
            |parser| {
                loop {
                    row.push(crate::expression::parse_expression(parser, true)?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                Ok(())
            },
        )?;
        parser.consume_token(Token::RParen)?;
        rows.push(row);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    // Ensure all rows have the same number of columns
    if let Some(first_row) = rows.first() {
        let cols = first_row.len();
        for row in rows.iter() {
            if row.len() != cols {
                parser
                    .err(
                        format!("This row has {} members", row.len()),
                        &row.opt_span().unwrap(),
                    )
                    .frag(
                        format!("Expected {} members", cols),
                        &first_row.opt_span().unwrap(),
                    );
            }
        }
    }

    // Parse optional ORDER BY
    let order_by = if let Some(order_span) = parser.skip_keyword(Keyword::ORDER) {
        let by_span = parser.consume_keyword(Keyword::BY)?;
        let span = order_span.join_span(&by_span);
        let mut items = Vec::new();
        loop {
            let expr = crate::expression::parse_expression(parser, false)?;
            let order_flag = match &parser.token {
                Token::Ident(_, Keyword::ASC) => crate::select::OrderFlag::Asc(parser.consume()),
                Token::Ident(_, Keyword::DESC) => crate::select::OrderFlag::Desc(parser.consume()),
                _ => crate::select::OrderFlag::None,
            };
            items.push((expr, order_flag));
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Some((span, items))
    } else {
        None
    };

    // Parse optional LIMIT
    let limit = if let Some(limit_span) = parser.skip_keyword(Keyword::LIMIT) {
        let expr = crate::expression::parse_expression(parser, true)?;
        Some((limit_span, expr))
    } else {
        None
    };

    // Parse optional OFFSET
    let offset = if let Some(offset_span) = parser.skip_keyword(Keyword::OFFSET) {
        let expr = crate::expression::parse_expression(parser, true)?;
        // Optionally consume ROW/ROWS
        if matches!(parser.token, Token::Ident(_, Keyword::ROW | Keyword::ROWS)) {
            parser.consume();
        }
        Some((offset_span, expr))
    } else {
        None
    };

    // Parse optional FETCH FIRST/NEXT ... ROW/ROWS ONLY
    let fetch = if let Some(fetch_span) = parser.skip_keyword(Keyword::FETCH) {
        Some(parse_fetch(parser, fetch_span)?)
    } else {
        None
    };

    Ok(Values {
        values_span,
        rows,
        order_by,
        limit,
        offset,
        fetch,
    })
}
