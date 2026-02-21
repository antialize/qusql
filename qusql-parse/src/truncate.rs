use crate::{
    QualifiedName, Span, Spanned,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

use alloc::vec::Vec;

/// Represent a truncate table statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, TruncateTable, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "TRUNCATE TABLE `t1`;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok(), "Issues: {}", issues);
/// #
/// let truncate_table: TruncateTable = match stmts.pop() {
///     Some(Statement::TruncateTable(c)) => c,
///     _ => panic!("We should get a truncate table statement")
/// };
///
/// assert!(truncate_table.tables.len() == 1);
/// assert!(truncate_table.tables[0].table_name.identifier.as_str() == "t1");
///
/// ```
/// A table specification in a TRUNCATE statement
#[derive(Debug, Clone)]
pub struct TruncateTableSpec<'a> {
    /// Span of "ONLY" if specified (PostgreSQL)
    pub only_span: Option<Span>,
    /// Name of the table to truncate
    pub table_name: QualifiedName<'a>,
    /// Span of "*" if specified (PostgreSQL - include descendants)
    pub descendants_span: Option<Span>,
}

impl<'a> Spanned for TruncateTableSpec<'a> {
    fn span(&self) -> Span {
        self.table_name
            .span()
            .join_span(&self.only_span)
            .join_span(&self.descendants_span)
    }
}

/// Identity restart option for TRUNCATE (PostgreSQL)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityOption {
    /// RESTART IDENTITY
    Restart(Span),
    /// CONTINUE IDENTITY
    Continue(Span),
}

impl Spanned for IdentityOption {
    fn span(&self) -> Span {
        match self {
            IdentityOption::Restart(span) => span.clone(),
            IdentityOption::Continue(span) => span.clone(),
        }
    }
}

/// Cascade option for TRUNCATE (PostgreSQL)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CascadeOption {
    /// CASCADE
    Cascade(Span),
    /// RESTRICT
    Restrict(Span),
}

impl Spanned for CascadeOption {
    fn span(&self) -> Span {
        match self {
            CascadeOption::Cascade(span) => span.clone(),
            CascadeOption::Restrict(span) => span.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TruncateTable<'a> {
    /// Span of "TRUNCATE"
    pub truncate_span: Span,
    /// Span of "TABLE" if specified
    pub table_span: Option<Span>,
    /// List of tables to truncate
    pub tables: Vec<TruncateTableSpec<'a>>,
    /// Identity restart option (PostgreSQL)
    pub identity_option: Option<IdentityOption>,
    /// Cascade option (PostgreSQL)
    pub cascade_option: Option<CascadeOption>,
}

impl<'a> Spanned for TruncateTable<'a> {
    fn span(&self) -> Span {
        self.truncate_span
            .join_span(&self.table_span)
            .join_span(&self.tables)
            .join_span(&self.identity_option)
            .join_span(&self.cascade_option)
    }
}

pub(crate) fn parse_truncate_table<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<TruncateTable<'a>, ParseError> {
    let truncate_span = parser.consume_keyword(Keyword::TRUNCATE)?;
    let table_span = parser.skip_keyword(Keyword::TABLE);

    let mut tables = Vec::new();

    // Parse comma-separated list of tables
    loop {
        let only_span = parser.skip_keyword(Keyword::ONLY);
        parser.postgres_only(&only_span);

        let table_name = parse_qualified_name(parser)?;

        let descendants_span = parser.skip_token(Token::Mul);
        parser.postgres_only(&descendants_span);

        tables.push(TruncateTableSpec {
            only_span,
            table_name,
            descendants_span,
        });

        let comma_span = parser.skip_token(Token::Comma);
        if comma_span.is_none() {
            break;
        }
        // Multiple tables in TRUNCATE is PostgreSQL-only
        parser.postgres_only(&comma_span);
    }

    // Parse RESTART IDENTITY or CONTINUE IDENTITY (PostgreSQL)
    let identity_option = if let Some(restart_span) = parser.skip_keyword(Keyword::RESTART) {
        let identity_span = parser.consume_keyword(Keyword::IDENTITY)?;
        let full_span = restart_span.join_span(&identity_span);
        parser.postgres_only(&full_span);
        Some(IdentityOption::Restart(full_span))
    } else if let Some(continue_span) = parser.skip_keyword(Keyword::CONTINUE) {
        let identity_span = parser.consume_keyword(Keyword::IDENTITY)?;
        let full_span = continue_span.join_span(&identity_span);
        parser.postgres_only(&full_span);
        Some(IdentityOption::Continue(full_span))
    } else {
        None
    };

    // Parse CASCADE or RESTRICT (PostgreSQL)
    let cascade_option = if let Some(cascade_span) = parser.skip_keyword(Keyword::CASCADE) {
        parser.postgres_only(&cascade_span);
        Some(CascadeOption::Cascade(cascade_span))
    } else if let Some(restrict_span) = parser.skip_keyword(Keyword::RESTRICT) {
        parser.postgres_only(&restrict_span);
        Some(CascadeOption::Restrict(restrict_span))
    } else {
        None
    };

    Ok(TruncateTable {
        truncate_span,
        table_span,
        tables,
        identity_option,
        cascade_option,
    })
}
