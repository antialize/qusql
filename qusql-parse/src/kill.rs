use crate::{
    Span, Spanned,
    expression::{Expression, parse_expression},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
};

#[derive(Clone, Debug)]
pub enum KillType {
    Connection(Span),
    Query(Span),
}

impl Spanned for KillType {
    fn span(&self) -> Span {
        match self {
            KillType::Connection(s) => s.clone(),
            KillType::Query(s) => s.clone(),
        }
    }
}

/// Represent a MySQL `KILL` statement
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "KILL CONNECTION 123;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// let kill_stmt = match stmts.pop() {
///     Some(Statement::Kill(k)) => k,
///     _ => panic!("We should get a kill statement"),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct Kill<'a> {
    pub kill_span: Span,
    pub kill_type: Option<KillType>,
    pub id: Expression<'a>,
}

impl<'a> Spanned for Kill<'a> {
    fn span(&self) -> Span {
        self.kill_span
            .join_span(&self.kill_type)
            .join_span(&self.id)
    }
}

pub(crate) fn parse_kill<'a>(parser: &mut Parser<'a, '_>) -> Result<Kill<'a>, ParseError> {
    let kill_span = parser.consume_keyword(Keyword::KILL)?;
    let kill_type = match &parser.token {
        Token::Ident(_, Keyword::CONNECTION) => Some(KillType::Connection(parser.consume())),
        Token::Ident(_, Keyword::QUERY) => Some(KillType::Query(parser.consume())),
        _ => None,
    };
    let id = parse_expression(parser, false)?;
    Ok(Kill {
        kill_span,
        kill_type,
        id,
    })
}
