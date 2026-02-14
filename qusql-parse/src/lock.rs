use alloc::vec::Vec;

use crate::{
    Identifier, QualifiedName, Span, Spanned,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

#[derive(Clone, Debug)]
pub enum LockType {
    Read,
    ReadLocal,
    Write,
}

#[derive(Clone, Debug)]
pub struct LockMember<'a> {
    pub table_name: QualifiedName<'a>,
    pub alias: Option<Identifier<'a>>,
    pub lock_type: LockType,
}

impl Spanned for LockMember<'_> {
    fn span(&self) -> Span {
        self.table_name.span().join_span(&self.alias)
    }
}

/// Represent a MySQL `LOCK` statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements,
///   Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "LOCK TABLES t1 AS a READ, t2 WRITE;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// let lock_stmt = match stmts.pop() {
///    Some(Statement::Lock(l)) => l,
///   _ => panic!("We should get a lock statement"),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct Lock<'a> {
    pub lock_span: Span,
    pub tables_span: Span,
    pub members: Vec<LockMember<'a>>,
}

impl Spanned for Lock<'_> {
    fn span(&self) -> Span {
        self.lock_span
            .join_span(&self.tables_span)
            .join_span(&self.members)
    }
}

pub(crate) fn parse_lock<'a>(parser: &mut Parser<'a, '_>) -> Result<Lock<'a>, ParseError> {
    let lock_span = parser.consume_keyword(Keyword::LOCK)?;

    let tables_span = match parser.token {
        Token::Ident(_, Keyword::TABLE) => parser.consume_keyword(Keyword::TABLE)?,
        Token::Ident(_, Keyword::TABLES) => parser.consume_keyword(Keyword::TABLES)?,
        _ => return parser.expected_failure("'TABLE' | 'TABLES'"),
    };

    let mut members = Vec::new();
    loop {
        let table_name = parse_qualified_name(parser)?;

        let alias = if parser.skip_keyword(Keyword::AS).is_some() {
            Some(parser.consume_plain_identifier()?)
        } else {
            None
        };

        let lock_type = match &parser.token {
            Token::Ident(_, Keyword::READ) => {
                parser.consume_keyword(Keyword::READ)?;
                if parser.skip_keyword(Keyword::LOCAL).is_some() {
                    LockType::ReadLocal
                } else {
                    LockType::Read
                }
            }
            Token::Ident(_, Keyword::WRITE) => {
                parser.consume_keyword(Keyword::WRITE)?;
                LockType::Write
            }
            _ => return parser.expected_failure("'READ' | 'WRITE'"),
        };

        members.push(LockMember {
            table_name,
            alias,
            lock_type,
        });

        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    Ok(Lock {
        lock_span,
        tables_span,
        members,
    })
}

/// Represent a MySQL `UNLOCK` statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements,
///  Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "UNLOCK TABLES;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// let unlock_stmt = match stmts.pop() {
///    Some(Statement::Unlock(u)) => u,
///  _ => panic!("We should get an unlock statement"),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct Unlock {
    pub unlock_span: Span,
    pub tables_span: Span,
}

impl Spanned for Unlock {
    fn span(&self) -> Span {
        self.unlock_span.join_span(&self.tables_span)
    }
}

pub(crate) fn parse_unlock<'a>(parser: &mut Parser<'a, '_>) -> Result<Unlock, ParseError> {
    let unlock_span = parser.consume_keyword(Keyword::UNLOCK)?;

    let tables_span = match parser.token {
        Token::Ident(_, Keyword::TABLE) => parser.consume_keyword(Keyword::TABLE)?,
        Token::Ident(_, Keyword::TABLES) => parser.consume_keyword(Keyword::TABLES)?,
        _ => return parser.expected_failure("'TABLE' | 'TABLES'"),
    };

    Ok(Unlock {
        unlock_span,
        tables_span,
    })
}
