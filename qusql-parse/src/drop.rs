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

use alloc::vec::Vec;

use crate::{
    Identifier, QualifiedName, Span, Spanned, Statement,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

/// Represent a drop table statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropTable, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP TABLE `Employees`, `Customers`;";
/// let mut issues = Issues::new(sql);
///
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let delete: DropTable = match stmts.pop() {
///     Some(Statement::DropTable(d)) => d,
///     _ => panic!("We should get a drop table statement")
/// };
///
/// assert!(delete.tables.get(0).unwrap().identifier.as_str() == "Employees");
/// ```
#[derive(Debug, Clone)]
pub struct DropTable<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "TEMPORARY" if specified
    pub temporary: Option<Span>,
    /// Span of "TABLE"
    pub table_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// List of tables to drops
    pub tables: Vec<QualifiedName<'a>>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
}

impl<'a> Spanned for DropTable<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.temporary)
            .join_span(&self.table_span)
            .join_span(&self.if_exists)
            .join_span(&self.tables)
    }
}

fn parse_drop_table<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
    temporary: Option<Span>,
) -> Result<Statement<'a>, ParseError> {
    let table_span = parser.consume_keyword(Keyword::TABLE)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let mut tables = Vec::new();
    loop {
        tables.push(parse_qualified_name(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    let cascade = if parser.options.dialect.is_postgresql() {
        parser.skip_keyword(Keyword::CASCADE)
    } else {
        None
    };
    Ok(Statement::DropTable(DropTable {
        drop_span,
        temporary,
        table_span,
        if_exists,
        tables,
        cascade,
    }))
}

/// Represent a drop view statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropView, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP VIEW `Employees`, `Customers`;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let delete: DropView = match stmts.pop() {
///     Some(Statement::DropView(d)) => d,
///     _ => panic!("We should get a drop table statement")
/// };
///
/// assert!(delete.views.get(0).unwrap().identifier.as_str() == "Employees");
/// ```
#[derive(Debug, Clone)]
pub struct DropView<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "TEMPORARY" if specified
    pub temporary: Option<Span>,
    /// Span of "VIEW"
    pub view_span: Span,
    /// Span of "IF EXISTS"
    pub if_exists: Option<Span>,
    /// List of views to drop
    pub views: Vec<QualifiedName<'a>>,
}

impl<'a> Spanned for DropView<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.temporary)
            .join_span(&self.view_span)
            .join_span(&self.if_exists)
            .join_span(&self.views)
    }
}

fn parse_drop_view<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
    temporary: Option<Span>,
) -> Result<Statement<'a>, ParseError> {
    let view_span = parser.consume_keyword(Keyword::VIEW)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let mut views = Vec::new();
    loop {
        views.push(parse_qualified_name(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    // TODO  [RESTRICT | CASCADE]
    Ok(Statement::DropView(DropView {
        drop_span,
        temporary,
        view_span,
        if_exists,
        views,
    }))
}

/// Represent a drop database statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropDatabase, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP DATABASE mydb;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// #
/// let s: DropDatabase = match stmts.pop() {
///     Some(Statement::DropDatabase(s)) => s,
///     _ => panic!("We should get a drop database statement")
/// };
///
/// assert!(s.database.as_str() == "mydb");
/// ```
#[derive(Debug, Clone)]
pub struct DropDatabase<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "DATABASE"
    pub database_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Name of database to drop
    pub database: Identifier<'a>,
}

impl<'a> Spanned for DropDatabase<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.database_span)
            .join_span(&self.if_exists)
            .join_span(&self.database)
    }
}

fn parse_drop_database<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
    kw: Keyword,
) -> Result<Statement<'a>, ParseError> {
    // TODO complain about temporary
    let database_span = parser.consume_keyword(kw)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let database = parser.consume_plain_identifier()?;
    Ok(Statement::DropDatabase(DropDatabase {
        drop_span,
        database_span,
        if_exists,
        database,
    }))
}

/// Represent a drop event statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropEvent, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP EVENT myevent;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: DropEvent = match stmts.pop() {
///     Some(Statement::DropEvent(s)) => s,
///     _ => panic!("We should get a drop event statement")
/// };
///
/// assert!(s.event.identifier.as_str() == "myevent");
/// ```
#[derive(Debug, Clone)]
pub struct DropEvent<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "EVENT"
    pub event_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Event to drop
    pub event: QualifiedName<'a>,
}

impl<'a> Spanned for DropEvent<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.event_span)
            .join_span(&self.if_exists)
            .join_span(&self.event)
    }
}

fn parse_drop_event<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    // TODO complain about temporary
    let event_span = parser.consume_keyword(Keyword::EVENT)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let event = parse_qualified_name(parser)?;
    Ok(Statement::DropEvent(DropEvent {
        drop_span,
        event_span,
        if_exists,
        event,
    }))
}

/// Represent a drop function statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropFunction, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP FUNCTION myfunc;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: DropFunction = match stmts.pop() {
///     Some(Statement::DropFunction(s)) => s,
///     _ => panic!("We should get a drop function statement")
/// };
///
/// assert!(s.function.identifier.as_str() == "myfunc");
/// ```
#[derive(Debug, Clone)]
pub struct DropFunction<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "FUNCTION"
    pub function_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Function to drop
    pub function: QualifiedName<'a>,
}

impl<'a> Spanned for DropFunction<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.function_span)
            .join_span(&self.if_exists)
            .join_span(&self.function)
    }
}

fn parse_drop_function<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    // TODO complain about temporary
    let function_span = parser.consume_keyword(Keyword::FUNCTION)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let function = parse_qualified_name(parser)?;
    Ok(Statement::DropFunction(DropFunction {
        drop_span,
        function_span,
        if_exists,
        function,
    }))
}

/// Represent a drop procedure statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropProcedure, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP PROCEDURE myproc;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: DropProcedure = match stmts.pop() {
///     Some(Statement::DropProcedure(s)) => s,
///     _ => panic!("We should get a drop procedure statement")
/// };
///
/// assert!(s.procedure.identifier.as_str() == "myproc");
/// ```
#[derive(Debug, Clone)]
pub struct DropProcedure<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "PROCEDURE"
    pub procedure_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Procedure to drop
    pub procedure: QualifiedName<'a>,
}

impl<'a> Spanned for DropProcedure<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.procedure_span)
            .join_span(&self.if_exists)
            .join_span(&self.procedure)
    }
}

fn parse_drop_procedure<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    // TODO complain about temporary
    let procedure_span = parser.consume_keyword(Keyword::PROCEDURE)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let procedure = parse_qualified_name(parser)?;
    Ok(Statement::DropProcedure(DropProcedure {
        drop_span,
        procedure_span,
        if_exists,
        procedure,
    }))
}

/// Represent a drop sequence statement (PostgreSQL)
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropSequence, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "DROP SEQUENCE myseq CASCADE;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: DropSequence = match stmts.pop() {
///     Some(Statement::DropSequence(s)) => s,
///     _ => panic!("We should get a drop sequence statement")
/// };
///
/// assert!(s.sequences.get(0).unwrap().identifier.as_str() == "myseq");
/// ```
#[derive(Debug, Clone)]
pub struct DropSequence<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "SEQUENCE"
    pub sequence_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// List of sequences to drop
    pub sequences: Vec<QualifiedName<'a>>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
    /// Span of "RESTRICT" if specified
    pub restrict: Option<Span>,
}

impl<'a> Spanned for DropSequence<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.sequence_span)
            .join_span(&self.if_exists)
            .join_span(&self.sequences)
            .join_span(&self.cascade)
            .join_span(&self.restrict)
    }
}

fn parse_drop_sequence<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    // DROP SEQUENCE [IF EXISTS] sequence_name [, sequence_name] ... [CASCADE | RESTRICT]
    let sequence_span = parser.consume_keyword(Keyword::SEQUENCE)?;
    parser.postgres_only(&sequence_span);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let mut sequences = Vec::new();
    loop {
        sequences.push(parse_qualified_name(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    let cascade = parser.skip_keyword(Keyword::CASCADE);
    let restrict = if cascade.is_none() {
        parser.skip_keyword(Keyword::RESTRICT)
    } else {
        None
    };
    Ok(Statement::DropSequence(DropSequence {
        drop_span,
        sequence_span,
        if_exists,
        sequences,
        cascade,
        restrict,
    }))
}

/// Represent a drop server statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropServer, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP SERVER myserver;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// #
/// let s: DropServer = match stmts.pop() {
///     Some(Statement::DropServer(s)) => s,
///     _ => panic!("We should get a drop server statement")
/// };
///
/// assert!(s.server.as_str() == "myserver");
/// ```
#[derive(Debug, Clone)]
pub struct DropServer<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "SERVER"
    pub server_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Server to drop
    pub server: Identifier<'a>,
}

impl<'a> Spanned for DropServer<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.server_span)
            .join_span(&self.if_exists)
            .join_span(&self.server)
    }
}

fn parse_drop_server<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    // TODO complain about temporary
    let server_span = parser.consume_keyword(Keyword::SERVER)?;
    parser.postgres_only(&server_span);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let server = parser.consume_plain_identifier()?;
    Ok(Statement::DropServer(DropServer {
        drop_span,
        server_span,
        if_exists,
        server,
    }))
}

/// Represent a drop trigger statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropTrigger, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP TRIGGER IF EXISTS `foo`.`mytrigger`;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: DropTrigger = match stmts.pop() {
///     Some(Statement::DropTrigger(s)) => s,
///     _ => panic!("We should get a drop trigger statement")
/// };
///
/// assert!(s.identifier.identifier.as_str() == "mytrigger");
/// ```
#[derive(Debug, Clone)]
pub struct DropTrigger<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "TRIGGER"
    pub trigger_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Trigger to drop
    pub identifier: QualifiedName<'a>,
}

impl<'a> Spanned for DropTrigger<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.trigger_span)
            .join_span(&self.if_exists)
            .join_span(&self.identifier)
    }
}

fn parse_drop_trigger<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    let trigger_span = parser.consume_keyword(Keyword::TRIGGER)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let identifier = parse_qualified_name(parser)?;
    Ok(Statement::DropTrigger(DropTrigger {
        drop_span,
        trigger_span,
        if_exists,
        identifier,
    }))
}

/// Represent a drop index statement.
///
/// MariaDB example
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropIndex, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP INDEX IF EXISTS `myindex` ON `bar`;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: DropIndex = match stmts.pop() {
///     Some(Statement::DropIndex(s)) => s,
///     _ => panic!("We should get a drop trigger statement")
/// };
///
/// assert!(s.index_name.as_str() == "myindex");
/// ```
///
/// PostgreSQL example
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropIndex, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "DROP INDEX IF EXISTS \"myindex\";";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok(), "{}", issues);
/// #
/// let s: DropIndex = match stmts.pop() {
///     Some(Statement::DropIndex(s)) => s,
///     _ => panic!("We should get a drop trigger statement")
/// };
///
/// assert!(s.index_name.as_str() == "myindex");
/// ```
#[derive(Debug, Clone)]
pub struct DropIndex<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "INDEX"
    pub index_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    pub index_name: Identifier<'a>,
    pub on: Option<(Span, QualifiedName<'a>)>,
}

impl<'a> Spanned for DropIndex<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.index_span)
            .join_span(&self.if_exists)
            .join_span(&self.index_name)
            .join_span(&self.on)
    }
}

fn parse_drop_index<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<Statement<'a>, ParseError> {
    // DROP INDEX [IF EXISTS] index_name ON tbl_name
    let index_span = parser.consume_keyword(Keyword::INDEX)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let index_name = parser.consume_plain_identifier()?;
    let on = if let Some(span) = parser.skip_keyword(Keyword::ON) {
        let table_name = parse_qualified_name(parser)?;
        Some((span, table_name))
    } else {
        None
    };

    let v = DropIndex {
        drop_span,
        index_span,
        if_exists,
        index_name,
        on,
    };

    if v.on.is_none() && parser.options.dialect.is_maria() {
        parser.err("On required for index drops in MariaDb", &v);
    }
    if v.on.is_some() && parser.options.dialect.is_postgresql() {
        parser.err("On not supported for index drops in PostgreSQL", &v);
    }
    Ok(Statement::DropIndex(v))
}

pub(crate) fn parse_drop<'a>(parser: &mut Parser<'a, '_>) -> Result<Statement<'a>, ParseError> {
    let drop_span = parser.consume_keyword(Keyword::DROP)?;
    let temporary = parser.skip_keyword(Keyword::TEMPORARY);
    match &parser.token {
        Token::Ident(_, Keyword::TABLE) => parse_drop_table(parser, drop_span, temporary),
        Token::Ident(_, Keyword::VIEW) => parse_drop_view(parser, drop_span, temporary),
        Token::Ident(_, kw @ Keyword::DATABASE | kw @ Keyword::SCHEMA) => {
            parse_drop_database(parser, drop_span, *kw)
        }
        Token::Ident(_, Keyword::EVENT) => parse_drop_event(parser, drop_span),
        Token::Ident(_, Keyword::FUNCTION) => parse_drop_function(parser, drop_span),
        Token::Ident(_, Keyword::INDEX) => parse_drop_index(parser, drop_span),
        Token::Ident(_, Keyword::PROCEDURE) => parse_drop_procedure(parser, drop_span),
        Token::Ident(_, Keyword::SEQUENCE) => parse_drop_sequence(parser, drop_span),
        Token::Ident(_, Keyword::SERVER) => parse_drop_server(parser, drop_span),
        Token::Ident(_, Keyword::TRIGGER) => parse_drop_trigger(parser, drop_span),
        Token::Ident(_, Keyword::USER) => {
            // DROP USER [IF EXISTS] user_name [, user_name] ..
            parser.todo(file!(), line!())
        }
        _ => parser.expected_failure("droppable"),
    }
}
