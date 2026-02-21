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

use alloc::{boxed::Box, vec::Vec};

use crate::{
    Identifier, QualifiedName, Span, Spanned, Statement,
    data_type::DataType,
    keywords::Keyword,
    lexer::Token,
    operator::parse_operator_name,
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
    /// List of tables to drop
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
) -> Result<DropTable<'a>, ParseError> {
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
    Ok(DropTable {
        drop_span,
        temporary,
        table_span,
        if_exists,
        tables,
        cascade,
    })
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
) -> Result<DropView<'a>, ParseError> {
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
    Ok(DropView {
        drop_span,
        temporary,
        view_span,
        if_exists,
        views,
    })
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
) -> Result<DropDatabase<'a>, ParseError> {
    // TODO complain about temporary
    let database_span = parser.consume_keyword(kw)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let database = parser.consume_plain_identifier()?;
    Ok(DropDatabase {
        drop_span,
        database_span,
        if_exists,
        database,
    })
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
) -> Result<DropEvent<'a>, ParseError> {
    // TODO complain about temporary
    let event_span = parser.consume_keyword(Keyword::EVENT)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let event = parse_qualified_name(parser)?;
    Ok(DropEvent {
        drop_span,
        event_span,
        if_exists,
        event,
    })
}

#[derive(Debug, Clone)]
pub enum DropFunctionArgMode {
    In(Span),
    Out(Span),
    InOut(Span),
}

impl Spanned for DropFunctionArgMode {
    fn span(&self) -> Span {
        match self {
            DropFunctionArgMode::In(s) => s.clone(),
            DropFunctionArgMode::Out(s) => s.clone(),
            DropFunctionArgMode::InOut(s) => s.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DropFunctionArg<'a> {
    pub mode: Option<DropFunctionArgMode>,
    pub name: Option<Identifier<'a>>,
    pub data_type: DataType<'a>,
    pub default: Option<Span>, // = value (span only, for now)
}

impl<'a> Spanned for DropFunctionArg<'a> {
    fn span(&self) -> Span {
        self.data_type
            .span()
            .join_span(&self.mode)
            .join_span(&self.name)
            .join_span(&self.default)
    }
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
    /// List of functions to drop (PostgreSQL: can be multiple)
    pub functions: Vec<(QualifiedName<'a>, Option<Vec<DropFunctionArg<'a>>>)>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
    /// Span of "RESTRICT" if specified
    pub restrict: Option<Span>,
}

impl<'a> Spanned for DropFunction<'a> {
    fn span(&self) -> Span {
        let mut span = self
            .drop_span
            .join_span(&self.function_span)
            .join_span(&self.if_exists)
            .join_span(&self.cascade)
            .join_span(&self.restrict);
        for (name, args) in &self.functions {
            span = span.join_span(name);
            if let Some(args) = args {
                for arg in args {
                    span = span.join_span(arg);
                }
            }
        }
        span
    }
}

fn parse_drop_function<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropFunction<'a>, ParseError> {
    let function_span = parser.consume_keyword(Keyword::FUNCTION)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let mut functions = Vec::new();
    loop {
        let name = parse_qualified_name(parser)?;
        let args = if parser.token == Token::LParen {
            let lparen = parser.consume_token(Token::LParen)?;
            let mut arg_list = Vec::new();
            parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                loop {
                    // Parse mode (IN, OUT, INOUT)
                    let mode = match &parser.token {
                        Token::Ident(_, Keyword::IN) => Some(DropFunctionArgMode::In(
                            parser.consume_keyword(Keyword::IN)?,
                        )),
                        Token::Ident(_, Keyword::OUT) => Some(DropFunctionArgMode::Out(
                            parser.consume_keyword(Keyword::OUT)?,
                        )),
                        Token::Ident(_, Keyword::INOUT) => Some(DropFunctionArgMode::InOut(
                            parser.consume_keyword(Keyword::INOUT)?,
                        )),
                        _ => None,
                    };
                    // Parse parameter name (optional)
                    let name = match &parser.token {
                        Token::Ident(_, kw) if !kw.reserved() => {
                            Some(parser.consume_plain_identifier()?)
                        }
                        _ => None,
                    };
                    // Parse data type
                    let data_type = crate::data_type::parse_data_type(parser, false)?;
                    // Parse default value (optional)
                    let default = if parser.skip_token(Token::Eq).is_some() {
                        // Just record the span for now
                        Some(parser.consume().clone())
                    } else {
                        None
                    };
                    arg_list.push(DropFunctionArg {
                        mode,
                        name,
                        data_type,
                        default,
                    });
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                Ok(())
            })?;
            parser.consume_token(Token::RParen)?;
            parser.postgres_only(&lparen.join_span(&arg_list));
            Some(arg_list)
        } else {
            None
        };
        functions.push((name, args));
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    if let [(first, _), (second, _), ..] = functions.as_slice()
        && !parser.options.dialect.is_postgresql()
    {
        parser
            .err("Multiple function only supported by ", second)
            .frag("First function supplied here", first);
    }

    let cascade = parser.skip_keyword(Keyword::CASCADE);
    parser.postgres_only(&cascade);
    let restrict = parser.skip_keyword(Keyword::RESTRICT);
    parser.postgres_only(&restrict);
    if let Some(cascade_span) = &cascade
        && let Some(restrict_span) = &restrict
    {
        parser
            .err("Cannot specify both CASCADE and RESTRICT", cascade_span)
            .frag("RESTRICT", restrict_span);
    }
    Ok(DropFunction {
        drop_span,
        function_span,
        if_exists,
        functions,
        cascade,
        restrict,
    })
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
) -> Result<DropProcedure<'a>, ParseError> {
    // TODO complain about temporary
    let procedure_span = parser.consume_keyword(Keyword::PROCEDURE)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let procedure = parse_qualified_name(parser)?;
    Ok(DropProcedure {
        drop_span,
        procedure_span,
        if_exists,
        procedure,
    })
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
) -> Result<DropSequence<'a>, ParseError> {
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
    Ok(DropSequence {
        drop_span,
        sequence_span,
        if_exists,
        sequences,
        cascade,
        restrict,
    })
}

/// Represent a drop server statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropServer, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
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
) -> Result<DropServer<'a>, ParseError> {
    // TODO complain about temporary
    let server_span = parser.consume_keyword(Keyword::SERVER)?;
    parser.postgres_only(&server_span);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let server = parser.consume_plain_identifier()?;
    Ok(DropServer {
        drop_span,
        server_span,
        if_exists,
        server,
    })
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
) -> Result<DropTrigger<'a>, ParseError> {
    let trigger_span = parser.consume_keyword(Keyword::TRIGGER)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let identifier = parse_qualified_name(parser)?;
    Ok(DropTrigger {
        drop_span,
        trigger_span,
        if_exists,
        identifier,
    })
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
) -> Result<DropIndex<'a>, ParseError> {
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

    if on.is_none() && parser.options.dialect.is_maria() {
        parser.err("On required for index drops in MariaDb", &drop_span);
    }
    if let Some((on_span, _)) = &on
        && parser.options.dialect.is_postgresql()
    {
        parser.err("On not supported for index drops in PostgreSQL", on_span);
    }

    Ok(DropIndex {
        drop_span,
        index_span,
        if_exists,
        index_name,
        on,
    })
}

/// Represent a drop domain statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropDomain, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "DROP DOMAIN IF EXISTS mydomain, otherdomain;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let drop: DropDomain = match stmts.pop() {
///     Some(Statement::DropDomain(d)) => d,
///     _ => panic!("We should get a drop domain statement")
/// };
/// assert!(drop.domains.get(0).unwrap().identifier.as_str() == "mydomain");
/// ```
#[derive(Debug, Clone)]
pub struct DropDomain<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "DOMAIN"
    pub domain_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// List of domains to drop
    pub domains: Vec<QualifiedName<'a>>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
    /// Span of "RESTRICT" if specified
    pub restrict: Option<Span>,
}

impl<'a> Spanned for DropDomain<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.domain_span)
            .join_span(&self.if_exists)
            .join_span(&self.domains)
            .join_span(&self.cascade)
            .join_span(&self.restrict)
    }
}

fn parse_drop_domain<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropDomain<'a>, ParseError> {
    let domain_span = parser.consume_keyword(Keyword::DOMAIN)?;
    parser.postgres_only(&domain_span);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let mut domains = Vec::new();
    loop {
        domains.push(parse_qualified_name(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    let cascade = parser.skip_keyword(Keyword::CASCADE);
    let restrict = parser.skip_keyword(Keyword::RESTRICT);
    if let Some(cascade_span) = &cascade
        && let Some(restrict_span) = &restrict
    {
        parser
            .err("Cannot specify both CASCADE and RESTRICT", cascade_span)
            .frag("RESTRICT", restrict_span);
    }
    Ok(DropDomain {
        drop_span,
        domain_span,
        if_exists,
        domains,
        cascade,
        restrict,
    })
}

/// Represent a drop extension statement (PostgreSQL)
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropExtension, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "DROP EXTENSION IF EXISTS myext, otherext CASCADE;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok());
/// let drop: DropExtension = match stmts.pop() {
///     Some(Statement::DropExtension(d)) => d,
///     _ => panic!("We should get a drop extension statement")
/// };
/// assert!(drop.extensions.get(0).unwrap().as_str() == "myext");
/// ```
#[derive(Debug, Clone)]
pub struct DropExtension<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "EXTENSION"
    pub extension_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// List of extensions to drop
    pub extensions: Vec<Identifier<'a>>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
    /// Span of "RESTRICT" if specified
    pub restrict: Option<Span>,
}

impl<'a> Spanned for DropExtension<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.extension_span)
            .join_span(&self.if_exists)
            .join_span(&self.extensions)
            .join_span(&self.cascade)
            .join_span(&self.restrict)
    }
}

fn parse_drop_extension<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropExtension<'a>, ParseError> {
    let extension_span = parser.consume_keyword(Keyword::EXTENSION)?;
    parser.postgres_only(&extension_span);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let mut extensions = Vec::new();
    loop {
        extensions.push(parser.consume_plain_identifier()?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    let cascade = parser.skip_keyword(Keyword::CASCADE);
    let restrict = parser.skip_keyword(Keyword::RESTRICT);
    if let Some(cascade_span) = &cascade
        && let Some(restrict_span) = &restrict
    {
        parser
            .err("Cannot specify both CASCADE and RESTRICT", cascade_span)
            .frag("RESTRICT", restrict_span);
    }

    Ok(DropExtension {
        drop_span,
        extension_span,
        if_exists,
        extensions,
        cascade,
        restrict,
    })
}

/// Represent a drop operator statement (PostgreSQL)
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, DropOperator, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "DROP OPERATOR IF EXISTS +(integer, integer) CASCADE;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok());
/// let drop: DropOperator = match stmts.pop() {
///     Some(Statement::DropOperator(d)) => d,
///     _ => panic!("We should get a drop operator statement")
/// };
/// assert!(drop.name.as_str() == "+");
/// ```

#[derive(Debug, Clone)]
pub struct DropOperatorItem<'a> {
    pub name: QualifiedName<'a>,
    pub left_type: Option<DataType<'a>>,
    pub right_type: Option<DataType<'a>>,
}

impl<'a> Spanned for DropOperatorItem<'a> {
    fn span(&self) -> Span {
        self.name
            .span()
            .join_span(&self.left_type)
            .join_span(&self.right_type)
    }
}

#[derive(Debug, Clone)]
pub struct DropOperator<'a> {
    /// Span of "DROP"
    pub drop_span: Span,
    /// Span of "OPERATOR"
    pub operator_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// List of operators to drop
    pub operators: Vec<DropOperatorItem<'a>>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
    /// Span of "RESTRICT" if specified
    pub restrict: Option<Span>,
}

impl<'a> Spanned for DropOperator<'a> {
    fn span(&self) -> Span {
        self.drop_span
            .join_span(&self.operator_span)
            .join_span(&self.if_exists)
            .join_span(&self.cascade)
            .join_span(&self.restrict)
            .join_span(&self.operators)
    }
}

fn parse_drop_operator<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropOperator<'a>, ParseError> {
    let operator_span = parser.consume_keyword(Keyword::OPERATOR)?;
    parser.postgres_only(&operator_span);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    // Parse comma-separated list of operators
    let mut operators = Vec::new();
    loop {
        if parser.token == Token::LParen {
            return Err(parser.expected_failure("operator name")?);
        }
        let name = parse_operator_name(parser)?;
        let (left_type, right_type) = if parser.token == Token::LParen {
            parser.consume_token(Token::LParen)?;
            let mut left = None;
            let mut right = None;
            parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                if parser.token != Token::Comma && parser.token != Token::RParen {
                    left = Some(crate::data_type::parse_data_type(parser, false)?);
                }
                if parser.skip_token(Token::Comma).is_some() && parser.token != Token::RParen {
                    right = Some(crate::data_type::parse_data_type(parser, false)?);
                }
                Ok(())
            })?;
            parser.consume_token(Token::RParen)?;
            (left, right)
        } else {
            (None, None)
        };
        operators.push(DropOperatorItem {
            name,
            left_type,
            right_type,
        });
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    let cascade = parser.skip_keyword(Keyword::CASCADE);
    let restrict = parser.skip_keyword(Keyword::RESTRICT);
    if let Some(cascade_span) = &cascade
        && let Some(restrict_span) = &restrict
    {
        parser
            .err("Cannot specify both CASCADE and RESTRICT", cascade_span)
            .frag("RESTRICT", restrict_span);
    }
    Ok(DropOperator {
        drop_span,
        operator_span,
        if_exists,
        operators,
        cascade,
        restrict,
    })
}

pub(crate) fn parse_drop<'a>(parser: &mut Parser<'a, '_>) -> Result<Statement<'a>, ParseError> {
    let drop_span = parser.consume_keyword(Keyword::DROP)?;
    let temporary = parser.skip_keyword(Keyword::TEMPORARY);
    match &parser.token {
        Token::Ident(_, Keyword::TABLE) => Ok(Statement::DropTable(Box::new(parse_drop_table(
            parser, drop_span, temporary,
        )?))),
        Token::Ident(_, Keyword::VIEW) => Ok(Statement::DropView(Box::new(parse_drop_view(
            parser, drop_span, temporary,
        )?))),
        Token::Ident(_, kw @ Keyword::DATABASE | kw @ Keyword::SCHEMA) => Ok(
            Statement::DropDatabase(Box::new(parse_drop_database(parser, drop_span, *kw)?)),
        ),
        Token::Ident(_, Keyword::DOMAIN) => Ok(Statement::DropDomain(Box::new(parse_drop_domain(
            parser, drop_span,
        )?))),
        Token::Ident(_, Keyword::EXTENSION) => Ok(Statement::DropExtension(Box::new(
            parse_drop_extension(parser, drop_span)?,
        ))),
        Token::Ident(_, Keyword::EVENT) => Ok(Statement::DropEvent(Box::new(parse_drop_event(
            parser, drop_span,
        )?))),
        Token::Ident(_, Keyword::FUNCTION) => Ok(Statement::DropFunction(Box::new(
            parse_drop_function(parser, drop_span)?,
        ))),
        Token::Ident(_, Keyword::OPERATOR) => Ok(Statement::DropOperator(Box::new(
            parse_drop_operator(parser, drop_span)?,
        ))),
        Token::Ident(_, Keyword::INDEX) => Ok(Statement::DropIndex(Box::new(parse_drop_index(
            parser, drop_span,
        )?))),
        Token::Ident(_, Keyword::PROCEDURE) => Ok(Statement::DropProcedure(Box::new(
            parse_drop_procedure(parser, drop_span)?,
        ))),
        Token::Ident(_, Keyword::SEQUENCE) => Ok(Statement::DropSequence(Box::new(
            parse_drop_sequence(parser, drop_span)?,
        ))),
        Token::Ident(_, Keyword::SERVER) => Ok(Statement::DropServer(Box::new(parse_drop_server(
            parser, drop_span,
        )?))),
        Token::Ident(_, Keyword::TRIGGER) => Ok(Statement::DropTrigger(Box::new(
            parse_drop_trigger(parser, drop_span)?,
        ))),
        Token::Ident(_, Keyword::USER) => {
            // DROP USER [IF EXISTS] user_name [, user_name] ..
            parser.todo(file!(), line!())
        }
        _ => parser.expected_failure("droppable"),
    }
}
