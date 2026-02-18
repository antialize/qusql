use alloc::vec::Vec;

use crate::{
    Identifier, QualifiedName, Span, Spanned,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

#[derive(Debug, Clone)]
pub enum FlushOption<'a> {
    BinaryLogs(Span),
    EngineLogs(Span),
    ErrorLogs(Span),
    GeneralLogs(Span),
    Logs(Span),
    Privileges(Span),
    OptimizerCosts(Span),
    RelayLogs(Span),
    RelayLogsForChannel {
        span: Span,
        channel: Identifier<'a>,
    },
    SlowLogs(Span),
    Status(Span),
    UserResources(Span),
    Table {
        span: Span,
        tables: Vec<QualifiedName<'a>>,
        with_read_lock: Option<Span>,
        for_export: Option<Span>,
    },
}

impl Spanned for FlushOption<'_> {
    fn span(&self) -> Span {
        match self {
            FlushOption::BinaryLogs(span)
            | FlushOption::EngineLogs(span)
            | FlushOption::ErrorLogs(span)
            | FlushOption::GeneralLogs(span)
            | FlushOption::Logs(span)
            | FlushOption::Privileges(span)
            | FlushOption::OptimizerCosts(span)
            | FlushOption::RelayLogs(span)
            | FlushOption::SlowLogs(span)
            | FlushOption::Status(span)
            | FlushOption::UserResources(span) => span.clone(),
            FlushOption::RelayLogsForChannel { span, channel } => span.join_span(channel),
            FlushOption::Table {
                span,
                tables,
                with_read_lock,
                for_export,
            } => span
                .join_span(tables)
                .join_span(with_read_lock)
                .join_span(for_export),
        }
    }
}

/// Parse mariadb FLUSH statement.
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "FLUSH TABLES t1, t2 WITH READ LOCK;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///   Some(Statement::Flush(f)) => f,
/// _ => panic!("We should get a flush statement"),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct Flush<'a> {
    pub flush_span: Span,
    pub no_write_to_binlog: Option<Span>,
    pub local: Option<Span>,
    pub options: Vec<FlushOption<'a>>,
}

impl Spanned for Flush<'_> {
    fn span(&self) -> Span {
        self.flush_span
            .join_span(&self.no_write_to_binlog)
            .join_span(&self.local)
            .join_span(&self.options)
    }
}

pub(crate) fn parse_flush<'a>(parser: &mut Parser<'a, '_>) -> Result<Flush<'a>, ParseError> {
    let flush_span = parser.consume_keyword(Keyword::FLUSH)?;

    let no_write_to_binlog = parser.skip_keyword(Keyword::NO_WRITE_TO_BINLOG);
    let local = parser.skip_keyword(Keyword::LOCAL);
    let mut options = Vec::new();
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::BINARY) => {
                let span = parser.consume_keywords(&[Keyword::BINARY, Keyword::LOGS])?;
                options.push(FlushOption::BinaryLogs(span));
            }
            Token::Ident(_, Keyword::ENGINE) => {
                let span = parser.consume_keywords(&[Keyword::ENGINE, Keyword::LOGS])?;
                options.push(FlushOption::EngineLogs(span));
            }
            Token::Ident(_, Keyword::ERROR) => {
                let span = parser.consume_keywords(&[Keyword::ERROR, Keyword::LOGS])?;
                options.push(FlushOption::ErrorLogs(span));
            }
            Token::Ident(_, Keyword::GENERAL) => {
                let span = parser.consume_keywords(&[Keyword::GENERAL, Keyword::LOGS])?;
                options.push(FlushOption::GeneralLogs(span));
            }
            Token::Ident(_, Keyword::LOGS) => {
                let span = parser.consume_keyword(Keyword::LOGS)?;
                options.push(FlushOption::Logs(span));
            }
            Token::Ident(_, Keyword::PRIVILEGES) => {
                let span = parser.consume_keyword(Keyword::PRIVILEGES)?;
                options.push(FlushOption::Privileges(span));
            }
            Token::Ident(_, Keyword::OPTIMIZER_COSTS) => {
                let span = parser.consume_keyword(Keyword::OPTIMIZER_COSTS)?;
                options.push(FlushOption::OptimizerCosts(span));
            }
            Token::Ident(_, Keyword::RELAY) => {
                let span = parser.consume_keywords(&[Keyword::RELAY, Keyword::LOGS])?;
                if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
                    let span = span
                        .join_span(&for_span)
                        .join_span(&parser.consume_keyword(Keyword::CHANNEL)?);
                    options.push(FlushOption::RelayLogsForChannel {
                        span,
                        channel: parser.consume_plain_identifier()?,
                    });
                } else {
                    options.push(FlushOption::RelayLogs(span));
                };
            }
            Token::Ident(_, Keyword::SLOW) => {
                let span = parser.consume_keywords(&[Keyword::SLOW, Keyword::LOGS])?;
                options.push(FlushOption::SlowLogs(span));
            }
            Token::Ident(_, Keyword::STATUS) => {
                let span = parser.consume_keyword(Keyword::STATUS)?;
                options.push(FlushOption::Status(span));
            }
            Token::Ident(_, Keyword::USER_RESOURCES) => {
                let span = parser.consume_keyword(Keyword::USER_RESOURCES)?;
                options.push(FlushOption::UserResources(span));
            }
            Token::Ident(_, Keyword::TABLE | Keyword::TABLES) => {
                let span = parser.consume_keyword(Keyword::TABLES)?;
                let mut tables = Vec::new();

                if let Token::Ident(_, kw) = parser.token
                    && !kw.reserved()
                {
                    loop {
                        tables.push(parse_qualified_name(parser)?);
                        if parser.skip_token(Token::Comma).is_none() {
                            break;
                        }
                    }
                }

                let with_read_lock = if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
                    Some(
                        with_span
                            .join_span(&parser.consume_keywords(&[Keyword::READ, Keyword::LOCK])?),
                    )
                } else {
                    None
                };
                let for_export = if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
                    Some(for_span.join_span(&parser.consume_keyword(Keyword::EXPORT)?))
                } else {
                    None
                };

                options.push(FlushOption::Table {
                    span,
                    tables,
                    with_read_lock,
                    for_export,
                });
            }
            _ => break,
        }
    }

    Ok(Flush {
        flush_span,
        no_write_to_binlog,
        local,
        options,
    })
}
