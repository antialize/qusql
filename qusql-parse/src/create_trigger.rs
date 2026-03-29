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
use crate::{
    Expression, Identifier, QualifiedName, Span, Spanned, Statement,
    create_option::CreateOption,
    expression::{PRIORITY_MAX, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
    statement::parse_statement,
};
use alloc::{boxed::Box, vec::Vec};

/// PostgreSQL trigger EXECUTE FUNCTION func_name(args...) body
#[derive(Clone, Debug)]
pub struct ExecuteFunction<'a> {
    /// Span of "EXECUTE FUNCTION" or "EXECUTE PROCEDURE"
    pub execute_span: Span,
    /// Name of the function to execute
    pub func_name: QualifiedName<'a>,
    /// Arguments passed to the function
    pub args: Vec<Expression<'a>>,
}

impl<'a> Spanned for ExecuteFunction<'a> {
    fn span(&self) -> Span {
        self.execute_span
            .join_span(&self.func_name)
            .join_span(&self.args)
    }
}

/// When to fire the trigger
#[derive(Clone, Debug)]
pub enum TriggerTime {
    Before(Span),
    After(Span),
    InsteadOf(Span),
}

impl Spanned for TriggerTime {
    fn span(&self) -> Span {
        match &self {
            TriggerTime::Before(v) => v.span(),
            TriggerTime::After(v) => v.span(),
            TriggerTime::InsteadOf(v) => v.span(),
        }
    }
}

/// On what event to fire the trigger
#[derive(Clone, Debug)]
pub enum TriggerEvent {
    Update(Span),
    Insert(Span),
    Delete(Span),
    Truncate(Span),
}

impl Spanned for TriggerEvent {
    fn span(&self) -> Span {
        match &self {
            TriggerEvent::Update(v) => v.span(),
            TriggerEvent::Insert(v) => v.span(),
            TriggerEvent::Delete(v) => v.span(),
            TriggerEvent::Truncate(v) => v.span(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum TriggerReferenceDirection {
    New(Span),
    Old(Span),
}

impl Spanned for TriggerReferenceDirection {
    fn span(&self) -> Span {
        match &self {
            TriggerReferenceDirection::New(v) => v.span(),
            TriggerReferenceDirection::Old(v) => v.span(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TriggerReference<'a> {
    direction: TriggerReferenceDirection,
    table_as_span: Span,
    alias: Identifier<'a>,
}

impl Spanned for TriggerReference<'_> {
    fn span(&self) -> Span {
        self.direction
            .join_span(&self.table_as_span)
            .join_span(&self.alias)
    }
}

/// Represent a create trigger statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, CreateTrigger, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DROP TRIGGER IF EXISTS `my_trigger`;
/// DELIMITER $$
/// CREATE TRIGGER `my_trigger` AFTER DELETE ON `things` FOR EACH ROW BEGIN
///     IF OLD.`value` IS NOT NULL THEN
///         UPDATE `t2` AS `j`
///             SET
///             `j`.`total_items` = `total_items` - 1
///             WHERE `j`.`id`=OLD.`value` AND NOT `j`.`frozen`;
///         END IF;
///     INSERT INTO `updated_things` (`thing`) VALUES (OLD.`id`);
/// END
/// $$
/// DELIMITER ;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert_eq!(issues.get(), &[]);
/// #
/// let create: CreateTrigger = match stmts.pop() {
///     Some(Statement::CreateTrigger(c)) => *c,
///     _ => panic!("We should get an create trigger statement")
/// };
///
/// assert!(create.name.as_str() == "my_trigger");
/// println!("{:#?}", create.statement)
/// ```
#[derive(Clone, Debug)]
pub struct CreateTrigger<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "TRIGGER"
    pub trigger_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the created trigger
    pub name: Identifier<'a>,
    /// Should the trigger be fired before or after the event
    pub trigger_time: TriggerTime,
    /// What events should the trigger be fired on (multiple events joined by OR)
    pub trigger_events: Vec<TriggerEvent>,
    /// Span of "ON"
    pub on_span: Span,
    /// Name of table to create the trigger on
    pub table: Identifier<'a>,
    /// Span of "FOR EACH ROW" or "FOR EACH STATEMENT" (None if omitted, PostgreSQL only)
    pub for_each_row_span: Option<Span>,
    /// Optional REFERENCING NEW TABLE AS alias / OLD TABLE AS alias clauses
    pub referencing: Vec<TriggerReference<'a>>,
    /// Optional WHEN (condition)
    pub when_condition: Option<(Span, Expression<'a>)>,
    /// Statement to execute
    pub statement: Statement<'a>,
}

impl<'a> Spanned for CreateTrigger<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.trigger_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.trigger_time)
            .join_span(&self.trigger_events)
            .join_span(&self.on_span)
            .join_span(&self.table)
            .join_span(&self.for_each_row_span)
            .join_span(&self.referencing)
            .join_span(&self.when_condition.as_ref().map(|(s, e)| s.join_span(e)))
            .join_span(&self.statement)
    }
}

pub(crate) fn parse_create_trigger<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateTrigger<'a>, ParseError> {
    let trigger_span = parser.consume_keyword(Keyword::TRIGGER)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let name = parser.consume_plain_identifier_unreserved()?;

    let trigger_time = match &parser.token {
        Token::Ident(_, Keyword::AFTER) => {
            TriggerTime::After(parser.consume_keyword(Keyword::AFTER)?)
        }
        Token::Ident(_, Keyword::BEFORE) => {
            TriggerTime::Before(parser.consume_keyword(Keyword::BEFORE)?)
        }
        Token::Ident(_, Keyword::INSTEAD) => {
            TriggerTime::InsteadOf(parser.consume_keywords(&[Keyword::INSTEAD, Keyword::OF])?)
        }
        _ => parser.expected_failure("'BEFORE', 'AFTER', or 'INSTEAD OF'")?,
    };

    let mut trigger_events = Vec::new();
    loop {
        let event = match &parser.token {
            Token::Ident(_, Keyword::UPDATE) => {
                TriggerEvent::Update(parser.consume_keyword(Keyword::UPDATE)?)
            }
            Token::Ident(_, Keyword::INSERT) => {
                TriggerEvent::Insert(parser.consume_keyword(Keyword::INSERT)?)
            }
            Token::Ident(_, Keyword::DELETE) => {
                TriggerEvent::Delete(parser.consume_keyword(Keyword::DELETE)?)
            }
            Token::Ident(_, Keyword::TRUNCATE) => {
                TriggerEvent::Truncate(parser.consume_keyword(Keyword::TRUNCATE)?)
            }
            _ => parser.expected_failure("'UPDATE', 'INSERT', 'DELETE', or 'TRUNCATE'")?,
        };
        trigger_events.push(event);
        if parser.skip_keyword(Keyword::OR).is_none() {
            break;
        }
    }

    let on_span = parser.consume_keyword(Keyword::ON)?;

    let table = parser.consume_plain_identifier_unreserved()?;

    let for_each_row_span = if parser.options.dialect.is_postgresql() {
        if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
            let each_span = parser.skip_keyword(Keyword::EACH);
            let clause_span = match &parser.token {
                Token::Ident(_, Keyword::ROW) => {
                    for_span.join_span(&each_span).join_span(&parser.consume_keyword(Keyword::ROW)?)
                }
                Token::Ident(_, Keyword::STATEMENT) => {
                    for_span.join_span(&each_span).join_span(&parser.consume_keyword(Keyword::STATEMENT)?)
                }
                _ => for_span.join_span(&each_span),
            };
            Some(clause_span)
        } else {
            None
        }
    } else {
        Some(parser.consume_keywords(&[Keyword::FOR, Keyword::EACH, Keyword::ROW])?)
    };

    // Parse optional REFERENCING clause (PostgreSQL transition table aliases)
    let mut referencing = Vec::new();
    if parser.skip_keyword(Keyword::REFERENCING).is_some() {
        // Each REFERENCING item: { NEW | OLD } TABLE AS alias
        loop {
            let direction = match &parser.token {
                Token::Ident(_, Keyword::NEW) => {
                    TriggerReferenceDirection::New(parser.consume_keyword(Keyword::NEW)?)
                }
                Token::Ident(_, Keyword::OLD) => {
                    TriggerReferenceDirection::Old(parser.consume_keyword(Keyword::OLD)?)
                }
                _ => break,
            };
            let table_as_span = parser.consume_keywords(&[Keyword::TABLE, Keyword::AS])?;
            let alias = parser.consume_plain_identifier_unreserved()?;
            referencing.push(TriggerReference {
                direction,
                table_as_span,
                alias,
            });
        }
    }

    // Parse optional WHEN (condition)
    let when_condition = if let Some(when_span) = parser.skip_keyword(Keyword::WHEN) {
        parser.consume_token(Token::LParen)?;
        let expr = parser.recovered(")", &|t| t == &Token::RParen, |parser| {
            Ok(Some(parse_expression_unreserved(parser, PRIORITY_MAX)?))
        })?;
        parser.consume_token(Token::RParen)?;
        expr.map(|e| (when_span, e))
    } else {
        None
    };

    // TODO [{ FOLLOWS | PRECEDES } other_trigger_name ]

    // PostgreSQL allows EXECUTE FUNCTION func_name(...) instead of a statement block
    let statement = if matches!(parser.token, Token::Ident(_, Keyword::EXECUTE)) {
        let execute_span = parser.consume_keyword(Keyword::EXECUTE)?;
        // Accept both FUNCTION and PROCEDURE (synonyms in this context)
        let execute_span = if let Some(s) = parser.skip_keyword(Keyword::FUNCTION) {
            execute_span.join_span(&s)
        } else {
            execute_span.join_span(&parser.consume_keyword(Keyword::PROCEDURE)?)
        };
        let func_name = parse_qualified_name_unreserved(parser)?;
        parser.consume_token(Token::LParen)?;
        let mut args = Vec::new();
        parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
            loop {
                if matches!(parser.token, Token::RParen) {
                    break;
                }
                args.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            Ok(())
        })?;
        parser.consume_token(Token::RParen)?;
        Statement::ExecuteFunction(Box::new(ExecuteFunction {
            execute_span,
            func_name,
            args,
        }))
    } else {
        let old = core::mem::replace(&mut parser.permit_compound_statements, true);
        let statement = match parse_statement(parser)? {
            Some(v) => v,
            None => parser.expected_failure("statement")?,
        };
        parser.permit_compound_statements = old;
        statement
    };

    Ok(CreateTrigger {
        create_span,
        create_options,
        trigger_span,
        if_not_exists,
        name,
        trigger_time,
        trigger_events,
        on_span,
        table,
        for_each_row_span,
        referencing,
        when_condition,
        statement,
    })
}
