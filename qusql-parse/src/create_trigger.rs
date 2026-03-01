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
    Identifier, Span, Spanned, Statement,
    create_option::CreateOption,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    statement::{Block, parse_statement},
};
use alloc::{boxed::Box, vec::Vec};

/// When to fire the trigger
#[derive(Clone, Debug)]

pub enum TriggerTime {
    Before(Span),
    After(Span),
}

impl Spanned for TriggerTime {
    fn span(&self) -> Span {
        match &self {
            TriggerTime::Before(v) => v.span(),
            TriggerTime::After(v) => v.span(),
        }
    }
}

/// On what event to fire the trigger
#[derive(Clone, Debug)]
pub enum TriggerEvent {
    Update(Span),
    Insert(Span),
    Delete(Span),
}

impl Spanned for TriggerEvent {
    fn span(&self) -> Span {
        match &self {
            TriggerEvent::Update(v) => v.span(),
            TriggerEvent::Insert(v) => v.span(),
            TriggerEvent::Delete(v) => v.span(),
        }
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
///     Some(Statement::CreateTrigger(c)) => c,
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
    /// What event should the trigger be fired on
    pub trigger_event: TriggerEvent,
    /// Span of "ON"
    pub on_span: Span,
    /// Name of table to create the trigger on
    pub table: Identifier<'a>,
    /// Span of "FOR EACH ROW"
    pub for_each_row_span: Span,
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
            .join_span(&self.trigger_event)
            .join_span(&self.on_span)
            .join_span(&self.table)
            .join_span(&self.for_each_row_span)
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

    let name = parser.consume_plain_identifier_unrestricted()?;

    let trigger_time = match &parser.token {
        Token::Ident(_, Keyword::AFTER) => {
            TriggerTime::After(parser.consume_keyword(Keyword::AFTER)?)
        }
        Token::Ident(_, Keyword::BEFORE) => {
            TriggerTime::Before(parser.consume_keyword(Keyword::BEFORE)?)
        }
        _ => parser.expected_failure("'BEFORE' or 'AFTER'")?,
    };

    let trigger_event = match &parser.token {
        Token::Ident(_, Keyword::UPDATE) => {
            TriggerEvent::Update(parser.consume_keyword(Keyword::UPDATE)?)
        }
        Token::Ident(_, Keyword::INSERT) => {
            TriggerEvent::Insert(parser.consume_keyword(Keyword::INSERT)?)
        }
        Token::Ident(_, Keyword::DELETE) => {
            TriggerEvent::Delete(parser.consume_keyword(Keyword::DELETE)?)
        }
        _ => parser.expected_failure("'UPDATE' or 'INSERT' or 'DELETE'")?,
    };

    let on_span = parser.consume_keyword(Keyword::ON)?;

    let table = parser.consume_plain_identifier_unrestricted()?;

    let for_each_row_span =
        parser.consume_keywords(&[Keyword::FOR, Keyword::EACH, Keyword::ROW])?;

    // TODO [{ FOLLOWS | PRECEDES } other_trigger_name ]

    // PostgreSQL allows EXECUTE FUNCTION func_name() instead of a statement block
    let statement = if matches!(parser.token, Token::Ident(_, Keyword::EXECUTE)) {
        // Parse EXECUTE FUNCTION func_name()
        let _execute_span = parser.consume_keyword(Keyword::EXECUTE)?;
        parser.consume_keyword(Keyword::FUNCTION)?;
        parser.consume_plain_identifier_unrestricted()?;
        let begin_span = parser.consume_token(Token::LParen)?;
        // TODO: parse function arguments if needed
        let end_span = parser.consume_token(Token::RParen)?;

        // Use an empty block as a placeholder for EXECUTE FUNCTION
        Statement::Block(Box::new(Block {
            begin_span,
            end_span,
            statements: Vec::new(),
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
        trigger_event,
        on_span,
        table,
        for_each_row_span,
        statement,
    })
}
