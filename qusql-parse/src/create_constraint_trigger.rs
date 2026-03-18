// Licensed under the Apache License, Version 2.0
// CREATE CONSTRAINT TRIGGER parser for PostgreSQL
use crate::{
    Identifier, Span, Spanned,
    create_option::CreateOption,
    expression::{Expression, PRIORITY_MAX, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
};
use alloc::vec::Vec;

/// Enum for constraint trigger events
#[derive(Clone, Debug)]
pub enum AfterEvent {
    Insert(Span),
    Update(Span),
    Delete(Span),
}

impl Spanned for AfterEvent {
    fn span(&self) -> Span {
        match self {
            AfterEvent::Insert(s) => s.clone(),
            AfterEvent::Update(s) => s.clone(),
            AfterEvent::Delete(s) => s.clone(),
        }
    }
}

/// Whether the trigger is deferrable
#[derive(Clone, Debug)]
pub enum Deferrable {
    /// DEFERRABLE
    Deferrable(Span),
    /// NOT DEFERRABLE
    NotDeferrable(Span),
}

impl Spanned for Deferrable {
    fn span(&self) -> Span {
        match self {
            Deferrable::Deferrable(s) => s.clone(),
            Deferrable::NotDeferrable(s) => s.clone(),
        }
    }
}

/// Initial timing of the constraint trigger
#[derive(Clone, Debug)]
pub enum Initially {
    /// INITIALLY IMMEDIATE
    Immediate(Span),
    /// INITIALLY DEFERRED
    Deferred(Span),
}

impl Spanned for Initially {
    fn span(&self) -> Span {
        match self {
            Initially::Immediate(s) => s.clone(),
            Initially::Deferred(s) => s.clone(),
        }
    }
}

/// Represent a create constraint trigger statement
#[derive(Clone, Debug)]
pub struct CreateConstraintTrigger<'a> {
    /// The span of the entire CREATE keyword
    pub create_span: Span,
    /// The span of the CONSTRAINT TRIGGER keywords
    pub constraint_trigger_span: Span,
    /// The name of the constraint trigger
    pub name: Identifier<'a>,
    /// The events that fire the trigger (AFTER INSERT, AFTER UPDATE, AFTER DELETE)
    pub after_span: Span,
    pub after_events: Vec<AfterEvent>,
    /// The table the trigger is on
    pub on_span: Span,
    pub table_name: Identifier<'a>,
    /// The referenced table for the trigger (optional, used for referencing foreign keys)
    pub referenced_table_name: Option<Identifier<'a>>,
    /// Whether the trigger is deferrable or not (optional, PostgreSQL specific)
    pub deferrable: Option<Deferrable>,
    /// The initial timing of the trigger (optional, PostgreSQL specific)
    pub initially: Option<Initially>,
    /// The span of the FOR EACH ROW keywords
    pub for_each_row_span: Span,
    /// The WHEN condition for the trigger (optional, PostgreSQL specific)
    pub when_condition: Option<(Span, Expression<'a>)>,
    /// The span of the EXECUTE PROCEDURE keywords
    pub execute_procedure_span: Span,
    /// The name of the function to execute when the trigger fires
    pub function_name: Identifier<'a>,
    /// The arguments to the function (optional)
    pub function_args: Vec<Expression<'a>>,
}

impl<'a> Spanned for CreateConstraintTrigger<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.constraint_trigger_span)
            .join_span(&self.name)
            .join_span(&self.after_span)
            .join_span(&self.after_events)
            .join_span(&self.on_span)
            .join_span(&self.table_name)
            .join_span(&self.referenced_table_name)
            .join_span(&self.deferrable)
            .join_span(&self.initially)
            .join_span(&self.for_each_row_span)
            .join_span(&self.when_condition)
            .join_span(&self.execute_procedure_span)
            .join_span(&self.function_name)
            .join_span(&self.function_args)
    }
}

pub(crate) fn parse_create_constraint_trigger<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateConstraintTrigger<'a>, ParseError> {
    let constraint_span = parser.consume_keywords(&[Keyword::CONSTRAINT, Keyword::TRIGGER])?;
    parser.postgres_only(&constraint_span);

    for option in create_options {
        parser.err(
            "Not supported for CREATE CONSTRAINT TRIGGER",
            &option.span(),
        );
    }
    let name = parser.consume_plain_identifier_unreserved()?;

    // Parse AFTER event(s)
    let mut after_events = Vec::new();
    let after_span = parser.consume_keyword(Keyword::AFTER)?;
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::INSERT) => {
                after_events.push(AfterEvent::Insert(parser.consume_keyword(Keyword::INSERT)?))
            }
            Token::Ident(_, Keyword::UPDATE) => {
                after_events.push(AfterEvent::Update(parser.consume_keyword(Keyword::UPDATE)?))
            }
            Token::Ident(_, Keyword::DELETE) => {
                after_events.push(AfterEvent::Delete(parser.consume_keyword(Keyword::DELETE)?))
            }
            Token::Ident(_, Keyword::OR) => {
                parser.consume_keyword(Keyword::OR)?;
            }
            _ => break,
        }
    }

    let on_span = parser.consume_keyword(Keyword::ON)?;
    let table_name = parser.consume_plain_identifier_unreserved()?;

    let referenced_table_name = if parser.skip_keyword(Keyword::FROM).is_some() {
        Some(parser.consume_plain_identifier_unreserved()?)
    } else {
        None
    };

    let deferrable = if let Some(span) = parser.skip_keyword(Keyword::DEFERRABLE) {
        Some(Deferrable::Deferrable(span))
    } else if let Some(not_span) = parser.skip_keyword(Keyword::NOT) {
        let deferrable_span = parser.consume_keyword(Keyword::DEFERRABLE)?;
        Some(Deferrable::NotDeferrable(
            not_span.join_span(&deferrable_span),
        ))
    } else {
        None
    };

    #[allow(clippy::manual_map)]
    let initially = if let Some(initially_span) = parser.skip_keyword(Keyword::INITIALLY) {
        if let Some(s) = parser.skip_keyword(Keyword::IMMEDIATE) {
            Some(Initially::Immediate(initially_span.join_span(&s)))
        } else if let Some(s) = parser.skip_keyword(Keyword::DEFERRED) {
            Some(Initially::Deferred(initially_span.join_span(&s)))
        } else {
            None
        }
    } else {
        None
    };

    let for_each_row_span =
        parser.consume_keywords(&[Keyword::FOR, Keyword::EACH, Keyword::ROW])?;

    let when_condition = if let Some(when_span) = parser.skip_keyword(Keyword::WHEN) {
        parser.consume_token(Token::LParen)?;
        let cond = parse_expression_unreserved(parser, PRIORITY_MAX)?;
        parser.consume_token(Token::RParen)?;
        Some((when_span, cond))
    } else {
        None
    };

    parser.consume_keyword(Keyword::EXECUTE)?;
    let execute_procedure_span = parser.consume_keyword(Keyword::PROCEDURE)?;
    let function_name = parser.consume_plain_identifier_unreserved()?;
    let mut function_args = Vec::new();
    if parser.skip_token(Token::LParen).is_some() {
        // Parse arguments as expressions
        loop {
            function_args.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        parser.consume_token(Token::RParen)?;
    }

    Ok(CreateConstraintTrigger {
        create_span,
        constraint_trigger_span: constraint_span,
        name,
        after_span,
        after_events,
        on_span,
        table_name,
        referenced_table_name,
        deferrable,
        initially,
        for_each_row_span,
        when_condition,
        execute_procedure_span,
        function_name,
        function_args,
    })
}
