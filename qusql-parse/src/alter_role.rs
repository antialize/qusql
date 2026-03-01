use alloc::vec::Vec;

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
    Expression, Identifier, Span, Spanned,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    create_role::parse_role_option,
    RoleOption,
};

/// Parameters value for ALTER ROLE SET/RESET
#[derive(Clone, Debug)]
pub enum AlterRoleValue<'a> {
    /// = value or TO value
    Value(Expression<'a>),
    /// TO DEFAULT or FROM CURRENT
    Default(Span),
    FromCurrent(Span),
}

impl<'a> Spanned for AlterRoleValue<'a> {
    fn span(&self) -> Span {
        match self {
            AlterRoleValue::Value(v) => v.span(),
            AlterRoleValue::Default(v) => v.span(),
            AlterRoleValue::FromCurrent(v) => v.span(),
        }
    }
}

/// Actions that can be performed in ALTER ROLE
#[derive(Clone, Debug)]
pub enum AlterRoleAction<'a> {
    /// RENAME TO new_name
    RenameTo {
        rename_to_span: Span,
        new_name: Identifier<'a>,
    },
    /// RESET ALL
    ResetAll { reset_all_span: Span },
    /// IN DATABASE database_name RESET parameter
    ResetInDatabase {
        in_database_span: Span,
        database_name: Identifier<'a>,
        reset_span: Span,
        parameter: Identifier<'a>,
    },
    /// IN DATABASE database_name SET parameter = value
    SetInDatabase {
        in_database_span: Span,
        database_name: Identifier<'a>,
        set_span: Span,
        parameter: Identifier<'a>,
        value: AlterRoleValue<'a>,
    },
    /// SET parameter = value
    Set {
        set_span: Span,
        parameter: Identifier<'a>,
        value: AlterRoleValue<'a>,
    },
    /// WITH options
    With {
        with_span: Span,
        options: Vec<RoleOption<'a>>,
    },
}

impl<'a> Spanned for AlterRoleAction<'a> {
    fn span(&self) -> Span {
        match self {
            AlterRoleAction::RenameTo {
                rename_to_span,
                new_name,
            } => rename_to_span.join_span(new_name),
            AlterRoleAction::ResetAll { reset_all_span } => reset_all_span.span(),
            AlterRoleAction::ResetInDatabase {
                in_database_span,
                database_name,
                reset_span,
                parameter,
            } => in_database_span
                .join_span(database_name)
                .join_span(reset_span)
                .join_span(parameter),
            AlterRoleAction::SetInDatabase {
                in_database_span,
                database_name,
                set_span,
                parameter,
                value,
            } => in_database_span
                .join_span(database_name)
                .join_span(set_span)
                .join_span(parameter)
                .join_span(value),
            AlterRoleAction::Set {
                set_span,
                parameter,
                value,
            } => set_span.join_span(parameter).join_span(value),
            AlterRoleAction::With { with_span, options } => with_span.join_span(options),
        }
    }
}

/// ALTER ROLE statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct AlterRole<'a> {
    /// Span of "ALTER"
    pub alter_span: Span,
    /// Span of "ROLE"
    pub role_span: Span,
    /// Name of the role to alter
    pub role_name: Identifier<'a>,
    /// Action to perform
    pub action: AlterRoleAction<'a>,
}

impl<'a> Spanned for AlterRole<'a> {
    fn span(&self) -> Span {
        self.alter_span
            .join_span(&self.role_span)
            .join_span(&self.role_name)
            .join_span(&self.action)
    }
}

pub(crate) fn parse_alter_role<'a>(
    parser: &mut Parser<'a, '_>,
    alter_span: Span,
) -> Result<AlterRole<'a>, ParseError> {
    let role_span = parser.consume_keyword(Keyword::ROLE)?;
    parser.postgres_only(&role_span);

    let role_name = parser.consume_plain_identifier()?;

    let action = match &parser.token {
        Token::Ident(_, Keyword::RENAME) => {
            let rename_to_span = parser.consume_keywords(&[Keyword::RENAME, Keyword::TO])?;
            let new_name = parser.consume_plain_identifier()?;
            AlterRoleAction::RenameTo {
                rename_to_span,
                new_name,
            }
        }
        Token::Ident(_, Keyword::IN) => {
            let in_database_span = parser.consume_keywords(&[Keyword::IN, Keyword::DATABASE])?;
            let database_name = parser.consume_plain_identifier()?;

            match &parser.token {
                Token::Ident(_, Keyword::SET) => {
                    let set_span = parser.consume_keyword(Keyword::SET)?;
                    let parameter = parser.consume_plain_identifier()?;

                    let value = if let Some(eq_span) = parser.skip_token(Token::Eq) {
                        if let Some(default_span) = parser.skip_keyword(Keyword::DEFAULT) {
                            AlterRoleValue::Default(eq_span.join_span(&default_span))
                        } else {
                            AlterRoleValue::Value(parse_expression(parser, false)?)
                        }
                    } else if let Some(to_span) = parser.skip_keyword(Keyword::TO) {
                        if let Some(default_span) = parser.skip_keyword(Keyword::DEFAULT) {
                            AlterRoleValue::Default(to_span.join_span(&default_span))
                        } else {
                            AlterRoleValue::Value(parse_expression(parser, false)?)
                        }
                    } else {
                        parser.expected_failure("'=' or 'TO'")?
                    };

                    AlterRoleAction::SetInDatabase {
                        in_database_span,
                        database_name,
                        set_span,
                        parameter,
                        value,
                    }
                }
                Token::Ident(_, Keyword::RESET) => {
                    let reset_span = parser.consume_keyword(Keyword::RESET)?;
                    let parameter = parser.consume_plain_identifier()?;

                    AlterRoleAction::ResetInDatabase {
                        in_database_span,
                        database_name,
                        reset_span,
                        parameter,
                    }
                }
                _ => parser.expected_failure("'SET' or 'RESET'")?,
            }
        }
        Token::Ident(_, Keyword::RESET) => {
            let reset_all_span = parser.consume_keywords(&[Keyword::RESET, Keyword::ALL])?;
            AlterRoleAction::ResetAll { reset_all_span }
        }
        Token::Ident(_, Keyword::SET) => {
            let set_span = parser.consume_keyword(Keyword::SET)?;
            let parameter = parser.consume_plain_identifier()?;

            let value = if matches!(parser.token, Token::Ident(_, Keyword::FROM)) {
                let from_current_span =
                    parser.consume_keywords(&[Keyword::FROM, Keyword::CURRENT])?;
                AlterRoleValue::FromCurrent(from_current_span)
            } else if let Some(eq_span) = parser.skip_token(Token::Eq) {
                if let Some(default_span) = parser.skip_keyword(Keyword::DEFAULT) {
                    AlterRoleValue::Default(eq_span.join_span(&default_span))
                } else {
                    AlterRoleValue::Value(parse_expression(parser, false)?)
                }
            } else if let Some(to_span) = parser.skip_keyword(Keyword::TO) {
                if let Some(default_span) = parser.skip_keyword(Keyword::DEFAULT) {
                    AlterRoleValue::Default(to_span.join_span(&default_span))
                } else {
                    AlterRoleValue::Value(parse_expression(parser, false)?)
                }
            } else {
                parser.expected_failure("'=', 'TO', or 'FROM'")?
            };

            AlterRoleAction::Set {
                set_span,
                parameter,
                value,
            }
        }
        Token::Ident(_, Keyword::WITH) => {
            let with_span = parser.consume_keyword(Keyword::WITH)?;
            let mut options = Vec::new();
            loop {
                if let Some(opt) = parse_role_option(parser)? {
                    options.push(opt);
                    continue;
                }
                break;
            }
            AlterRoleAction::With { with_span, options }
        }
        _ => parser.expected_failure("ALTER ROLE action (RENAME, IN, RESET, SET, or WITH)")?,
    };

    Ok(AlterRole {
        alter_span,
        role_span,
        role_name,
        action,
    })
}
