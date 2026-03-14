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
    Identifier, OptSpanned, QualifiedName, SString, Span, Spanned,
    alter_table::AlterTableOwner,
    data_type::{DataType, DataTypeContext, parse_data_type},
    drop::{CascadeOrRestrict, parse_cascade_or_restrict},
    expression::{Expression, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
};

/// ALTER TYPE actions
#[derive(Clone, Debug)]
pub enum AlterTypeAction<'a> {
    /// OWNER TO new_owner
    OwnerTo {
        owner_to_span: Span,
        new_owner: AlterTableOwner<'a>,
    },
    /// RENAME TO new_name
    RenameTo {
        rename_to_span: Span,
        new_name: Identifier<'a>,
    },
    /// SET SCHEMA new_schema
    SetSchema {
        set_schema_span: Span,
        new_schema: QualifiedName<'a>,
    },
    /// RENAME ATTRIBUTE attribute_name TO new_attribute_name [ CASCADE | RESTRICT ]
    RenameAttribute {
        rename_attribute_span: Span,
        attribute_name: Identifier<'a>,
        to_span: Span,
        new_attribute_name: Identifier<'a>,
        cascade_or_restrict: Option<CascadeOrRestrict>,
    },
    /// ADD VALUE [ IF NOT EXISTS ] new_enum_value [ { BEFORE | AFTER } neighbor_enum_value ]
    AddValue {
        add_value_span: Span,
        if_not_exists_span: Option<Span>, // IF NOT EXISTS
        new_enum_value: SString<'a>,
        placement: Option<(Span, SString<'a>)>, // (BEFORE/AFTER span, neighbor value)
    },
    /// RENAME VALUE existing_enum_value TO new_enum_value
    RenameValue {
        rename_value_span: Span,
        existing_enum_value: SString<'a>,
        to_span: Span,
        new_enum_value: SString<'a>,
    },
    /// ADD ATTRIBUTE attribute_name data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
    /// DROP ATTRIBUTE [ IF EXISTS ] attribute_name [ CASCADE | RESTRICT ]
    /// ALTER ATTRIBUTE attribute_name [ SET DATA ] TYPE data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
    Attributes { items: Vec<AttributeAction<'a>> },
    /// SET ( property = value [, ... ] )
    SetProperties {
        set_span: Span,
        properties: Vec<(Identifier<'a>, Expression<'a>)>,
    },
}

impl<'a> Spanned for AlterTypeAction<'a> {
    fn span(&self) -> Span {
        match self {
            AlterTypeAction::OwnerTo {
                owner_to_span,
                new_owner,
            } => owner_to_span.join_span(new_owner),
            AlterTypeAction::RenameTo {
                rename_to_span,
                new_name,
            } => rename_to_span.join_span(new_name),
            AlterTypeAction::SetSchema {
                set_schema_span,
                new_schema,
            } => set_schema_span.join_span(new_schema),
            AlterTypeAction::RenameAttribute {
                rename_attribute_span,
                attribute_name,
                to_span,
                new_attribute_name,
                cascade_or_restrict,
            } => rename_attribute_span
                .join_span(attribute_name)
                .join_span(to_span)
                .join_span(new_attribute_name)
                .join_span(cascade_or_restrict),
            AlterTypeAction::AddValue {
                add_value_span,
                if_not_exists_span,
                new_enum_value,
                placement,
            } => add_value_span
                .join_span(if_not_exists_span)
                .join_span(new_enum_value)
                .join_span(placement),
            AlterTypeAction::RenameValue {
                rename_value_span,
                existing_enum_value,
                to_span,
                new_enum_value,
            } => rename_value_span
                .join_span(existing_enum_value)
                .join_span(to_span)
                .join_span(new_enum_value),
            AlterTypeAction::Attributes { items } => items.opt_span().expect("Empty attributes"),
            AlterTypeAction::SetProperties {
                set_span,
                properties,
            } => set_span.join_span(properties),
        }
    }
}

/// Attribute actions for composite types
#[derive(Clone, Debug)]
pub enum AttributeAction<'a> {
    /// ADD ATTRIBUTE attribute_name data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
    Add {
        add_attribute_span: Span,
        attribute_name: Identifier<'a>,
        data_type: DataType<'a>,
        collate: Option<(Span, QualifiedName<'a>)>,
        cascade_or_restrict: Option<CascadeOrRestrict>,
    },
    /// DROP ATTRIBUTE [ IF EXISTS ] attribute_name [ CASCADE | RESTRICT ]
    Drop {
        drop_attribute_span: Span,
        if_exists_span: Option<Span>, // IF EXISTS
        attribute_name: Identifier<'a>,
        cascade_or_restrict: Option<CascadeOrRestrict>,
    },
    /// ALTER ATTRIBUTE attribute_name [ SET DATA ] TYPE data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
    Alter {
        alter_attribute_span: Span,
        attribute_name: Identifier<'a>,
        set_data_span: Option<Span>, // SET DATA
        type_span: Span,
        data_type: DataType<'a>,
        collate: Option<(Span, QualifiedName<'a>)>,
        cascade_or_restrict: Option<CascadeOrRestrict>,
    },
}

impl<'a> Spanned for AttributeAction<'a> {
    fn span(&self) -> Span {
        match self {
            AttributeAction::Add {
                add_attribute_span,
                attribute_name,
                data_type,
                collate,
                cascade_or_restrict,
            } => add_attribute_span
                .join_span(attribute_name)
                .join_span(data_type)
                .join_span(collate)
                .join_span(cascade_or_restrict),
            AttributeAction::Drop {
                drop_attribute_span,
                if_exists_span,
                attribute_name,
                cascade_or_restrict,
            } => drop_attribute_span
                .join_span(if_exists_span)
                .join_span(attribute_name)
                .join_span(cascade_or_restrict),
            AttributeAction::Alter {
                alter_attribute_span,
                attribute_name,
                set_data_span,
                type_span,
                data_type,
                collate,
                cascade_or_restrict,
            } => alter_attribute_span
                .join_span(attribute_name)
                .join_span(set_data_span)
                .join_span(type_span)
                .join_span(data_type)
                .join_span(collate)
                .join_span(cascade_or_restrict),
        }
    }
}

/// ALTER TYPE statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct AlterType<'a> {
    /// Span of "ALTER TYPE"
    pub alter_type_span: Span,
    /// Name of the type (possibly schema-qualified)
    pub name: QualifiedName<'a>,
    /// Action to perform
    pub action: AlterTypeAction<'a>,
}

impl<'a> Spanned for AlterType<'a> {
    fn span(&self) -> Span {
        self.alter_type_span
            .join_span(&self.name)
            .join_span(&self.action)
    }
}

pub(crate) fn parse_alter_type<'a>(
    parser: &mut Parser<'a, '_>,
    alter_type_span: Span,
) -> Result<AlterType<'a>, ParseError> {
    parser.postgres_only(&alter_type_span);
    let name = parse_qualified_name_unreserved(parser)?;

    let action = match &parser.token {
        Token::Ident(_, Keyword::OWNER) => {
            let owner_to_span = parser.consume_keywords(&[Keyword::OWNER, Keyword::TO])?;
            let new_owner = crate::alter_table::parse_alter_owner(parser)?;
            AlterTypeAction::OwnerTo {
                owner_to_span,
                new_owner,
            }
        }
        Token::Ident(_, Keyword::RENAME) => {
            let rename_span = parser.consume_keyword(Keyword::RENAME)?;
            match &parser.token {
                Token::Ident(_, Keyword::TO) => {
                    // RENAME TO new_name
                    let to_span = parser.consume_keyword(Keyword::TO)?;
                    let rename_to_span = rename_span.join_span(&to_span);
                    let new_name = parser.consume_plain_identifier_unreserved()?;
                    AlterTypeAction::RenameTo {
                        rename_to_span,
                        new_name,
                    }
                }
                Token::Ident(_, Keyword::ATTRIBUTE) => {
                    // RENAME ATTRIBUTE attribute_name TO new_attribute_name
                    let attribute_span = parser.consume_keyword(Keyword::ATTRIBUTE)?;
                    let rename_attribute_span = rename_span.join_span(&attribute_span);
                    let attribute_name = parser.consume_plain_identifier_unreserved()?;
                    let to_span = parser.consume_keyword(Keyword::TO)?;
                    let new_attribute_name = parser.consume_plain_identifier_unreserved()?;
                    let cascade_or_restrict = parse_cascade_or_restrict(parser);
                    AlterTypeAction::RenameAttribute {
                        rename_attribute_span,
                        attribute_name,
                        to_span,
                        new_attribute_name,
                        cascade_or_restrict,
                    }
                }
                Token::Ident(_, Keyword::VALUE) => {
                    // RENAME VALUE existing_enum_value TO new_enum_value
                    let value_span = parser.consume_keyword(Keyword::VALUE)?;
                    let rename_value_span = rename_span.join_span(&value_span);
                    let existing_enum_value = parser.consume_string()?;
                    let to_span = parser.consume_keyword(Keyword::TO)?;
                    let new_enum_value = parser.consume_string()?;
                    AlterTypeAction::RenameValue {
                        rename_value_span,
                        existing_enum_value,
                        to_span,
                        new_enum_value,
                    }
                }
                _ => parser.expected_failure("'TO', 'ATTRIBUTE', or 'VALUE' after 'RENAME'")?,
            }
        }
        Token::Ident(_, Keyword::SET) => {
            let set_span = parser.consume_keyword(Keyword::SET)?;
            match &parser.token {
                Token::Ident(_, Keyword::SCHEMA) => {
                    // SET SCHEMA new_schema
                    let schema_span = parser.consume_keyword(Keyword::SCHEMA)?;
                    let set_schema_span = set_span.join_span(&schema_span);
                    let new_schema = parse_qualified_name_unreserved(parser)?;
                    AlterTypeAction::SetSchema {
                        set_schema_span,
                        new_schema,
                    }
                }
                Token::LParen => {
                    // SET ( property = value [, ... ] )
                    parser.consume_token(Token::LParen)?;
                    let mut properties = Vec::new();
                    loop {
                        let property = parser.consume_plain_identifier_unreserved()?;
                        parser.consume_token(Token::Eq)?;
                        let value = parse_expression_unreserved(parser, false)?;
                        properties.push((property, value));
                        if parser.skip_token(Token::Comma).is_none() {
                            break;
                        }
                    }
                    parser.consume_token(Token::RParen)?;
                    AlterTypeAction::SetProperties {
                        set_span,
                        properties,
                    }
                }
                _ => parser.expected_failure("'SCHEMA' or '(' after 'SET'")?,
            }
        }
        Token::Ident(_, Keyword::ADD) => {
            let add_span = parser.consume_keyword(Keyword::ADD)?;
            match &parser.token {
                Token::Ident(_, Keyword::VALUE) => {
                    // ADD VALUE [ IF NOT EXISTS ] new_enum_value [ { BEFORE | AFTER } neighbor_enum_value ]
                    let value_span = parser.consume_keyword(Keyword::VALUE)?;
                    let add_value_span = add_span.join_span(&value_span);
                    let if_not_exists_span = if let Some(if_span) = parser.skip_keyword(Keyword::IF)
                    {
                        Some(
                            parser
                                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                                .join_span(&if_span),
                        )
                    } else {
                        None
                    };
                    let new_enum_value = parser.consume_string()?;
                    let placement = if let Some(before_span) = parser.skip_keyword(Keyword::BEFORE)
                    {
                        let neighbor = parser.consume_string()?;
                        Some((before_span, neighbor))
                    } else if let Some(after_span) = parser.skip_keyword(Keyword::AFTER) {
                        let neighbor = parser.consume_string()?;
                        Some((after_span, neighbor))
                    } else {
                        None
                    };
                    AlterTypeAction::AddValue {
                        add_value_span,
                        if_not_exists_span,
                        new_enum_value,
                        placement,
                    }
                }
                Token::Ident(_, Keyword::ATTRIBUTE) => {
                    // ADD ATTRIBUTE or composite type operations with multiple items
                    let mut items = Vec::new();
                    loop {
                        items.push(parse_attribute_action(parser)?);
                        if parser.skip_token(Token::Comma).is_none() {
                            break;
                        }
                    }
                    AlterTypeAction::Attributes { items }
                }
                _ => parser.expected_failure("'VALUE' or 'ATTRIBUTE' after 'ADD'")?,
            }
        }
        Token::Ident(_, Keyword::DROP) => {
            // DROP ATTRIBUTE or multiple operations
            let mut items = Vec::new();
            loop {
                items.push(parse_attribute_action(parser)?);
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            AlterTypeAction::Attributes { items }
        }
        Token::Ident(_, Keyword::ALTER) => {
            // ALTER ATTRIBUTE or multiple operations
            let mut items = Vec::new();
            loop {
                items.push(parse_attribute_action(parser)?);
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            AlterTypeAction::Attributes { items }
        }
        _ => parser.expected_failure(
            "'OWNER', 'RENAME', 'SET', 'ADD', 'DROP', or 'ALTER' after type name",
        )?,
    };

    Ok(AlterType {
        alter_type_span,
        name,
        action,
    })
}

fn parse_attribute_action<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<AttributeAction<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::ADD) => {
            // ADD ATTRIBUTE attribute_name data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
            let add_attribute_span =
                parser.consume_keywords(&[Keyword::ADD, Keyword::ATTRIBUTE])?;
            let attribute_name = parser.consume_plain_identifier_unreserved()?;
            let data_type = parse_data_type(parser, DataTypeContext::Column)?;
            let collate = if let Some(collate_span) = parser.skip_keyword(Keyword::COLLATE) {
                let collation = parse_qualified_name_unreserved(parser)?;
                Some((collate_span, collation))
            } else {
                None
            };
            let cascade_or_restrict = parse_cascade_or_restrict(parser);
            Ok(AttributeAction::Add {
                add_attribute_span,
                attribute_name,
                data_type,
                collate,
                cascade_or_restrict,
            })
        }
        Token::Ident(_, Keyword::DROP) => {
            // DROP ATTRIBUTE [ IF EXISTS ] attribute_name [ CASCADE | RESTRICT ]
            let drop_attribute_span =
                parser.consume_keywords(&[Keyword::DROP, Keyword::ATTRIBUTE])?;
            let if_exists_span = if let Some(if_span) = parser.skip_keyword(Keyword::IF) {
                Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&if_span))
            } else {
                None
            };
            let attribute_name = parser.consume_plain_identifier_unreserved()?;
            let cascade_or_restrict = parse_cascade_or_restrict(parser);
            Ok(AttributeAction::Drop {
                drop_attribute_span,
                if_exists_span,
                attribute_name,
                cascade_or_restrict,
            })
        }
        Token::Ident(_, Keyword::ALTER) => {
            // ALTER ATTRIBUTE attribute_name [ SET DATA ] TYPE data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
            let alter_attribute_span =
                parser.consume_keywords(&[Keyword::ALTER, Keyword::ATTRIBUTE])?;
            let attribute_name = parser.consume_plain_identifier_unreserved()?;
            let set_data_span = if let Some(set_span) = parser.skip_keyword(Keyword::SET) {
                Some(parser.consume_keyword(Keyword::DATA)?.join_span(&set_span))
            } else {
                None
            };
            let type_span = parser.consume_keyword(Keyword::TYPE)?;
            let data_type = parse_data_type(parser, DataTypeContext::Column)?;
            let collate = if let Some(collate_span) = parser.skip_keyword(Keyword::COLLATE) {
                let collation = parse_qualified_name_unreserved(parser)?;
                Some((collate_span, collation))
            } else {
                None
            };
            let cascade_or_restrict = parse_cascade_or_restrict(parser);
            Ok(AttributeAction::Alter {
                alter_attribute_span,
                attribute_name,
                set_data_span,
                type_span,
                data_type,
                collate,
                cascade_or_restrict,
            })
        }
        _ => parser.expected_failure("'ADD', 'DROP', or 'ALTER' for attribute action"),
    }
}
