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
    DataType, Expression, Identifier, QualifiedName, SString, Span, Spanned, Statement,
    create_function::parse_create_function,
    create_index::parse_create_index,
    create_option::{CreateAlgorithm, CreateOption},
    create_role::parse_create_role,
    create_table::parse_create_table,
    create_trigger::parse_create_trigger,
    create_view::parse_create_view,
    data_type::parse_data_type,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    operator::{parse_create_operator, parse_create_operator_class, parse_create_operator_family},
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};
use alloc::{boxed::Box, vec::Vec};
#[derive(Clone, Debug)]
pub struct CreateTypeEnum<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "TYPE"
    pub type_span: Span,
    /// Name of the created type
    pub name: Identifier<'a>,
    /// Span of "AS ENUM"
    pub as_enum_span: Span,
    /// Enum values
    pub values: Vec<SString<'a>>,
}

impl<'a> Spanned for CreateTypeEnum<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.type_span)
            .join_span(&self.name)
            .join_span(&self.as_enum_span)
            .join_span(&self.values)
    }
}

fn parse_create_type<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateTypeEnum<'a>, ParseError> {
    let type_span = parser.consume_keyword(Keyword::TYPE)?;
    parser.postgres_only(&type_span);
    let name = parser.consume_plain_identifier()?;
    let as_enum_span = parser.consume_keywords(&[Keyword::AS, Keyword::ENUM])?;
    parser.consume_token(Token::LParen)?;
    let mut values = Vec::new();
    loop {
        parser.recovered(
            "')' or ','",
            &|t| matches!(t, Token::RParen | Token::Comma),
            |parser| {
                values.push(parser.consume_string()?);
                Ok(())
            },
        )?;
        if matches!(parser.token, Token::RParen) {
            break;
        }
        parser.consume_token(Token::Comma)?;
    }
    parser.consume_token(Token::RParen)?;
    Ok(CreateTypeEnum {
        create_span,
        create_options,
        type_span,
        name,
        as_enum_span,
        values,
    })
}

#[derive(Clone, Debug)]
pub enum CreateDatabaseOption<'a> {
    CharSet {
        identifier: Span,
        default_span: Option<Span>,
        value: Identifier<'a>,
    },
    Collate {
        identifier: Span,
        default_span: Option<Span>,
        value: Identifier<'a>,
    },
    Encryption {
        identifier: Span,
        default_span: Option<Span>,
        value: SString<'a>,
    },
}

impl Spanned for CreateDatabaseOption<'_> {
    fn span(&self) -> Span {
        match self {
            CreateDatabaseOption::CharSet {
                identifier,
                default_span,
                value,
            } => identifier.join_span(default_span).join_span(value),
            CreateDatabaseOption::Collate {
                identifier,
                default_span,
                value,
            } => identifier.join_span(default_span).join_span(value),
            CreateDatabaseOption::Encryption {
                identifier,
                default_span,
                value,
            } => identifier.join_span(default_span).join_span(value),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateDatabase<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "DATABASE"
    pub database_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the created database
    pub name: Identifier<'a>,
    /// Options specified for database creation
    pub create_options: Vec<CreateDatabaseOption<'a>>,
}

impl Spanned for CreateDatabase<'_> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.database_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
    }
}

/// CREATE SCHEMA statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateSchema<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "SCHEMA"
    pub schema_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the created schema (optional if AUTHORIZATION is present)
    pub name: Option<Identifier<'a>>,
    /// AUTHORIZATION clause with role name
    pub authorization: Option<(Span, Identifier<'a>)>,
}

impl Spanned for CreateSchema<'_> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.schema_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.authorization)
    }
}

/// Sequence option for CREATE SEQUENCE / ALTER SEQUENCE
#[derive(Clone, Debug)]
pub enum SequenceOption<'a> {
    /// AS data_type
    As(Span, DataType<'a>),
    /// INCREMENT BY value
    IncrementBy(Span, Expression<'a>),
    /// MINVALUE value
    MinValue(Span, Expression<'a>),
    /// NO MINVALUE
    NoMinValue(Span),
    /// MAXVALUE value
    MaxValue(Span, Expression<'a>),
    /// NO MAXVALUE
    NoMaxValue(Span),
    /// START WITH value
    StartWith(Span, Expression<'a>),
    /// CACHE value
    Cache(Span, Expression<'a>),
    /// CYCLE
    Cycle(Span),
    /// NO CYCLE
    NoCycle(Span),
    /// OWNED BY table.column
    OwnedBy(Span, QualifiedName<'a>),
    /// OWNED BY NONE
    OwnedByNone(Span),
}

impl<'a> Spanned for SequenceOption<'a> {
    fn span(&self) -> Span {
        match self {
            SequenceOption::As(s, t) => s.join_span(t),
            SequenceOption::IncrementBy(s, e) => s.join_span(e),
            SequenceOption::MinValue(s, e) => s.join_span(e),
            SequenceOption::NoMinValue(s) => s.span(),
            SequenceOption::MaxValue(s, e) => s.join_span(e),
            SequenceOption::NoMaxValue(s) => s.span(),
            SequenceOption::StartWith(s, e) => s.join_span(e),
            SequenceOption::Cache(s, e) => s.join_span(e),
            SequenceOption::Cycle(s) => s.span(),
            SequenceOption::NoCycle(s) => s.span(),
            SequenceOption::OwnedBy(s, q) => s.join_span(q),
            SequenceOption::OwnedByNone(s) => s.span(),
        }
    }
}

/// CREATE SEQUENCE statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateSequence<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of optional TEMPORARY/TEMP keyword
    pub temporary: Option<Span>,
    /// Span of "SEQUENCE"
    pub sequence_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the created sequence
    pub name: QualifiedName<'a>,
    /// Sequence options
    pub options: Vec<SequenceOption<'a>>,
}

impl Spanned for CreateSequence<'_> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.temporary)
            .join_span(&self.sequence_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.options)
    }
}

fn parse_create_database<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateDatabase<'a>, ParseError> {
    for option in create_options {
        parser.err("Not supported fo CREATE DATABASE", &option.span());
    }

    let database_span = parser.consume();

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let mut create_options = Vec::new();
    let name = parser.consume_plain_identifier()?;
    loop {
        let default_span = parser.skip_keyword(Keyword::DEFAULT);
        match &parser.token {
            Token::Ident(_, Keyword::CHARSET) => {
                let identifier = parser.consume_keyword(Keyword::CHARSET)?;
                parser.skip_token(Token::Eq);
                create_options.push(CreateDatabaseOption::CharSet {
                    default_span,
                    identifier,
                    value: parser.consume_plain_identifier()?,
                });
            }
            Token::Ident(_, Keyword::CHARACTER) => {
                let identifier = parser.consume_keywords(&[Keyword::CHARACTER, Keyword::SET])?;
                parser.skip_token(Token::Eq);
                create_options.push(CreateDatabaseOption::CharSet {
                    default_span,
                    identifier,
                    value: parser.consume_plain_identifier()?,
                });
            }
            Token::Ident(_, Keyword::COLLATE) => {
                let identifier = parser.consume_keyword(Keyword::COLLATE)?;
                parser.skip_token(Token::Eq);
                create_options.push(CreateDatabaseOption::Collate {
                    default_span,
                    identifier,
                    value: parser.consume_plain_identifier()?,
                });
            }
            Token::Ident(_, Keyword::ENCRYPTION) => {
                let identifier = parser.consume_keyword(Keyword::ENCRYPTION)?;
                parser.skip_token(Token::Eq);
                let value = parser.consume_string()?;

                create_options.push(CreateDatabaseOption::Encryption {
                    default_span,
                    identifier,
                    value,
                });
            }
            _ => {
                if default_span.is_some() {
                    parser.expected_failure("'CHARSET', 'COLLATE' or 'ENCRYPTION'")?;
                }
                break;
            }
        }
    }

    Ok(CreateDatabase {
        create_span,
        create_options,
        database_span,
        if_not_exists,
        name,
    })
}

fn parse_create_schema<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateSchema<'a>, ParseError> {
    let schema_span = parser.consume_keyword(Keyword::SCHEMA)?;
    parser.postgres_only(&schema_span);

    for option in create_options {
        parser.err("Not supported for CREATE SCHEMA", &option.span());
    }

    let if_not_exists = if let Some(if_span) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_span),
        )
    } else {
        None
    };

    // Parse schema name or AUTHORIZATION
    let mut name = None;
    let mut authorization = None;

    // Check if next token is AUTHORIZATION
    if let Token::Ident(_, Keyword::AUTHORIZATION) = parser.token {
        let auth_span = parser.consume_keyword(Keyword::AUTHORIZATION)?;
        let role_name = parser.consume_plain_identifier()?;
        authorization = Some((auth_span, role_name));
    } else {
        // Parse schema name
        name = Some(parser.consume_plain_identifier()?);

        // Optional AUTHORIZATION after name
        if let Token::Ident(_, Keyword::AUTHORIZATION) = parser.token {
            let auth_span = parser.consume_keyword(Keyword::AUTHORIZATION)?;
            let role_name = parser.consume_plain_identifier()?;
            authorization = Some((auth_span, role_name));
        }
    }

    // TODO: Parse schema elements (CREATE TABLE, CREATE VIEW, GRANT, etc.)

    Ok(CreateSchema {
        create_span,
        schema_span,
        if_not_exists,
        name,
        authorization,
    })
}

fn parse_create_sequence<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateSequence<'a>, ParseError> {
    let sequence_span = parser.consume_keyword(Keyword::SEQUENCE)?;
    parser.postgres_only(&sequence_span);

    // Extract TEMPORARY option if present, reject others
    let mut temporary = None;
    for option in create_options {
        match option {
            CreateOption::Temporary {
                local_span,
                temporary_span,
            } => {
                temporary = Some(temporary_span.join_span(&local_span));
            }
            _ => {
                parser.err("Not supported for CREATE SEQUENCE", &option.span());
            }
        }
    }

    let if_not_exists = if let Some(if_span) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_span),
        )
    } else {
        None
    };

    // Parse sequence name
    let name = parse_qualified_name(parser)?;

    // Parse sequence options
    let mut options = Vec::new();
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::AS) => {
                let as_span = parser.consume_keyword(Keyword::AS)?;
                let data_type = parse_data_type(parser, false)?;
                options.push(SequenceOption::As(as_span, data_type));
            }
            Token::Ident(_, Keyword::INCREMENT) => {
                let increment_span = parser.consume_keyword(Keyword::INCREMENT)?;
                parser.skip_keyword(Keyword::BY); // BY is optional
                let expr = parse_expression(parser, true)?;
                let span = increment_span.join_span(&expr);
                options.push(SequenceOption::IncrementBy(span, expr));
            }
            Token::Ident(_, Keyword::MINVALUE) => {
                let minvalue_span = parser.consume_keyword(Keyword::MINVALUE)?;
                let expr = parse_expression(parser, true)?;
                let span = minvalue_span.join_span(&expr);
                options.push(SequenceOption::MinValue(span, expr));
            }
            Token::Ident(_, Keyword::MAXVALUE) => {
                let maxvalue_span = parser.consume_keyword(Keyword::MAXVALUE)?;
                let expr = parse_expression(parser, true)?;
                let span = maxvalue_span.join_span(&expr);
                options.push(SequenceOption::MaxValue(span, expr));
            }
            Token::Ident(_, Keyword::START) => {
                let start_span = parser.consume_keyword(Keyword::START)?;
                parser.skip_keyword(Keyword::WITH); // WITH is optional
                let expr = parse_expression(parser, true)?;
                let span = start_span.join_span(&expr);
                options.push(SequenceOption::StartWith(span, expr));
            }
            Token::Ident(_, Keyword::CACHE) => {
                let cache_span = parser.consume_keyword(Keyword::CACHE)?;
                let expr = parse_expression(parser, true)?;
                let span = cache_span.join_span(&expr);
                options.push(SequenceOption::Cache(span, expr));
            }
            Token::Ident(_, Keyword::CYCLE) => {
                let cycle_span = parser.consume_keyword(Keyword::CYCLE)?;
                options.push(SequenceOption::Cycle(cycle_span));
            }
            Token::Ident(_, Keyword::NO) => {
                // Could be NO MINVALUE, NO MAXVALUE, or NO CYCLE
                let no_span = parser.consume_keyword(Keyword::NO)?;
                match &parser.token {
                    Token::Ident(_, Keyword::MINVALUE) => {
                        let minvalue_span = parser.consume_keyword(Keyword::MINVALUE)?;
                        let span = no_span.join_span(&minvalue_span);
                        options.push(SequenceOption::NoMinValue(span));
                    }
                    Token::Ident(_, Keyword::MAXVALUE) => {
                        let maxvalue_span = parser.consume_keyword(Keyword::MAXVALUE)?;
                        let span = no_span.join_span(&maxvalue_span);
                        options.push(SequenceOption::NoMaxValue(span));
                    }
                    Token::Ident(_, Keyword::CYCLE) => {
                        let cycle_span = parser.consume_keyword(Keyword::CYCLE)?;
                        let span = no_span.join_span(&cycle_span);
                        options.push(SequenceOption::NoCycle(span));
                    }
                    _ => parser.expected_failure("'MINVALUE', 'MAXVALUE' or 'CYCLE' after 'NO'")?,
                }
            }
            Token::Ident(_, Keyword::OWNED) => {
                let owned_span = parser.consume_keyword(Keyword::OWNED)?;
                parser.consume_keyword(Keyword::BY)?;
                if let Token::Ident(_, Keyword::NONE) = parser.token {
                    let none_span = parser.consume_keyword(Keyword::NONE)?;
                    let span = owned_span.join_span(&none_span);
                    options.push(SequenceOption::OwnedByNone(span));
                } else {
                    let qualified_name = parse_qualified_name(parser)?;
                    let span = owned_span.join_span(&qualified_name);
                    options.push(SequenceOption::OwnedBy(span, qualified_name));
                }
            }
            _ => break,
        }
    }

    Ok(CreateSequence {
        create_span,
        temporary,
        sequence_span,
        if_not_exists,
        name,
        options,
    })
}

/// CREATE SERVER statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateServer<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "SERVER"
    pub server_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the server
    pub server_name: Identifier<'a>,
    /// Optional TYPE 'server_type'
    pub type_: Option<(Span, SString<'a>)>,
    /// Optional VERSION 'server_version'
    pub version: Option<(Span, SString<'a>)>,
    /// FOREIGN DATA WRAPPER fdw_name
    pub foreign_data_wrapper: (Span, Identifier<'a>),
    /// OPTIONS (option 'value', ...)
    pub options: Vec<(Identifier<'a>, SString<'a>)>,
}

impl Spanned for CreateServer<'_> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.server_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.server_name)
            .join_span(&self.type_)
            .join_span(&self.version)
            .join_span(&self.foreign_data_wrapper)
            .join_span(&self.options)
    }
}

fn parse_create_server<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateServer<'a>, ParseError> {
    let server_span = parser.consume_keyword(Keyword::SERVER)?;
    parser.postgres_only(&server_span);

    for option in create_options {
        parser.err("Not supported for CREATE SERVER", &option.span());
    }

    let if_not_exists = if let Some(if_span) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_span),
        )
    } else {
        None
    };

    // Parse server name
    let server_name = parser.consume_plain_identifier()?;

    // Parse optional TYPE 'server_type'
    let type_ = if let Some(type_span) = parser.skip_keyword(Keyword::TYPE) {
        let type_value = parser.consume_string()?;
        Some((type_span, type_value))
    } else {
        None
    };

    // Parse optional VERSION 'server_version'
    let version = if let Some(version_span) = parser.skip_keyword(Keyword::VERSION) {
        let version_value = parser.consume_string()?;
        Some((version_span, version_value))
    } else {
        None
    };

    // Parse FOREIGN DATA WRAPPER fdw_name
    let fdw_span = parser.consume_keywords(&[Keyword::FOREIGN, Keyword::DATA, Keyword::WRAPPER])?;
    let fdw_name = parser.consume_plain_identifier()?;
    let foreign_data_wrapper = (fdw_span, fdw_name);

    // Parse optional OPTIONS (option 'value', ...)
    let mut options = Vec::new();
    if parser.skip_keyword(Keyword::OPTIONS).is_some() {
        parser.consume_token(Token::LParen)?;
        loop {
            let option_name = parser.consume_plain_identifier()?;
            let option_value = parser.consume_string()?;
            options.push((option_name, option_value));

            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        parser.consume_token(Token::RParen)?;
    }

    Ok(CreateServer {
        create_span,
        server_span,
        if_not_exists,
        server_name,
        type_,
        version,
        foreign_data_wrapper,
        options,
    })
}

pub(crate) fn parse_create<'a>(parser: &mut Parser<'a, '_>) -> Result<Statement<'a>, ParseError> {
    let create_span = parser.span.clone();
    parser.consume_keyword(Keyword::CREATE)?;

    let mut create_options = Vec::new();
    const CREATABLE: &str = "'TABLE' | 'VIEW' | 'TRIGGER' | 'FUNCTION' | 'INDEX' | 'TYPE' | 'DATABASE' | 'SCHEMA' | 'SEQUENCE' | 'ROLE' | 'SERVER' | 'OPERATOR'";

    parser.recovered(
        CREATABLE,
        &|t| {
            matches!(
                t,
                Token::Ident(
                    _,
                    Keyword::TABLE
                        | Keyword::MATERIALIZED
                        | Keyword::VIEW
                        | Keyword::TRIGGER
                        | Keyword::FUNCTION
                        | Keyword::INDEX
                        | Keyword::TYPE
                        | Keyword::DATABASE
                        | Keyword::SCHEMA
                        | Keyword::SEQUENCE
                        | Keyword::ROLE
                        | Keyword::SERVER
                        | Keyword::OPERATOR
                )
            )
        },
        |parser| {
            loop {
                let v = match &parser.token {
                    Token::Ident(_, Keyword::OR) => CreateOption::OrReplace(
                        parser.consume_keywords(&[Keyword::OR, Keyword::REPLACE])?,
                    ),
                    Token::Ident(_, Keyword::LOCAL) => {
                        // LOCAL TEMPORARY
                        let local_span = parser.consume_keyword(Keyword::LOCAL)?;
                        parser.postgres_only(&local_span);
                        let temporary_span = parser.consume_keyword(Keyword::TEMPORARY)?;
                        CreateOption::Temporary {
                            local_span: Some(local_span),
                            temporary_span,
                        }
                    }
                    Token::Ident(_, Keyword::TEMPORARY) => {
                        let temporary_span = parser.consume_keyword(Keyword::TEMPORARY)?;
                        CreateOption::Temporary {
                            local_span: None,
                            temporary_span,
                        }
                    }
                    Token::Ident(_, Keyword::UNIQUE) => {
                        CreateOption::Unique(parser.consume_keyword(Keyword::UNIQUE)?)
                    }
                    Token::Ident(_, Keyword::ALGORITHM) => {
                        let algorithm_span = parser.consume_keyword(Keyword::ALGORITHM)?;
                        parser.consume_token(Token::Eq)?;
                        let algorithm = match &parser.token {
                            Token::Ident(_, Keyword::UNDEFINED) => CreateAlgorithm::Undefined(
                                parser.consume_keyword(Keyword::UNDEFINED)?,
                            ),
                            Token::Ident(_, Keyword::MERGE) => {
                                CreateAlgorithm::Merge(parser.consume_keyword(Keyword::MERGE)?)
                            }
                            Token::Ident(_, Keyword::TEMPTABLE) => CreateAlgorithm::TempTable(
                                parser.consume_keyword(Keyword::TEMPTABLE)?,
                            ),
                            _ => parser.expected_failure("'UNDEFINED', 'MERGE' or 'TEMPTABLE'")?,
                        };
                        CreateOption::Algorithm(algorithm_span, algorithm)
                    }
                    Token::Ident(_, Keyword::DEFINER) => {
                        let definer_span = parser.consume_keyword(Keyword::DEFINER)?;
                        parser.consume_token(Token::Eq)?;
                        // TODO user | CURRENT_USER | role | CURRENT_ROLE
                        // Accept both plain identifiers and string literals
                        let user = match &parser.token {
                            Token::SingleQuotedString(v) => {
                                let v = *v;
                                Identifier::new(v, parser.consume())
                            }
                            _ => parser.consume_plain_identifier()?,
                        };
                        parser.consume_token(Token::At)?;
                        let host = match &parser.token {
                            Token::SingleQuotedString(v) => {
                                let v = *v;
                                Identifier::new(v, parser.consume())
                            }
                            _ => parser.consume_plain_identifier()?,
                        };
                        CreateOption::Definer {
                            definer_span,
                            user,
                            host,
                        }
                    }
                    Token::Ident(_, Keyword::SQL) => {
                        let sql_security =
                            parser.consume_keywords(&[Keyword::SQL, Keyword::SECURITY])?;
                        match &parser.token {
                            Token::Ident(_, Keyword::DEFINER) => CreateOption::SqlSecurityDefiner(
                                sql_security,
                                parser.consume_keyword(Keyword::DEFINER)?,
                            ),
                            Token::Ident(_, Keyword::INVOKER) => CreateOption::SqlSecurityInvoker(
                                sql_security,
                                parser.consume_keyword(Keyword::INVOKER)?,
                            ),
                            Token::Ident(_, Keyword::USER) => CreateOption::SqlSecurityUser(
                                sql_security,
                                parser.consume_keyword(Keyword::USER)?,
                            ),
                            _ => parser.expected_failure("'DEFINER', 'INVOKER', 'USER'")?,
                        }
                    }
                    _ => break,
                };
                create_options.push(v);
            }
            Ok(())
        },
    )?;

    let r =
        match &parser.token {
            Token::Ident(_, Keyword::INDEX) => Statement::CreateIndex(Box::new(
                parse_create_index(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::TABLE) => Statement::CreateTable(Box::new(
                parse_create_table(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::MATERIALIZED) => {
                // MATERIALIZED VIEW
                let materialized_span = parser.consume_keyword(Keyword::MATERIALIZED)?;
                parser.postgres_only(&materialized_span);
                // Don't consume VIEW here, parse_create_view will do it
                create_options.push(CreateOption::Materialized(materialized_span));
                Statement::CreateView(Box::new(parse_create_view(
                    parser,
                    create_span,
                    create_options,
                )?))
            }
            Token::Ident(_, Keyword::VIEW) => Statement::CreateView(Box::new(parse_create_view(
                parser,
                create_span,
                create_options,
            )?)),
            Token::Ident(_, Keyword::DATABASE) => Statement::CreateDatabase(Box::new(
                parse_create_database(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::SCHEMA) => Statement::CreateSchema(Box::new(
                parse_create_schema(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::SEQUENCE) => Statement::CreateSequence(Box::new(
                parse_create_sequence(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::FUNCTION) => Statement::CreateFunction(Box::new(
                parse_create_function(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::TRIGGER) => Statement::CreateTrigger(Box::new(
                parse_create_trigger(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::TYPE) => Statement::CreateTypeEnum(Box::new(
                parse_create_type(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::ROLE) => Statement::CreateRole(Box::new(parse_create_role(
                parser,
                create_span,
                create_options,
            )?)),
            Token::Ident(_, Keyword::SERVER) => Statement::CreateServer(Box::new(
                parse_create_server(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::OPERATOR) => {
                let operator_span = parser.consume_keyword(Keyword::OPERATOR)?;
                match parser.token {
                    Token::Ident(_, Keyword::FAMILY) => {
                        // CREATE OPERATOR FAMILY
                        let family_span = parser.consume_keyword(Keyword::FAMILY)?;
                        parser.postgres_only(&family_span);
                        Statement::CreateOperatorFamily(Box::new(parse_create_operator_family(
                            parser,
                            create_span
                                .join_span(&operator_span)
                                .join_span(&family_span),
                            create_options,
                        )?))
                    }
                    Token::Ident(_, Keyword::CLASS) => {
                        // CREATE OPERATOR CLASS
                        Statement::CreateOperatorClass(Box::new(parse_create_operator_class(
                            parser,
                            create_span.join_span(&operator_span),
                            create_options,
                        )?))
                    }
                    _ => {
                        // CREATE OPERATOR
                        Statement::CreateOperator(Box::new(parse_create_operator(
                            parser,
                            create_span.join_span(&operator_span),
                            create_options,
                        )?))
                    }
                }
            }
            _ => return parser.expected_failure(CREATABLE),
        };
    Ok(r)
}
