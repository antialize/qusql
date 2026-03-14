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
    DataType, Identifier, QualifiedName, SString, Span, Spanned, Statement,
    create_constraint_trigger::parse_create_constraint_trigger,
    create_function::parse_create_function,
    create_index::parse_create_index,
    create_option::{CreateAlgorithm, CreateOption},
    create_role::parse_create_role,
    create_table::parse_create_table_or_partition_of,
    create_trigger::parse_create_trigger,
    create_view::parse_create_view,
    data_type::{DataTypeContext, parse_data_type},
    expression::{Expression, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    operator::{parse_create_operator, parse_create_operator_class, parse_create_operator_family},
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
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
    let name = parser.consume_plain_identifier_unreserved()?;
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

/// CREATE EXTENSION statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateExtension<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "EXTENSION"
    pub extension_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the extension
    pub name: Identifier<'a>,
    /// Optional SCHEMA clause
    pub schema: Option<(Span, Identifier<'a>)>,
    /// Optional VERSION clause
    pub version: Option<(Span, SString<'a>)>,
    /// CASCADE option
    pub cascade: Option<Span>,
}

impl Spanned for CreateExtension<'_> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.extension_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.schema)
            .join_span(&self.version)
            .join_span(&self.cascade)
    }
}

fn parse_create_extension<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateExtension<'a>, ParseError> {
    let extension_span = parser.consume_keyword(Keyword::EXTENSION)?;
    parser.postgres_only(&extension_span);

    for option in create_options {
        parser.err("Not supported for CREATE EXTENSION", &option.span());
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

    let name = parser.consume_plain_identifier_unreserved()?;

    // Optional WITH
    parser.skip_keyword(Keyword::WITH);

    // Parse optional SCHEMA, VERSION, CASCADE (any order)
    let mut schema = None;
    let mut version = None;
    let mut cascade = None;
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::SCHEMA) => {
                let schema_span = parser.consume_keyword(Keyword::SCHEMA)?;
                let schema_name = parser.consume_plain_identifier_unreserved()?;
                schema = Some((schema_span, schema_name));
            }
            Token::Ident(_, Keyword::VERSION) => {
                let version_span = parser.consume_keyword(Keyword::VERSION)?;
                // Version can be identifier or string
                let version_value = match &parser.token {
                    Token::String(v, _) => SString::new((*v).into(), parser.consume()),
                    _ => {
                        let ident = parser.consume_plain_identifier_unreserved()?;
                        SString::new(ident.value.into(), ident.span)
                    }
                };
                version = Some((version_span, version_value));
            }
            Token::Ident(_, Keyword::CASCADE) => {
                cascade = Some(parser.consume_keyword(Keyword::CASCADE)?);
            }
            _ => break,
        }
    }

    Ok(CreateExtension {
        create_span,
        extension_span,
        if_not_exists,
        name,
        schema,
        version,
        cascade,
    })
}

/// CREATE DOMAIN statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateDomain<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "DOMAIN"
    pub domain_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the domain (optionally schema-qualified)
    pub name: QualifiedName<'a>,
    /// Underlying data type
    pub data_type: DataType<'a>,
    /// Optional COLLATE clause
    pub collate: Option<(Span, Identifier<'a>)>,
    /// Optional DEFAULT clause
    pub default: Option<(Span, Expression<'a>)>,
    /// List of domain constraints (CONSTRAINT name, NOT NULL, NULL, CHECK)
    pub constraints: Vec<DomainConstraint<'a>>,
}

impl<'a> Spanned for CreateDomain<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.domain_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.data_type)
            .join_span(&self.collate)
            .join_span(&self.default)
            .join_span(&self.constraints)
    }
}

/// Domain constraint for CREATE DOMAIN
#[derive(Clone, Debug)]
pub enum DomainConstraint<'a> {
    ConstraintName(Span, Identifier<'a>),
    NotNull(Span),
    Null(Span),
    Check(Span, Expression<'a>),
}

impl<'a> Spanned for DomainConstraint<'a> {
    fn span(&self) -> Span {
        match self {
            DomainConstraint::ConstraintName(span, name) => span.join_span(name),
            DomainConstraint::NotNull(span) => span.clone(),
            DomainConstraint::Null(span) => span.clone(),
            DomainConstraint::Check(span, expr) => span.join_span(expr),
        }
    }
}

/// Parse CREATE DOMAIN statement (PostgreSQL)
pub fn parse_create_domain<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateDomain<'a>, ParseError> {
    let domain_span = parser.consume_keyword(Keyword::DOMAIN)?;
    parser.postgres_only(&domain_span);

    for option in create_options {
        parser.err("Not supported for CREATE DOMAIN", &option.span());
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

    let name = parse_qualified_name_unreserved(parser)?;

    // Optional AS
    parser.skip_keyword(Keyword::AS);

    let data_type = parse_data_type(parser, DataTypeContext::Column)?;

    // Optional COLLATE
    let collate = if let Some(collate_span) = parser.skip_keyword(Keyword::COLLATE) {
        let collate_name = parser.consume_plain_identifier_unreserved()?;
        Some((collate_span, collate_name))
    } else {
        None
    };

    // Optional DEFAULT
    let default = if let Some(default_span) = parser.skip_keyword(Keyword::DEFAULT) {
        let expr = parse_expression_unreserved(parser, false)?;
        Some((default_span, expr))
    } else {
        None
    };

    // Parse domain constraints
    let mut constraints = Vec::new();
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::CONSTRAINT) => {
                let constraint_span = parser.consume_keyword(Keyword::CONSTRAINT)?;
                let name = parser.consume_plain_identifier_unreserved()?;
                constraints.push(DomainConstraint::ConstraintName(constraint_span, name));
            }
            Token::Ident(_, Keyword::NOT) => {
                let not_span = parser.consume_keyword(Keyword::NOT)?;
                let null_span = parser.consume_keyword(Keyword::NULL)?;
                constraints.push(DomainConstraint::NotNull(not_span.join_span(&null_span)));
            }
            Token::Ident(_, Keyword::NULL) => {
                let null_span = parser.consume_keyword(Keyword::NULL)?;
                constraints.push(DomainConstraint::Null(null_span));
            }
            Token::Ident(_, Keyword::CHECK) => {
                let check_span = parser.consume_keyword(Keyword::CHECK)?;
                parser.consume_token(Token::LParen)?;
                let expr = parse_expression_unreserved(parser, false)?;
                parser.consume_token(Token::RParen)?;
                constraints.push(DomainConstraint::Check(check_span, expr));
            }
            _ => break,
        }
    }

    Ok(CreateDomain {
        create_span,
        domain_span,
        if_not_exists,
        name,
        data_type,
        collate,
        default,
        constraints,
    })
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
    As {
        as_span: Span,
        data_type: DataType<'a>,
    },
    /// INCREMENT [BY] value
    IncrementBy {
        increment_span: Span,
        by_span: Option<Span>,
        value: i64,
        value_span: Span,
    },
    /// MINVALUE value
    MinValue {
        minvalue_span: Span,
        value: i64,
        value_span: Span,
    },
    /// NO MINVALUE
    NoMinValue(Span),
    /// MAXVALUE value
    MaxValue {
        maxvalue_span: Span,
        value: i64,
        value_span: Span,
    },
    /// NO MAXVALUE
    NoMaxValue(Span),
    /// START [WITH] value
    StartWith {
        start_span: Span,
        with_span: Option<Span>,
        value: i64,
        value_span: Span,
    },
    /// CACHE value
    Cache {
        cache_span: Span,
        value: i64,
        value_span: Span,
    },
    /// CYCLE
    Cycle(Span),
    /// NO CYCLE
    NoCycle(Span),
    /// OWNED BY table.column
    OwnedBy {
        owned_span: Span,
        by_span: Span,
        table_column: QualifiedName<'a>,
    },
    /// OWNED BY NONE
    OwnedByNone {
        owned_span: Span,
        by_span: Span,
        none_span: Span,
    },
}

impl<'a> Spanned for SequenceOption<'a> {
    fn span(&self) -> Span {
        match self {
            SequenceOption::As { as_span, data_type } => as_span.join_span(data_type),
            SequenceOption::IncrementBy {
                increment_span,
                value_span,
                ..
            } => increment_span.join_span(value_span),
            SequenceOption::MinValue {
                minvalue_span,
                value_span,
                ..
            } => minvalue_span.join_span(value_span),
            SequenceOption::NoMinValue(s) => s.span(),
            SequenceOption::MaxValue {
                maxvalue_span,
                value_span,
                ..
            } => maxvalue_span.join_span(value_span),
            SequenceOption::NoMaxValue(s) => s.span(),
            SequenceOption::StartWith {
                start_span,
                value_span,
                ..
            } => start_span.join_span(value_span),
            SequenceOption::Cache {
                cache_span,
                value_span,
                ..
            } => cache_span.join_span(value_span),
            SequenceOption::Cycle(s) => s.span(),
            SequenceOption::NoCycle(s) => s.span(),
            SequenceOption::OwnedBy {
                owned_span,
                table_column,
                ..
            } => owned_span.join_span(table_column),
            SequenceOption::OwnedByNone {
                owned_span,
                none_span,
                ..
            } => owned_span.join_span(none_span),
        }
    }
}

/// Parse sequence options (used by CREATE SEQUENCE and ALTER TABLE ADD GENERATED AS IDENTITY)
pub(crate) fn parse_sequence_options<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<SequenceOption<'a>>, ParseError> {
    let mut options = Vec::new();
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::AS) => {
                let as_span = parser.consume_keyword(Keyword::AS)?;
                let data_type = parse_data_type(parser, DataTypeContext::TypeRef)?;
                options.push(SequenceOption::As { as_span, data_type });
            }
            Token::Ident(_, Keyword::INCREMENT) => {
                let increment_span = parser.consume_keyword(Keyword::INCREMENT)?;
                let by_span = parser.skip_keyword(Keyword::BY); // BY is optional
                let (value, value_span) = parser.consume_signed_int::<i64>()?;
                options.push(SequenceOption::IncrementBy {
                    increment_span,
                    by_span,
                    value,
                    value_span,
                });
            }
            Token::Ident(_, Keyword::MINVALUE) => {
                let minvalue_span = parser.consume_keyword(Keyword::MINVALUE)?;
                let (value, value_span) = parser.consume_signed_int::<i64>()?;
                options.push(SequenceOption::MinValue {
                    minvalue_span,
                    value,
                    value_span,
                });
            }
            Token::Ident(_, Keyword::MAXVALUE) => {
                let maxvalue_span = parser.consume_keyword(Keyword::MAXVALUE)?;
                let (value, value_span) = parser.consume_signed_int::<i64>()?;
                options.push(SequenceOption::MaxValue {
                    maxvalue_span,
                    value,
                    value_span,
                });
            }
            Token::Ident(_, Keyword::START) => {
                let start_span = parser.consume_keyword(Keyword::START)?;
                let with_span = parser.skip_keyword(Keyword::WITH); // WITH is optional
                let (value, value_span) = parser.consume_signed_int::<i64>()?;
                options.push(SequenceOption::StartWith {
                    start_span,
                    with_span,
                    value,
                    value_span,
                });
            }
            Token::Ident(_, Keyword::CACHE) => {
                let cache_span = parser.consume_keyword(Keyword::CACHE)?;
                let (value, value_span) = parser.consume_signed_int::<i64>()?;
                options.push(SequenceOption::Cache {
                    cache_span,
                    value,
                    value_span,
                });
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
                let by_span = parser.consume_keyword(Keyword::BY)?;
                if let Token::Ident(_, Keyword::NONE) = parser.token {
                    let none_span = parser.consume_keyword(Keyword::NONE)?;
                    options.push(SequenceOption::OwnedByNone {
                        owned_span,
                        by_span,
                        none_span,
                    });
                } else {
                    let table_column = parse_qualified_name_unreserved(parser)?;
                    options.push(SequenceOption::OwnedBy {
                        owned_span,
                        by_span,
                        table_column,
                    });
                }
            }
            _ => break,
        }
    }
    Ok(options)
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
    let name = parser.consume_plain_identifier_unreserved()?;
    loop {
        let default_span = parser.skip_keyword(Keyword::DEFAULT);
        match &parser.token {
            Token::Ident(_, Keyword::CHARSET) => {
                let identifier = parser.consume_keyword(Keyword::CHARSET)?;
                parser.skip_token(Token::Eq);
                create_options.push(CreateDatabaseOption::CharSet {
                    default_span,
                    identifier,
                    value: parser.consume_plain_identifier_unreserved()?,
                });
            }
            Token::Ident(_, Keyword::CHARACTER) => {
                let identifier = parser.consume_keywords(&[Keyword::CHARACTER, Keyword::SET])?;
                parser.skip_token(Token::Eq);
                create_options.push(CreateDatabaseOption::CharSet {
                    default_span,
                    identifier,
                    value: parser.consume_plain_identifier_unreserved()?,
                });
            }
            Token::Ident(_, Keyword::COLLATE) => {
                let identifier = parser.consume_keyword(Keyword::COLLATE)?;
                parser.skip_token(Token::Eq);
                create_options.push(CreateDatabaseOption::Collate {
                    default_span,
                    identifier,
                    value: parser.consume_plain_identifier_unreserved()?,
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
    let name = if matches!(parser.token, Token::Ident(_, Keyword::AUTHORIZATION)) {
        None
    } else {
        Some(parser.consume_plain_identifier_unreserved()?)
    };

    let authorization = if let Token::Ident(_, Keyword::AUTHORIZATION) = parser.token {
        let auth_span = parser.consume_keyword(Keyword::AUTHORIZATION)?;
        let role_name = parser.consume_plain_identifier_unreserved()?;
        Some((auth_span, role_name))
    } else {
        None
    };

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
    let name = parse_qualified_name_unreserved(parser)?;

    // Parse sequence options
    let options = parse_sequence_options(parser)?;

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
    let server_name = parser.consume_plain_identifier_unreserved()?;

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
    let fdw_name = parser.consume_plain_identifier_unreserved()?;
    let foreign_data_wrapper = (fdw_span, fdw_name);

    // Parse optional OPTIONS (option 'value', ...)
    let mut options = Vec::new();
    if parser.skip_keyword(Keyword::OPTIONS).is_some() {
        parser.consume_token(Token::LParen)?;
        loop {
            let option_name = parser.consume_plain_identifier_unreserved()?;
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
    const CREATABLE: &str = "'TABLE' | 'VIEW' | 'TRIGGER' | 'FUNCTION' | 'INDEX' | 'TYPE' | 'DATABASE' | 'DOMAIN' |'EXTENSION' | 'SCHEMA' | 'SEQUENCE' | 'ROLE' | 'SERVER' | 'OPERATOR' | 'CONSTRAINT'";

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
                        | Keyword::EXTENSION
                        | Keyword::DOMAIN
                        | Keyword::CONSTRAINT
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
                            Token::String(v, _) => {
                                let v = *v;
                                Identifier::new(v, parser.consume())
                            }
                            _ => parser.consume_plain_identifier_unreserved()?,
                        };
                        parser.consume_token(Token::At)?;
                        let host = match &parser.token {
                            Token::String(v, _) => {
                                let v = *v;
                                Identifier::new(v, parser.consume())
                            }
                            _ => parser.consume_plain_identifier_unreserved()?,
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
            Token::Ident(_, Keyword::TABLE) => {
                parse_create_table_or_partition_of(parser, create_span, create_options)?
            }
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
            Token::Ident(_, Keyword::CONSTRAINT) => Statement::CreateConstraintTrigger(Box::new(
                parse_create_constraint_trigger(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::DATABASE) => Statement::CreateDatabase(Box::new(
                parse_create_database(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::DOMAIN) => Statement::CreateDomain(Box::new(
                parse_create_domain(parser, create_span, create_options)?,
            )),
            Token::Ident(_, Keyword::EXTENSION) => Statement::CreateExtension(Box::new(
                parse_create_extension(parser, create_span, create_options)?,
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
