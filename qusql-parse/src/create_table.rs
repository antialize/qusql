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
    alter_table::{
        ForeignKeyOn, ForeignKeyOnAction, ForeignKeyOnType, IndexCol, IndexOption, IndexType,
        parse_index_cols, parse_index_options, parse_index_type,
    },
    create_option::CreateOption,
    data_type::parse_data_type,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
    statement::parse_compound_query,
};
use alloc::vec::Vec;

/// Options on created table
#[derive(Clone, Debug)]
pub enum TableOption<'a> {
    AutoExtendSize {
        identifier: Span,
        value: (usize, Span),
    },
    AutoIncrement {
        identifier: Span,
        value: (u64, Span),
    },
    AvgRowLength {
        identifier: Span,
        value: (usize, Span),
    },
    CharSet {
        identifier: Span,
        value: Identifier<'a>,
    },
    DefaultCharSet {
        identifier: Span,
        value: Identifier<'a>,
    },
    Checksum {
        identifier: Span,
        value: (bool, Span),
    },
    Collate {
        identifier: Span,
        value: Identifier<'a>,
    },
    DefaultCollate {
        identifier: Span,
        value: Identifier<'a>,
    },
    Comment {
        identifier: Span,
        value: SString<'a>,
    },
    Compression {
        identifier: Span,
        value: SString<'a>,
    },
    Connection {
        identifier: Span,
        value: SString<'a>,
    },
    DataDirectory {
        identifier: Span,
        value: SString<'a>,
    },
    IndexDirectory {
        identifier: Span,
        value: SString<'a>,
    },
    DelayKeyWrite {
        identifier: Span,
        value: (bool, Span),
    },
    Encryption {
        identifier: Span,
        value: (bool, Span),
    },
    Engine {
        identifier: Span,
        value: Identifier<'a>,
    },
    EngineAttribute {
        identifier: Span,
        value: SString<'a>,
    },
    InsertMethod {
        identifier: Span,
        value: Identifier<'a>,
    },
    KeyBlockSize {
        identifier: Span,
        value: (usize, Span),
    },
    MaxRows {
        identifier: Span,
        value: (usize, Span),
    },
    MinRows {
        identifier: Span,
        value: (usize, Span),
    },
    PackKeys {
        identifier: Span,
        value: (usize, Span),
    },
    Password {
        identifier: Span,
        value: SString<'a>,
    },
    RowFormat {
        identifier: Span,
        value: Identifier<'a>,
    },
    SecondaryEngineAttribute {
        identifier: Span,
        value: SString<'a>,
    },
    StartTransaction {
        identifier: Span,
    },
    StatsAutoRecalc {
        identifier: Span,
        value: (usize, Span),
    },
    StatsPersistent {
        identifier: Span,
        value: (usize, Span),
    },
    StatsSamplePages {
        identifier: Span,
        value: (usize, Span),
    },
    Storage {
        identifier: Span,
        value: Identifier<'a>,
    },
    Strict {
        identifier: Span,
    },
    Tablespace {
        identifier: Span,
        value: Identifier<'a>,
    },
    Union {
        identifier: Span,
        value: Vec<Identifier<'a>>,
    },
    Inherits {
        identifier: Span,
        value: Vec<QualifiedName<'a>>,
    },
}

impl<'a> Spanned for TableOption<'a> {
    fn span(&self) -> Span {
        match &self {
            TableOption::AutoExtendSize { identifier, value } => identifier.span().join_span(value),
            TableOption::AutoIncrement { identifier, value } => identifier.span().join_span(value),
            TableOption::AvgRowLength { identifier, value } => identifier.span().join_span(value),
            TableOption::CharSet { identifier, value } => identifier.span().join_span(value),
            TableOption::DefaultCharSet { identifier, value } => identifier.span().join_span(value),
            TableOption::Checksum { identifier, value } => identifier.span().join_span(value),
            TableOption::Collate { identifier, value } => identifier.span().join_span(value),
            TableOption::DefaultCollate { identifier, value } => identifier.span().join_span(value),
            TableOption::Comment { identifier, value } => identifier.span().join_span(value),
            TableOption::Compression { identifier, value } => identifier.span().join_span(value),
            TableOption::Connection { identifier, value } => identifier.span().join_span(value),
            TableOption::DataDirectory { identifier, value } => identifier.span().join_span(value),
            TableOption::IndexDirectory { identifier, value } => identifier.span().join_span(value),
            TableOption::DelayKeyWrite { identifier, value } => identifier.span().join_span(value),
            TableOption::Encryption { identifier, value } => identifier.span().join_span(value),
            TableOption::Engine { identifier, value } => identifier.span().join_span(value),
            TableOption::EngineAttribute { identifier, value } => {
                identifier.span().join_span(value)
            }
            TableOption::InsertMethod { identifier, value } => identifier.span().join_span(value),
            TableOption::KeyBlockSize { identifier, value } => identifier.span().join_span(value),
            TableOption::MaxRows { identifier, value } => identifier.span().join_span(value),
            TableOption::MinRows { identifier, value } => identifier.span().join_span(value),
            TableOption::PackKeys { identifier, value } => identifier.span().join_span(value),
            TableOption::Password { identifier, value } => identifier.span().join_span(value),
            TableOption::RowFormat { identifier, value } => identifier.span().join_span(value),
            TableOption::SecondaryEngineAttribute { identifier, value } => {
                identifier.span().join_span(value)
            }
            TableOption::StartTransaction { identifier } => identifier.span(),
            TableOption::StatsAutoRecalc { identifier, value } => {
                identifier.span().join_span(value)
            }
            TableOption::StatsPersistent { identifier, value } => {
                identifier.span().join_span(value)
            }
            TableOption::StatsSamplePages { identifier, value } => {
                identifier.span().join_span(value)
            }
            TableOption::Storage { identifier, value } => identifier.span().join_span(value),
            TableOption::Strict { identifier } => identifier.span(),
            TableOption::Tablespace { identifier, value } => identifier.span().join_span(value),
            TableOption::Union { identifier, value } => {
                if let Some(last) = value.last() {
                    identifier.span().join_span(last)
                } else {
                    identifier.span()
                }
            }
            TableOption::Inherits { identifier, value } => {
                if let Some(last) = value.last() {
                    identifier.span().join_span(last)
                } else {
                    identifier.span()
                }
            }
        }
    }
}

/// Definition in create table
#[derive(Clone, Debug)]
pub enum CreateDefinition<'a> {
    ColumnDefinition {
        /// Name of column
        identifier: Identifier<'a>,
        /// Datatype and options for column
        data_type: DataType<'a>,
    },
    /// Index definition (PRIMARY KEY, UNIQUE, INDEX, KEY, FULLTEXT, SPATIAL)
    IndexDefinition {
        /// Optional "CONSTRAINT" span
        constraint_span: Option<Span>,
        /// Optional constraint symbol
        constraint_symbol: Option<Identifier<'a>>,
        /// The type of index
        index_type: IndexType,
        /// Optional index name
        index_name: Option<Identifier<'a>>,
        /// Columns in the index
        cols: Vec<IndexCol<'a>>,
        /// Index options
        index_options: Vec<IndexOption<'a>>,
    },
    /// Foreign key definition
    ForeignKeyDefinition {
        /// Optional "CONSTRAINT" span
        constraint_span: Option<Span>,
        /// Optional constraint symbol
        constraint_symbol: Option<Identifier<'a>>,
        /// Span of "FOREIGN KEY"
        foreign_key_span: Span,
        /// Optional index name
        index_name: Option<Identifier<'a>>,
        /// Columns in this table
        cols: Vec<IndexCol<'a>>,
        /// Span of "REFERENCES"
        references_span: Span,
        /// Referenced table name
        references_table: Identifier<'a>,
        /// Referenced columns
        references_cols: Vec<Identifier<'a>>,
        /// ON UPDATE/DELETE actions
        ons: Vec<ForeignKeyOn>,
    },
    /// Check constraint definition
    CheckConstraintDefinition {
        /// Optional "CONSTRAINT" span
        constraint_span: Option<Span>,
        /// Optional constraint symbol
        constraint_symbol: Option<Identifier<'a>>,
        /// Span of "CHECK"
        check_span: Span,
        /// Check expression
        expression: Expression<'a>,
        /// Optional ENFORCED/NOT ENFORCED
        enforced: Option<(bool, Span)>,
    },
}

impl<'a> Spanned for CreateDefinition<'a> {
    fn span(&self) -> Span {
        match &self {
            CreateDefinition::ColumnDefinition {
                identifier,
                data_type,
            } => identifier.span().join_span(data_type),
            CreateDefinition::IndexDefinition {
                constraint_span,
                constraint_symbol,
                index_type,
                index_name,
                cols,
                index_options,
            } => index_type
                .span()
                .join_span(constraint_span)
                .join_span(constraint_symbol)
                .join_span(index_name)
                .join_span(cols)
                .join_span(index_options),
            CreateDefinition::ForeignKeyDefinition {
                constraint_span,
                constraint_symbol,
                foreign_key_span,
                index_name,
                cols,
                references_span,
                references_table,
                references_cols,
                ons,
            } => foreign_key_span
                .span()
                .join_span(constraint_span)
                .join_span(constraint_symbol)
                .join_span(index_name)
                .join_span(cols)
                .join_span(references_span)
                .join_span(references_table)
                .join_span(references_cols)
                .join_span(ons),
            CreateDefinition::CheckConstraintDefinition {
                constraint_span,
                constraint_symbol,
                check_span,
                expression,
                enforced,
            } => check_span
                .span()
                .join_span(constraint_span)
                .join_span(constraint_symbol)
                .join_span(expression)
                .join_span(enforced),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateTableAs<'a> {
    pub ignore_span: Option<Span>,
    pub replace_span: Option<Span>,
    pub as_span: Span,
    pub query: Statement<'a>,
}

impl Spanned for CreateTableAs<'_> {
    fn span(&self) -> Span {
        self.as_span
            .join_span(&self.replace_span)
            .join_span(&self.ignore_span)
            .join_span(&self.query)
    }
}

/// Represent a create table statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, CreateTable, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "CREATE TABLE `parts` (
///         `id` int(11) NOT NULL COMMENT 'THIS IS THE ID FIELD',
///         `hash` varchar(64) COLLATE utf8_bin NOT NULL,
///         `destination` varchar(64) COLLATE utf8_bin NOT NULL,
///         `part` varchar(64) COLLATE utf8_bin NOT NULL,
///         `success` tinyint(1) NOT NULL
///     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;";
///
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let create: CreateTable = match stmts.pop() {
///     Some(Statement::CreateTable(c)) => c,
///     _ => panic!("We should get an create table statement")
/// };
///
/// assert!(create.identifier.identifier.as_str() == "parts");
/// println!("{:#?}", create.create_definitions)
/// ```

#[derive(Clone, Debug)]
pub struct CreateTable<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options specified after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "TABLE"
    pub table_span: Span,
    /// Name of the table
    pub identifier: QualifiedName<'a>,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Definitions of table members
    pub create_definitions: Vec<CreateDefinition<'a>>,
    /// Options specified after the table creation
    pub options: Vec<TableOption<'a>>,
    /// Create table as
    pub table_as: Option<CreateTableAs<'a>>,
}

impl<'a> Spanned for CreateTable<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.table_span)
            .join_span(&self.identifier)
            .join_span(&self.if_not_exists)
            .join_span(&self.create_definitions)
            .join_span(&self.options)
            .join_span(&self.table_as)
    }
}

/// Parse a foreign key definition
fn parse_foreign_key_definition<'a>(
    parser: &mut Parser<'a, '_>,
    constraint_span: Option<Span>,
    constraint_symbol: Option<Identifier<'a>>,
) -> Result<CreateDefinition<'a>, ParseError> {
    let foreign_span = parser.consume_keyword(Keyword::FOREIGN)?;
    let key_span = parser.consume_keyword(Keyword::KEY)?;
    let foreign_key_span = foreign_span.join_span(&key_span);

    // Parse optional index name
    let index_name = if let Token::Ident(_, _) = parser.token {
        if !matches!(parser.token, Token::LParen) {
            Some(parser.consume_plain_identifier()?)
        } else {
            None
        }
    } else {
        None
    };

    // Parse columns
    let cols = parse_index_cols(parser)?;

    // Parse REFERENCES
    let references_span = parser.consume_keyword(Keyword::REFERENCES)?;
    let references_table = parser.consume_plain_identifier()?;

    // Parse referenced columns
    parser.consume_token(Token::LParen)?;
    let mut references_cols = Vec::new();
    loop {
        references_cols.push(parser.consume_plain_identifier()?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    parser.consume_token(Token::RParen)?;

    // Parse ON UPDATE/DELETE actions
    let mut ons = Vec::new();
    while parser.skip_keyword(Keyword::ON).is_some() {
        let on_type = match &parser.token {
            Token::Ident(_, Keyword::UPDATE) => {
                ForeignKeyOnType::Update(parser.consume_keyword(Keyword::UPDATE)?)
            }
            Token::Ident(_, Keyword::DELETE) => {
                ForeignKeyOnType::Delete(parser.consume_keyword(Keyword::DELETE)?)
            }
            _ => parser.expected_failure("UPDATE or DELETE")?,
        };

        let on_action = match &parser.token {
            Token::Ident(_, Keyword::CASCADE) => {
                ForeignKeyOnAction::Cascade(parser.consume_keyword(Keyword::CASCADE)?)
            }
            Token::Ident(_, Keyword::RESTRICT) => {
                ForeignKeyOnAction::Restrict(parser.consume_keyword(Keyword::RESTRICT)?)
            }
            Token::Ident(_, Keyword::SET) => {
                let set_span = parser.consume_keyword(Keyword::SET)?;
                if parser.skip_keyword(Keyword::NULL).is_some() {
                    ForeignKeyOnAction::SetNull(set_span)
                } else if parser.skip_keyword(Keyword::DEFAULT).is_some() {
                    ForeignKeyOnAction::SetDefault(set_span)
                } else {
                    parser.expected_failure("NULL or DEFAULT after SET")?
                }
            }
            Token::Ident(_, Keyword::NO) => {
                let no_span = parser.consume_keyword(Keyword::NO)?;
                parser.consume_keyword(Keyword::ACTION)?;
                ForeignKeyOnAction::NoAction(no_span)
            }
            _ => {
                parser.expected_failure("CASCADE, RESTRICT, SET NULL, SET DEFAULT, or NO ACTION")?
            }
        };

        ons.push(ForeignKeyOn {
            type_: on_type,
            action: on_action,
        });
    }

    Ok(CreateDefinition::ForeignKeyDefinition {
        constraint_span,
        constraint_symbol,
        foreign_key_span,
        index_name,
        cols,
        references_span,
        references_table,
        references_cols,
        ons,
    })
}

/// Parse a check constraint definition
fn parse_check_constraint_definition<'a>(
    parser: &mut Parser<'a, '_>,
    constraint_span: Option<Span>,
    constraint_symbol: Option<Identifier<'a>>,
) -> Result<CreateDefinition<'a>, ParseError> {
    let check_span = parser.consume_keyword(Keyword::CHECK)?;

    // Parse the check expression
    parser.consume_token(Token::LParen)?;
    let expression = parse_expression(parser, false)?;
    parser.consume_token(Token::RParen)?;

    // Parse optional ENFORCED / NOT ENFORCED
    // Note: ENFORCED keyword may not be in the keyword enum, so we skip this for now
    let enforced = None;

    Ok(CreateDefinition::CheckConstraintDefinition {
        constraint_span,
        constraint_symbol,
        check_span,
        expression,
        enforced,
    })
}

pub(crate) fn parse_create_definition<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<CreateDefinition<'a>, ParseError> {
    // Check for optional CONSTRAINT keyword
    let constraint_span = parser.skip_keyword(Keyword::CONSTRAINT);

    // Parse optional constraint symbol (name) if CONSTRAINT was present
    let constraint_symbol = if constraint_span.is_some() {
        if let Token::Ident(_, keyword) = parser.token {
            // Check if the next token is a constraint keyword, meaning no symbol was provided
            match keyword {
                Keyword::PRIMARY
                | Keyword::UNIQUE
                | Keyword::FULLTEXT
                | Keyword::SPATIAL
                | Keyword::INDEX
                | Keyword::KEY
                | Keyword::FOREIGN
                | Keyword::CHECK => None,
                _ => Some(parser.consume_plain_identifier()?),
            }
        } else {
            None
        }
    } else {
        None
    };

    let index_type = match &parser.token {
        Token::Ident(_, Keyword::PRIMARY) => {
            let span = parser.consume_keywords(&[Keyword::PRIMARY, Keyword::KEY])?;
            IndexType::Primary(span)
        }
        Token::Ident(_, Keyword::UNIQUE) => {
            let span = parser.consume_keyword(Keyword::UNIQUE)?;
            let span = if let Some(s) = parser.skip_keyword(Keyword::INDEX) {
                span.join_span(&s)
            } else if let Some(s) = parser.skip_keyword(Keyword::KEY) {
                span.join_span(&s)
            } else {
                span
            };
            IndexType::Unique(span)
        }
        Token::Ident(_, Keyword::FULLTEXT) => {
            let span = parser.consume_keyword(Keyword::FULLTEXT)?;
            let span = if let Some(s) = parser.skip_keyword(Keyword::INDEX) {
                span.join_span(&s)
            } else if let Some(s) = parser.skip_keyword(Keyword::KEY) {
                span.join_span(&s)
            } else {
                span
            };
            IndexType::FullText(span)
        }
        Token::Ident(_, Keyword::SPATIAL) => {
            let span = parser.consume_keyword(Keyword::SPATIAL)?;
            let span = if let Some(s) = parser.skip_keyword(Keyword::INDEX) {
                span.join_span(&s)
            } else if let Some(s) = parser.skip_keyword(Keyword::KEY) {
                span.join_span(&s)
            } else {
                span
            };
            IndexType::Spatial(span)
        }
        Token::Ident(_, Keyword::INDEX) => {
            IndexType::Index(parser.consume_keyword(Keyword::INDEX)?)
        }
        Token::Ident(_, Keyword::KEY) => IndexType::Index(parser.consume_keyword(Keyword::KEY)?),
        Token::Ident(_, Keyword::FOREIGN) => {
            return parse_foreign_key_definition(parser, constraint_span, constraint_symbol);
        }
        Token::Ident(_, Keyword::CHECK) => {
            return parse_check_constraint_definition(parser, constraint_span, constraint_symbol);
        }
        Token::Ident(_, _) => {
            // If we had CONSTRAINT keyword, this is an error
            if constraint_span.is_some() {
                parser.expected_failure(
                    "PRIMARY, UNIQUE, INDEX, KEY, FULLTEXT, SPATIAL, FOREIGN, or CHECK",
                )?
            }
            return Ok(CreateDefinition::ColumnDefinition {
                identifier: parser.consume_plain_identifier()?,
                data_type: parse_data_type(parser, false)?,
            });
        }
        _ => return parser.expected_failure("identifier"),
    };

    // Parse optional index name
    let index_name = match &index_type {
        IndexType::Primary(_) => {
            // PRIMARY KEY may optionally have a name before the column list
            match &parser.token {
                Token::Ident(_, _) if !matches!(parser.token, Token::LParen) => {
                    Some(parser.consume_plain_identifier()?)
                }
                Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => {
                    let val = *s;
                    let span = parser.consume();
                    Some(Identifier { value: val, span })
                }
                _ => None,
            }
        }
        _ => {
            // Other index types can optionally have a name
            match &parser.token {
                Token::Ident(_, _)
                    if !matches!(
                        parser.token,
                        Token::LParen | Token::Ident(_, Keyword::USING)
                    ) =>
                {
                    Some(parser.consume_plain_identifier()?)
                }
                Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => {
                    let val = *s;
                    let span = parser.consume();
                    Some(Identifier { value: val, span })
                }
                _ => None,
            }
        }
    };

    // Parse optional USING BTREE/HASH/RTREE before column list
    let mut index_options = Vec::new();
    if matches!(parser.token, Token::Ident(_, Keyword::USING)) {
        parse_index_type(parser, &mut index_options)?;
    }

    // Parse index columns
    let cols = parse_index_cols(parser)?;

    // Parse index options (USING, COMMENT, etc.) after column list
    parse_index_options(parser, &mut index_options)?;

    Ok(CreateDefinition::IndexDefinition {
        constraint_span,
        constraint_symbol,
        index_type,
        index_name,
        cols,
        index_options,
    })
}

pub(crate) fn parse_create_table<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateTable<'a>, ParseError> {
    let table_span = parser.consume_keyword(Keyword::TABLE)?;

    let mut identifier = QualifiedName {
        identifier: Identifier::new("", 0..0),
        prefix: Default::default(),
    };
    let mut if_not_exists = None;

    parser.recovered("'('", &|t| t == &Token::LParen, |parser| {
        if let Some(if_) = parser.skip_keyword(Keyword::IF) {
            if_not_exists = Some(
                if_.start
                    ..parser
                        .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                        .end,
            );
        }
        identifier = parse_qualified_name(parser)?;
        Ok(())
    })?;

    parser.consume_token(Token::LParen)?;

    let mut create_definitions = Vec::new();
    if !matches!(parser.token, Token::RParen) {
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    create_definitions.push(parse_create_definition(parser)?);
                    Ok(())
                },
            )?;
            if matches!(parser.token, Token::RParen) {
                break;
            }
            parser.consume_token(Token::Comma)?;
        }
    }
    parser.consume_token(Token::RParen)?;

    let mut options = Vec::new();
    let mut table_as: Option<CreateTableAs<'_>> = None;
    let delimiter = parser.delimiter.clone();
    parser.recovered(
        delimiter.name(),
        &|t| t == &Token::Eof || t == &delimiter,
        |parser| {
            loop {
                match &parser.token {
                    Token::Ident(_, Keyword::ENGINE) => {
                        let identifier = parser.consume_keyword(Keyword::ENGINE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Engine {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                    }
                    Token::Ident(_, Keyword::DEFAULT) => {
                        let default_span = parser.consume_keyword(Keyword::DEFAULT)?;
                        match &parser.token {
                            Token::Ident(_, Keyword::CHARSET) => {
                                let identifier = default_span
                                    .join_span(&parser.consume_keyword(Keyword::CHARSET)?);
                                parser.skip_token(Token::Eq);
                                options.push(TableOption::DefaultCharSet {
                                    identifier,
                                    value: parser.consume_plain_identifier()?,
                                });
                            }
                            Token::Ident(_, Keyword::COLLATE) => {
                                let identifier = default_span
                                    .join_span(&parser.consume_keyword(Keyword::COLLATE)?);
                                parser.skip_token(Token::Eq);
                                options.push(TableOption::DefaultCollate {
                                    identifier,
                                    value: parser.consume_plain_identifier()?,
                                });
                            }
                            _ => parser.expected_failure("'CHARSET' or 'COLLATE'")?,
                        }
                    }
                    Token::Ident(_, Keyword::CHARSET) => {
                        let identifier = parser.consume_keyword(Keyword::CHARSET)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::CharSet {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                    }
                    Token::Ident(_, Keyword::COLLATE) => {
                        let identifier = parser.consume_keyword(Keyword::COLLATE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Collate {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                    }
                    Token::Ident(_, Keyword::ROW_FORMAT) => {
                        let identifier = parser.consume_keyword(Keyword::ROW_FORMAT)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::RowFormat {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                        //TODO validate raw format is in the keyword set
                    }
                    Token::Ident(_, Keyword::KEY_BLOCK_SIZE) => {
                        let identifier = parser.consume_keywords(&[Keyword::KEY_BLOCK_SIZE])?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::KeyBlockSize {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::COMMENT) => {
                        let identifier = parser.consume_keyword(Keyword::COMMENT)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Comment {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::STRICT) => {
                        let identifier = parser.consume_keyword(Keyword::STRICT)?;
                        options.push(TableOption::Strict { identifier });
                    }
                    Token::Ident(_, Keyword::AUTO_INCREMENT) => {
                        let identifier = parser.consume_keyword(Keyword::AUTO_INCREMENT)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::AutoIncrement {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::DATA) => {
                        let identifier =
                            parser.consume_keywords(&[Keyword::DATA, Keyword::DIRECTORY])?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::DataDirectory {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::INDEX) => {
                        let identifier =
                            parser.consume_keywords(&[Keyword::INDEX, Keyword::DIRECTORY])?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::IndexDirectory {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::INSERT_METHOD) => {
                        let identifier = parser.consume_keyword(Keyword::INSERT_METHOD)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::InsertMethod {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                    }
                    Token::Ident(_, Keyword::PACK_KEYS) => {
                        let identifier = parser.consume_keyword(Keyword::PACK_KEYS)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::PackKeys {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::STATS_AUTO_RECALC) => {
                        let identifier = parser.consume_keyword(Keyword::STATS_AUTO_RECALC)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::StatsAutoRecalc {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::STATS_PERSISTENT) => {
                        let identifier = parser.consume_keyword(Keyword::STATS_PERSISTENT)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::StatsPersistent {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::STATS_SAMPLE_PAGES) => {
                        let identifier = parser.consume_keyword(Keyword::STATS_SAMPLE_PAGES)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::StatsSamplePages {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::DELAY_KEY_WRITE) => {
                        let identifier = parser.consume_keyword(Keyword::DELAY_KEY_WRITE)?;
                        parser.skip_token(Token::Eq);
                        let (val, span) = parser.consume_int::<usize>()?;
                        options.push(TableOption::DelayKeyWrite {
                            identifier,
                            value: (val != 0, span),
                        });
                    }
                    Token::Ident(_, Keyword::COMPRESSION) => {
                        let identifier = parser.consume_keyword(Keyword::COMPRESSION)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Compression {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::ENCRYPTION) => {
                        let identifier = parser.consume_keyword(Keyword::ENCRYPTION)?;
                        parser.skip_token(Token::Eq);
                        // ENCRYPTION can be 'Y'/'N' string or YES/NO keyword
                        let value = match &parser.token {
                            Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) => {
                                let s = parser.consume_string()?;
                                let is_yes = s.as_str().eq_ignore_ascii_case("y")
                                    || s.as_str().eq_ignore_ascii_case("yes");
                                (is_yes, s.span())
                            }
                            _ => {
                                let id = parser.consume_plain_identifier()?;
                                let is_yes = id.value.eq_ignore_ascii_case("yes");
                                (is_yes, id.span())
                            }
                        };
                        options.push(TableOption::Encryption { identifier, value });
                    }
                    Token::Ident(_, Keyword::MAX_ROWS) => {
                        let identifier = parser.consume_keyword(Keyword::MAX_ROWS)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::MaxRows {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::MIN_ROWS) => {
                        let identifier = parser.consume_keyword(Keyword::MIN_ROWS)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::MinRows {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::AUTOEXTEND_SIZE) => {
                        let identifier = parser.consume_keyword(Keyword::AUTOEXTEND_SIZE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::AutoExtendSize {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::AVG_ROW_LENGTH) => {
                        let identifier = parser.consume_keyword(Keyword::AVG_ROW_LENGTH)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::AvgRowLength {
                            identifier,
                            value: parser.consume_int()?,
                        });
                    }
                    Token::Ident(_, Keyword::CHECKSUM) => {
                        let identifier = parser.consume_keyword(Keyword::CHECKSUM)?;
                        parser.skip_token(Token::Eq);
                        let (val, span) = parser.consume_int::<usize>()?;
                        options.push(TableOption::Checksum {
                            identifier,
                            value: (val != 0, span),
                        });
                    }
                    Token::Ident(_, Keyword::CONNECTION) => {
                        let identifier = parser.consume_keyword(Keyword::CONNECTION)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Connection {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::ENGINE_ATTRIBUTE) => {
                        let identifier = parser.consume_keyword(Keyword::ENGINE_ATTRIBUTE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::EngineAttribute {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::PASSWORD) => {
                        let identifier = parser.consume_keyword(Keyword::PASSWORD)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Password {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::SECONDARY_ENGINE_ATTRIBUTE) => {
                        let identifier =
                            parser.consume_keyword(Keyword::SECONDARY_ENGINE_ATTRIBUTE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::SecondaryEngineAttribute {
                            identifier,
                            value: parser.consume_string()?,
                        });
                    }
                    Token::Ident(_, Keyword::START) => {
                        let identifier =
                            parser.consume_keywords(&[Keyword::START, Keyword::TRANSACTION])?;
                        options.push(TableOption::StartTransaction { identifier });
                    }
                    Token::Ident(_, Keyword::TABLESPACE) => {
                        let identifier = parser.consume_keyword(Keyword::TABLESPACE)?;
                        options.push(TableOption::Tablespace {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                    }
                    Token::Ident(_, Keyword::STORAGE) => {
                        let identifier = parser.consume_keyword(Keyword::STORAGE)?;
                        options.push(TableOption::Storage {
                            identifier,
                            value: parser.consume_plain_identifier()?,
                        });
                    }
                    Token::Ident(_, Keyword::UNION) => {
                        let identifier = parser.consume_keyword(Keyword::UNION)?;
                        parser.skip_token(Token::Eq);
                        parser.consume_token(Token::LParen)?;
                        let mut tables = Vec::new();
                        loop {
                            tables.push(parser.consume_plain_identifier()?);
                            if parser.skip_token(Token::Comma).is_none() {
                                break;
                            }
                        }
                        parser.consume_token(Token::RParen)?;
                        options.push(TableOption::Union {
                            identifier,
                            value: tables,
                        });
                    }
                    Token::Ident(_, Keyword::INHERITS) => {
                        let identifier = parser.consume_keyword(Keyword::INHERITS)?;
                        parser.postgres_only(&identifier);
                        parser.consume_token(Token::LParen)?;
                        let mut tables = Vec::new();
                        parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                            loop {
                                tables.push(parse_qualified_name(parser)?);
                                if parser.skip_token(Token::Comma).is_none() {
                                    break;
                                }
                            }
                            Ok(())
                        })?;
                        parser.consume_token(Token::RParen)?;
                        options.push(TableOption::Inherits {
                            identifier,
                            value: tables,
                        });
                    }
                    Token::Ident(_, Keyword::IGNORE)
                    | Token::Ident(_, Keyword::REPLACE)
                    | Token::Ident(_, Keyword::AS) => {
                        let ignore_span = parser.skip_keyword(Keyword::IGNORE);
                        let replace_span = parser.skip_keyword(Keyword::REPLACE);
                        let as_span = parser.consume_keyword(Keyword::AS)?;

                        if let Some(table_as) = &table_as {
                            parser.err("Multiple AS clauses not supported", table_as);
                        }

                        let query = parse_compound_query(parser)?;
                        table_as = Some(CreateTableAs {
                            as_span,
                            replace_span,
                            ignore_span,
                            query,
                        });
                    }
                    Token::Comma => {
                        parser.consume_token(Token::Comma)?;
                    }
                    t if t == &parser.delimiter => break,
                    Token::Eof => break,
                    _ => {
                        parser.expected_failure("table option or delimiter")?;
                    }
                }
            }
            Ok(())
        },
    )?;

    Ok(CreateTable {
        create_span,
        create_options,
        table_span,
        identifier,
        if_not_exists,
        options,
        create_definitions,
        table_as,
    })
}
