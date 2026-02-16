use alloc::{boxed::Box, vec::Vec};

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
    alter::{
        ForeignKeyOn, ForeignKeyOnAction, ForeignKeyOnType, IndexCol, IndexOption, IndexType,
        parse_index_cols, parse_index_options,
    },
    data_type::parse_data_type,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
    statement::{parse_compound_query, parse_statement},
};

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

/// Special algorithm used for table creation
#[derive(Clone, Debug)]
pub enum CreateAlgorithm {
    Undefined(Span),
    Merge(Span),
    TempTable(Span),
}
impl Spanned for CreateAlgorithm {
    fn span(&self) -> Span {
        match &self {
            CreateAlgorithm::Undefined(s) => s.span(),
            CreateAlgorithm::Merge(s) => s.span(),
            CreateAlgorithm::TempTable(s) => s.span(),
        }
    }
}

/// Options for create statement
#[derive(Clone, Debug)]
pub enum CreateOption<'a> {
    OrReplace(Span),
    Temporary(Span),
    Unique(Span),
    Algorithm(Span, CreateAlgorithm),
    Definer {
        definer_span: Span,
        user: Identifier<'a>,
        host: Identifier<'a>,
    },
    SqlSecurityDefiner(Span, Span),
    SqlSecurityInvoker(Span, Span),
    SqlSecurityUser(Span, Span),
}
impl<'a> Spanned for CreateOption<'a> {
    fn span(&self) -> Span {
        match &self {
            CreateOption::OrReplace(v) => v.span(),
            CreateOption::Temporary(v) => v.span(),
            CreateOption::Algorithm(s, a) => s.join_span(a),
            CreateOption::Definer {
                definer_span,
                user,
                host,
            } => definer_span.join_span(user).join_span(host),
            CreateOption::SqlSecurityDefiner(a, b) => a.join_span(b),
            CreateOption::SqlSecurityInvoker(a, b) => a.join_span(b),
            CreateOption::SqlSecurityUser(a, b) => a.join_span(b),
            CreateOption::Unique(v) => v.span(),
        }
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
    }
}

/// Represent a create view statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, CreateView, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "CREATE ALGORITHM=UNDEFINED DEFINER=`phpmyadmin`@`localhost` SQL SECURITY DEFINER
///    VIEW `v1`
///    AS SELECT
///         `t1`.`id` AS `id`,
///         `t1`.`c1` AS `c1`,
///         (SELECT `t2`.`c2` FROM `t2` WHERE `t2`.`id` = `t1`.`c3`) AS `c2`
///         FROM `t1` WHERE `t1`.`deleted` IS NULL;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let create: CreateView = match stmts.pop() {
///     Some(Statement::CreateView(c)) => c,
///     _ => panic!("We should get an create view statement")
/// };
///
/// assert!(create.name.identifier.as_str() == "v1");
/// println!("{:#?}", create.select)
/// ```

#[derive(Clone, Debug)]
pub struct CreateView<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "VIEW"
    pub view_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the created view
    pub name: QualifiedName<'a>,
    /// Span of "AS"
    pub as_span: Span,
    /// The select statement following "AS"
    pub select: Box<Statement<'a>>,
}

impl<'a> Spanned for CreateView<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.view_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.as_span)
            .join_span(&self.select)
    }
}

/// Parse an index definition (PRIMARY KEY, UNIQUE, INDEX, KEY, FULLTEXT, SPATIAL)
fn parse_index_definition<'a>(
    parser: &mut Parser<'a, '_>,
    constraint_span: Option<Span>,
    constraint_symbol: Option<Identifier<'a>>,
) -> Result<CreateDefinition<'a>, ParseError> {
    // Parse index type
    let index_type = match &parser.token {
        Token::Ident(_, Keyword::PRIMARY) => {
            let span = parser.consume_keyword(Keyword::PRIMARY)?;
            parser.consume_keyword(Keyword::KEY)?;
            IndexType::Primary(span)
        }
        Token::Ident(_, Keyword::UNIQUE) => {
            let span = parser.consume_keyword(Keyword::UNIQUE)?;
            // UNIQUE can be followed by INDEX or KEY (optional)
            if parser.skip_keyword(Keyword::INDEX).is_some()
                || parser.skip_keyword(Keyword::KEY).is_some()
            {
                // consumed INDEX or KEY
            }
            IndexType::Unique(span)
        }
        Token::Ident(_, Keyword::FULLTEXT) => {
            let span = parser.consume_keyword(Keyword::FULLTEXT)?;
            // FULLTEXT can be followed by INDEX or KEY (optional)
            if parser.skip_keyword(Keyword::INDEX).is_some()
                || parser.skip_keyword(Keyword::KEY).is_some()
            {
                // consumed INDEX or KEY
            }
            IndexType::FullText(span)
        }
        Token::Ident(_, Keyword::SPATIAL) => {
            let span = parser.consume_keyword(Keyword::SPATIAL)?;
            // SPATIAL can be followed by INDEX or KEY (optional)
            if parser.skip_keyword(Keyword::INDEX).is_some()
                || parser.skip_keyword(Keyword::KEY).is_some()
            {
                // consumed INDEX or KEY
            }
            IndexType::Spatial(span)
        }
        Token::Ident(_, Keyword::INDEX) => {
            IndexType::Index(parser.consume_keyword(Keyword::INDEX)?)
        }
        Token::Ident(_, Keyword::KEY) => IndexType::Index(parser.consume_keyword(Keyword::KEY)?),
        _ => parser.expected_failure("PRIMARY, UNIQUE, INDEX, KEY, FULLTEXT, or SPATIAL")?,
    };

    // Parse optional index name (not for PRIMARY KEY)
    let index_name = match &index_type {
        IndexType::Primary(_) => {
            // PRIMARY KEY may optionally have a name before the column list
            if let Token::Ident(_, _) = parser.token {
                // Check if this is not a '(' which starts the column list
                if !matches!(parser.token, Token::LParen) {
                    Some(parser.consume_plain_identifier()?)
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => {
            // Other index types can optionally have a name
            if let Token::Ident(_, _) = parser.token {
                // Check if this is not a '(' which starts the column list
                if !matches!(parser.token, Token::LParen) {
                    Some(parser.consume_plain_identifier()?)
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    // Parse index columns
    let cols = parse_index_cols(parser)?;

    // Parse index options (USING, COMMENT, etc.)
    let mut index_options = Vec::new();
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

/// Parse a constraint definition (with CONSTRAINT keyword)
pub(crate) fn parse_create_constraint_definition<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<CreateDefinition<'a>, ParseError> {
    let constraint_span = parser.consume_keyword(Keyword::CONSTRAINT)?;

    // Parse optional constraint symbol (name)
    let constraint_symbol = if let Token::Ident(_, keyword) = parser.token {
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
    };

    // Dispatch to the appropriate parser based on the constraint type
    match &parser.token {
        Token::Ident(_, Keyword::PRIMARY) => {
            parse_index_definition(parser, Some(constraint_span), constraint_symbol)
        }
        Token::Ident(_, Keyword::UNIQUE) => {
            parse_index_definition(parser, Some(constraint_span), constraint_symbol)
        }
        Token::Ident(_, Keyword::FULLTEXT) => {
            parse_index_definition(parser, Some(constraint_span), constraint_symbol)
        }
        Token::Ident(_, Keyword::SPATIAL) => {
            parse_index_definition(parser, Some(constraint_span), constraint_symbol)
        }
        Token::Ident(_, Keyword::INDEX) | Token::Ident(_, Keyword::KEY) => {
            parse_index_definition(parser, Some(constraint_span), constraint_symbol)
        }
        Token::Ident(_, Keyword::FOREIGN) => {
            parse_foreign_key_definition(parser, Some(constraint_span), constraint_symbol)
        }
        Token::Ident(_, Keyword::CHECK) => {
            parse_check_constraint_definition(parser, Some(constraint_span), constraint_symbol)
        }
        _ => parser
            .expected_failure("PRIMARY, UNIQUE, INDEX, KEY, FULLTEXT, SPATIAL, FOREIGN, or CHECK"),
    }
}

pub(crate) fn parse_create_definition<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<CreateDefinition<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::CONSTRAINT) => parse_create_constraint_definition(parser),
        Token::Ident(_, Keyword::PRIMARY) => parse_index_definition(parser, None, None),
        Token::Ident(_, Keyword::UNIQUE) => parse_index_definition(parser, None, None),
        Token::Ident(_, Keyword::FULLTEXT) => parse_index_definition(parser, None, None),
        Token::Ident(_, Keyword::SPATIAL) => parse_index_definition(parser, None, None),
        Token::Ident(_, Keyword::INDEX) | Token::Ident(_, Keyword::KEY) => {
            parse_index_definition(parser, None, None)
        }
        Token::Ident(_, Keyword::FOREIGN) => parse_foreign_key_definition(parser, None, None),
        Token::Ident(_, Keyword::CHECK) => parse_check_constraint_definition(parser, None, None),
        Token::Ident(_, _) => Ok(CreateDefinition::ColumnDefinition {
            identifier: parser.consume_plain_identifier()?,
            data_type: parse_data_type(parser, false)?,
        }),
        _ => parser.expected_failure("identifier"),
    }
}

fn parse_create_view<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
    let view_span = parser.consume_keyword(Keyword::VIEW)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let name = parse_qualified_name(parser)?;
    // TODO (column_list)

    let as_span = parser.consume_keyword(Keyword::AS)?;

    let select = parse_compound_query(parser)?;

    // TODO [WITH [CASCADED | LOCAL] CHECK OPTION]

    Ok(Statement::CreateView(CreateView {
        create_span,
        create_options,
        view_span,
        if_not_exists,
        name,
        as_span,
        select: Box::new(select),
    }))
}

/// Characteristic of a function
#[derive(Clone, Debug)]
pub enum FunctionCharacteristic<'a> {
    LanguageSql(Span),
    LanguagePlpgsql(Span),
    NotDeterministic(Span),
    Deterministic(Span),
    ContainsSql(Span),
    NoSql(Span),
    ReadsSqlData(Span),
    ModifiesSqlData(Span),
    SqlSecurityDefiner(Span),
    SqlSecurityUser(Span),
    Comment(SString<'a>),
}

impl<'a> Spanned for FunctionCharacteristic<'a> {
    fn span(&self) -> Span {
        match &self {
            FunctionCharacteristic::LanguageSql(v) => v.span(),
            FunctionCharacteristic::NotDeterministic(v) => v.span(),
            FunctionCharacteristic::Deterministic(v) => v.span(),
            FunctionCharacteristic::ContainsSql(v) => v.span(),
            FunctionCharacteristic::NoSql(v) => v.span(),
            FunctionCharacteristic::ReadsSqlData(v) => v.span(),
            FunctionCharacteristic::ModifiesSqlData(v) => v.span(),
            FunctionCharacteristic::SqlSecurityDefiner(v) => v.span(),
            FunctionCharacteristic::SqlSecurityUser(v) => v.span(),
            FunctionCharacteristic::Comment(v) => v.span(),
            FunctionCharacteristic::LanguagePlpgsql(v) => v.span(),
        }
    }
}

/// Direction of a function argument
#[derive(Clone, Debug)]
pub enum FunctionParamDirection {
    In(Span),
    Out(Span),
    InOut(Span),
}

impl Spanned for FunctionParamDirection {
    fn span(&self) -> Span {
        match &self {
            FunctionParamDirection::In(v) => v.span(),
            FunctionParamDirection::Out(v) => v.span(),
            FunctionParamDirection::InOut(v) => v.span(),
        }
    }
}

/// Representation of Create Function Statement
///
/// This is not fully implemented yet
///
/// ```ignore
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, CreateFunction, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DELIMITER $$
/// CREATE FUNCTION add_func3(IN a INT, IN b INT, OUT c INT) RETURNS INT
/// BEGIN
///     SET c = 100;
///     RETURN a + b;
/// END;
/// $$
/// DELIMITER ;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// assert!(issues.is_empty());
/// #
/// let create: CreateFunction = match stmts.pop() {
///     Some(Statement::CreateFunction(c)) => c,
///     _ => panic!("We should get an create function statement")
/// };
///
/// assert!(create.name.as_str() == "add_func3");
/// println!("{:#?}", create.return_)
/// ```
#[derive(Clone, Debug)]
pub struct CreateFunction<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "FUNCTION"
    pub function_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name o created function
    pub name: Identifier<'a>,
    /// Names and types of function arguments
    pub params: Vec<(Option<FunctionParamDirection>, Identifier<'a>, DataType<'a>)>,
    /// Span of "RETURNS"
    pub returns_span: Span,
    /// Type of return value
    pub return_type: DataType<'a>,
    /// Characteristics of created function
    pub characteristics: Vec<FunctionCharacteristic<'a>>,
    /// Statement computing return value
    pub return_: Option<Box<Statement<'a>>>,
}

impl<'a> Spanned for CreateFunction<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.function_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.return_type)
            .join_span(&self.characteristics)
            .join_span(&self.return_)
    }
}

fn parse_create_function<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
    let function_span = parser.consume_keyword(Keyword::FUNCTION)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let name = parser.consume_plain_identifier()?;
    let mut params = Vec::new();
    parser.consume_token(Token::LParen)?;
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            let direction = match &parser.token {
                Token::Ident(_, Keyword::IN) => {
                    let in_ = parser.consume_keyword(Keyword::IN)?;
                    if let Some(out) = parser.skip_keyword(Keyword::OUT) {
                        Some(FunctionParamDirection::InOut(in_.join_span(&out)))
                    } else {
                        Some(FunctionParamDirection::In(in_))
                    }
                }
                Token::Ident(_, Keyword::OUT) => Some(FunctionParamDirection::Out(
                    parser.consume_keyword(Keyword::OUT)?,
                )),
                Token::Ident(_, Keyword::INOUT) => Some(FunctionParamDirection::InOut(
                    parser.consume_keyword(Keyword::INOUT)?,
                )),
                _ => None,
            };

            let name = parser.consume_plain_identifier()?;
            let type_ = parse_data_type(parser, false)?;
            params.push((direction, name, type_));
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    parser.consume_token(Token::RParen)?;
    let returns_span = parser.consume_keyword(Keyword::RETURNS)?;
    let return_type = parse_data_type(parser, true)?;
    if parser.options.dialect.is_postgresql() && parser.skip_keyword(Keyword::AS).is_some() {
        parser.consume_token(Token::DoubleDollar)?;
        loop {
            match &parser.token {
                Token::Eof | Token::DoubleDollar => {
                    parser.consume_token(Token::DoubleDollar)?;
                    break;
                }
                _ => {
                    parser.consume();
                }
            }
        }
    }

    let mut characteristics = Vec::new();
    loop {
        let f = match &parser.token {
            Token::Ident(_, Keyword::LANGUAGE) => {
                let lg = parser.consume();
                match &parser.token {
                    Token::Ident(_, Keyword::SQL) => {
                        FunctionCharacteristic::LanguageSql(lg.join_span(&parser.consume()))
                    }
                    Token::Ident(_, Keyword::PLPGSQL) => {
                        FunctionCharacteristic::LanguagePlpgsql(lg.join_span(&parser.consume()))
                    }
                    _ => parser.expected_failure("language name")?,
                }
            }
            Token::Ident(_, Keyword::NOT) => FunctionCharacteristic::NotDeterministic(
                parser.consume_keywords(&[Keyword::NOT, Keyword::DETERMINISTIC])?,
            ),
            Token::Ident(_, Keyword::DETERMINISTIC) => FunctionCharacteristic::Deterministic(
                parser.consume_keyword(Keyword::DETERMINISTIC)?,
            ),
            Token::Ident(_, Keyword::CONTAINS) => FunctionCharacteristic::ContainsSql(
                parser.consume_keywords(&[Keyword::CONTAINS, Keyword::SQL])?,
            ),
            Token::Ident(_, Keyword::NO) => FunctionCharacteristic::NoSql(
                parser.consume_keywords(&[Keyword::NO, Keyword::SQL])?,
            ),
            Token::Ident(_, Keyword::READS) => {
                FunctionCharacteristic::ReadsSqlData(parser.consume_keywords(&[
                    Keyword::READS,
                    Keyword::SQL,
                    Keyword::DATA,
                ])?)
            }
            Token::Ident(_, Keyword::MODIFIES) => {
                FunctionCharacteristic::ModifiesSqlData(parser.consume_keywords(&[
                    Keyword::MODIFIES,
                    Keyword::SQL,
                    Keyword::DATA,
                ])?)
            }
            Token::Ident(_, Keyword::COMMENT) => {
                parser.consume_keyword(Keyword::COMMENT)?;
                FunctionCharacteristic::Comment(parser.consume_string()?)
            }
            Token::Ident(_, Keyword::SQL) => {
                let span = parser.consume_keywords(&[Keyword::SQL, Keyword::SECURITY])?;
                match &parser.token {
                    Token::Ident(_, Keyword::DEFINER) => {
                        FunctionCharacteristic::SqlSecurityDefiner(
                            parser.consume_keyword(Keyword::DEFINER)?.join_span(&span),
                        )
                    }
                    Token::Ident(_, Keyword::USER) => FunctionCharacteristic::SqlSecurityUser(
                        parser.consume_keyword(Keyword::USER)?.join_span(&span),
                    ),
                    _ => parser.expected_failure("'DEFINER' or 'USER'")?,
                }
            }
            _ => break,
        };
        characteristics.push(f);
    }

    let return_ = if parser.options.dialect.is_maria() {
        match parse_statement(parser)? {
            Some(v) => Some(Box::new(v)),
            None => parser.expected_failure("statement")?,
        }
    } else {
        None
    };

    Ok(Statement::CreateFunction(CreateFunction {
        create_span,
        create_options,
        function_span,
        if_not_exists,
        name,
        params,
        return_type,
        characteristics,
        return_,
        returns_span,
    }))
}

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
    pub statement: Box<Statement<'a>>,
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

fn parse_create_trigger<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
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

    let name = parser.consume_plain_identifier()?;

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

    let table = parser.consume_plain_identifier()?;

    let for_each_row_span =
        parser.consume_keywords(&[Keyword::FOR, Keyword::EACH, Keyword::ROW])?;

    // TODO [{ FOLLOWS | PRECEDES } other_trigger_name ]

    let old = core::mem::replace(&mut parser.permit_compound_statements, true);
    let statement = match parse_statement(parser)? {
        Some(v) => v,
        None => parser.expected_failure("statement")?,
    };
    parser.permit_compound_statements = old;

    Ok(Statement::CreateTrigger(CreateTrigger {
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
        statement: Box::new(statement),
    }))
}

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
) -> Result<Statement<'a>, ParseError> {
    let type_span = parser.consume_keyword(Keyword::TYPE)?;
    if !parser.options.dialect.is_postgresql() {
        parser.err("CREATE TYPE only supported by postgresql", &type_span);
    }
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
    Ok(Statement::CreateTypeEnum(CreateTypeEnum {
        create_span,
        create_options,
        type_span,
        name,
        as_enum_span,
        values,
    }))
}

#[derive(Clone, Debug)]
pub enum CreateIndexOption<'a> {
    UsingGist(Span),
    UsingBTree(Span),
    UsingHash(Span),
    UsingRTree(Span),
    Algorithm(Span, Identifier<'a>),
    Lock(Span, Identifier<'a>),
}

impl<'a> Spanned for CreateIndexOption<'a> {
    fn span(&self) -> Span {
        match self {
            CreateIndexOption::UsingGist(s) => s.clone(),
            CreateIndexOption::UsingBTree(s) => s.clone(),
            CreateIndexOption::UsingHash(s) => s.clone(),
            CreateIndexOption::UsingRTree(s) => s.clone(),
            CreateIndexOption::Algorithm(s, i) => s.join_span(i),
            CreateIndexOption::Lock(s, i) => s.join_span(i),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateIndex<'a> {
    pub create_span: Span,
    pub create_options: Vec<CreateOption<'a>>,
    pub index_span: Span,
    pub index_name: Identifier<'a>,
    pub if_not_exists: Option<Span>,
    pub on_span: Span,
    pub table_name: QualifiedName<'a>,
    pub index_options: Vec<CreateIndexOption<'a>>,
    pub l_paren_span: Span,
    pub column_names: Vec<IndexCol<'a>>,
    pub r_paren_span: Span,
    pub where_: Option<(Span, Expression<'a>)>,
}

impl<'a> Spanned for CreateIndex<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.index_span)
            .join_span(&self.index_name)
            .join_span(&self.on_span)
            .join_span(&self.table_name)
            .join_span(&self.index_options)
            .join_span(&self.l_paren_span)
            .join_span(&self.column_names)
            .join_span(&self.r_paren_span)
            .join_span(&self.where_)
    }
}

fn parse_create_index<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
    let index_span = parser.consume_keyword(Keyword::INDEX)?;
    let if_not_exists = if let Some(s) = parser.skip_keyword(Keyword::IF) {
        Some(s.join_span(&parser.consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?))
    } else {
        None
    };
    let index_name = parser.consume_plain_identifier()?;
    let on_span = parser.consume_keyword(Keyword::ON)?;
    let table_name = parse_qualified_name(parser)?;

    // PostgreSQL: USING GIST before column list
    let mut index_options = Vec::new();
    if let Some(using_span) = parser.skip_keyword(Keyword::USING) {
        if let Token::Ident(_, Keyword::GIST) = &parser.token {
            let gist_span = parser.consume_keyword(Keyword::GIST)?;
            index_options.push(CreateIndexOption::UsingGist(
                using_span.join_span(&gist_span),
            ));
        } else {
            // Error - USING before column list requires GIST for PostgreSQL
            parser
                .err_here("Expected GIST after USING (or use USING after column list for MySQL)")?;
        }
    }

    let l_paren_span = parser.consume_token(Token::LParen)?;
    let mut column_names = Vec::new();
    loop {
        let name = parser.consume_plain_identifier()?;
        let size = if parser.skip_token(Token::LParen).is_some() {
            let size = parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                parser.consume_int()
            })?;
            parser.consume_token(Token::RParen)?;
            Some(size)
        } else {
            None
        };
        column_names.push(IndexCol { name, size });

        if let Token::Ident(
            _,
            Keyword::TEXT_PATTERN_OPS
            | Keyword::VARCHAR_PATTERN_OPS
            | Keyword::BPCHAR_PATTERN_OPS
            | Keyword::INT8_OPS
            | Keyword::INT4_OPS
            | Keyword::INT2_OPS,
        ) = &parser.token
        {
            let range = parser.consume();
            if !parser.options.dialect.is_postgresql() {
                parser.err("Opclasses not supporetd", &range);
            }
        }
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }

    let r_paren_span = parser.consume_token(Token::RParen)?;

    // Parse index options after column list (MySQL/MariaDB)
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::USING) => {
                let using_span = parser.consume_keyword(Keyword::USING)?;
                match &parser.token {
                    Token::Ident(_, Keyword::BTREE) => {
                        let btree_span = parser.consume_keyword(Keyword::BTREE)?;
                        index_options.push(CreateIndexOption::UsingBTree(
                            using_span.join_span(&btree_span),
                        ));
                    }
                    Token::Ident(_, Keyword::HASH) => {
                        let hash_span = parser.consume_keyword(Keyword::HASH)?;
                        index_options.push(CreateIndexOption::UsingHash(
                            using_span.join_span(&hash_span),
                        ));
                    }
                    Token::Ident(_, Keyword::RTREE) => {
                        let rtree_span = parser.consume_keyword(Keyword::RTREE)?;
                        index_options.push(CreateIndexOption::UsingRTree(
                            using_span.join_span(&rtree_span),
                        ));
                    }
                    _ => parser.err_here("Expected BTREE, HASH, or RTREE after USING")?,
                }
            }
            Token::Ident(_, Keyword::ALGORITHM) => {
                let algorithm_span = parser.consume_keyword(Keyword::ALGORITHM)?;
                parser.skip_token(Token::Eq); // Optional =
                let algorithm_value = parser.consume_plain_identifier()?;
                index_options.push(CreateIndexOption::Algorithm(
                    algorithm_span,
                    algorithm_value,
                ));
            }
            Token::Ident(_, Keyword::LOCK) => {
                let lock_span = parser.consume_keyword(Keyword::LOCK)?;
                parser.skip_token(Token::Eq); // Optional =
                let lock_value = parser.consume_plain_identifier()?;
                index_options.push(CreateIndexOption::Lock(lock_span, lock_value));
            }
            _ => break,
        }
    }

    let mut where_ = None;
    if let Some(where_span) = parser.skip_keyword(Keyword::WHERE) {
        let where_expr = parse_expression(parser, false)?;
        if parser.options.dialect.is_maria() {
            parser.err(
                "Partial indexes not supported",
                &where_span.join_span(&where_expr),
            );
        }
        where_ = Some((where_span, where_expr));
    }

    Ok(Statement::CreateIndex(CreateIndex {
        create_span,
        create_options,
        index_span,
        index_name,
        if_not_exists,
        on_span,
        table_name,
        index_options,
        l_paren_span,
        column_names,
        r_paren_span,
        where_,
    }))
}

fn parse_create_table<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
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

    Ok(Statement::CreateTable(CreateTable {
        create_span,
        create_options,
        table_span,
        identifier,
        if_not_exists,
        options,
        create_definitions,
    }))
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

fn parse_create_database<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
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

    Ok(Statement::CreateDatabase(CreateDatabase {
        create_span,
        create_options,
        database_span,
        if_not_exists,
        name,
    }))
}

pub(crate) fn parse_create<'a>(parser: &mut Parser<'a, '_>) -> Result<Statement<'a>, ParseError> {
    let create_span = parser.span.clone();
    parser.consume_keyword(Keyword::CREATE)?;

    let mut create_options = Vec::new();
    const CREATABLE: &str =
        "'TABLE' | 'VIEW' | 'TRIGGER' | 'FUNCTION' | 'INDEX' | 'TYPE' | 'DATABASE' | 'SCHEMA'";

    parser.recovered(
        CREATABLE,
        &|t| {
            matches!(
                t,
                Token::Ident(
                    _,
                    Keyword::TABLE
                        | Keyword::VIEW
                        | Keyword::TRIGGER
                        | Keyword::FUNCTION
                        | Keyword::INDEX
                        | Keyword::TYPE
                        | Keyword::DATABASE
                        | Keyword::SCHEMA
                )
            )
        },
        |parser| {
            loop {
                let v = match &parser.token {
                    Token::Ident(_, Keyword::OR) => CreateOption::OrReplace(
                        parser.consume_keywords(&[Keyword::OR, Keyword::REPLACE])?,
                    ),
                    Token::Ident(_, Keyword::TEMPORARY) => {
                        CreateOption::Temporary(parser.consume_keyword(Keyword::TEMPORARY)?)
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
                        let user = parser.consume_plain_identifier()?;
                        parser.consume_token(Token::At)?;
                        let host = parser.consume_plain_identifier()?;
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

    match &parser.token {
        Token::Ident(_, Keyword::INDEX) => parse_create_index(parser, create_span, create_options),
        Token::Ident(_, Keyword::TABLE) => parse_create_table(parser, create_span, create_options),
        Token::Ident(_, Keyword::VIEW) => parse_create_view(parser, create_span, create_options),
        Token::Ident(_, Keyword::DATABASE | Keyword::SCHEMA) => {
            parse_create_database(parser, create_span, create_options)
        }
        Token::Ident(_, Keyword::FUNCTION) => {
            parse_create_function(parser, create_span, create_options)
        }
        Token::Ident(_, Keyword::TRIGGER) => {
            parse_create_trigger(parser, create_span, create_options)
        }
        Token::Ident(_, Keyword::TYPE) => parse_create_type(parser, create_span, create_options),
        _ => parser.expected_failure(CREATABLE),
    }
}
