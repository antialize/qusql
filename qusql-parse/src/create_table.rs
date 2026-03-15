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
        ForeignKeyMatch, ForeignKeyOn, ForeignKeyOnAction, ForeignKeyOnType, IndexCol, IndexOption,
        IndexType, parse_index_cols, parse_index_options, parse_index_type,
    },
    create_option::CreateOption,
    data_type::{DataTypeContext, parse_data_type},
    expression::parse_expression_unreserved,
    keywords::{Keyword, Restrict},
    lexer::{StringType, Token},
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
    statement::parse_compound_query,
};
use alloc::boxed::Box;
use alloc::vec::Vec;

/// Action for ON COMMIT clause on temporary tables
#[derive(Clone, Debug)]
pub enum OnCommitAction {
    PreserveRows(Span),
    DeleteRows(Span),
    Drop(Span),
}

impl Spanned for OnCommitAction {
    fn span(&self) -> Span {
        match self {
            OnCommitAction::PreserveRows(s) => s.span(),
            OnCommitAction::DeleteRows(s) => s.span(),
            OnCommitAction::Drop(s) => s.span(),
        }
    }
}

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
    /// PostgreSQL WITH (storage_parameter = value, ...) table options
    WithOptions {
        identifier: Span,
        options: Vec<(Identifier<'a>, Expression<'a>)>,
    },
    /// PostgreSQL ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP }
    OnCommit {
        identifier: Span,
        action: OnCommitAction,
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
            TableOption::WithOptions {
                identifier,
                options,
            } => {
                if let Some((_, last)) = options.last() {
                    identifier.span().join_span(last)
                } else {
                    identifier.span()
                }
            }
            TableOption::OnCommit { identifier, action } => identifier.span().join_span(action),
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
        /// Optional MATCH FULL / MATCH SIMPLE / MATCH PARTIAL
        match_type: Option<ForeignKeyMatch>,
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
                match_type,
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
                .join_span(match_type)
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

/// The partitioning method for PARTITION BY
#[derive(Clone, Debug)]
pub enum PartitionMethod {
    Range(Span),
    List(Span),
    Hash(Span),
}

impl Spanned for PartitionMethod {
    fn span(&self) -> Span {
        match self {
            PartitionMethod::Range(s) => s.span(),
            PartitionMethod::List(s) => s.span(),
            PartitionMethod::Hash(s) => s.span(),
        }
    }
}

/// PARTITION BY clause, appended to a CREATE TABLE statement
#[derive(Clone, Debug)]
pub struct PartitionBy<'a> {
    /// Span of "PARTITION BY"
    pub partition_by_span: Span,
    /// The partitioning method: RANGE, LIST, or HASH
    pub method: PartitionMethod,
    /// The partition key expressions
    pub keys: Vec<Expression<'a>>,
}

impl<'a> Spanned for PartitionBy<'a> {
    fn span(&self) -> Span {
        self.partition_by_span
            .join_span(&self.method)
            .join_span(&self.keys)
    }
}

/// A single value in a partition bound specification: an expression, MINVALUE, or MAXVALUE
#[derive(Clone, Debug)]
pub enum PartitionBoundExpr<'a> {
    Expr(Expression<'a>),
    MinValue(Span),
    MaxValue(Span),
}

impl<'a> Spanned for PartitionBoundExpr<'a> {
    fn span(&self) -> Span {
        match self {
            PartitionBoundExpr::Expr(e) => e.span(),
            PartitionBoundExpr::MinValue(s) => s.span(),
            PartitionBoundExpr::MaxValue(s) => s.span(),
        }
    }
}

/// The partition bound specification for CREATE TABLE ... PARTITION OF
#[derive(Clone, Debug)]
pub enum PartitionBoundSpec<'a> {
    /// FOR VALUES IN (expr [, ...]) — list partitioning
    In {
        in_span: Span,
        values: Vec<PartitionBoundExpr<'a>>,
    },
    /// FOR VALUES FROM (...) TO (...) — range partitioning
    FromTo {
        from_span: Span,
        from_values: Vec<PartitionBoundExpr<'a>>,
        to_span: Span,
        to_values: Vec<PartitionBoundExpr<'a>>,
    },
    /// FOR VALUES WITH (MODULUS n, REMAINDER r) — hash partitioning
    WithModulusRemainder {
        with_span: Span,
        modulus_span: Span,
        modulus: (u64, Span),
        remainder_span: Span,
        remainder: (u64, Span),
    },
}

impl<'a> Spanned for PartitionBoundSpec<'a> {
    fn span(&self) -> Span {
        match self {
            PartitionBoundSpec::In { in_span, values } => in_span.join_span(values),
            PartitionBoundSpec::FromTo {
                from_span,
                from_values: _,
                to_span,
                to_values,
            } => from_span.join_span(to_span).join_span(to_values),
            PartitionBoundSpec::WithModulusRemainder {
                with_span,
                modulus_span,
                modulus,
                remainder_span,
                remainder,
            } => with_span
                .join_span(modulus_span)
                .join_span(modulus)
                .join_span(remainder_span)
                .join_span(remainder),
        }
    }
}

/// The bound clause of a PARTITION OF statement
#[derive(Clone, Debug)]
pub enum PartitionOfBound<'a> {
    /// FOR VALUES partition_bound_spec
    ForValues {
        for_values_span: Span,
        spec: PartitionBoundSpec<'a>,
    },
    /// DEFAULT partition
    Default(Span),
}

impl<'a> Spanned for PartitionOfBound<'a> {
    fn span(&self) -> Span {
        match self {
            PartitionOfBound::ForValues {
                for_values_span,
                spec,
            } => for_values_span.join_span(spec),
            PartitionOfBound::Default(s) => s.span(),
        }
    }
}

/// CREATE TABLE name PARTITION OF parent_table (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateTablePartitionOf<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options specified after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "TABLE"
    pub table_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the new partition table
    pub identifier: QualifiedName<'a>,
    /// Span of "PARTITION OF"
    pub partition_of_span: Span,
    /// The parent partitioned table name
    pub parent_table: QualifiedName<'a>,
    /// Optional column definitions / constraint overrides
    pub create_definitions: Vec<CreateDefinition<'a>>,
    /// FOR VALUES ... | DEFAULT
    pub bound: PartitionOfBound<'a>,
    /// Optional sub-partitioning specification
    pub partition_by: Option<PartitionBy<'a>>,
}

impl<'a> Spanned for CreateTablePartitionOf<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.table_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.identifier)
            .join_span(&self.partition_of_span)
            .join_span(&self.parent_table)
            .join_span(&self.create_definitions)
            .join_span(&self.bound)
            .join_span(&self.partition_by)
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
///     Some(Statement::CreateTable(c)) => *c,
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
    /// Optional PARTITION BY clause (PostgreSQL declarative partitioning)
    pub partition_by: Option<PartitionBy<'a>>,
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
            .join_span(&self.partition_by)
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
            Some(parser.consume_plain_identifier_unreserved()?)
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
    let references_table = parser.consume_plain_identifier_unreserved()?;

    // Parse referenced columns
    parser.consume_token(Token::LParen)?;
    let mut references_cols = Vec::new();
    loop {
        references_cols.push(parser.consume_plain_identifier_unreserved()?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    parser.consume_token(Token::RParen)?;

    let match_type = if parser.skip_keyword(Keyword::MATCH).is_some() {
        match &parser.token {
            Token::Ident(_, Keyword::FULL) => Some(ForeignKeyMatch::Full(parser.consume())),
            Token::Ident(_, Keyword::SIMPLE) => Some(ForeignKeyMatch::Simple(parser.consume())),
            Token::Ident(_, Keyword::PARTIAL) => Some(ForeignKeyMatch::Partial(parser.consume())),
            _ => None,
        }
    } else {
        None
    };

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
        match_type,
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
    let expression = parse_expression_unreserved(parser, false)?;
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
                _ => Some(parser.consume_plain_identifier_unreserved()?),
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
        Token::String(_, StringType::DoubleQuoted) if parser.options.dialect.is_postgresql() => {
            // PostgreSQL allows double-quoted identifiers as column names
            // If we had CONSTRAINT keyword, this is an error
            if constraint_span.is_some() {
                parser.expected_failure(
                    "PRIMARY, UNIQUE, INDEX, KEY, FULLTEXT, SPATIAL, FOREIGN, or CHECK",
                )?
            }
            return Ok(CreateDefinition::ColumnDefinition {
                identifier: parser.consume_plain_identifier_unreserved()?,
                data_type: parse_data_type(parser, DataTypeContext::Column)?,
            });
        }
        Token::Ident(_, _) => {
            // If we had CONSTRAINT keyword, this is an error
            if constraint_span.is_some() {
                parser.expected_failure(
                    "PRIMARY, UNIQUE, INDEX, KEY, FULLTEXT, SPATIAL, FOREIGN, or CHECK",
                )?
            }
            return Ok(CreateDefinition::ColumnDefinition {
                identifier: parser.consume_plain_identifier_unreserved()?,
                data_type: parse_data_type(parser, DataTypeContext::Column)?,
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
                    Some(parser.consume_plain_identifier_unreserved()?)
                }
                Token::String(s, _) => {
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
                    Some(parser.consume_plain_identifier_restrict(Restrict::USING)?)
                }
                Token::String(s, _) => {
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

/// Parse PARTITION BY RANGE|LIST|HASH (key [, ...])
fn parse_partition_by<'a>(parser: &mut Parser<'a, '_>) -> Result<PartitionBy<'a>, ParseError> {
    let partition_span = parser.consume_keyword(Keyword::PARTITION)?;
    let by_span = parser.consume_keyword(Keyword::BY)?;
    let partition_by_span = partition_span.join_span(&by_span);

    let method = match &parser.token {
        Token::Ident(_, Keyword::RANGE) => {
            PartitionMethod::Range(parser.consume_keyword(Keyword::RANGE)?)
        }
        Token::Ident(_, Keyword::LIST) => {
            PartitionMethod::List(parser.consume_keyword(Keyword::LIST)?)
        }
        Token::Ident(_, Keyword::HASH) => {
            PartitionMethod::Hash(parser.consume_keyword(Keyword::HASH)?)
        }
        _ => parser.expected_failure("RANGE, LIST, or HASH")?,
    };

    parser.consume_token(Token::LParen)?;
    let mut keys = Vec::new();
    loop {
        // Key element is either a parenthesised expression or a bare column/expression
        let key = if matches!(parser.token, Token::LParen) {
            parser.consume_token(Token::LParen)?;
            let expr = parse_expression_unreserved(parser, false)?;
            parser.consume_token(Token::RParen)?;
            expr
        } else {
            parse_expression_unreserved(parser, false)?
        };
        keys.push(key);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    parser.consume_token(Token::RParen)?;

    Ok(PartitionBy {
        partition_by_span,
        method,
        keys,
    })
}

/// Parse a single partition bound expression: value expression, MINVALUE, or MAXVALUE
fn parse_partition_bound_expr<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<PartitionBoundExpr<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::MINVALUE) => Ok(PartitionBoundExpr::MinValue(
            parser.consume_keyword(Keyword::MINVALUE)?,
        )),
        Token::Ident(_, Keyword::MAXVALUE) => Ok(PartitionBoundExpr::MaxValue(
            parser.consume_keyword(Keyword::MAXVALUE)?,
        )),
        _ => Ok(PartitionBoundExpr::Expr(parse_expression_unreserved(
            parser, false,
        )?)),
    }
}

/// Parse a parenthesised list of partition bound expressions
fn parse_partition_bound_exprs<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<PartitionBoundExpr<'a>>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut values = Vec::new();
    loop {
        values.push(parse_partition_bound_expr(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    parser.consume_token(Token::RParen)?;
    Ok(values)
}

/// Parse a partition_bound_spec: IN (...) | FROM (...) TO (...) | WITH (MODULUS n, REMAINDER r)
fn parse_partition_bound_spec<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<PartitionBoundSpec<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::IN) => {
            let in_span = parser.consume_keyword(Keyword::IN)?;
            let values = parse_partition_bound_exprs(parser)?;
            Ok(PartitionBoundSpec::In { in_span, values })
        }
        Token::Ident(_, Keyword::FROM) => {
            let from_span = parser.consume_keyword(Keyword::FROM)?;
            let from_values = parse_partition_bound_exprs(parser)?;
            let to_span = parser.consume_keyword(Keyword::TO)?;
            let to_values = parse_partition_bound_exprs(parser)?;
            Ok(PartitionBoundSpec::FromTo {
                from_span,
                from_values,
                to_span,
                to_values,
            })
        }
        Token::Ident(_, Keyword::WITH) => {
            let with_span = parser.consume_keyword(Keyword::WITH)?;
            parser.consume_token(Token::LParen)?;
            let modulus_span = parser.consume_keyword(Keyword::MODULUS)?;
            let modulus = parser.consume_int::<u64>()?;
            parser.consume_token(Token::Comma)?;
            let remainder_span = parser.consume_keyword(Keyword::REMAINDER)?;
            let remainder = parser.consume_int::<u64>()?;
            parser.consume_token(Token::RParen)?;
            Ok(PartitionBoundSpec::WithModulusRemainder {
                with_span,
                modulus_span,
                modulus,
                remainder_span,
                remainder,
            })
        }
        _ => parser.expected_failure("IN, FROM, or WITH"),
    }
}

pub(crate) fn parse_create_table<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
    table_span: Span,
    if_not_exists: Option<Span>,
    identifier: QualifiedName<'a>,
) -> Result<CreateTable<'a>, ParseError> {
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
    let mut partition_by: Option<PartitionBy<'_>> = None;
    let delimiter_name = parser.lexer.delimiter_name();
    parser.recovered(
        delimiter_name,
        &|t| t == &Token::Eof || t == &Token::Delimiter,
        |parser| {
            loop {
                match &parser.token {
                    Token::Ident(_, Keyword::ENGINE) => {
                        let identifier = parser.consume_keyword(Keyword::ENGINE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Engine {
                            identifier,
                            value: parser.consume_plain_identifier_unreserved()?,
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
                                    value: parser.consume_plain_identifier_unreserved()?,
                                });
                            }
                            Token::Ident(_, Keyword::COLLATE) => {
                                let identifier = default_span
                                    .join_span(&parser.consume_keyword(Keyword::COLLATE)?);
                                parser.skip_token(Token::Eq);
                                options.push(TableOption::DefaultCollate {
                                    identifier,
                                    value: parser.consume_plain_identifier_unreserved()?,
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
                            value: parser.consume_plain_identifier_unreserved()?,
                        });
                    }
                    Token::Ident(_, Keyword::COLLATE) => {
                        let identifier = parser.consume_keyword(Keyword::COLLATE)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::Collate {
                            identifier,
                            value: parser.consume_plain_identifier_unreserved()?,
                        });
                    }
                    Token::Ident(_, Keyword::ROW_FORMAT) => {
                        let identifier = parser.consume_keyword(Keyword::ROW_FORMAT)?;
                        parser.skip_token(Token::Eq);
                        options.push(TableOption::RowFormat {
                            identifier,
                            value: parser.consume_plain_identifier_unreserved()?,
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
                            value: parser.consume_plain_identifier_unreserved()?,
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
                            Token::String(..) => {
                                let s = parser.consume_string()?;
                                let is_yes = s.as_str().eq_ignore_ascii_case("y")
                                    || s.as_str().eq_ignore_ascii_case("yes");
                                (is_yes, s.span())
                            }
                            _ => {
                                let id = parser.consume_plain_identifier_unreserved()?;
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
                            value: parser.consume_plain_identifier_unreserved()?,
                        });
                    }
                    Token::Ident(_, Keyword::STORAGE) => {
                        let identifier = parser.consume_keyword(Keyword::STORAGE)?;
                        options.push(TableOption::Storage {
                            identifier,
                            value: parser.consume_plain_identifier_unreserved()?,
                        });
                    }
                    Token::Ident(_, Keyword::UNION) => {
                        let identifier = parser.consume_keyword(Keyword::UNION)?;
                        parser.skip_token(Token::Eq);
                        parser.consume_token(Token::LParen)?;
                        let mut tables = Vec::new();
                        loop {
                            tables.push(parser.consume_plain_identifier_unreserved()?);
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
                                tables.push(parse_qualified_name_unreserved(parser)?);
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
                    Token::Ident(_, Keyword::WITH) => {
                        let identifier = parser.consume_keyword(Keyword::WITH)?;
                        parser.postgres_only(&identifier);
                        parser.consume_token(Token::LParen)?;
                        let mut params = Vec::new();
                        parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                            loop {
                                let key = parser.consume_plain_identifier_unreserved()?;
                                parser.consume_token(Token::Eq)?;
                                let val = parse_expression_unreserved(parser, false)?;
                                params.push((key, val));
                                if parser.skip_token(Token::Comma).is_none() {
                                    break;
                                }
                            }
                            Ok(())
                        })?;
                        parser.consume_token(Token::RParen)?;
                        options.push(TableOption::WithOptions {
                            identifier,
                            options: params,
                        });
                    }
                    Token::Ident(_, Keyword::ON) => {
                        let identifier =
                            parser.consume_keywords(&[Keyword::ON, Keyword::COMMIT])?;
                        parser.postgres_only(&identifier);
                        let action = match &parser.token {
                            Token::Ident(_, Keyword::PRESERVE) => OnCommitAction::PreserveRows(
                                parser.consume_keywords(&[Keyword::PRESERVE, Keyword::ROWS])?,
                            ),
                            Token::Ident(_, Keyword::DELETE) => OnCommitAction::DeleteRows(
                                parser.consume_keywords(&[Keyword::DELETE, Keyword::ROWS])?,
                            ),
                            Token::Ident(_, Keyword::DROP) => {
                                OnCommitAction::Drop(parser.consume_keyword(Keyword::DROP)?)
                            }
                            _ => parser.expected_failure("PRESERVE ROWS, DELETE ROWS, or DROP")?,
                        };
                        options.push(TableOption::OnCommit { identifier, action });
                    }
                    Token::Ident(_, Keyword::PARTITION) => {
                        partition_by = Some(parse_partition_by(parser)?);
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
                    Token::Delimiter => break,
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
        partition_by,
    })
}

fn parse_create_table_partition_of<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
    table_span: Span,
    if_not_exists: Option<Span>,
    identifier: QualifiedName<'a>,
) -> Result<CreateTablePartitionOf<'a>, ParseError> {
    let partition_span = parser.consume_keyword(Keyword::PARTITION)?;
    let of_span = parser.consume_keyword(Keyword::OF)?;
    let partition_of_span = partition_span.join_span(&of_span);

    let parent_table = parse_qualified_name_unreserved(parser)?;

    // Optional column definitions / constraint overrides
    let mut create_definitions = Vec::new();
    if matches!(parser.token, Token::LParen) {
        parser.consume_token(Token::LParen)?;
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
    }

    // FOR VALUES … | DEFAULT
    let bound = match &parser.token {
        Token::Ident(_, Keyword::DEFAULT) => {
            PartitionOfBound::Default(parser.consume_keyword(Keyword::DEFAULT)?)
        }
        Token::Ident(_, Keyword::FOR) => {
            let for_values_span = parser.consume_keywords(&[Keyword::FOR, Keyword::VALUES])?;
            let spec = parse_partition_bound_spec(parser)?;
            PartitionOfBound::ForValues {
                for_values_span,
                spec,
            }
        }
        _ => parser.expected_failure("FOR VALUES or DEFAULT")?,
    };

    // Optional sub-partitioning
    let partition_by = if matches!(parser.token, Token::Ident(_, Keyword::PARTITION)) {
        Some(parse_partition_by(parser)?)
    } else {
        None
    };

    Ok(CreateTablePartitionOf {
        create_span,
        create_options,
        table_span,
        if_not_exists,
        identifier,
        partition_of_span,
        parent_table,
        create_definitions,
        bound,
        partition_by,
    })
}

/// Entry point for `CREATE TABLE` dispatching.
///
/// Parses TABLE + optional IF NOT EXISTS + table identifier, then dispatches to either
/// `parse_create_table_partition_of` (for `PARTITION OF`) or `parse_create_table`.
pub(crate) fn parse_create_table_or_partition_of<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<Statement<'a>, ParseError> {
    let table_span = parser.consume_keyword(Keyword::TABLE)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            if_.start
                ..parser
                    .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                    .end,
        )
    } else {
        None
    };
    let identifier = parse_qualified_name_unreserved(parser)?;

    if matches!(parser.token, Token::Ident(_, Keyword::PARTITION)) {
        Ok(Statement::CreateTablePartitionOf(Box::new(
            parse_create_table_partition_of(
                parser,
                create_span,
                create_options,
                table_span,
                if_not_exists,
                identifier,
            )?,
        )))
    } else if matches!(parser.token, Token::Ident(_, Keyword::AS))
        && !matches!(parser.peek(), Token::LParen)
    {
        // CREATE TABLE foo AS SELECT ... (CTAS — no column list)
        let as_span = parser.consume_keyword(Keyword::AS)?;
        let query = parse_compound_query(parser)?;
        Ok(Statement::CreateTable(Box::new(CreateTable {
            create_span,
            create_options,
            table_span,
            identifier,
            if_not_exists,
            options: alloc::vec![],
            create_definitions: alloc::vec![],
            table_as: Some(CreateTableAs {
                as_span,
                replace_span: None,
                ignore_span: None,
                query,
            }),
            partition_by: None,
        })))
    } else {
        Ok(Statement::CreateTable(Box::new(parse_create_table(
            parser,
            create_span,
            create_options,
            table_span,
            if_not_exists,
            identifier,
        )?)))
    }
}
