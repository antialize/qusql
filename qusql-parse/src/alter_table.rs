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

use crate::create::parse_sequence_options;
use crate::qualified_name::parse_qualified_name_unreserved;
use crate::{
    DataType, Expression, Identifier, QualifiedName, SString, SequenceOption, Span, Spanned,
    data_type::{DataTypeContext, parse_data_type},
    expression::{PRIORITY_MAX, parse_expression_unreserved},
    keywords::{Keyword, Restrict},
    lexer::{StringType, Token},
    parser::{ParseError, Parser},
};
use alloc::vec::Vec;

/// Parse OWNER TO ... for ALTER TABLE/ALTER OPERATOR CLASS (PostgreSQL)
pub(crate) fn parse_alter_owner<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<AlterTableOwner<'a>, ParseError> {
    // In PostgreSQL, CURRENT_ROLE, CURRENT_USER, and SESSION_USER are valid without quotes
    match &parser.token {
        Token::Ident(_, Keyword::CURRENT_ROLE) => {
            Ok(AlterTableOwner::CurrentRole(parser.consume()))
        }
        Token::Ident(_, Keyword::CURRENT_USER) => {
            Ok(AlterTableOwner::CurrentUser(parser.consume()))
        }
        Token::Ident(_, Keyword::SESSION_USER) => {
            Ok(AlterTableOwner::SessionUser(parser.consume()))
        }
        _ => Ok(AlterTableOwner::Identifier(
            parser.consume_plain_identifier_unreserved()?,
        )),
    }
}

/// Option on an index
#[derive(Clone, Debug)]
pub enum IndexOption<'a> {
    /// The index should be a BTree
    IndexTypeBTree(Span),
    /// The index should be hashed
    IndexTypeHash(Span),
    /// The index should be an RTree
    IndexTypeRTree(Span),
    /// Attach a comment to the index
    Comment(SString<'a>),
}

impl<'a> Spanned for IndexOption<'a> {
    fn span(&self) -> Span {
        match &self {
            IndexOption::IndexTypeBTree(v) => v.span(),
            IndexOption::IndexTypeHash(v) => v.span(),
            IndexOption::IndexTypeRTree(v) => v.span(),
            IndexOption::Comment(v) => v.span(),
        }
    }
}

/// Type of index to add
#[derive(Clone, Debug)]
pub enum IndexType {
    Index(Span),
    Primary(Span),
    Unique(Span),
    FullText(Span),
    Spatial(Span),
}

impl Spanned for IndexType {
    fn span(&self) -> Span {
        match &self {
            IndexType::Index(v) => v.span(),
            IndexType::Primary(v) => v.span(),
            IndexType::Unique(v) => v.span(),
            IndexType::FullText(v) => v.span(),
            IndexType::Spatial(v) => v.span(),
        }
    }
}

/// When to take a foreign key action
#[derive(Clone, Debug)]
pub enum ForeignKeyOnType {
    Update(Span),
    Delete(Span),
}

impl Spanned for ForeignKeyOnType {
    fn span(&self) -> Span {
        match &self {
            ForeignKeyOnType::Update(v) => v.span(),
            ForeignKeyOnType::Delete(v) => v.span(),
        }
    }
}

/// Action to take on event for foreign key
#[derive(Clone, Debug)]
pub enum ForeignKeyOnAction {
    Restrict(Span),
    Cascade(Span),
    SetNull(Span),
    NoAction(Span),
    SetDefault(Span),
}

impl Spanned for ForeignKeyOnAction {
    fn span(&self) -> Span {
        match &self {
            ForeignKeyOnAction::Restrict(v) => v.span(),
            ForeignKeyOnAction::Cascade(v) => v.span(),
            ForeignKeyOnAction::SetNull(v) => v.span(),
            ForeignKeyOnAction::NoAction(v) => v.span(),
            ForeignKeyOnAction::SetDefault(v) => v.span(),
        }
    }
}

/// MATCH type for a foreign key reference
#[derive(Clone, Debug)]
pub enum ForeignKeyMatch {
    Full(Span),
    Simple(Span),
    Partial(Span),
}

impl Spanned for ForeignKeyMatch {
    fn span(&self) -> Span {
        match self {
            ForeignKeyMatch::Full(v) => v.span(),
            ForeignKeyMatch::Simple(v) => v.span(),
            ForeignKeyMatch::Partial(v) => v.span(),
        }
    }
}

/// Action to perform on events on foreign keys
#[derive(Clone, Debug)]
pub struct ForeignKeyOn {
    pub type_: ForeignKeyOnType,
    pub action: ForeignKeyOnAction,
}

impl Spanned for ForeignKeyOn {
    fn span(&self) -> Span {
        self.type_.join_span(&self.action)
    }
}

/// Column or expression specification for an index
#[derive(Clone, Debug)]
pub enum IndexColExpr<'a> {
    /// Regular column name
    Column(Identifier<'a>),
    /// Functional index expression (wrapped in parentheses)
    Expression(Expression<'a>),
}

impl<'a> Spanned for IndexColExpr<'a> {
    fn span(&self) -> Span {
        match self {
            IndexColExpr::Column(id) => id.span(),
            IndexColExpr::Expression(expr) => expr.span(),
        }
    }
}

/// Specify a column for an index, together with a with
#[derive(Clone, Debug)]
pub struct IndexCol<'a> {
    /// The column name or expression
    pub expr: IndexColExpr<'a>,
    /// Optional width of index together with its span
    pub size: Option<(u32, Span)>,
    /// Optional operator class (PostgreSQL)
    pub opclass: Option<QualifiedName<'a>>,
    /// Optional ASC ordering
    pub asc: Option<Span>,
    /// Optional DESC ordering
    pub desc: Option<Span>,
}

impl<'a> Spanned for IndexCol<'a> {
    fn span(&self) -> Span {
        self.expr
            .join_span(&self.size)
            .join_span(&self.opclass)
            .join_span(&self.asc)
            .join_span(&self.desc)
    }
}

/// Enum of alterations to perform on a column
#[derive(Clone, Debug)]
pub enum AlterColumnAction<'a> {
    SetDefault {
        set_default_span: Span,
        value: Expression<'a>,
    },
    DropDefault {
        drop_default_span: Span,
    },
    Type {
        type_span: Span,
        type_: DataType<'a>,
        using: Option<(Span, Expression<'a>)>,
    },
    SetNotNull {
        set_not_null_span: Span,
    },
    DropNotNull {
        drop_not_null_span: Span,
    },
    AddGenerated {
        add_span: Span,
        generated_span: Span,
        always_or_default: Option<(Span, Span)>, // (ALWAYS|BY, DEFAULT)
        as_span: Span,
        identity_span: Span,
        sequence_options: Vec<SequenceOption<'a>>,
    },
}

impl<'a> Spanned for AlterColumnAction<'a> {
    fn span(&self) -> Span {
        match self {
            AlterColumnAction::SetDefault {
                set_default_span,
                value,
            } => set_default_span.join_span(value),
            AlterColumnAction::DropDefault { drop_default_span } => drop_default_span.clone(),
            AlterColumnAction::Type {
                type_span,
                type_,
                using,
            } => type_span.join_span(type_).join_span(using),
            AlterColumnAction::SetNotNull { set_not_null_span } => set_not_null_span.clone(),
            AlterColumnAction::DropNotNull { drop_not_null_span } => drop_not_null_span.clone(),
            AlterColumnAction::AddGenerated {
                add_span,
                generated_span,
                always_or_default,
                as_span,
                identity_span,
                sequence_options,
            } => add_span
                .join_span(generated_span)
                .join_span(always_or_default)
                .join_span(as_span)
                .join_span(identity_span)
                .join_span(sequence_options),
        }
    }
}

#[derive(Clone, Debug)]
pub enum AlterLock {
    Default(Span),
    None(Span),
    Shared(Span),
    Exclusive(Span),
}

impl Spanned for AlterLock {
    fn span(&self) -> Span {
        match self {
            AlterLock::Default(v) => v.span(),
            AlterLock::None(v) => v.span(),
            AlterLock::Shared(v) => v.span(),
            AlterLock::Exclusive(v) => v.span(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum AlterAlgorithm {
    Default(Span),
    Instant(Span),
    Inplace(Span),
    Copy(Span),
}

impl Spanned for AlterAlgorithm {
    fn span(&self) -> Span {
        match self {
            AlterAlgorithm::Default(v) => v.span(),
            AlterAlgorithm::Instant(v) => v.span(),
            AlterAlgorithm::Inplace(v) => v.span(),
            AlterAlgorithm::Copy(v) => v.span(),
        }
    }
}

/// Owner value for ALTER TABLE OWNER TO
#[derive(Clone, Debug)]
pub enum AlterTableOwner<'a> {
    /// Regular identifier (role name)
    Identifier(Identifier<'a>),
    /// CURRENT_ROLE keyword
    CurrentRole(Span),
    /// CURRENT_USER keyword
    CurrentUser(Span),
    /// SESSION_USER keyword
    SessionUser(Span),
}

impl<'a> Spanned for AlterTableOwner<'a> {
    fn span(&self) -> Span {
        match self {
            AlterTableOwner::Identifier(i) => i.span(),
            AlterTableOwner::CurrentRole(s) => s.span(),
            AlterTableOwner::CurrentUser(s) => s.span(),
            AlterTableOwner::SessionUser(s) => s.span(),
        }
    }
}

/// ADD COLUMN specification
#[derive(Clone, Debug)]
pub struct AddColumn<'a> {
    pub add_span: Span,
    pub if_not_exists_span: Option<Span>,
    pub identifier: Identifier<'a>,
    pub data_type: DataType<'a>,
    /// Optional "FIRST"
    pub first: Option<Span>,
    /// Optional "AFTER col_name"
    pub after: Option<(Span, Identifier<'a>)>,
}

impl<'a> Spanned for AddColumn<'a> {
    fn span(&self) -> Span {
        self.add_span
            .join_span(&self.if_not_exists_span)
            .join_span(&self.identifier)
            .join_span(&self.data_type)
            .join_span(&self.first)
            .join_span(&self.after)
    }
}

fn parse_add_column<'a>(
    parser: &mut Parser<'a, '_>,
    add_span: Span,
) -> Result<AddColumn<'a>, ParseError> {
    parser.consume_keyword(Keyword::COLUMN)?;
    let mut if_not_exists_span = None;
    if matches!(parser.token, Token::Ident(_, Keyword::IF)) {
        if_not_exists_span =
            Some(parser.consume_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])?);
    }

    parser.postgres_only(&if_not_exists_span);

    let identifier = parser.consume_plain_identifier_unreserved()?;
    let data_type = parse_data_type(parser, DataTypeContext::Column)?;

    let mut first = None;
    let mut after = None;
    match &parser.token {
        Token::Ident(_, Keyword::FIRST) => {
            first = Some(parser.consume_keyword(Keyword::FIRST)?);
        }
        Token::Ident(_, Keyword::AFTER) => {
            let after_span = parser.consume_keyword(Keyword::AFTER)?;
            let after_col = parser.consume_plain_identifier_unreserved()?;
            after = Some((after_span, after_col));
        }
        _ => {}
    }

    Ok(AddColumn {
        add_span,
        if_not_exists_span,
        identifier,
        data_type,
        first,
        after,
    })
}

/// Add an index
#[derive(Clone, Debug)]
pub struct AddIndex<'a> {
    /// Span of "ADD"
    pub add_span: Span,
    /// The type of index to add
    pub index_type: IndexType,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Named of index if specified
    pub name: Option<Identifier<'a>>,
    /// Optional "CONSTRAINT" with symbol if specified
    pub constraint: Option<(Span, Option<Identifier<'a>>)>,
    /// Columns to add the index over
    pub cols: Vec<IndexCol<'a>>,
    /// Span of ")" closing the cols list
    pub cols_r_paren: Span,
    /// Options on the index
    pub index_options: Vec<IndexOption<'a>>,
}

impl<'a> Spanned for AddIndex<'a> {
    fn span(&self) -> Span {
        self.add_span
            .join_span(&self.index_type)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.constraint)
            .join_span(&self.cols)
            .join_span(&self.cols_r_paren)
            .join_span(&self.index_options)
    }
}

fn parse_add_index<'a>(
    parser: &mut Parser<'a, '_>,
    add_span: Span,
    constraint: Option<(Span, Option<Identifier<'a>>)>,
) -> Result<AddIndex<'a>, ParseError> {
    let index_type = match &parser.token {
        Token::Ident(_, Keyword::PRIMARY) => {
            IndexType::Primary(parser.consume_keywords(&[Keyword::PRIMARY, Keyword::KEY])?)
        }
        Token::Ident(_, Keyword::INDEX | Keyword::KEY) => IndexType::Index(parser.consume()),
        Token::Ident(_, Keyword::FULLTEXT) => {
            let s = parser.consume_keyword(Keyword::FULLTEXT)?;
            match &parser.token {
                Token::Ident(_, kw @ Keyword::INDEX | kw @ Keyword::KEY) => {
                    let kw = *kw;
                    IndexType::FullText(parser.consume_keyword(kw)?.join_span(&s))
                }
                _ => parser.expected_failure("'KEY' or 'INDEX'")?,
            }
        }
        Token::Ident(_, Keyword::SPATIAL) => {
            let s = parser.consume_keyword(Keyword::SPATIAL)?;
            match &parser.token {
                Token::Ident(_, kw @ Keyword::INDEX | kw @ Keyword::KEY) => {
                    let kw = *kw;
                    IndexType::Spatial(parser.consume_keyword(kw)?.join_span(&s))
                }
                _ => parser.expected_failure("'KEY' or 'INDEX'")?,
            }
        }
        Token::Ident(_, Keyword::UNIQUE) => {
            let s = parser.consume_keyword(Keyword::UNIQUE)?;
            match &parser.token {
                Token::Ident(_, kw @ Keyword::INDEX | kw @ Keyword::KEY) => {
                    let kw = *kw;
                    IndexType::Unique(parser.consume_keyword(kw)?.join_span(&s))
                }
                _ => parser.expected_failure("'KEY' or 'INDEX'")?,
            }
        }
        _ => parser.ice(file!(), line!())?,
    };

    let if_not_exists = if let Some(s) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&s),
        )
    } else {
        None
    };

    let name = match &parser.token {
        Token::Ident(_, kw) if !kw.restricted(parser.reserved()) => {
            Some(parser.consume_plain_identifier_unreserved()?)
        }
        _ => None,
    };

    let mut index_options = Vec::new();
    if matches!(parser.token, Token::Ident(_, Keyword::USING)) {
        parse_index_type(parser, &mut index_options)?;
    }
    let (cols, cols_r_paren) = parse_index_cols(parser)?;
    parse_index_options(parser, &mut index_options)?;

    Ok(AddIndex {
        add_span,
        index_type,
        if_not_exists,
        name,
        constraint,
        cols,
        cols_r_paren,
        index_options,
    })
}

/// Add a foreign key
#[derive(Clone, Debug)]
pub struct AddForeignKey<'a> {
    /// Span of "ADD"
    pub add_span: Span,
    /// Optional "CONSTRAINT" with symbol if specified
    pub constraint: Option<(Span, Option<Identifier<'a>>)>,
    /// Span of "FOREIGN KEY"
    pub foreign_key_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Named of index if specified
    pub name: Option<Identifier<'a>>,
    /// Columns to add the index over
    pub cols: Vec<IndexCol<'a>>,
    /// Span of ")" closing the cols list
    pub cols_r_paren: Span,
    /// Span of "REFERENCES"
    pub references_span: Span,
    /// Refereed table
    pub references_table: QualifiedName<'a>,
    /// Columns in referred table
    pub references_cols: Vec<Identifier<'a>>,
    /// Span of ")" closing the references_cols list, if specified
    pub references_cols_r_paren: Option<Span>,
    /// List of what should happen at specified events
    pub ons: Vec<ForeignKeyOn>,
    /// Span of "NOT VALID" if specified
    pub not_valid: Option<Span>,
}

impl<'a> Spanned for AddForeignKey<'a> {
    fn span(&self) -> Span {
        self.add_span
            .join_span(&self.constraint)
            .join_span(&self.foreign_key_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.cols)
            .join_span(&self.cols_r_paren)
            .join_span(&self.references_span)
            .join_span(&self.references_table)
            .join_span(&self.references_cols)
            .join_span(&self.references_cols_r_paren)
            .join_span(&self.not_valid)
            .join_span(&self.ons)
    }
}

fn parse_add_foreign_key<'a>(
    parser: &mut Parser<'a, '_>,
    add_span: Span,
    constraint: Option<(Span, Option<Identifier<'a>>)>,
) -> Result<AddForeignKey<'a>, ParseError> {
    let foregin_key_span = parser.consume_keywords(&[Keyword::FOREIGN, Keyword::KEY])?;
    let if_not_exists = if let Some(s) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&s),
        )
    } else {
        None
    };
    let name = match &parser.token {
        Token::Ident(_, kw) if !kw.restricted(parser.reserved()) => {
            Some(parser.consume_plain_identifier_unreserved()?)
        }
        _ => None,
    };

    let (cols, cols_r_paren) = parse_index_cols(parser)?;
    let references_span = parser.consume_keyword(Keyword::REFERENCES)?;
    let references_table = parse_qualified_name_unreserved(parser)?;
    // Reference columns are optional (omitting uses the referenced table's primary key)
    let (references_cols, references_cols_r_paren) = if matches!(parser.token, Token::LParen) {
        let (c, s) = parse_cols(parser)?;
        (c, Some(s))
    } else {
        (Vec::new(), None)
    };
    let mut ons = Vec::new();
    while let Some(on) = parser.skip_keyword(Keyword::ON) {
        let type_ = match parser.token {
            Token::Ident(_, Keyword::UPDATE) => {
                ForeignKeyOnType::Update(parser.consume_keyword(Keyword::UPDATE)?.join_span(&on))
            }
            Token::Ident(_, Keyword::DELETE) => {
                ForeignKeyOnType::Delete(parser.consume_keyword(Keyword::DELETE)?.join_span(&on))
            }
            _ => parser.expected_failure("'UPDATE' or 'DELETE'")?,
        };

        let action = match parser.token {
            Token::Ident(_, Keyword::RESTRICT) => {
                ForeignKeyOnAction::Restrict(parser.consume_keyword(Keyword::RESTRICT)?)
            }
            Token::Ident(_, Keyword::CASCADE) => {
                ForeignKeyOnAction::Cascade(parser.consume_keyword(Keyword::CASCADE)?)
            }
            Token::Ident(_, Keyword::SET) => {
                let set = parser.consume_keyword(Keyword::SET)?;
                match parser.token {
                    Token::Ident(_, Keyword::NULL) => ForeignKeyOnAction::SetNull(
                        parser.consume_keyword(Keyword::NULL)?.join_span(&set),
                    ),
                    Token::Ident(_, Keyword::DELETE) => ForeignKeyOnAction::SetDefault(
                        parser.consume_keyword(Keyword::DEFAULT)?.join_span(&set),
                    ),
                    _ => parser.expected_failure("'NULL' or 'DEFAULT'")?,
                }
            }
            Token::Ident(_, Keyword::NO) => ForeignKeyOnAction::SetNull(
                parser.consume_keywords(&[Keyword::NO, Keyword::ACTION])?,
            ),
            _ => parser.expected_failure("'RESTRICT' or 'CASCADE', 'SET' or 'NO")?,
        };
        ons.push(ForeignKeyOn { type_, action })
    }
    // Parse optional NOT VALID
    let not_valid = if let Some(span) = parser.skip_keyword(Keyword::NOT) {
        Some(span.join_span(&parser.consume_keyword(Keyword::VALID)?))
    } else {
        None
    };
    Ok(AddForeignKey {
        add_span,
        constraint,
        foreign_key_span: foregin_key_span,
        if_not_exists,
        name,
        cols,
        cols_r_paren,
        references_span,
        references_table,
        references_cols,
        references_cols_r_paren,
        ons,
        not_valid,
    })
}

/// Modify a column
#[derive(Clone, Debug)]
pub struct ModifyColumn<'a> {
    /// Span of "MODIFY"
    pub modify_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Name of column to modify
    pub col: Identifier<'a>,
    /// New definition of column
    pub definition: DataType<'a>,
    /// Optional "FIRST"
    pub first: Option<Span>,
    /// Optional "AFTER col_name"
    pub after: Option<(Span, Identifier<'a>)>,
}

impl<'a> Spanned for ModifyColumn<'a> {
    fn span(&self) -> Span {
        self.modify_span
            .join_span(&self.if_exists)
            .join_span(&self.col)
            .join_span(&self.definition)
            .join_span(&self.first)
            .join_span(&self.after)
    }
}

fn parse_modify_column<'a>(
    parser: &mut Parser<'a, '_>,
    mut modify_span: Span,
) -> Result<ModifyColumn<'a>, ParseError> {
    parser.maria_only(&modify_span);
    if let Some(v) = parser.skip_keyword(Keyword::COLUMN) {
        modify_span = modify_span.join_span(&v);
    }
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let col = parser.consume_plain_identifier_unreserved()?;
    let definition = parse_data_type(parser, DataTypeContext::Column)?;

    let mut first = None;
    let mut after = None;
    match parser.token {
        Token::Ident(_, Keyword::FIRST) => {
            let first_span = parser.consume_keyword(Keyword::FIRST)?;
            parser.maria_only(&first_span);
            first = Some(first_span);
        }
        Token::Ident(_, Keyword::AFTER) => {
            let after_span = parser.consume_keyword(Keyword::AFTER)?;
            parser.maria_only(&after_span);
            let col = parser.consume_plain_identifier_unreserved()?;
            after = Some((after_span, col));
        }
        _ => {}
    }

    Ok(ModifyColumn {
        modify_span,
        if_exists,
        col,
        definition,
        first,
        after,
    })
}

/// DROP COLUMN specification
#[derive(Clone, Debug)]
pub struct DropColumn<'a> {
    /// Span of "DROP COLUMN"
    pub drop_column_span: Span,
    /// Span of "IF EXISTS" if specified (PostgreSQL)
    pub if_exists: Option<Span>,
    /// Name of column to drop
    pub column: Identifier<'a>,
    /// Span of "CASCADE" if specified
    pub cascade: Option<Span>,
}

impl<'a> Spanned for DropColumn<'a> {
    fn span(&self) -> Span {
        self.drop_column_span
            .join_span(&self.if_exists)
            .join_span(&self.column)
            .join_span(&self.cascade)
    }
}

fn parse_drop_column<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropColumn<'a>, ParseError> {
    let drop_column_span = drop_span.join_span(&parser.consume_keyword(Keyword::COLUMN)?);
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        let exists_span = parser.consume_keyword(Keyword::EXISTS)?.join_span(&span);
        parser.postgres_only(&exists_span);
        Some(exists_span)
    } else {
        None
    };
    let column = parser.consume_plain_identifier_unreserved()?;
    let cascade = parser.skip_keyword(Keyword::CASCADE);
    if let Some(span) = &cascade {
        parser.postgres_only(span);
    }
    Ok(DropColumn {
        drop_column_span,
        if_exists,
        column,
        cascade,
    })
}

/// DROP INDEX specification
#[derive(Clone, Debug)]
pub struct DropIndex<'a> {
    /// Span of "DROP INDEX"
    pub drop_index_span: Span,
    /// Name of index to drop
    pub name: Identifier<'a>,
}

impl<'a> Spanned for DropIndex<'a> {
    fn span(&self) -> Span {
        self.drop_index_span.join_span(&self.name)
    }
}

fn parse_drop_index<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropIndex<'a>, ParseError> {
    let index_span = parser.consume();
    let name = parser.consume_plain_identifier_unreserved()?;
    Ok(DropIndex {
        drop_index_span: drop_span.join_span(&index_span).join_span(&name),
        name,
    })
}

/// DROP FOREIGN KEY specification
#[derive(Clone, Debug)]
pub struct DropForeignKey<'a> {
    /// Span of "DROP FOREIGN KEY"
    pub drop_foreign_key_span: Span,
    /// Name of foreign key to drop
    pub name: Identifier<'a>,
}

impl<'a> Spanned for DropForeignKey<'a> {
    fn span(&self) -> Span {
        self.drop_foreign_key_span.join_span(&self.name)
    }
}

fn parse_drop_foreign_key<'a>(
    parser: &mut Parser<'a, '_>,
    drop_span: Span,
) -> Result<DropForeignKey<'a>, ParseError> {
    let foreign_span = parser.consume_keywords(&[Keyword::FOREIGN, Keyword::KEY])?;
    let name = parser.consume_plain_identifier_unreserved()?;
    Ok(DropForeignKey {
        drop_foreign_key_span: drop_span.join_span(&foreign_span).join_span(&name),
        name,
    })
}

/// DROP PRIMARY KEY specification
#[derive(Clone, Debug)]
pub struct DropPrimaryKey {
    /// Span of "DROP PRIMARY KEY"
    pub drop_primary_key_span: Span,
}

impl Spanned for DropPrimaryKey {
    fn span(&self) -> Span {
        self.drop_primary_key_span.clone()
    }
}

fn parse_drop_primary_key(
    parser: &mut Parser<'_, '_>,
    drop_span: Span,
) -> Result<DropPrimaryKey, ParseError> {
    let primary_key_span = parser.consume_keywords(&[Keyword::PRIMARY, Keyword::KEY])?;
    Ok(DropPrimaryKey {
        drop_primary_key_span: drop_span.join_span(&primary_key_span),
    })
}

/// ALTER COLUMN specification
#[derive(Clone, Debug)]
pub struct AlterColumn<'a> {
    /// Span of "ALTER COLUMN"
    pub alter_column_span: Span,
    /// Name of column to alter
    pub column: Identifier<'a>,
    pub alter_column_action: AlterColumnAction<'a>,
}

impl<'a> Spanned for AlterColumn<'a> {
    fn span(&self) -> Span {
        self.alter_column_span
            .join_span(&self.column)
            .join_span(&self.alter_column_action)
    }
}

// Note: parse_alter_column is complex and stays in parse_alter_table

/// OWNER TO specification
#[derive(Clone, Debug)]
pub struct OwnerTo<'a> {
    /// Span of "OWNER TO"
    pub span: Span,
    /// Name of owner
    pub owner: AlterTableOwner<'a>,
}

impl<'a> Spanned for OwnerTo<'a> {
    fn span(&self) -> Span {
        self.span.join_span(&self.owner)
    }
}

fn parse_owner_to<'a>(parser: &mut Parser<'a, '_>, span: Span) -> Result<OwnerTo<'a>, ParseError> {
    let owner = parse_alter_owner(parser)?;
    Ok(OwnerTo { span, owner })
}

/// LOCK specification
#[derive(Clone, Debug)]
pub struct Lock {
    /// Span of "LOCK"
    pub lock_span: Span,
    pub lock: AlterLock,
}

impl Spanned for Lock {
    fn span(&self) -> Span {
        self.lock_span.join_span(&self.lock)
    }
}

fn parse_lock(parser: &mut Parser<'_, '_>, lock_span: Span) -> Result<Lock, ParseError> {
    parser.skip_token(Token::Eq);
    let lock = match &parser.token {
        Token::Ident(_, Keyword::DEFAULT) => {
            AlterLock::Default(parser.consume_keyword(Keyword::DEFAULT)?)
        }
        Token::Ident(_, Keyword::NONE) => AlterLock::None(parser.consume_keyword(Keyword::NONE)?),
        Token::Ident(_, Keyword::SHARED) => {
            AlterLock::Shared(parser.consume_keyword(Keyword::SHARED)?)
        }
        Token::Ident(_, Keyword::EXCLUSIVE) => {
            AlterLock::Exclusive(parser.consume_keyword(Keyword::EXCLUSIVE)?)
        }
        _ => parser.expected_failure("'DEFAULT', 'NONE', 'SHARED' or 'EXCLUSIVE'")?,
    };
    Ok(Lock { lock_span, lock })
}

/// RENAME COLUMN specification
#[derive(Clone, Debug)]
pub struct RenameColumn<'a> {
    /// Span of "RENAME COLUMN"
    pub rename_column_span: Span,
    /// Old name of column
    pub old_col_name: Identifier<'a>,
    /// Span of "TO"
    pub to_span: Span,
    /// New name of column
    pub new_col_name: Identifier<'a>,
}

impl<'a> Spanned for RenameColumn<'a> {
    fn span(&self) -> Span {
        self.rename_column_span
            .join_span(&self.old_col_name)
            .join_span(&self.to_span)
            .join_span(&self.new_col_name)
    }
}

fn parse_rename_column<'a>(
    parser: &mut Parser<'a, '_>,
    rename_span: Span,
) -> Result<RenameColumn<'a>, ParseError> {
    let column_span = parser.consume_keyword(Keyword::COLUMN)?;
    let old_col_name = parser.consume_plain_identifier_unreserved()?;
    let to_span = parser.consume_keyword(Keyword::TO)?;
    let new_col_name = parser.consume_plain_identifier_unreserved()?;
    Ok(RenameColumn {
        rename_column_span: rename_span.join_span(&column_span),
        old_col_name,
        to_span,
        new_col_name,
    })
}

/// RENAME INDEX specification
#[derive(Clone, Debug)]
pub struct RenameIndex<'a> {
    /// Span of "RENAME INDEX" or "RENAME KEY"
    pub rename_index_span: Span,
    /// Old name of index
    pub old_index_name: Identifier<'a>,
    /// Span of "TO"
    pub to_span: Span,
    /// New name of index
    pub new_index_name: Identifier<'a>,
}

impl<'a> Spanned for RenameIndex<'a> {
    fn span(&self) -> Span {
        self.rename_index_span
            .join_span(&self.old_index_name)
            .join_span(&self.to_span)
            .join_span(&self.new_index_name)
    }
}

fn parse_rename_index<'a>(
    parser: &mut Parser<'a, '_>,
    rename_span: Span,
) -> Result<RenameIndex<'a>, ParseError> {
    let index_span = parser.consume();
    let old_index_name = parser.consume_plain_identifier_unreserved()?;
    let to_span = parser.consume_keyword(Keyword::TO)?;
    let new_index_name = parser.consume_plain_identifier_unreserved()?;
    Ok(RenameIndex {
        rename_index_span: rename_span.join_span(&index_span),
        old_index_name,
        to_span,
        new_index_name,
    })
}

/// RENAME CONSTRAINT specification
#[derive(Clone, Debug)]
pub struct RenameConstraint<'a> {
    /// Span of "RENAME CONSTRAINT"
    pub rename_constraint_span: Span,
    /// Old name of constraint
    pub old_constraint_name: Identifier<'a>,
    /// Span of "TO"
    pub to_span: Span,
    /// New name of constraint
    pub new_constraint_name: Identifier<'a>,
}

impl<'a> Spanned for RenameConstraint<'a> {
    fn span(&self) -> Span {
        self.rename_constraint_span
            .join_span(&self.old_constraint_name)
            .join_span(&self.to_span)
            .join_span(&self.new_constraint_name)
    }
}

fn parse_rename_constraint<'a>(
    parser: &mut Parser<'a, '_>,
    rename_span: Span,
) -> Result<RenameConstraint<'a>, ParseError> {
    let constraint_span = parser.consume_keyword(Keyword::CONSTRAINT)?;
    parser.postgres_only(&constraint_span);
    let old_constraint_name = parser.consume_plain_identifier_unreserved()?;
    let to_span = parser.consume_keyword(Keyword::TO)?;
    let new_constraint_name = parser.consume_plain_identifier_unreserved()?;
    Ok(RenameConstraint {
        rename_constraint_span: rename_span.join_span(&constraint_span),
        old_constraint_name,
        to_span,
        new_constraint_name,
    })
}

/// RENAME TO specification
#[derive(Clone, Debug)]
pub struct RenameTo<'a> {
    /// Span of "RENAME"
    pub rename_span: Span,
    /// Span of "TO" or "AS"
    pub to_span: Span,
    /// New name of table
    pub new_table_name: Identifier<'a>,
}

impl<'a> Spanned for RenameTo<'a> {
    fn span(&self) -> Span {
        self.rename_span
            .join_span(&self.to_span)
            .join_span(&self.new_table_name)
    }
}

fn parse_rename_to<'a>(
    parser: &mut Parser<'a, '_>,
    rename_span: Span,
) -> Result<RenameTo<'a>, ParseError> {
    let to_span = parser.consume();
    let new_table_name = parser.consume_plain_identifier_unreserved()?;
    Ok(RenameTo {
        rename_span,
        to_span,
        new_table_name,
    })
}

/// ALGORITHM specification
#[derive(Clone, Debug)]
pub struct Algorithm {
    /// Span of "ALGORITHM"
    pub algorithm_span: Span,
    pub algorithm: AlterAlgorithm,
}

impl Spanned for Algorithm {
    fn span(&self) -> Span {
        self.algorithm_span.join_span(&self.algorithm)
    }
}

fn parse_algorithm(
    parser: &mut Parser<'_, '_>,
    algorithm_span: Span,
) -> Result<Algorithm, ParseError> {
    parser.skip_token(Token::Eq);
    let algorithm = match &parser.token {
        Token::Ident(_, Keyword::DEFAULT) => {
            AlterAlgorithm::Default(parser.consume_keyword(Keyword::DEFAULT)?)
        }
        Token::Ident(_, Keyword::INSTANT) => {
            AlterAlgorithm::Instant(parser.consume_keyword(Keyword::INSTANT)?)
        }
        Token::Ident(_, Keyword::INPLACE) => {
            AlterAlgorithm::Inplace(parser.consume_keyword(Keyword::INPLACE)?)
        }
        Token::Ident(_, Keyword::COPY) => {
            AlterAlgorithm::Copy(parser.consume_keyword(Keyword::COPY)?)
        }
        _ => parser.expected_failure("'DEFAULT', 'INSTANT', 'INPLACE' or 'COPY'")?,
    };
    Ok(Algorithm {
        algorithm_span,
        algorithm,
    })
}

/// AUTO_INCREMENT specification
#[derive(Clone, Debug)]
pub struct AutoIncrement {
    /// Span of "AUTO_INCREMENT"
    pub auto_increment_span: Span,
    pub value_span: Span,
    /// New value for auto_increment
    pub value: u64,
}

impl Spanned for AutoIncrement {
    fn span(&self) -> Span {
        self.auto_increment_span.join_span(&self.value_span)
    }
}

fn parse_auto_increment(
    parser: &mut Parser<'_, '_>,
    auto_increment_span: Span,
) -> Result<AutoIncrement, ParseError> {
    parser.skip_token(Token::Eq);
    let (value, value_span) = parser.consume_int()?;
    Ok(AutoIncrement {
        auto_increment_span,
        value_span,
        value,
    })
}

/// CHANGE specification
#[derive(Clone, Debug)]
pub struct Change<'a> {
    /// Span of "CHANGE"
    pub change_span: Span,
    /// Optional span of "COLUMN"
    pub column_span: Option<Span>,
    /// Old name of column
    pub column: Identifier<'a>,
    /// New name of column
    pub new_column: Identifier<'a>,
    /// New definition of column
    pub definition: DataType<'a>,
    /// Optional "FIRST"
    pub first: Option<Span>,
    /// Optional "AFTER col_name"
    pub after: Option<(Span, Identifier<'a>)>,
}

impl<'a> Spanned for Change<'a> {
    fn span(&self) -> Span {
        self.change_span
            .join_span(&self.column_span)
            .join_span(&self.column)
            .join_span(&self.new_column)
            .join_span(&self.definition)
            .join_span(&self.first)
            .join_span(&self.after)
    }
}

fn parse_change<'a>(
    parser: &mut Parser<'a, '_>,
    change_span: Span,
) -> Result<Change<'a>, ParseError> {
    let column_span = parser.skip_keyword(Keyword::COLUMN);

    let column = parser.consume_plain_identifier_unreserved()?;
    let new_column = parser.consume_plain_identifier_unreserved()?;
    let definition = parse_data_type(parser, DataTypeContext::Column)?;

    let mut first = None;
    let mut after = None;
    match parser.token {
        Token::Ident(_, Keyword::FIRST) => {
            let first_span = parser.consume_keyword(Keyword::FIRST)?;
            parser.maria_only(&first_span);
            first = Some(first_span);
        }
        Token::Ident(_, Keyword::AFTER) => {
            let after_span = parser.consume_keyword(Keyword::AFTER)?;
            parser.maria_only(&after_span);
            let col = parser.consume_plain_identifier_unreserved()?;
            after = Some((after_span, col));
        }
        _ => {}
    }

    Ok(Change {
        change_span,
        column_span,
        column,
        new_column,
        definition,
        first,
        after,
    })
}

/// PostgreSQL: REPLICA IDENTITY { DEFAULT | USING INDEX <name> | FULL | NOTHING }
#[derive(Clone, Debug)]
pub struct ReplicaIdentity<'a> {
    pub replica_span: Span,
    pub identity_span: Span,
    pub option: ReplicaIdentityOption<'a>,
}

impl<'a> Spanned for ReplicaIdentity<'a> {
    fn span(&self) -> Span {
        self.replica_span
            .join_span(&self.identity_span)
            .join_span(&match &self.option {
                ReplicaIdentityOption::Default(s) => s.clone(),
                ReplicaIdentityOption::Full(s) => s.clone(),
                ReplicaIdentityOption::Nothing(s) => s.clone(),
                ReplicaIdentityOption::UsingIndex {
                    using_span,
                    index_span,
                    name,
                } => using_span.join_span(index_span).join_span(name),
            })
    }
}

// Note: parse_replica_identity stays inline in parse_alter_table due to complexity

/// PostgreSQL: VALIDATE CONSTRAINT constraint_name
#[derive(Clone, Debug)]
pub struct ValidateConstraint<'a> {
    pub validate_span: Span,
    pub constraint_span: Span,
    pub constraint_name: Identifier<'a>,
}

impl<'a> Spanned for ValidateConstraint<'a> {
    fn span(&self) -> Span {
        self.validate_span
            .join_span(&self.constraint_span)
            .join_span(&self.constraint_name)
    }
}

fn parse_validate_constraint<'a>(
    parser: &mut Parser<'a, '_>,
    validate_span: Span,
) -> Result<ValidateConstraint<'a>, ParseError> {
    let constraint_span = parser.consume_keyword(Keyword::CONSTRAINT)?;
    let constraint_name = parser.consume_plain_identifier_unreserved()?;
    Ok(ValidateConstraint {
        validate_span,
        constraint_span,
        constraint_name,
    })
}

/// PostgreSQL: ADD CONSTRAINT ... UNIQUE/PRIMARY KEY/CHECK
#[derive(Clone, Debug)]
pub struct AddTableConstraint<'a> {
    pub add_span: Span,
    pub constraint: Option<(Span, Option<Identifier<'a>>)>,
    pub constraint_type: TableConstraintType<'a>,
    pub not_valid: Option<Span>,
}

impl<'a> Spanned for AddTableConstraint<'a> {
    fn span(&self) -> Span {
        let type_span = match &self.constraint_type {
            TableConstraintType::Unique {
                unique_span,
                nulls_clause,
                cols,
                r_paren,
            } => unique_span
                .join_span(nulls_clause)
                .join_span(cols)
                .join_span(r_paren),
            TableConstraintType::PrimaryKey {
                primary_span,
                key_span,
                cols,
                r_paren,
            } => primary_span
                .join_span(key_span)
                .join_span(cols)
                .join_span(r_paren),
            TableConstraintType::Check { check_span, expr } => check_span.join_span(expr),
        };
        self.add_span
            .join_span(&self.constraint)
            .join_span(&type_span)
            .join_span(&self.not_valid)
    }
}

// Note: parse_add_table_constraint variants stay inline in parse_add_alter_specification

/// PostgreSQL: DISABLE TRIGGER { trigger_name | ALL | USER }
#[derive(Clone, Debug)]
pub struct DisableTrigger<'a> {
    pub disable_span: Span,
    pub trigger_span: Span,
    pub trigger_name: TriggerName<'a>,
}

impl<'a> Spanned for DisableTrigger<'a> {
    fn span(&self) -> Span {
        let name_span = match &self.trigger_name {
            TriggerName::Named(n) => n.span(),
            TriggerName::All(s) => s.clone(),
            TriggerName::User(s) => s.clone(),
        };
        self.disable_span
            .join_span(&self.trigger_span)
            .join_span(&name_span)
    }
}

// Note: parse_disable_trigger stays inline due to DISABLE/ENABLE branching logic

/// PostgreSQL: ENABLE [ REPLICA | ALWAYS ] TRIGGER { trigger_name | ALL | USER }
#[derive(Clone, Debug)]
pub struct EnableTrigger<'a> {
    pub enable_span: Span,
    pub modifier: Option<Span>, // REPLICA or ALWAYS
    pub trigger_span: Span,
    pub trigger_name: TriggerName<'a>,
}

impl<'a> Spanned for EnableTrigger<'a> {
    fn span(&self) -> Span {
        let name_span = match &self.trigger_name {
            TriggerName::Named(n) => n.span(),
            TriggerName::All(s) => s.clone(),
            TriggerName::User(s) => s.clone(),
        };
        self.enable_span
            .join_span(&self.modifier)
            .join_span(&self.trigger_span)
            .join_span(&name_span)
    }
}

// Note: parse_enable_trigger stays inline due to DISABLE/ENABLE branching logic

/// PostgreSQL: DISABLE RULE rule_name
#[derive(Clone, Debug)]
pub struct DisableRule<'a> {
    pub disable_span: Span,
    pub rule_span: Span,
    pub rule_name: Identifier<'a>,
}

impl<'a> Spanned for DisableRule<'a> {
    fn span(&self) -> Span {
        self.disable_span
            .join_span(&self.rule_span)
            .join_span(&self.rule_name)
    }
}

// Note: parse_disable_rule stays inline due to DISABLE/ENABLE branching logic

/// PostgreSQL: ENABLE [ REPLICA | ALWAYS ] RULE rule_name
#[derive(Clone, Debug)]
pub struct EnableRule<'a> {
    pub enable_span: Span,
    pub modifier: Option<Span>, // REPLICA or ALWAYS
    pub rule_span: Span,
    pub rule_name: Identifier<'a>,
}

impl<'a> Spanned for EnableRule<'a> {
    fn span(&self) -> Span {
        self.enable_span
            .join_span(&self.modifier)
            .join_span(&self.rule_span)
            .join_span(&self.rule_name)
    }
}

// Note: parse_enable_rule stays inline due to DISABLE/ENABLE branching logic

/// PostgreSQL: DISABLE ROW LEVEL SECURITY
#[derive(Clone, Debug)]
pub struct DisableRowLevelSecurity {
    pub disable_span: Span,
    pub row_span: Span,
    pub level_span: Span,
    pub security_span: Span,
}

impl Spanned for DisableRowLevelSecurity {
    fn span(&self) -> Span {
        self.disable_span
            .join_span(&self.row_span)
            .join_span(&self.level_span)
            .join_span(&self.security_span)
    }
}

// Note: parse_disable_row_level_security stays inline due to DISABLE/ENABLE branching logic

/// PostgreSQL: ENABLE ROW LEVEL SECURITY
#[derive(Clone, Debug)]
pub struct EnableRowLevelSecurity {
    pub enable_span: Span,
    pub row_span: Span,
    pub level_span: Span,
    pub security_span: Span,
}

impl Spanned for EnableRowLevelSecurity {
    fn span(&self) -> Span {
        self.enable_span
            .join_span(&self.row_span)
            .join_span(&self.level_span)
            .join_span(&self.security_span)
    }
}

// Note: parse_enable_row_level_security stays inline due to DISABLE/ENABLE branching logic

/// PostgreSQL: FORCE ROW LEVEL SECURITY
#[derive(Clone, Debug)]
pub struct ForceRowLevelSecurity {
    pub force_span: Span,
    pub row_span: Span,
    pub level_span: Span,
    pub security_span: Span,
}

impl Spanned for ForceRowLevelSecurity {
    fn span(&self) -> Span {
        self.force_span
            .join_span(&self.row_span)
            .join_span(&self.level_span)
            .join_span(&self.security_span)
    }
}

// Note: parse_force_row_level_security stays inline due to FORCE/NO FORCE branching

/// PostgreSQL: NO FORCE ROW LEVEL SECURITY
#[derive(Clone, Debug)]
pub struct NoForceRowLevelSecurity {
    pub no_span: Span,
    pub force_span: Span,
    pub row_span: Span,
    pub level_span: Span,
    pub security_span: Span,
}

impl Spanned for NoForceRowLevelSecurity {
    fn span(&self) -> Span {
        self.no_span
            .join_span(&self.force_span)
            .join_span(&self.row_span)
            .join_span(&self.level_span)
            .join_span(&self.security_span)
    }
}

// Note: parse_no_force_row_level_security stays inline due to FORCE/NO FORCE branching

/// Enum of alterations to perform on a table
#[derive(Clone, Debug)]
pub enum AlterSpecification<'a> {
    AddColumn(AddColumn<'a>),
    AddIndex(AddIndex<'a>),
    AddForeignKey(AddForeignKey<'a>),
    Modify(ModifyColumn<'a>),
    DropColumn(DropColumn<'a>),
    DropIndex(DropIndex<'a>),
    DropForeignKey(DropForeignKey<'a>),
    DropPrimaryKey(DropPrimaryKey),
    AlterColumn(AlterColumn<'a>),
    OwnerTo(OwnerTo<'a>),
    Lock(Lock),
    RenameColumn(RenameColumn<'a>),
    RenameIndex(RenameIndex<'a>),
    RenameConstraint(RenameConstraint<'a>),
    RenameTo(RenameTo<'a>),
    Algorithm(Algorithm),
    AutoIncrement(AutoIncrement),
    Change(Change<'a>),
    ReplicaIdentity(ReplicaIdentity<'a>),
    ValidateConstraint(ValidateConstraint<'a>),
    AddTableConstraint(AddTableConstraint<'a>),
    DisableTrigger(DisableTrigger<'a>),
    EnableTrigger(EnableTrigger<'a>),
    DisableRule(DisableRule<'a>),
    EnableRule(EnableRule<'a>),
    DisableRowLevelSecurity(DisableRowLevelSecurity),
    EnableRowLevelSecurity(EnableRowLevelSecurity),
    ForceRowLevelSecurity(ForceRowLevelSecurity),
    NoForceRowLevelSecurity(NoForceRowLevelSecurity),
}

/// Options for REPLICA IDENTITY
#[derive(Clone, Debug)]
pub enum ReplicaIdentityOption<'a> {
    Default(Span),
    Full(Span),
    Nothing(Span),
    UsingIndex {
        using_span: Span,
        index_span: Span,
        name: Identifier<'a>,
    },
}

/// Table constraint types for ADD CONSTRAINT
#[derive(Clone, Debug)]
pub enum TableConstraintType<'a> {
    Unique {
        unique_span: Span,
        nulls_clause: Option<(Span, Option<Span>)>, // (NULLS, NOT?)
        cols: Vec<Identifier<'a>>,
        r_paren: Span,
    },
    PrimaryKey {
        primary_span: Span,
        key_span: Span,
        cols: Vec<Identifier<'a>>,
        r_paren: Span,
    },
    Check {
        check_span: Span,
        expr: Expression<'a>,
    },
}

/// Trigger name variants for ENABLE/DISABLE TRIGGER
#[derive(Clone, Debug)]
pub enum TriggerName<'a> {
    Named(Identifier<'a>),
    All(Span),
    User(Span),
}

impl<'a> Spanned for AlterSpecification<'a> {
    fn span(&self) -> Span {
        match self {
            AlterSpecification::AddColumn(v) => v.span(),
            AlterSpecification::AddIndex(v) => v.span(),
            AlterSpecification::AddForeignKey(v) => v.span(),
            AlterSpecification::Modify(v) => v.span(),
            AlterSpecification::DropColumn(v) => v.span(),
            AlterSpecification::DropIndex(v) => v.span(),
            AlterSpecification::DropForeignKey(v) => v.span(),
            AlterSpecification::DropPrimaryKey(v) => v.span(),
            AlterSpecification::AlterColumn(v) => v.span(),
            AlterSpecification::OwnerTo(v) => v.span(),
            AlterSpecification::Lock(v) => v.span(),
            AlterSpecification::RenameColumn(v) => v.span(),
            AlterSpecification::RenameIndex(v) => v.span(),
            AlterSpecification::RenameConstraint(v) => v.span(),
            AlterSpecification::RenameTo(v) => v.span(),
            AlterSpecification::Algorithm(v) => v.span(),
            AlterSpecification::AutoIncrement(v) => v.span(),
            AlterSpecification::Change(v) => v.span(),
            AlterSpecification::ReplicaIdentity(v) => v.span(),
            AlterSpecification::ValidateConstraint(v) => v.span(),
            AlterSpecification::AddTableConstraint(v) => v.span(),
            AlterSpecification::DisableTrigger(v) => v.span(),
            AlterSpecification::EnableTrigger(v) => v.span(),
            AlterSpecification::DisableRule(v) => v.span(),
            AlterSpecification::EnableRule(v) => v.span(),
            AlterSpecification::DisableRowLevelSecurity(v) => v.span(),
            AlterSpecification::EnableRowLevelSecurity(v) => v.span(),
            AlterSpecification::ForceRowLevelSecurity(v) => v.span(),
            AlterSpecification::NoForceRowLevelSecurity(v) => v.span(),
        }
    }
}

pub(crate) fn parse_index_type<'a>(
    parser: &mut Parser<'a, '_>,
    out: &mut Vec<IndexOption<'a>>,
) -> Result<(), ParseError> {
    parser.consume_keyword(Keyword::USING)?;
    out.push(match &parser.token {
        Token::Ident(_, Keyword::BTREE) => {
            IndexOption::IndexTypeBTree(parser.consume_keyword(Keyword::BTREE)?)
        }
        Token::Ident(_, Keyword::HASH) => {
            IndexOption::IndexTypeHash(parser.consume_keyword(Keyword::HASH)?)
        }
        Token::Ident(_, Keyword::RTREE) => {
            IndexOption::IndexTypeRTree(parser.consume_keyword(Keyword::RTREE)?)
        }
        _ => parser.expected_failure("'BTREE', 'RTREE' or 'HASH'")?,
    });
    Ok(())
}

pub(crate) fn parse_index_options<'a>(
    parser: &mut Parser<'a, '_>,
    out: &mut Vec<IndexOption<'a>>,
) -> Result<(), ParseError> {
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::USING) => parse_index_type(parser, out)?,
            Token::Ident(_, Keyword::COMMENT) => {
                parser.consume_keyword(Keyword::COMMENT)?;
                out.push(IndexOption::Comment(parser.consume_string()?))
            }
            _ => break,
        }
    }
    Ok(())
}

/// Parse optional operator class (PostgreSQL)
/// This can be a qualified name like public.vector_cosine_ops or a known pattern_ops keyword
pub(crate) fn parse_operator_class<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Option<QualifiedName<'a>>, ParseError> {
    if matches!(
        parser.token,
        Token::Ident(
            _,
            Keyword::TEXT_PATTERN_OPS
                | Keyword::VARCHAR_PATTERN_OPS
                | Keyword::BPCHAR_PATTERN_OPS
                | Keyword::INT8_OPS
                | Keyword::INT4_OPS
                | Keyword::INT2_OPS
        )
    ) {
        // Known pattern_ops keywords
        match parser.token {
            Token::Ident(v, _) => {
                let value = v;
                let span = parser.consume();
                parser.postgres_only(&span);
                Ok(Some(QualifiedName {
                    prefix: Vec::new(),
                    identifier: Identifier {
                        value,
                        span,
                        case_sensitive: false,
                    },
                }))
            }
            _ => Ok(None),
        }
    } else if matches!(parser.token, Token::Ident(_, _))
        && !matches!(
            parser.token,
            Token::Ident(_, Keyword::ASC | Keyword::DESC | Keyword::NULLS)
        )
        && *parser.peek() != Token::Comma
        && *parser.peek() != Token::RParen
    {
        // Try to parse as qualified name (for things like public.vector_cosine_ops)
        let qname = parse_qualified_name_unreserved(parser)?;
        parser.postgres_only(&qname);
        Ok(Some(qname))
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_index_cols<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<(Vec<IndexCol<'a>>, Span), ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut ans = Vec::new();
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            // Check if this is a functional index expression (starts with '(')
            let expr = if parser.token == Token::LParen {
                // Functional index: parse expression
                parser.consume_token(Token::LParen)?;
                let expression = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                parser.consume_token(Token::RParen)?;
                IndexColExpr::Expression(expression)
            } else {
                // Regular column name
                let name = parser.consume_plain_identifier_unreserved()?;
                IndexColExpr::Column(name)
            };

            let size = if parser.skip_token(Token::LParen).is_some() {
                let size = parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                    parser.consume_int()
                })?;
                parser.consume_token(Token::RParen)?;
                Some(size)
            } else {
                None
            };

            // Parse optional operator class (PostgreSQL)
            let opclass = parse_operator_class(parser)?;

            // Parse optional ASC | DESC
            let asc = parser.skip_keyword(Keyword::ASC);
            let desc = if asc.is_none() {
                parser.skip_keyword(Keyword::DESC)
            } else {
                None
            };

            ans.push(IndexCol {
                expr,
                size,
                opclass,
                asc,
                desc,
            });
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    let r_paren_span = parser.consume_token(Token::RParen)?;
    Ok((ans, r_paren_span))
}

fn parse_cols<'a>(parser: &mut Parser<'a, '_>) -> Result<(Vec<Identifier<'a>>, Span), ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut ans = Vec::new();
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            ans.push(parser.consume_plain_identifier_unreserved()?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    let r_paren_span = parser.consume_token(Token::RParen)?;
    Ok((ans, r_paren_span))
}

fn parse_add_alter_specification<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<AlterSpecification<'a>, ParseError> {
    let add_span = parser.consume_keyword(Keyword::ADD)?;
    let constraint = if let Some(span) = parser.skip_keyword(Keyword::CONSTRAINT) {
        let v = match &parser.token {
            Token::Ident(_, kw)
                if !kw.restricted(parser.reserved()) || kw == &Keyword::QUOTED_IDENTIFIER =>
            {
                Some(parser.consume_plain_identifier_restrict(Restrict::EMPTY)?)
            }
            Token::String(_, StringType::DoubleQuoted)
                if parser.options.dialect.is_postgresql() =>
            {
                Some(parser.consume_plain_identifier_restrict(Restrict::EMPTY)?)
            }
            _ => None,
        };
        Some((span, v))
    } else {
        None
    };
    // Peek ahead to check if PRIMARY is followed by KEY (for distinguishing table constraints)
    let primary_followed_by_key = matches!(&parser.token, Token::Ident(_, Keyword::PRIMARY))
        && matches!(parser.peek(), Token::Ident(_, Keyword::KEY));

    match &parser.token {
        // Check for table constraints (UNIQUE/PRIMARY KEY without explicit INDEX/KEY keyword)
        Token::Ident(_, Keyword::UNIQUE) if constraint.is_some() => {
            // This is ADD CONSTRAINT ... UNIQUE (...) - a table constraint, not an index
            let unique_span = parser.consume_keyword(Keyword::UNIQUE)?;
            parser.postgres_only(&unique_span);
            // Parse optional NULLS [NOT] DISTINCT
            let nulls_clause = if let Some(nulls_span) = parser.skip_keyword(Keyword::NULLS) {
                let not_span = parser.skip_keyword(Keyword::NOT);
                parser.consume_keyword(Keyword::DISTINCT)?;
                Some((nulls_span, not_span))
            } else {
                None
            };
            let (cols, r_paren) = parse_cols(parser)?;
            let not_valid = if let Some(span) = parser.skip_keyword(Keyword::NOT) {
                Some(span.join_span(&parser.consume_keyword(Keyword::VALID)?))
            } else {
                None
            };
            Ok(AlterSpecification::AddTableConstraint(AddTableConstraint {
                add_span,
                constraint,
                constraint_type: TableConstraintType::Unique {
                    unique_span,
                    nulls_clause,
                    cols,
                    r_paren,
                },
                not_valid,
            }))
        }
        Token::Ident(_, Keyword::PRIMARY) if constraint.is_some() && primary_followed_by_key => {
            // This is ADD CONSTRAINT ... PRIMARY KEY (...) - a table constraint
            let primary_span = parser.consume_keyword(Keyword::PRIMARY)?;
            parser.postgres_only(&primary_span);
            let key_span = parser.consume_keyword(Keyword::KEY)?;
            let (cols, r_paren) = parse_cols(parser)?;
            let not_valid = if let Some(span) = parser.skip_keyword(Keyword::NOT) {
                Some(span.join_span(&parser.consume_keyword(Keyword::VALID)?))
            } else {
                None
            };
            Ok(AlterSpecification::AddTableConstraint(AddTableConstraint {
                add_span,
                constraint,
                constraint_type: TableConstraintType::PrimaryKey {
                    primary_span,
                    key_span,
                    cols,
                    r_paren,
                },
                not_valid,
            }))
        }
        Token::Ident(_, Keyword::CHECK) if constraint.is_some() => {
            // This is ADD CONSTRAINT ... CHECK (...)
            let check_span = parser.consume_keyword(Keyword::CHECK)?;
            parser.postgres_only(&check_span);
            parser.consume_token(Token::LParen)?;
            let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            parser.consume_token(Token::RParen)?;
            let not_valid = if let Some(span) = parser.skip_keyword(Keyword::NOT) {
                Some(span.join_span(&parser.consume_keyword(Keyword::VALID)?))
            } else {
                None
            };
            Ok(AlterSpecification::AddTableConstraint(AddTableConstraint {
                add_span,
                constraint,
                constraint_type: TableConstraintType::Check { check_span, expr },
                not_valid,
            }))
        }
        Token::Ident(_, Keyword::FOREIGN) => Ok(AlterSpecification::AddForeignKey(
            parse_add_foreign_key(parser, add_span, constraint)?,
        )),
        Token::Ident(
            _,
            Keyword::PRIMARY
            | Keyword::INDEX
            | Keyword::KEY
            | Keyword::FULLTEXT
            | Keyword::UNIQUE
            | Keyword::SPATIAL,
        ) => Ok(AlterSpecification::AddIndex(parse_add_index(
            parser, add_span, constraint,
        )?)),
        Token::Ident(_, Keyword::COLUMN) => Ok(AlterSpecification::AddColumn(parse_add_column(
            parser, add_span,
        )?)),
        _ => parser.expected_failure("addable"),
    }
}

fn parse_rename_alter_specification<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<AlterSpecification<'a>, ParseError> {
    let rename_span = parser.consume_keyword(Keyword::RENAME)?;

    match parser.token {
        Token::Ident(_, Keyword::COLUMN) => Ok(AlterSpecification::RenameColumn(
            parse_rename_column(parser, rename_span)?,
        )),
        Token::Ident(_, Keyword::INDEX | Keyword::KEY) => Ok(AlterSpecification::RenameIndex(
            parse_rename_index(parser, rename_span)?,
        )),
        Token::Ident(_, Keyword::CONSTRAINT) => Ok(AlterSpecification::RenameConstraint(
            parse_rename_constraint(parser, rename_span)?,
        )),
        Token::Ident(_, Keyword::TO | Keyword::AS) => Ok(AlterSpecification::RenameTo(
            parse_rename_to(parser, rename_span)?,
        )),
        _ => parser.expected_failure("'COLUMN', 'INDEX', 'CONSTRAINT' or 'TO'")?,
    }
}

fn parse_drop<'a>(parser: &mut Parser<'a, '_>) -> Result<AlterSpecification<'a>, ParseError> {
    let drop_span = parser.consume_keyword(Keyword::DROP)?;
    match parser.token {
        Token::Ident(_, Keyword::INDEX | Keyword::KEY) => Ok(AlterSpecification::DropIndex(
            parse_drop_index(parser, drop_span)?,
        )),
        Token::Ident(_, Keyword::FOREIGN) => Ok(AlterSpecification::DropForeignKey(
            parse_drop_foreign_key(parser, drop_span)?,
        )),
        Token::Ident(_, Keyword::PRIMARY) => Ok(AlterSpecification::DropPrimaryKey(
            parse_drop_primary_key(parser, drop_span)?,
        )),
        Token::Ident(_, Keyword::COLUMN) => Ok(AlterSpecification::DropColumn(parse_drop_column(
            parser, drop_span,
        )?)),
        _ => parser.expected_failure("'COLUMN' or 'INDEX'")?,
    }
}

/// Represent an alter table statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, AlterTable, Statement, Issues};
/// let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
///
/// let sql = "ALTER TABLE `t1`
///     MODIFY `id` int(11) NOT NULL AUTO_INCREMENT,
///     ADD CONSTRAINT `t1_t2` FOREIGN KEY (`two`) REFERENCES `t2` (`id`);";
///
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let alter: AlterTable = match stmts.pop() {
///     Some(Statement::AlterTable(a)) => *a,
///     _ => panic!("We should get an alter table statement")
/// };
///
/// assert!(alter.table.identifier.as_str() == "t1");
/// println!("{:#?}", alter.alter_specifications);
///
/// let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// let sql = "ALTER TABLE t1
///     ALTER COLUMN id DROP NOT NULL,
///     ALTER COLUMN id SET NOT NULL,
///     ALTER COLUMN id SET DEFAULT 47,
///     ALTER COLUMN id DROP DEFAULT,
///     ALTER COLUMN id TYPE int;";
///
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let alter: AlterTable = match stmts.pop() {
///     Some(Statement::AlterTable(a)) => *a,
///     _ => panic!("We should get an alter table statement")
/// };
///
#[derive(Clone, Debug)]
pub struct AlterTable<'a> {
    /// Span of "ALTER"
    pub alter_span: Span,
    /// Span of "ONLINE" if specified
    pub online: Option<Span>,
    /// Span of "IGNORE" if specified
    pub ignore: Option<Span>,
    /// Span of "TABLE"
    pub table_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Span of "ONLY" if specified after IF EXISTS
    pub only: Option<Span>,
    /// The identifier of the table to alter
    pub table: QualifiedName<'a>,
    /// List of alterations to do
    pub alter_specifications: Vec<AlterSpecification<'a>>,
}

impl<'a> Spanned for AlterTable<'a> {
    fn span(&self) -> Span {
        self.alter_span
            .join_span(&self.online)
            .join_span(&self.ignore)
            .join_span(&self.table_span)
            .join_span(&self.if_exists)
            .join_span(&self.only)
            .join_span(&self.table)
            .join_span(&self.alter_specifications)
    }
}

pub(crate) fn parse_alter_table<'a>(
    parser: &mut Parser<'a, '_>,
    alter_span: Span,
    online: Option<Span>,
    ignore: Option<Span>,
) -> Result<AlterTable<'a>, ParseError> {
    // ONLINE and IGNORE are MariaDB/MySQL-specific
    if let Some(span) = &online {
        parser.maria_only(span);
    }
    if let Some(span) = &ignore {
        parser.maria_only(span);
    }

    let table_span = parser.consume_keyword(Keyword::TABLE)?;
    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
    } else {
        None
    };
    let only = if let Some(span) = parser.skip_keyword(Keyword::ONLY) {
        parser.postgres_only(&span);
        Some(span)
    } else {
        None
    };
    let table = parse_qualified_name_unreserved(parser)?;
    let delimeter_name = parser.lexer.delimiter_name();
    let mut alter_specifications = Vec::new();
    parser.recovered(
        delimeter_name,
        &|t| matches!(t, Token::Delimiter | Token::Eof),
        |parser| {
            loop {
                alter_specifications.push(match parser.token {
                    Token::Ident(_, Keyword::ADD) => parse_add_alter_specification(parser)?,
                    Token::Ident(_, Keyword::MODIFY) => {
                        let modify_span = parser.consume_keyword(Keyword::MODIFY)?;
                        parser.maria_only(&modify_span);
                        AlterSpecification::Modify(parse_modify_column(parser, modify_span)?)
                    }
                    Token::Ident(_, Keyword::OWNER) => {
                        let span = parser.consume_keywords(&[Keyword::OWNER, Keyword::TO])?;
                        parser.postgres_only(&span);
                        AlterSpecification::OwnerTo(parse_owner_to(parser, span)?)
                    }
                    Token::Ident(_, Keyword::DROP) => parse_drop(parser)?,
                    Token::Ident(_, Keyword::ALTER) => {
                        let span = parser.consume_keywords(&[Keyword::ALTER, Keyword::COLUMN])?;
                        parser.postgres_only(&span);
                        let column = parser.consume_plain_identifier_unreserved()?;

                        let alter_column_action = match parser.token {
                            Token::Ident(_, Keyword::SET) => {
                                let set_span = parser.consume();
                                match parser.token {
                                    Token::Ident(_, Keyword::DEFAULT) => {
                                        let set_default_span =
                                            parser.consume().join_span(&set_span);
                                        let value =
                                            parse_expression_unreserved(parser, PRIORITY_MAX)?;
                                        AlterColumnAction::SetDefault {
                                            set_default_span,
                                            value,
                                        }
                                    }
                                    Token::Ident(_, Keyword::NOT) => {
                                        let set_not_null_span =
                                            set_span.join_span(&parser.consume_keywords(&[
                                                Keyword::NOT,
                                                Keyword::NULL,
                                            ])?);
                                        AlterColumnAction::SetNotNull { set_not_null_span }
                                    }
                                    Token::Ident(_, Keyword::DATA) => {
                                        // SET DATA TYPE
                                        parser.consume_keyword(Keyword::DATA)?;
                                        let type_span = parser.consume_keyword(Keyword::TYPE)?;
                                        let type_span = set_span.join_span(&type_span);
                                        let type_ =
                                            parse_data_type(parser, DataTypeContext::Column)?;
                                        let using = if let Some(using_span) =
                                            parser.skip_keyword(Keyword::USING)
                                        {
                                            let expr =
                                                parse_expression_unreserved(parser, PRIORITY_MAX)?;
                                            Some((using_span, expr))
                                        } else {
                                            None
                                        };
                                        AlterColumnAction::Type {
                                            type_span,
                                            type_,
                                            using,
                                        }
                                    }
                                    _ => parser
                                        .expected_failure("'DEFAULT', 'NOT NULL', or 'DATA'")?,
                                }
                            }
                            Token::Ident(_, Keyword::DROP) => {
                                let set_span = parser.consume();
                                match parser.token {
                                    Token::Ident(_, Keyword::DEFAULT) => {
                                        let drop_default_span =
                                            parser.consume().join_span(&set_span);
                                        AlterColumnAction::DropDefault { drop_default_span }
                                    }
                                    Token::Ident(_, Keyword::NOT) => {
                                        let drop_not_null_span =
                                            set_span.join_span(&parser.consume_keywords(&[
                                                Keyword::NOT,
                                                Keyword::NULL,
                                            ])?);
                                        AlterColumnAction::DropNotNull { drop_not_null_span }
                                    }
                                    _ => parser.expected_failure("'DEFAULT' or 'NOT NULL'")?,
                                }
                            }
                            Token::Ident(_, Keyword::TYPE) => {
                                let type_span = parser.consume();
                                let type_ = parse_data_type(parser, DataTypeContext::Column)?;
                                let using = if let Some(using_span) =
                                    parser.skip_keyword(Keyword::USING)
                                {
                                    let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                                    Some((using_span, expr))
                                } else {
                                    None
                                };
                                AlterColumnAction::Type {
                                    type_span,
                                    type_,
                                    using,
                                }
                            }
                            Token::Ident(_, Keyword::ADD) => {
                                let add_span = parser.consume_keyword(Keyword::ADD)?;
                                let generated_span = parser.consume_keyword(Keyword::GENERATED)?;
                                // Parse optional ALWAYS or BY DEFAULT
                                let always_or_default = if let Some(always_span) =
                                    parser.skip_keyword(Keyword::ALWAYS)
                                {
                                    Some((always_span.clone(), always_span))
                                } else if let Some(by_span) = parser.skip_keyword(Keyword::BY) {
                                    let default_span = parser.consume_keyword(Keyword::DEFAULT)?;
                                    Some((by_span, default_span))
                                } else {
                                    None
                                };
                                let as_span = parser.consume_keyword(Keyword::AS)?;
                                let identity_span = parser.consume_keyword(Keyword::IDENTITY)?;
                                // Parse optional sequence options in parentheses
                                let sequence_options = if parser.skip_token(Token::LParen).is_some()
                                {
                                    let options = parse_sequence_options(parser)?;
                                    if options.is_empty() {
                                        parser.expected_failure("sequence option")?;
                                    }
                                    parser.consume_token(Token::RParen)?;
                                    options
                                } else {
                                    Vec::new()
                                };
                                AlterColumnAction::AddGenerated {
                                    add_span,
                                    generated_span,
                                    always_or_default,
                                    as_span,
                                    identity_span,
                                    sequence_options,
                                }
                            }
                            _ => parser.expected_failure("alter column action")?,
                        };
                        AlterSpecification::AlterColumn(AlterColumn {
                            alter_column_span: span,
                            column,
                            alter_column_action,
                        })
                    }
                    Token::Ident(_, Keyword::LOCK) => {
                        let lock_span = parser.consume_keyword(Keyword::LOCK)?;
                        parser.maria_only(&lock_span);
                        AlterSpecification::Lock(parse_lock(parser, lock_span)?)
                    }
                    Token::Ident(_, Keyword::ALGORITHM) => {
                        let algorithm_span = parser.consume_keyword(Keyword::ALGORITHM)?;
                        parser.maria_only(&algorithm_span);
                        AlterSpecification::Algorithm(parse_algorithm(parser, algorithm_span)?)
                    }
                    Token::Ident(_, Keyword::AUTO_INCREMENT) => {
                        let auto_increment_span =
                            parser.consume_keyword(Keyword::AUTO_INCREMENT)?;
                        parser.maria_only(&auto_increment_span);
                        AlterSpecification::AutoIncrement(parse_auto_increment(
                            parser,
                            auto_increment_span,
                        )?)
                    }
                    Token::Ident(_, Keyword::RENAME) => parse_rename_alter_specification(parser)?,
                    Token::Ident(_, Keyword::CHANGE) => {
                        let change_span = parser.consume_keyword(Keyword::CHANGE)?;
                        parser.maria_only(&change_span);
                        AlterSpecification::Change(parse_change(parser, change_span)?)
                    }
                    Token::Ident(_, Keyword::REPLICA) => {
                        let replica_span = parser.consume_keyword(Keyword::REPLICA)?;
                        parser.postgres_only(&replica_span);
                        let identity_span = parser.consume_keyword(Keyword::IDENTITY)?;
                        let option = match &parser.token {
                            Token::Ident(_, Keyword::DEFAULT) => ReplicaIdentityOption::Default(
                                parser.consume_keyword(Keyword::DEFAULT)?,
                            ),
                            Token::Ident(_, Keyword::FULL) => {
                                ReplicaIdentityOption::Full(parser.consume_keyword(Keyword::FULL)?)
                            }
                            Token::Ident(_, Keyword::NOTHING) => ReplicaIdentityOption::Nothing(
                                parser.consume_keyword(Keyword::NOTHING)?,
                            ),
                            Token::Ident(_, Keyword::USING) => {
                                let using_span = parser.consume_keyword(Keyword::USING)?;
                                let index_span = parser.consume_keyword(Keyword::INDEX)?;
                                let name = parser.consume_plain_identifier_unreserved()?;
                                ReplicaIdentityOption::UsingIndex {
                                    using_span,
                                    index_span,
                                    name,
                                }
                            }
                            _ => parser.expected_failure("REPLICA IDENTITY option")?,
                        };
                        AlterSpecification::ReplicaIdentity(ReplicaIdentity {
                            replica_span,
                            identity_span,
                            option,
                        })
                    }
                    Token::Ident(_, Keyword::VALIDATE) => {
                        let validate_span = parser.consume_keyword(Keyword::VALIDATE)?;
                        parser.postgres_only(&validate_span);
                        AlterSpecification::ValidateConstraint(parse_validate_constraint(
                            parser,
                            validate_span,
                        )?)
                    }
                    Token::Ident(_, Keyword::DISABLE) => {
                        let disable_span = parser.consume_keyword(Keyword::DISABLE)?;
                        parser.postgres_only(&disable_span);
                        match &parser.token {
                            Token::Ident(_, Keyword::TRIGGER) => {
                                let trigger_span = parser.consume_keyword(Keyword::TRIGGER)?;
                                let trigger_name = match &parser.token {
                                    Token::Ident(_, Keyword::ALL) => {
                                        TriggerName::All(parser.consume_keyword(Keyword::ALL)?)
                                    }
                                    Token::Ident(_, Keyword::USER) => {
                                        TriggerName::User(parser.consume_keyword(Keyword::USER)?)
                                    }
                                    _ => TriggerName::Named(
                                        parser.consume_plain_identifier_unreserved()?,
                                    ),
                                };
                                AlterSpecification::DisableTrigger(DisableTrigger {
                                    disable_span,
                                    trigger_span,
                                    trigger_name,
                                })
                            }
                            Token::Ident(_, Keyword::RULE) => {
                                let rule_span = parser.consume_keyword(Keyword::RULE)?;
                                let rule_name = parser.consume_plain_identifier_unreserved()?;
                                AlterSpecification::DisableRule(DisableRule {
                                    disable_span,
                                    rule_span,
                                    rule_name,
                                })
                            }
                            Token::Ident(_, Keyword::ROW) => {
                                let row_span = parser.consume_keyword(Keyword::ROW)?;
                                let level_span = parser.consume_keyword(Keyword::LEVEL)?;
                                let security_span = parser.consume_keyword(Keyword::SECURITY)?;
                                AlterSpecification::DisableRowLevelSecurity(
                                    DisableRowLevelSecurity {
                                        disable_span,
                                        row_span,
                                        level_span,
                                        security_span,
                                    },
                                )
                            }
                            _ => parser.expected_failure("'TRIGGER', 'RULE', or 'ROW'")?,
                        }
                    }
                    Token::Ident(_, Keyword::ENABLE) => {
                        let enable_span = parser.consume_keyword(Keyword::ENABLE)?;
                        parser.postgres_only(&enable_span);
                        let modifier = if let Some(span) = parser.skip_keyword(Keyword::REPLICA) {
                            Some(span)
                        } else {
                            parser.skip_keyword(Keyword::ALWAYS)
                        };
                        match &parser.token {
                            Token::Ident(_, Keyword::TRIGGER) => {
                                let trigger_span = parser.consume_keyword(Keyword::TRIGGER)?;
                                let trigger_name = match &parser.token {
                                    Token::Ident(_, Keyword::ALL) => {
                                        TriggerName::All(parser.consume_keyword(Keyword::ALL)?)
                                    }
                                    Token::Ident(_, Keyword::USER) => {
                                        TriggerName::User(parser.consume_keyword(Keyword::USER)?)
                                    }
                                    _ => TriggerName::Named(
                                        parser.consume_plain_identifier_unreserved()?,
                                    ),
                                };
                                AlterSpecification::EnableTrigger(EnableTrigger {
                                    enable_span,
                                    modifier,
                                    trigger_span,
                                    trigger_name,
                                })
                            }
                            Token::Ident(_, Keyword::RULE) => {
                                let rule_span = parser.consume_keyword(Keyword::RULE)?;
                                let rule_name = parser.consume_plain_identifier_unreserved()?;
                                AlterSpecification::EnableRule(EnableRule {
                                    enable_span,
                                    modifier,
                                    rule_span,
                                    rule_name,
                                })
                            }
                            Token::Ident(_, Keyword::ROW) => {
                                let row_span = parser.consume_keyword(Keyword::ROW)?;
                                let level_span = parser.consume_keyword(Keyword::LEVEL)?;
                                let security_span = parser.consume_keyword(Keyword::SECURITY)?;
                                AlterSpecification::EnableRowLevelSecurity(EnableRowLevelSecurity {
                                    enable_span,
                                    row_span,
                                    level_span,
                                    security_span,
                                })
                            }
                            _ => parser.expected_failure("'TRIGGER', 'RULE', or 'ROW'")?,
                        }
                    }
                    Token::Ident(_, Keyword::FORCE) => {
                        let force_span = parser.consume_keyword(Keyword::FORCE)?;
                        parser.postgres_only(&force_span);
                        let row_span = parser.consume_keyword(Keyword::ROW)?;
                        let level_span = parser.consume_keyword(Keyword::LEVEL)?;
                        let security_span = parser.consume_keyword(Keyword::SECURITY)?;
                        AlterSpecification::ForceRowLevelSecurity(ForceRowLevelSecurity {
                            force_span,
                            row_span,
                            level_span,
                            security_span,
                        })
                    }
                    Token::Ident(_, Keyword::NO) => {
                        let no_span = parser.consume_keyword(Keyword::NO)?;
                        parser.postgres_only(&no_span);
                        let force_span = parser.consume_keyword(Keyword::FORCE)?;
                        let row_span = parser.consume_keyword(Keyword::ROW)?;
                        let level_span = parser.consume_keyword(Keyword::LEVEL)?;
                        let security_span = parser.consume_keyword(Keyword::SECURITY)?;
                        AlterSpecification::NoForceRowLevelSecurity(NoForceRowLevelSecurity {
                            no_span,
                            force_span,
                            row_span,
                            level_span,
                            security_span,
                        })
                    }
                    _ => parser.expected_failure("alter specification")?,
                });
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            Ok(())
        },
    )?;
    Ok(AlterTable {
        alter_span,
        online,
        ignore,
        table_span,
        if_exists,
        only,
        table,
        alter_specifications,
    })
}
