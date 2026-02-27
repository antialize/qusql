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
    DataType, Expression, Identifier, QualifiedName, SString, Span, Spanned,
    data_type::parse_data_type,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

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
            parser.consume_plain_identifier()?,
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
    },
    SetNotNull {
        set_not_null_span: Span,
    },
    DropNotNull {
        drop_not_null_span: Span,
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
            AlterColumnAction::Type { type_span, type_ } => type_span.join_span(type_),
            AlterColumnAction::SetNotNull { set_not_null_span } => set_not_null_span.clone(),
            AlterColumnAction::DropNotNull { drop_not_null_span } => drop_not_null_span.clone(),
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

/// Enum of alterations to perform on a table
#[derive(Clone, Debug)]
pub enum AlterSpecification<'a> {
    AddColumn {
        add_span: Span,
        if_not_exists_span: Option<Span>,
        identifier: Identifier<'a>,
        data_type: DataType<'a>,
        /// Optional "FIRST"
        first: Option<Span>,
        /// Optional "AFTER col_name"
        after: Option<(Span, Identifier<'a>)>,
    },
    /// Add an index
    AddIndex {
        /// Span of "ADD"
        add_span: Span,
        /// The type of index to add
        index_type: IndexType,
        /// Span of "IF NOT EXISTS" if specified
        if_not_exists: Option<Span>,
        /// Named of index if specified
        name: Option<Identifier<'a>>,
        /// Optional "CONSTRAINT" with symbol if specified
        constraint: Option<(Span, Option<Identifier<'a>>)>,
        /// Columns to add the index over
        cols: Vec<IndexCol<'a>>,
        /// Options on the index
        index_options: Vec<IndexOption<'a>>,
    },
    /// Add a foreign key
    AddForeignKey {
        /// Span of "ADD"
        add_span: Span,
        /// Optional "CONSTRAINT" with symbol if specified
        constraint: Option<(Span, Option<Identifier<'a>>)>,
        /// Span of "FOREIGN KEY"
        foreign_key_span: Span,
        /// Span of "IF NOT EXISTS" if specified
        if_not_exists: Option<Span>,
        /// Named of index if specified
        name: Option<Identifier<'a>>,
        /// Columns to add the index over
        cols: Vec<IndexCol<'a>>,
        /// Span of "REFERENCES"
        references_span: Span,
        /// Refereed table
        references_table: Identifier<'a>,
        /// Columns in referred table
        references_cols: Vec<Identifier<'a>>,
        /// List of what should happen at specified events
        ons: Vec<ForeignKeyOn>,
    },
    /// Modify a column
    Modify {
        /// Span of "MODIFY"
        modify_span: Span,
        /// Span of "IF EXISTS" if specified
        if_exists: Option<Span>,
        /// Name of column to modify
        col: Identifier<'a>,
        /// New definition of column
        definition: DataType<'a>,
        /// Optional "FIRST"
        first: Option<Span>,
        /// Optional "AFTER col_name"
        after: Option<(Span, Identifier<'a>)>,
    },
    DropColumn {
        /// Span of "DROP COLUMN"
        drop_column_span: Span,
        /// Name of column to drop
        column: Identifier<'a>,
        /// Span of "CASCADE" if specified
        cascade: Option<Span>,
    },
    DropIndex {
        /// Span of "DROP INDEX"
        drop_index_span: Span,
        /// Name of index to drop
        name: Identifier<'a>,
    },
    DropForeignKey {
        /// Span of "DROP FOREIGN KEY"
        drop_foreign_key_span: Span,
        /// Name of foreign key to drop
        name: Identifier<'a>,
    },
    DropPrimaryKey {
        /// Span of "DROP PRIMARY KEY"
        drop_primary_key_span: Span,
    },
    AlterColumn {
        /// Span of "ALTER COLUMN"
        alter_column_span: Span,
        /// Name of column to drop
        column: Identifier<'a>,
        alter_column_action: AlterColumnAction<'a>,
    },
    /// Modify a column
    OwnerTo {
        // Span of "OWNER TO"
        span: Span,
        /// Name of owner
        owner: AlterTableOwner<'a>,
    },
    Lock {
        /// Span of "LOCK"
        lock_span: Span,
        lock: AlterLock,
    },
    RenameColumn {
        /// Span of "RENAME COLUMN"
        rename_column_span: Span,
        /// Old name of column
        old_col_name: Identifier<'a>,
        /// Span of "TO"
        to_span: Span,
        /// New name of column
        new_col_name: Identifier<'a>,
    },
    RenameIndex {
        /// Span of "RENAME INDEX" "or RENAME KEY"
        rename_index_span: Span,
        /// Old name of index
        old_index_name: Identifier<'a>,
        /// Span of "TO"
        to_span: Span,
        /// New name of index
        new_index_name: Identifier<'a>,
    },
    RenameConstraint {
        /// Span of "RENAME CONSTRAINT"
        rename_constraint_span: Span,
        /// Old name of constraint
        old_constraint_name: Identifier<'a>,
        /// Span of "TO"
        to_span: Span,
        /// New name of constraint
        new_constraint_name: Identifier<'a>,
    },
    RenameTo {
        /// Span of "RENAME"
        rename_span: Span,
        /// Span of "TO" or "AS"
        to_span: Span,
        /// New name of table
        new_table_name: Identifier<'a>,
    },
    Algorithm {
        /// Span of "ALGORITHM"
        algorithm_span: Span,
        algorithm: AlterAlgorithm,
    },
    AutoIncrement {
        /// Span of "AUTO_INCREMENT"
        auto_increment_span: Span,
        value_span: Span,
        /// New value for auto_increment
        value: u64,
    },
    Change {
        /// Span of "CHANGE"
        change_span: Span,
        /// Optional span of "COLUMN"
        column_span: Option<Span>,
        /// Old name of column
        column: Identifier<'a>,
        /// New name of column
        new_column: Identifier<'a>,
        /// New definition of column
        definition: DataType<'a>,
        // Optional "FIRST"
        first: Option<Span>,
        // Optional "AFTER col_name"
        after: Option<(Span, Identifier<'a>)>,
    },
}

impl<'a> Spanned for AlterSpecification<'a> {
    fn span(&self) -> Span {
        match &self {
            AlterSpecification::AddColumn {
                add_span,
                if_not_exists_span,
                identifier,
                data_type,
                first,
                after,
            } => add_span
                .join_span(if_not_exists_span)
                .join_span(identifier)
                .join_span(data_type)
                .join_span(first)
                .join_span(after),
            AlterSpecification::AddIndex {
                add_span,
                index_type,
                if_not_exists,
                name,
                constraint,
                cols,
                index_options,
            } => add_span
                .join_span(index_type)
                .join_span(if_not_exists)
                .join_span(name)
                .join_span(constraint)
                .join_span(cols)
                .join_span(index_options),
            AlterSpecification::AddForeignKey {
                add_span,
                constraint,
                foreign_key_span: foregin_key_span,
                if_not_exists,
                name,
                cols,
                references_span,
                references_table,
                references_cols,
                ons,
            } => add_span
                .join_span(constraint)
                .join_span(foregin_key_span)
                .join_span(if_not_exists)
                .join_span(name)
                .join_span(cols)
                .join_span(references_span)
                .join_span(references_table)
                .join_span(references_cols)
                .join_span(ons),
            AlterSpecification::Modify {
                modify_span,
                if_exists,
                col,
                definition,
                first,
                after,
            } => modify_span
                .join_span(if_exists)
                .join_span(col)
                .join_span(definition)
                .join_span(first)
                .join_span(after),
            AlterSpecification::OwnerTo { span, owner } => span.join_span(owner),
            AlterSpecification::DropColumn {
                drop_column_span,
                column: col,
                cascade,
            } => drop_column_span.join_span(col).join_span(cascade),
            AlterSpecification::DropForeignKey {
                drop_foreign_key_span,
                name,
            } => drop_foreign_key_span.join_span(name),
            AlterSpecification::DropPrimaryKey {
                drop_primary_key_span,
            } => drop_primary_key_span.clone(),
            AlterSpecification::DropIndex {
                drop_index_span,
                name,
            } => drop_index_span.join_span(name),
            AlterSpecification::AlterColumn {
                alter_column_span,
                column: col,
                alter_column_action,
            } => alter_column_span
                .join_span(col)
                .join_span(alter_column_action),
            AlterSpecification::Lock { lock_span, lock } => lock_span.join_span(lock),
            AlterSpecification::RenameColumn {
                rename_column_span,
                old_col_name,
                to_span,
                new_col_name,
            } => rename_column_span
                .join_span(old_col_name)
                .join_span(to_span)
                .join_span(new_col_name),
            AlterSpecification::RenameIndex {
                rename_index_span,
                old_index_name,
                to_span,
                new_index_name,
            } => rename_index_span
                .join_span(old_index_name)
                .join_span(to_span)
                .join_span(new_index_name),
            AlterSpecification::RenameConstraint {
                rename_constraint_span,
                old_constraint_name,
                to_span,
                new_constraint_name,
            } => rename_constraint_span
                .join_span(old_constraint_name)
                .join_span(to_span)
                .join_span(new_constraint_name),
            AlterSpecification::RenameTo {
                rename_span,
                to_span,
                new_table_name,
            } => rename_span.join_span(to_span).join_span(new_table_name),
            AlterSpecification::Algorithm {
                algorithm_span,
                algorithm,
            } => algorithm_span.join_span(algorithm),
            AlterSpecification::AutoIncrement {
                auto_increment_span,
                value_span,
                ..
            } => auto_increment_span.join_span(value_span),
            AlterSpecification::Change {
                change_span,
                column_span,
                column,
                new_column,
                definition,
                first,
                after,
            } => change_span
                .join_span(column_span)
                .join_span(column)
                .join_span(new_column)
                .join_span(definition)
                .join_span(first)
                .join_span(after),
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
                    identifier: Identifier { value, span },
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
        let qname = parse_qualified_name(parser)?;
        parser.postgres_only(&qname);
        Ok(Some(qname))
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_index_cols<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<IndexCol<'a>>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut ans = Vec::new();
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            // Check if this is a functional index expression (starts with '(')
            let expr = if parser.token == Token::LParen {
                // Functional index: parse expression
                parser.consume_token(Token::LParen)?;
                let expression = parse_expression(parser, false)?;
                parser.consume_token(Token::RParen)?;
                IndexColExpr::Expression(expression)
            } else {
                // Regular column name
                let name = parser.consume_plain_identifier()?;
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
    parser.consume_token(Token::RParen)?;
    Ok(ans)
}

fn parse_cols<'a>(parser: &mut Parser<'a, '_>) -> Result<Vec<Identifier<'a>>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut ans = Vec::new();
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            ans.push(parser.consume_plain_identifier()?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    parser.consume_token(Token::RParen)?;
    Ok(ans)
}

fn parse_add_alter_specification<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<AlterSpecification<'a>, ParseError> {
    let add_span = parser.consume_keyword(Keyword::ADD)?;
    let constraint = if let Some(span) = parser.skip_keyword(Keyword::CONSTRAINT) {
        let v = match &parser.token {
            Token::Ident(_, kw) if !kw.reserved() => Some(parser.consume_plain_identifier()?),
            _ => None,
        };
        Some((span, v))
    } else {
        None
    };
    match &parser.token {
        Token::Ident(_, Keyword::FOREIGN) => {
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
                Token::Ident(_, kw) if !kw.reserved() => Some(parser.consume_plain_identifier()?),
                _ => None,
            };

            let cols = parse_index_cols(parser)?;
            let references_span = parser.consume_keyword(Keyword::REFERENCES)?;
            let references_table = parser.consume_plain_identifier()?;
            let references_cols = parse_cols(parser)?;
            let mut ons = Vec::new();
            while let Some(on) = parser.skip_keyword(Keyword::ON) {
                let type_ = match parser.token {
                    Token::Ident(_, Keyword::UPDATE) => ForeignKeyOnType::Update(
                        parser.consume_keyword(Keyword::UPDATE)?.join_span(&on),
                    ),
                    Token::Ident(_, Keyword::DELETE) => ForeignKeyOnType::Delete(
                        parser.consume_keyword(Keyword::DELETE)?.join_span(&on),
                    ),
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
            Ok(AlterSpecification::AddForeignKey {
                add_span,
                constraint,
                foreign_key_span: foregin_key_span,
                if_not_exists,
                name,
                cols,
                references_span,
                references_table,
                references_cols,
                ons,
            })
        }
        Token::Ident(
            _,
            Keyword::PRIMARY
            | Keyword::INDEX
            | Keyword::KEY
            | Keyword::FULLTEXT
            | Keyword::UNIQUE
            | Keyword::SPATIAL,
        ) => {
            let index_type = match &parser.token {
                Token::Ident(_, Keyword::PRIMARY) => {
                    IndexType::Primary(parser.consume_keywords(&[Keyword::PRIMARY, Keyword::KEY])?)
                }
                Token::Ident(_, Keyword::INDEX | Keyword::KEY) => {
                    IndexType::Index(parser.consume())
                }
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
                            IndexType::FullText(parser.consume_keyword(kw)?.join_span(&s))
                        }
                        _ => parser.expected_failure("'KEY' or 'INDEX'")?,
                    }
                }
                Token::Ident(_, Keyword::UNIQUE) => {
                    let s = parser.consume_keyword(Keyword::UNIQUE)?;
                    match &parser.token {
                        Token::Ident(_, kw @ Keyword::INDEX | kw @ Keyword::KEY) => {
                            let kw = *kw;
                            IndexType::FullText(parser.consume_keyword(kw)?.join_span(&s))
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
                Token::Ident(_, kw) if !kw.reserved() => Some(parser.consume_plain_identifier()?),
                _ => None,
            };

            let mut index_options = Vec::new();
            if matches!(parser.token, Token::Ident(_, Keyword::USING)) {
                parse_index_type(parser, &mut index_options)?;
            }
            let cols = parse_index_cols(parser)?;
            parse_index_options(parser, &mut index_options)?;

            Ok(AlterSpecification::AddIndex {
                add_span,
                constraint,
                index_type,
                if_not_exists,
                name,
                cols,
                index_options,
            })
        }
        Token::Ident(_, Keyword::COLUMN) => {
            parser.consume_keyword(Keyword::COLUMN)?;
            let mut if_not_exists_span = None;
            if matches!(parser.token, Token::Ident(_, Keyword::IF)) {
                if_not_exists_span =
                    Some(parser.consume_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])?);
            }

            if let Some(s) = &if_not_exists_span
                && parser.options.dialect.is_maria()
            {
                parser.err("IF NOT EXIST is not supported", s);
            }

            let identifier = parser.consume_plain_identifier()?;
            let data_type = parse_data_type(parser, false)?;

            let mut first = None;
            let mut after = None;
            match &parser.token {
                Token::Ident(_, Keyword::FIRST) => {
                    first = Some(parser.consume_keyword(Keyword::FIRST)?);
                }
                Token::Ident(_, Keyword::AFTER) => {
                    let after_span = parser.consume_keyword(Keyword::AFTER)?;
                    let after_col = parser.consume_plain_identifier()?;
                    after = Some((after_span, after_col));
                }
                _ => {}
            }

            Ok(AlterSpecification::AddColumn {
                add_span,
                if_not_exists_span,
                identifier,
                first,
                after,
                data_type,
            })
        }
        _ => parser.expected_failure("addable"),
    }
}

fn parse_rename_alter_specification<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<AlterSpecification<'a>, ParseError> {
    let rename_span = parser.consume_keyword(Keyword::RENAME)?;

    match parser.token {
        Token::Ident(_, Keyword::COLUMN) => {
            let column_span = parser.consume_keyword(Keyword::COLUMN)?;
            let old_col_name = parser.consume_plain_identifier()?;
            let to_span = parser.consume_keyword(Keyword::TO)?;
            let new_col_name = parser.consume_plain_identifier()?;
            Ok(AlterSpecification::RenameColumn {
                rename_column_span: rename_span.join_span(&column_span),
                old_col_name,
                to_span,
                new_col_name,
            })
        }
        Token::Ident(_, Keyword::INDEX | Keyword::KEY) => {
            let index_span = parser.consume();
            let old_index_name = parser.consume_plain_identifier()?;
            let to_span = parser.consume_keyword(Keyword::TO)?;
            let new_index_name = parser.consume_plain_identifier()?;
            Ok(AlterSpecification::RenameIndex {
                rename_index_span: rename_span.join_span(&index_span),
                old_index_name,
                to_span,
                new_index_name,
            })
        }
        Token::Ident(_, Keyword::CONSTRAINT) => {
            let constraint_span = parser.consume_keyword(Keyword::CONSTRAINT)?;
            parser.postgres_only(&constraint_span);
            let old_constraint_name = parser.consume_plain_identifier()?;
            let to_span = parser.consume_keyword(Keyword::TO)?;
            let new_constraint_name = parser.consume_plain_identifier()?;
            Ok(AlterSpecification::RenameConstraint {
                rename_constraint_span: rename_span.join_span(&constraint_span),
                old_constraint_name,
                to_span,
                new_constraint_name,
            })
        }
        Token::Ident(_, Keyword::TO) | Token::Ident(_, Keyword::AS) => {
            let to_span = parser.consume();
            let new_table_name = parser.consume_plain_identifier()?;
            Ok(AlterSpecification::RenameTo {
                rename_span,
                to_span,
                new_table_name,
            })
        }
        _ => parser.expected_failure("'COLUMN', 'INDEX', 'CONSTRAINT' or 'TO'")?,
    }
}

fn parse_drop<'a>(parser: &mut Parser<'a, '_>) -> Result<AlterSpecification<'a>, ParseError> {
    let drop_span = parser.consume_keyword(Keyword::DROP)?;
    match parser.token {
        Token::Ident(_, Keyword::INDEX | Keyword::KEY) => {
            let index_span = parser.consume();
            let name = parser.consume_plain_identifier()?;
            Ok(AlterSpecification::DropIndex {
                drop_index_span: drop_span.join_span(&index_span).join_span(&name),
                name,
            })
        }
        Token::Ident(_, Keyword::FOREIGN) => {
            let foreign_span = parser.consume_keywords(&[Keyword::FOREIGN, Keyword::KEY])?;
            let name = parser.consume_plain_identifier()?;
            Ok(AlterSpecification::DropForeignKey {
                drop_foreign_key_span: drop_span.join_span(&foreign_span).join_span(&name),
                name,
            })
        }
        Token::Ident(_, Keyword::PRIMARY) => {
            let primary_key_span = parser.consume_keywords(&[Keyword::PRIMARY, Keyword::KEY])?;
            Ok(AlterSpecification::DropPrimaryKey {
                drop_primary_key_span: drop_span.join_span(&primary_key_span),
            })
        }
        Token::Ident(_, Keyword::COLUMN) => {
            let drop_column_span = drop_span.join_span(&parser.consume_keyword(Keyword::COLUMN)?);
            let column = parser.consume_plain_identifier()?;
            let cascade = parser.skip_keyword(Keyword::CASCADE);
            if let Some(span) = &cascade {
                parser.postgres_only(span);
            }
            Ok(AlterSpecification::DropColumn {
                drop_column_span,
                column,
                cascade,
            })
        }
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
///     Some(Statement::AlterTable(a)) => a,
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
///     Some(Statement::AlterTable(a)) => a,
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
    let table = parse_qualified_name(parser)?;
    let d = parser.delimiter.clone();
    let mut alter_specifications = Vec::new();
    parser.recovered(d.name(), &|t| t == &d || t == &Token::Eof, |parser| {
        loop {
            alter_specifications.push(match parser.token {
                Token::Ident(_, Keyword::ADD) => parse_add_alter_specification(parser)?,
                Token::Ident(_, Keyword::MODIFY) => {
                    let mut modify_span = parser.consume_keyword(Keyword::MODIFY)?;
                    parser.maria_only(&modify_span);
                    if let Some(v) = parser.skip_keyword(Keyword::COLUMN) {
                        modify_span = modify_span.join_span(&v);
                    }
                    let if_exists = if let Some(span) = parser.skip_keyword(Keyword::IF) {
                        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&span))
                    } else {
                        None
                    };
                    let col = parser.consume_plain_identifier()?;
                    let definition = parse_data_type(parser, false)?;

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
                            let col = parser.consume_plain_identifier()?;
                            after = Some((after_span, col));
                        }
                        _ => {}
                    }

                    AlterSpecification::Modify {
                        modify_span,
                        if_exists,
                        col,
                        definition,
                        first,
                        after,
                    }
                }
                Token::Ident(_, Keyword::OWNER) => {
                    let span = parser.consume_keywords(&[Keyword::OWNER, Keyword::TO])?;
                    parser.postgres_only(&span);
                    let owner = parse_alter_owner(parser)?;
                    AlterSpecification::OwnerTo { span, owner }
                }
                Token::Ident(_, Keyword::DROP) => parse_drop(parser)?,
                Token::Ident(_, Keyword::ALTER) => {
                    let span = parser.consume_keywords(&[Keyword::ALTER, Keyword::COLUMN])?;
                    parser.postgres_only(&span);
                    let column = parser.consume_plain_identifier()?;

                    let alter_column_action = match parser.token {
                        Token::Ident(_, Keyword::SET) => {
                            let set_span = parser.consume();
                            match parser.token {
                                Token::Ident(_, Keyword::DEFAULT) => {
                                    let set_default_span = parser.consume().join_span(&set_span);
                                    let value = parse_expression(parser, false)?;
                                    AlterColumnAction::SetDefault {
                                        set_default_span,
                                        value,
                                    }
                                }
                                Token::Ident(_, Keyword::NOT) => {
                                    let set_not_null_span = set_span.join_span(
                                        &parser.consume_keywords(&[Keyword::NOT, Keyword::NULL])?,
                                    );
                                    AlterColumnAction::SetNotNull { set_not_null_span }
                                }
                                _ => parser.expected_failure("'DEFAULT' or 'NOT NULL'")?,
                            }
                        }
                        Token::Ident(_, Keyword::DROP) => {
                            let set_span = parser.consume();
                            match parser.token {
                                Token::Ident(_, Keyword::DEFAULT) => {
                                    let drop_default_span = parser.consume().join_span(&set_span);
                                    AlterColumnAction::DropDefault { drop_default_span }
                                }
                                Token::Ident(_, Keyword::NOT) => {
                                    let drop_not_null_span = set_span.join_span(
                                        &parser.consume_keywords(&[Keyword::NOT, Keyword::NULL])?,
                                    );
                                    AlterColumnAction::DropNotNull { drop_not_null_span }
                                }
                                _ => parser.expected_failure("'DEFAULT' or 'NOT NULL'")?,
                            }
                        }
                        Token::Ident(_, Keyword::TYPE) => {
                            let type_span = parser.consume();
                            let type_ = parse_data_type(parser, false)?;
                            AlterColumnAction::Type { type_span, type_ }
                        }
                        _ => parser.expected_failure("alter column action")?,
                    };
                    AlterSpecification::AlterColumn {
                        alter_column_span: span,
                        column,
                        alter_column_action,
                    }
                }
                Token::Ident(_, Keyword::LOCK) => {
                    let lock_span = parser.consume_keyword(Keyword::LOCK)?;
                    parser.maria_only(&lock_span);
                    parser.skip_token(Token::Eq);
                    let lock = match &parser.token {
                        Token::Ident(_, Keyword::DEFAULT) => {
                            AlterLock::Default(parser.consume_keyword(Keyword::DEFAULT)?)
                        }
                        Token::Ident(_, Keyword::NONE) => {
                            AlterLock::None(parser.consume_keyword(Keyword::NONE)?)
                        }
                        Token::Ident(_, Keyword::SHARED) => {
                            AlterLock::Shared(parser.consume_keyword(Keyword::SHARED)?)
                        }
                        Token::Ident(_, Keyword::EXCLUSIVE) => {
                            AlterLock::Exclusive(parser.consume_keyword(Keyword::EXCLUSIVE)?)
                        }
                        _ => {
                            parser.expected_failure("'DEFAULT', 'NONE', 'SHARED' or 'EXCLUSIVE'")?
                        }
                    };
                    AlterSpecification::Lock { lock_span, lock }
                }
                Token::Ident(_, Keyword::ALGORITHM) => {
                    let algorithm_span = parser.consume_keyword(Keyword::ALGORITHM)?;
                    parser.maria_only(&algorithm_span);
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
                        _ => {
                            parser.expected_failure("'DEFAULT', 'INSTANT', 'INPLACE' or 'COPY'")?
                        }
                    };
                    AlterSpecification::Algorithm {
                        algorithm_span,
                        algorithm,
                    }
                }
                Token::Ident(_, Keyword::AUTO_INCREMENT) => {
                    let auto_increment_span = parser.consume_keyword(Keyword::AUTO_INCREMENT)?;
                    parser.maria_only(&auto_increment_span);
                    parser.skip_token(Token::Eq);
                    let (value, value_span) = parser.consume_int()?;
                    AlterSpecification::AutoIncrement {
                        auto_increment_span,
                        value_span,
                        value,
                    }
                }
                Token::Ident(_, Keyword::RENAME) => parse_rename_alter_specification(parser)?,
                Token::Ident(_, Keyword::CHANGE) => {
                    let change_span = parser.consume_keyword(Keyword::CHANGE)?;
                    parser.maria_only(&change_span);
                    let column_span = parser.skip_keyword(Keyword::COLUMN);

                    let column = parser.consume_plain_identifier()?;
                    let new_column = parser.consume_plain_identifier()?;
                    let definition = parse_data_type(parser, false)?;

                    let mut first = None;
                    let mut after = None;
                    match &parser.token {
                        Token::Ident(_, Keyword::FIRST) => {
                            let first_span = parser.consume_keyword(Keyword::FIRST)?;
                            parser.maria_only(&first_span);
                            first = Some(first_span);
                        }
                        Token::Ident(_, Keyword::AFTER) => {
                            let after_span = parser.consume_keyword(Keyword::AFTER)?;
                            parser.maria_only(&after_span);
                            let after_col = parser.consume_plain_identifier()?;
                            after = Some((after_span, after_col));
                        }
                        _ => (),
                    }
                    AlterSpecification::Change {
                        change_span,
                        column_span,
                        column,
                        new_column,
                        definition,
                        first,
                        after,
                    }
                }
                _ => parser.expected_failure("alter specification")?,
            });
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    Ok(AlterTable {
        alter_span,
        online,
        ignore,
        table_span,
        if_exists,
        table,
        alter_specifications,
    })
}
