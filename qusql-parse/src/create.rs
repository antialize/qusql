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
    alter_table::{IndexCol, IndexColExpr, parse_operator_class},
    create_function::parse_create_function,
    create_option::{CreateAlgorithm, CreateOption},
    create_table::parse_create_table,
    create_view::parse_create_view,
    data_type::parse_data_type,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    operator::parse_create_operator,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
    statement::{Block, parse_statement},
};
use alloc::{boxed::Box, vec::Vec};

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
    pub statement: Statement<'a>,
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
) -> Result<CreateTrigger<'a>, ParseError> {
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

    // PostgreSQL allows EXECUTE FUNCTION func_name() instead of a statement block
    let statement = if matches!(parser.token, Token::Ident(_, Keyword::EXECUTE)) {
        // Parse EXECUTE FUNCTION func_name()
        let _execute_span = parser.consume_keyword(Keyword::EXECUTE)?;
        parser.consume_keyword(Keyword::FUNCTION)?;
        parser.consume_plain_identifier()?;
        let begin_span = parser.consume_token(Token::LParen)?;
        // TODO: parse function arguments if needed
        let end_span = parser.consume_token(Token::RParen)?;

        // Use an empty block as a placeholder for EXECUTE FUNCTION
        Statement::Block(Box::new(Block {
            begin_span,
            end_span,
            statements: Vec::new(),
        }))
    } else {
        let old = core::mem::replace(&mut parser.permit_compound_statements, true);
        let statement = match parse_statement(parser)? {
            Some(v) => v,
            None => parser.expected_failure("statement")?,
        };
        parser.permit_compound_statements = old;
        statement
    };

    Ok(CreateTrigger {
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
        statement,
    })
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
pub enum CreateIndexOption<'a> {
    UsingGist(Span),
    UsingBTree(Span),
    UsingHash(Span),
    UsingRTree(Span),
    UsingBloom(Span),
    UsingBrin(Span),
    UsingHnsw(Span),
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
            CreateIndexOption::UsingBloom(s) => s.clone(),
            CreateIndexOption::UsingBrin(s) => s.clone(),
            CreateIndexOption::UsingHnsw(s) => s.clone(),
            CreateIndexOption::Algorithm(s, i) => s.join_span(i),
            CreateIndexOption::Lock(s, i) => s.join_span(i),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IncludeClause<'a> {
    pub include_span: Span,
    pub l_paren_span: Span,
    pub columns: Vec<Identifier<'a>>,
    pub r_paren_span: Span,
}

impl<'a> Spanned for IncludeClause<'a> {
    fn span(&self) -> Span {
        self.include_span
            .join_span(&self.l_paren_span)
            .join_span(&self.columns)
            .join_span(&self.r_paren_span)
    }
}

#[derive(Clone, Debug)]
pub struct CreateIndex<'a> {
    pub create_span: Span,
    pub create_options: Vec<CreateOption<'a>>,
    pub index_span: Span,
    pub index_name: Option<Identifier<'a>>,
    pub if_not_exists: Option<Span>,
    pub on_span: Span,
    pub table_name: QualifiedName<'a>,
    pub index_options: Vec<CreateIndexOption<'a>>,
    pub l_paren_span: Span,
    pub column_names: Vec<IndexCol<'a>>,
    pub r_paren_span: Span,
    pub include_clause: Option<IncludeClause<'a>>,
    pub where_: Option<(Span, Expression<'a>)>,
    pub nulls_distinct: Option<(Span, Option<Span>)>,
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
            .join_span(&self.include_clause)
            .join_span(&self.where_)
            .join_span(&self.nulls_distinct)
    }
}

fn parse_create_index<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    mut create_options: Vec<CreateOption<'a>>,
) -> Result<CreateIndex<'a>, ParseError> {
    let index_span = parser.consume_keyword(Keyword::INDEX)?;

    // PostgreSQL: CONCURRENTLY
    if let Some(concurrently_span) = parser.skip_keyword(Keyword::CONCURRENTLY) {
        parser.postgres_only(&concurrently_span);
        create_options.push(CreateOption::Concurrently(concurrently_span));
    }

    let if_not_exists = if let Some(s) = parser.skip_keyword(Keyword::IF) {
        Some(s.join_span(&parser.consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?))
    } else {
        None
    };

    // PostgreSQL: index name is optional, ON can come directly after INDEX
    let index_name = if let Token::Ident(_, Keyword::ON) = &parser.token {
        // Unnamed index
        None
    } else {
        // Named index
        Some(parser.consume_plain_identifier()?)
    };

    // MySQL/MariaDB require index names
    if index_name.is_none() && parser.options.dialect.is_maria() {
        parser.err("Index name required", &index_span);
    }

    let on_span = parser.consume_keyword(Keyword::ON)?;
    let table_name = parse_qualified_name(parser)?;

    // PostgreSQL: USING (GIST|BLOOM|BRIN|HNSW) before column list
    let mut index_options = Vec::new();
    if let Some(using_span) = parser.skip_keyword(Keyword::USING) {
        match &parser.token {
            Token::Ident(_, Keyword::GIST) => {
                let gist_span = parser.consume_keyword(Keyword::GIST)?;
                index_options.push(CreateIndexOption::UsingGist(
                    using_span.join_span(&gist_span),
                ));
            }
            Token::Ident(_, Keyword::BLOOM) => {
                let bloom_span = parser.consume_keyword(Keyword::BLOOM)?;
                index_options.push(CreateIndexOption::UsingBloom(
                    using_span.join_span(&bloom_span),
                ));
            }
            Token::Ident(_, Keyword::BRIN) => {
                let brin_span = parser.consume_keyword(Keyword::BRIN)?;
                index_options.push(CreateIndexOption::UsingBrin(
                    using_span.join_span(&brin_span),
                ));
            }
            Token::Ident(_, Keyword::HNSW) => {
                let hnsw_span = parser.consume_keyword(Keyword::HNSW)?;
                index_options.push(CreateIndexOption::UsingHnsw(
                    using_span.join_span(&hnsw_span),
                ));
            }
            _ => {
                // Error - USING before column list requires GIST/BLOOM/BRIN/HNSW for PostgreSQL
                parser
                    .err_here("Expected GIST, BLOOM, BRIN, or HNSW after USING (or use USING after column list for MySQL)")?;
            }
        }
    }

    let l_paren_span = parser.consume_token(Token::LParen)?;
    let mut column_names = Vec::new();
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

        column_names.push(IndexCol {
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

    let r_paren_span = parser.consume_token(Token::RParen)?;

    // PostgreSQL: INCLUDE clause
    let include_clause = if let Some(include_span) = parser.skip_keyword(Keyword::INCLUDE) {
        let l_paren = parser.consume_token(Token::LParen)?;
        let mut include_cols = Vec::new();
        loop {
            include_cols.push(parser.consume_plain_identifier()?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        let r_paren = parser.consume_token(Token::RParen)?;
        parser.postgres_only(&include_span);
        Some(IncludeClause {
            include_span,
            l_paren_span: l_paren,
            columns: include_cols,
            r_paren_span: r_paren,
        })
    } else {
        None
    };

    // Parse index options after column list (MySQL/MariaDB)

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

    // PostgreSQL: NULLS [NOT] DISTINCT
    let nulls_distinct = if let Some(nulls_span) = parser.skip_keyword(Keyword::NULLS) {
        let not_span = parser.skip_keyword(Keyword::NOT);
        let distinct_span = parser.consume_keyword(Keyword::DISTINCT)?;
        parser.postgres_only(&nulls_span.join_span(&distinct_span));
        Some((nulls_span, not_span))
    } else {
        None
    };

    Ok(CreateIndex {
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
        include_clause,
        where_,
        nulls_distinct,
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

/// Role option for CREATE ROLE / ALTER ROLE
#[derive(Clone, Debug)]
pub enum RoleOption<'a> {
    SuperUser(Span),
    NoSuperUser(Span),
    CreateDb(Span),
    NoCreateDb(Span),
    CreateRole(Span),
    NoCreateRole(Span),
    Inherit(Span),
    NoInherit(Span),
    Login(Span),
    NoLogin(Span),
    Replication(Span),
    NoReplication(Span),
    BypassRls(Span),
    NoBypassRls(Span),
    ConnectionLimit(Span, Expression<'a>),
    EncryptedPassword(Span, Expression<'a>),
    Password(Span, Expression<'a>),
    PasswordNull(Span),
    ValidUntil(Span, Expression<'a>),
    Sysid(Span, Expression<'a>),
}

impl<'a> Spanned for RoleOption<'a> {
    fn span(&self) -> Span {
        match self {
            RoleOption::SuperUser(s) => s.span(),
            RoleOption::NoSuperUser(s) => s.span(),
            RoleOption::CreateDb(s) => s.span(),
            RoleOption::NoCreateDb(s) => s.span(),
            RoleOption::CreateRole(s) => s.span(),
            RoleOption::NoCreateRole(s) => s.span(),
            RoleOption::Inherit(s) => s.span(),
            RoleOption::NoInherit(s) => s.span(),
            RoleOption::Login(s) => s.span(),
            RoleOption::NoLogin(s) => s.span(),
            RoleOption::Replication(s) => s.span(),
            RoleOption::NoReplication(s) => s.span(),
            RoleOption::BypassRls(s) => s.span(),
            RoleOption::NoBypassRls(s) => s.span(),
            RoleOption::ConnectionLimit(s, e) => s.join_span(e),
            RoleOption::EncryptedPassword(s, e) => s.join_span(e),
            RoleOption::Password(s, e) => s.join_span(e),
            RoleOption::PasswordNull(s) => s.span(),
            RoleOption::ValidUntil(s, e) => s.join_span(e),
            RoleOption::Sysid(s, e) => s.join_span(e),
        }
    }
}

/// Role membership type for CREATE ROLE
#[derive(Clone, Debug)]
pub enum RoleMembershipType {
    User(Span),
    Role(Span),
    Admin(Span),
    InRole(Span),
}

impl Spanned for RoleMembershipType {
    fn span(&self) -> Span {
        match self {
            RoleMembershipType::User(s) => s.span(),
            RoleMembershipType::Role(s) => s.span(),
            RoleMembershipType::Admin(s) => s.span(),
            RoleMembershipType::InRole(s) => s.span(),
        }
    }
}

/// Role membership clause in CREATE ROLE
#[derive(Clone, Debug)]
pub struct RoleMembership<'a> {
    pub type_: RoleMembershipType,
    pub roles: Vec<Identifier<'a>>,
}

impl<'a> Spanned for RoleMembership<'a> {
    fn span(&self) -> Span {
        self.type_.join_span(&self.roles)
    }
}

/// CREATE ROLE statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateRole<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "ROLE"
    pub role_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Names of the roles to create
    pub role_names: Vec<Identifier<'a>>,
    /// Optional WITH keyword span
    pub with_span: Option<Span>,
    /// Role options
    pub options: Vec<RoleOption<'a>>,
    /// Role membership clauses (USER, ROLE, ADMIN, IN ROLE)
    pub memberships: Vec<RoleMembership<'a>>,
}

impl<'a> Spanned for CreateRole<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.role_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.role_names)
            .join_span(&self.with_span)
            .join_span(&self.options)
            .join_span(&self.memberships)
    }
}

fn parse_create_role<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateRole<'a>, ParseError> {
    let role_span = parser.consume_keyword(Keyword::ROLE)?;
    parser.postgres_only(&role_span);

    for option in create_options {
        parser.err("Not supported for CREATE ROLE", &option.span());
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

    // Parse role names (comma-separated list)
    let mut role_names = Vec::new();
    loop {
        role_names.push(parser.consume_plain_identifier()?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }

    // Optional WITH keyword
    let with_span = parser.skip_keyword(Keyword::WITH);

    // Parse options and memberships
    let mut options = Vec::new();
    let mut memberships = Vec::new();

    loop {
        match &parser.token {
            // Membership clauses
            Token::Ident(_, Keyword::USER) => {
                let user_span = parser.consume_keyword(Keyword::USER)?;
                let mut roles = Vec::new();
                loop {
                    roles.push(parser.consume_plain_identifier()?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                memberships.push(RoleMembership {
                    type_: RoleMembershipType::User(user_span),
                    roles,
                });
            }
            Token::Ident(_, Keyword::ROLE) => {
                let role_span = parser.consume_keyword(Keyword::ROLE)?;
                let mut roles = Vec::new();
                loop {
                    roles.push(parser.consume_plain_identifier()?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                memberships.push(RoleMembership {
                    type_: RoleMembershipType::Role(role_span),
                    roles,
                });
            }
            Token::Ident(_, Keyword::ADMIN) => {
                let admin_span = parser.consume_keyword(Keyword::ADMIN)?;
                let mut roles = Vec::new();
                loop {
                    roles.push(parser.consume_plain_identifier()?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                memberships.push(RoleMembership {
                    type_: RoleMembershipType::Admin(admin_span),
                    roles,
                });
            }
            Token::Ident(_, Keyword::IN) => {
                let in_role_span = parser.consume_keywords(&[Keyword::IN, Keyword::ROLE])?;
                let mut roles = Vec::new();
                loop {
                    roles.push(parser.consume_plain_identifier()?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                memberships.push(RoleMembership {
                    type_: RoleMembershipType::InRole(in_role_span),
                    roles,
                });
            }
            // Role options
            Token::Ident(_, Keyword::SUPERUSER) => {
                let span = parser.consume_keyword(Keyword::SUPERUSER)?;
                options.push(RoleOption::SuperUser(span));
            }
            Token::Ident(_, Keyword::NOSUPERUSER) => {
                let span = parser.consume_keyword(Keyword::NOSUPERUSER)?;
                options.push(RoleOption::NoSuperUser(span));
            }
            Token::Ident(_, Keyword::CREATEDB) => {
                let span = parser.consume_keyword(Keyword::CREATEDB)?;
                options.push(RoleOption::CreateDb(span));
            }
            Token::Ident(_, Keyword::NOCREATEDB) => {
                let span = parser.consume_keyword(Keyword::NOCREATEDB)?;
                options.push(RoleOption::NoCreateDb(span));
            }
            Token::Ident(_, Keyword::CREATEROLE) => {
                let span = parser.consume_keyword(Keyword::CREATEROLE)?;
                options.push(RoleOption::CreateRole(span));
            }
            Token::Ident(_, Keyword::NOCREATEROLE) => {
                let span = parser.consume_keyword(Keyword::NOCREATEROLE)?;
                options.push(RoleOption::NoCreateRole(span));
            }
            Token::Ident(_, Keyword::INHERIT) => {
                let span = parser.consume_keyword(Keyword::INHERIT)?;
                options.push(RoleOption::Inherit(span));
            }
            Token::Ident(_, Keyword::NOINHERIT) => {
                let span = parser.consume_keyword(Keyword::NOINHERIT)?;
                options.push(RoleOption::NoInherit(span));
            }
            Token::Ident(_, Keyword::LOGIN) => {
                let span = parser.consume_keyword(Keyword::LOGIN)?;
                options.push(RoleOption::Login(span));
            }
            Token::Ident(_, Keyword::NOLOGIN) => {
                let span = parser.consume_keyword(Keyword::NOLOGIN)?;
                options.push(RoleOption::NoLogin(span));
            }
            Token::Ident(_, Keyword::REPLICATION) => {
                let span = parser.consume_keyword(Keyword::REPLICATION)?;
                options.push(RoleOption::Replication(span));
            }
            Token::Ident(_, Keyword::NOREPLICATION) => {
                let span = parser.consume_keyword(Keyword::NOREPLICATION)?;
                options.push(RoleOption::NoReplication(span));
            }
            Token::Ident(_, Keyword::BYPASSRLS) => {
                let span = parser.consume_keyword(Keyword::BYPASSRLS)?;
                options.push(RoleOption::BypassRls(span));
            }
            Token::Ident(_, Keyword::NOBYPASSRLS) => {
                let span = parser.consume_keyword(Keyword::NOBYPASSRLS)?;
                options.push(RoleOption::NoBypassRls(span));
            }
            Token::Ident(_, Keyword::CONNECTION) => {
                let span = parser.consume_keywords(&[Keyword::CONNECTION, Keyword::LIMIT])?;
                let expr = parse_expression(parser, false)?;
                options.push(RoleOption::ConnectionLimit(span, expr));
            }
            Token::Ident(_, Keyword::ENCRYPTED) => {
                let span = parser.consume_keywords(&[Keyword::ENCRYPTED, Keyword::PASSWORD])?;
                let expr = parse_expression(parser, false)?;
                options.push(RoleOption::EncryptedPassword(span, expr));
            }
            Token::Ident(_, Keyword::PASSWORD) => {
                let password_span = parser.consume_keyword(Keyword::PASSWORD)?;
                if parser.skip_keyword(Keyword::NULL).is_some() {
                    options.push(RoleOption::PasswordNull(password_span));
                } else {
                    let expr = parse_expression(parser, false)?;
                    options.push(RoleOption::Password(password_span, expr));
                }
            }
            Token::Ident(_, Keyword::VALID) => {
                let span = parser.consume_keywords(&[Keyword::VALID, Keyword::UNTIL])?;
                let expr = parse_expression(parser, false)?;
                options.push(RoleOption::ValidUntil(span, expr));
            }
            Token::Ident(_, Keyword::SYSID) => {
                let sysid_span = parser.consume_keyword(Keyword::SYSID)?;
                let expr = parse_expression(parser, false)?;
                options.push(RoleOption::Sysid(sysid_span, expr));
            }
            _ => break,
        }
    }

    Ok(CreateRole {
        create_span,
        role_span,
        if_not_exists,
        role_names,
        with_span,
        options,
        memberships,
    })
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
            Token::Ident(_, Keyword::OPERATOR) => Statement::CreateOperator(Box::new(
                parse_create_operator(parser, create_span, create_options)?,
            )),
            _ => return parser.expected_failure(CREATABLE),
        };
    Ok(r)
}
