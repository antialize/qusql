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

use alloc::{boxed::Box, vec::Vec};

use crate::{
    AlterOperator, AlterOperatorClass, AlterOperatorFamily, AlterRole, AlterTable, AlterType,
    CreateConstraintTrigger, CreateDomain, CreateExtension, CreateIndex, CreateOperator,
    CreateOperatorClass, CreateOperatorFamily, CreateRole, CreateTrigger, DropOperatorClass,
    DropOperatorFamily, QualifiedName, RenameTable, Span, Spanned, WithQuery,
    alter_role::parse_alter_role,
    alter_table::parse_alter_table,
    copy::{CopyFrom, CopyTo, parse_copy_statement},
    create::{
        CreateDatabase, CreateSchema, CreateSequence, CreateServer, CreateTypeEnum, parse_create,
    },
    create_function::CreateFunction,
    create_table::{CreateTable, CreateTablePartitionOf},
    create_view::CreateView,
    delete::{Delete, parse_delete},
    drop::{
        DropDatabase, DropDomain, DropEvent, DropExtension, DropFunction, DropIndex, DropOperator,
        DropProcedure, DropSequence, DropServer, DropTable, DropTrigger, DropView, parse_drop,
    },
    expression::{Expression, PRIORITY_MAX, parse_expression_unreserved},
    flush::{Flush, parse_flush},
    insert_replace::{InsertReplace, parse_insert_replace},
    keywords::Keyword,
    kill::{Kill, parse_kill},
    lexer::{StringType, Token},
    lock::{Lock, Unlock, parse_lock, parse_unlock},
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
    rename::parse_rename_table,
    select::{OrderFlag, Select, parse_select},
    show::{
        ShowCharacterSet, ShowCollation, ShowColumns, ShowCreateDatabase, ShowCreateTable,
        ShowCreateView, ShowDatabases, ShowEngines, ShowProcessList, ShowStatus, ShowTables,
        ShowVariables, parse_show,
    },
    span::OptSpanned,
    truncate::{TruncateTable, parse_truncate_table},
    update::{Update, parse_update},
    values::parse_values,
    with_query::parse_with_query,
};

#[derive(Clone, Debug)]
pub struct Set<'a> {
    pub set_span: Span,
    pub values: Vec<(QualifiedName<'a>, Expression<'a>)>,
}

impl<'a> Spanned for Set<'a> {
    fn span(&self) -> Span {
        self.set_span.join_span(&self.values)
    }
}

fn parse_set<'a>(parser: &mut Parser<'a, '_>) -> Result<Set<'a>, ParseError> {
    let set_span = parser.consume_keyword(Keyword::SET)?;
    let mut values = Vec::new();
    loop {
        let name = parse_qualified_name_unreserved(parser)?;
        parser.consume_token(Token::Eq)?;
        let val = parse_expression_unreserved(parser, PRIORITY_MAX)?;
        values.push((name, val));
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    Ok(Set { set_span, values })
}

fn parse_statement_list_inner<'a>(
    parser: &mut Parser<'a, '_>,
    out: &mut Vec<Statement<'a>>,
) -> Result<(), ParseError> {
    loop {
        while parser.skip_token(Token::Delimiter).is_some() {}
        let stdin = match parse_statement(parser)? {
            Some(v) => {
                let stdin = v.reads_from_stdin();
                out.push(v);
                stdin
            }
            None => break,
        };
        if !matches!(parser.token, Token::Delimiter) {
            break;
        }
        if stdin {
            let (s, span) = parser.read_from_stdin_and_next();
            out.push(Statement::Stdin(Box::new(Stdin { input: s, span })));
        } else {
            parser.consume_token(Token::Delimiter)?;
        }
    }
    Ok(())
}

fn parse_statement_list<'a>(
    parser: &mut Parser<'a, '_>,
    out: &mut Vec<Statement<'a>>,
) -> Result<(), ParseError> {
    let old_delimiter = parser.lexer.replace_delimitor(None);
    let r = parse_statement_list_inner(parser, out);
    parser.lexer.replace_delimitor(old_delimiter);
    r
}

fn parse_begin(parser: &mut Parser<'_, '_>) -> Result<Begin, ParseError> {
    Ok(Begin {
        span: parser.consume_keyword(Keyword::BEGIN)?,
    })
}

fn parse_end(parser: &mut Parser<'_, '_>) -> Result<End, ParseError> {
    Ok(End {
        span: parser.consume_keyword(Keyword::END)?,
    })
}

fn parse_start_transaction<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<StartTransaction, ParseError> {
    Ok(StartTransaction {
        span: parser.consume_keywords(&[Keyword::START, Keyword::TRANSACTION])?,
    })
}

fn parse_commit(parser: &mut Parser<'_, '_>) -> Result<Commit, ParseError> {
    Ok(Commit {
        span: parser.consume_keyword(Keyword::COMMIT)?,
    })
}

fn parse_block<'a>(parser: &mut Parser<'a, '_>) -> Result<Block<'a>, ParseError> {
    let begin_span = parser.consume_keyword(Keyword::BEGIN)?;
    let mut statements = Vec::new();
    parser.recovered(
        "'END' | 'EXCEPTION'",
        &|e| matches!(e, Token::Ident(_, Keyword::END | Keyword::EXCEPTION)),
        |parser| parse_statement_list(parser, &mut statements),
    )?;
    if let Some(_exception_span) = parser.skip_keyword(Keyword::EXCEPTION) {
        while let Some(_when_span) = parser.skip_keyword(Keyword::WHEN) {
            parser.consume_plain_identifier_unreserved()?;
            parser.consume_keyword(Keyword::THEN)?;
            parse_expression_unreserved(parser, PRIORITY_MAX)?;
            parser.consume_token(Token::SemiColon)?;
        }
    }
    let end_span = parser.consume_keyword(Keyword::END)?;
    Ok(Block {
        begin_span,
        statements,
        end_span,
    })
}

/// Condition in if statement
#[derive(Clone, Debug)]
pub struct IfCondition<'a> {
    /// Span of "ELSEIF" if specified
    pub elseif_span: Option<Span>,
    /// Expression that must be true for `then` to be executed
    pub search_condition: Expression<'a>,
    /// Span of "THEN"
    pub then_span: Span,
    /// List of statement to be executed if `search_condition` is true
    pub then: Vec<Statement<'a>>,
}

impl<'a> Spanned for IfCondition<'a> {
    fn span(&self) -> Span {
        self.then_span
            .join_span(&self.elseif_span)
            .join_span(&self.search_condition)
            .join_span(&self.then_span)
            .join_span(&self.then)
    }
}

/// If statement
#[derive(Clone, Debug)]
pub struct If<'a> {
    /// Span of "IF"
    pub if_span: Span,
    // List of if a then v parts
    pub conditions: Vec<IfCondition<'a>>,
    /// Span of "ELSE" and else Statement if specified
    pub else_: Option<(Span, Vec<Statement<'a>>)>,
    /// Span of "ENDIF"
    pub endif_span: Span,
}

impl<'a> Spanned for If<'a> {
    fn span(&self) -> Span {
        self.if_span
            .join_span(&self.conditions)
            .join_span(&self.else_)
            .join_span(&self.endif_span)
    }
}

fn parse_if<'a>(parser: &mut Parser<'a, '_>) -> Result<If<'a>, ParseError> {
    let if_span = parser.consume_keyword(Keyword::IF)?;
    let mut conditions = Vec::new();
    let mut else_ = None;
    parser.recovered(
        "'END'",
        &|e| matches!(e, Token::Ident(_, Keyword::END)),
        |parser| {
            let search_condition = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            let then_span = parser.consume_keyword(Keyword::THEN)?;
            let mut then = Vec::new();
            parse_statement_list(parser, &mut then)?;
            conditions.push(IfCondition {
                elseif_span: None,
                search_condition,
                then_span,
                then,
            });
            while let Some(elseif_span) = parser.skip_keyword(Keyword::ELSEIF) {
                let search_condition = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                let then_span = parser.consume_keyword(Keyword::THEN)?;
                let mut then = Vec::new();
                parse_statement_list(parser, &mut then)?;
                conditions.push(IfCondition {
                    elseif_span: Some(elseif_span),
                    search_condition,
                    then_span,
                    then,
                })
            }
            if let Some(else_span) = parser.skip_keyword(Keyword::ELSE) {
                let mut o = Vec::new();
                parse_statement_list(parser, &mut o)?;
                else_ = Some((else_span, o));
            }
            Ok(())
        },
    )?;
    let endif_span = parser.consume_keywords(&[Keyword::END, Keyword::IF])?;
    Ok(If {
        if_span,
        conditions,
        else_,
        endif_span,
    })
}

/// Return statement
#[derive(Clone, Debug)]
pub struct Return<'a> {
    /// Span of "Return"
    pub return_span: Span,
    pub expr: Expression<'a>,
}

impl<'a> Spanned for Return<'a> {
    fn span(&self) -> Span {
        self.return_span.join_span(&self.expr)
    }
}

fn parse_return<'a>(parser: &mut Parser<'a, '_>) -> Result<Return<'a>, ParseError> {
    let return_span = parser.consume_keyword(Keyword::RETURN)?;
    let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
    Ok(Return { return_span, expr })
}

#[derive(Clone, Debug)]
pub enum SignalConditionInformationName {
    ClassOrigin(Span),
    SubclassOrigin(Span),
    MessageText(Span),
    MysqlErrno(Span),
    ConstraintCatalog(Span),
    ConstraintSchema(Span),
    ConstraintName(Span),
    CatalogName(Span),
    SchemaName(Span),
    TableName(Span),
    ColumnName(Span),
    CursorName(Span),
}

impl Spanned for SignalConditionInformationName {
    fn span(&self) -> Span {
        match self {
            SignalConditionInformationName::ClassOrigin(span) => span.clone(),
            SignalConditionInformationName::SubclassOrigin(span) => span.clone(),
            SignalConditionInformationName::MessageText(span) => span.clone(),
            SignalConditionInformationName::MysqlErrno(span) => span.clone(),
            SignalConditionInformationName::ConstraintCatalog(span) => span.clone(),
            SignalConditionInformationName::ConstraintSchema(span) => span.clone(),
            SignalConditionInformationName::ConstraintName(span) => span.clone(),
            SignalConditionInformationName::CatalogName(span) => span.clone(),
            SignalConditionInformationName::SchemaName(span) => span.clone(),
            SignalConditionInformationName::TableName(span) => span.clone(),
            SignalConditionInformationName::ColumnName(span) => span.clone(),
            SignalConditionInformationName::CursorName(span) => span.clone(),
        }
    }
}

/// Return statement
#[derive(Clone, Debug)]
pub struct Signal<'a> {
    pub signal_span: Span,
    pub sqlstate_span: Span,
    pub value_span: Option<Span>,
    pub sql_state: Expression<'a>,
    pub set_span: Option<Span>,
    pub sets: Vec<(SignalConditionInformationName, Span, Expression<'a>)>,
}

impl<'a> Spanned for Signal<'a> {
    fn span(&self) -> Span {
        self.signal_span
            .join_span(&self.sqlstate_span)
            .join_span(&self.value_span)
            .join_span(&self.sql_state)
            .join_span(&self.set_span)
            .join_span(&self.sets)
    }
}

fn parse_signal<'a>(parser: &mut Parser<'a, '_>) -> Result<Signal<'a>, ParseError> {
    let signal_span = parser.consume_keyword(Keyword::SIGNAL)?;
    let sqlstate_span = parser.consume_keyword(Keyword::SQLSTATE)?;
    let value_span = parser.skip_keyword(Keyword::VALUE);
    let sql_state = parse_expression_unreserved(parser, PRIORITY_MAX)?;
    let mut sets = Vec::new();
    let set_span = parser.skip_keyword(Keyword::SET);
    if set_span.is_some() {
        loop {
            let v = match &parser.token {
                Token::Ident(_, Keyword::CLASS_ORIGIN) => {
                    SignalConditionInformationName::ClassOrigin(parser.consume())
                }
                Token::Ident(_, Keyword::SUBCLASS_ORIGIN) => {
                    SignalConditionInformationName::SubclassOrigin(parser.consume())
                }
                Token::Ident(_, Keyword::MESSAGE_TEXT) => {
                    SignalConditionInformationName::MessageText(parser.consume())
                }
                Token::Ident(_, Keyword::MYSQL_ERRNO) => {
                    SignalConditionInformationName::MysqlErrno(parser.consume())
                }
                Token::Ident(_, Keyword::CONSTRAINT_CATALOG) => {
                    SignalConditionInformationName::ConstraintCatalog(parser.consume())
                }
                Token::Ident(_, Keyword::CONSTRAINT_SCHEMA) => {
                    SignalConditionInformationName::ConstraintSchema(parser.consume())
                }
                Token::Ident(_, Keyword::CONSTRAINT_NAME) => {
                    SignalConditionInformationName::ConstraintName(parser.consume())
                }
                Token::Ident(_, Keyword::CATALOG_NAME) => {
                    SignalConditionInformationName::CatalogName(parser.consume())
                }
                Token::Ident(_, Keyword::SCHEMA_NAME) => {
                    SignalConditionInformationName::SchemaName(parser.consume())
                }
                Token::Ident(_, Keyword::TABLE_NAME) => {
                    SignalConditionInformationName::TableName(parser.consume())
                }
                Token::Ident(_, Keyword::COLUMN_NAME) => {
                    SignalConditionInformationName::ColumnName(parser.consume())
                }
                Token::Ident(_, Keyword::CURSOR_NAME) => {
                    SignalConditionInformationName::CursorName(parser.consume())
                }
                _ => parser.expected_failure("Condition information item name")?,
            };
            let eq_span = parser.consume_token(Token::Eq)?;
            let value = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            sets.push((v, eq_span, value));
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
    }
    Ok(Signal {
        signal_span,
        sqlstate_span,
        value_span,
        sql_state,
        set_span,
        sets,
    })
}

/// Block statement, for example in stored procedures
#[derive(Clone, Debug)]
pub struct Block<'a> {
    /// Span of "BEGIN"
    pub begin_span: Span,
    /// Statements in block
    pub statements: Vec<Statement<'a>>,
    /// Span of "END"
    pub end_span: Span,
}

impl Spanned for Block<'_> {
    fn span(&self) -> Span {
        self.begin_span
            .join_span(&self.statements)
            .join_span(&self.end_span)
    }
}

/// Begin statement
#[derive(Clone, Debug)]
pub struct Begin {
    /// Span of "BEGIN"
    pub span: Span,
}

impl Spanned for Begin {
    fn span(&self) -> Span {
        self.span.clone()
    }
}

/// End statement
#[derive(Clone, Debug)]
pub struct End {
    /// Span of "END"
    pub span: Span,
}

impl Spanned for End {
    fn span(&self) -> Span {
        self.span.clone()
    }
}

/// Commit statement
#[derive(Clone, Debug)]
pub struct Commit {
    /// Span of "COMMIT"
    pub span: Span,
}

impl Spanned for Commit {
    fn span(&self) -> Span {
        self.span.clone()
    }
}

/// Start transaction statement
#[derive(Clone, Debug)]
pub struct StartTransaction {
    /// Span of "START TRANSACTION"
    pub span: Span,
}

impl Spanned for StartTransaction {
    fn span(&self) -> Span {
        self.span.clone()
    }
}

/// Do statement
#[derive(Clone, Debug)]
pub struct Do<'a> {
    /// Span of "DO"
    pub do_span: Span,
    /// Statements in "DO"
    pub statements: Vec<Statement<'a>>,
}

impl<'a> Spanned for Do<'a> {
    fn span(&self) -> Span {
        self.do_span.join_span(&self.statements)
    }
}

/// Invalid statement produced after recovering from parse error
#[derive(Clone, Debug)]
pub struct Invalid {
    /// Span of invalid statement
    pub span: Span,
}

impl Spanned for Invalid {
    fn span(&self) -> Span {
        self.span.clone()
    }
}

/// Stdin statement, used to represent input from stdin after a COPY statement
#[derive(Clone, Debug)]
pub struct Stdin<'a> {
    /// The input from stdin
    pub input: &'a str,
    /// Span of the input
    pub span: Span,
}

impl Spanned for Stdin<'_> {
    fn span(&self) -> Span {
        self.span.clone()
    }
}

/// ALTER SCHEMA statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct AlterSchema<'a> {
    /// Span of "ALTER SCHEMA"
    pub alter_schema_span: Span,
    /// The schema name
    pub name: QualifiedName<'a>,
    /// The action (RENAME TO, OWNER TO)
    pub action: AlterSchemaAction<'a>,
}

#[derive(Clone, Debug)]
pub enum AlterSchemaAction<'a> {
    /// RENAME TO new_name
    RenameTo {
        rename_to_span: Span,
        new_name: QualifiedName<'a>,
    },
    /// OWNER TO new_owner
    OwnerTo {
        owner_to_span: Span,
        new_owner: crate::alter_table::AlterTableOwner<'a>,
    },
}

impl<'a> Spanned for AlterSchemaAction<'a> {
    fn span(&self) -> Span {
        match self {
            AlterSchemaAction::RenameTo {
                rename_to_span,
                new_name,
            } => rename_to_span.join_span(new_name),
            AlterSchemaAction::OwnerTo {
                owner_to_span,
                new_owner,
            } => owner_to_span.join_span(new_owner),
        }
    }
}

impl<'a> Spanned for AlterSchema<'a> {
    fn span(&self) -> Span {
        self.alter_schema_span
            .join_span(&self.name)
            .join_span(&self.action)
    }
}

/// Parse ALTER SCHEMA statement (PostgreSQL)
pub(crate) fn parse_alter_schema<'a>(
    parser: &mut Parser<'a, '_>,
    alter_schema_span: Span,
) -> Result<AlterSchema<'a>, ParseError> {
    parser.postgres_only(&alter_schema_span);
    let name = parse_qualified_name_unreserved(parser)?;
    let action = match &parser.token {
        Token::Ident(_, Keyword::RENAME) => {
            let rename_to_span = parser.consume_keywords(&[Keyword::RENAME, Keyword::TO])?;
            let new_name = parse_qualified_name_unreserved(parser)?;
            AlterSchemaAction::RenameTo {
                rename_to_span,
                new_name,
            }
        }
        Token::Ident(_, Keyword::OWNER) => {
            let owner_to_span = parser.consume_keywords(&[Keyword::OWNER, Keyword::TO])?;
            let new_owner = crate::alter_table::parse_alter_owner(parser)?;
            AlterSchemaAction::OwnerTo {
                owner_to_span,
                new_owner,
            }
        }
        _ => parser.expected_failure("'RENAME TO' or 'OWNER TO' after ALTER SCHEMA ...")?,
    };
    Ok(AlterSchema {
        alter_schema_span,
        name,
        action,
    })
}

pub fn parse_alter<'a>(parser: &mut Parser<'a, '_>) -> Result<Statement<'a>, ParseError> {
    let alter_span = parser.consume_keyword(Keyword::ALTER)?;
    let online = parser.skip_keyword(Keyword::ONLINE);
    let ignore = parser.skip_keyword(Keyword::IGNORE);

    match &parser.token {
        Token::Ident(_, Keyword::SCHEMA) => {
            let schema_span = parser.consume_keyword(Keyword::SCHEMA)?;
            Ok(Statement::AlterSchema(Box::new(parse_alter_schema(
                parser,
                alter_span.join_span(&schema_span),
            )?)))
        }
        Token::Ident(_, Keyword::TABLE) => Ok(Statement::AlterTable(Box::new(parse_alter_table(
            parser, alter_span, online, ignore,
        )?))),
        Token::Ident(_, Keyword::ROLE) => Ok(Statement::AlterRole(Box::new(parse_alter_role(
            parser, alter_span,
        )?))),
        Token::Ident(_, Keyword::TYPE) => {
            let type_span = parser.consume_keyword(Keyword::TYPE)?;
            Ok(Statement::AlterType(Box::new(
                crate::alter_type::parse_alter_type(parser, alter_span.join_span(&type_span))?,
            )))
        }
        Token::Ident(_, Keyword::OPERATOR) => {
            let operator_span = parser.consume_keyword(Keyword::OPERATOR)?;
            match &parser.token {
                Token::Ident(_, Keyword::CLASS) => {
                    let class_span = parser.consume_keyword(Keyword::CLASS)?;
                    Ok(Statement::AlterOperatorClass(Box::new(
                        crate::operator::parse_alter_operator_class(
                            parser,
                            alter_span.join_span(&operator_span).join_span(&class_span),
                        )?,
                    )))
                }
                Token::Ident(_, Keyword::FAMILY) => {
                    let family_span = parser.consume_keyword(Keyword::FAMILY)?;
                    Ok(Statement::AlterOperatorFamily(Box::new(
                        crate::operator::parse_alter_operator_family(
                            parser,
                            alter_span.join_span(&operator_span).join_span(&family_span),
                        )?,
                    )))
                }
                _ => Ok(Statement::AlterOperator(Box::new(
                    crate::operator::parse_alter_operator(
                        parser,
                        alter_span.join_span(&operator_span),
                    )?,
                ))),
            }
        }
        _ => parser.expected_failure("alterable"),
    }
}

/// SQL statement
#[derive(Clone, Debug)]
pub enum Statement<'a> {
    AlterSchema(Box<AlterSchema<'a>>),
    CreateIndex(Box<CreateIndex<'a>>),
    CreateTable(Box<CreateTable<'a>>),
    CreateView(Box<CreateView<'a>>),
    CreateTrigger(Box<CreateTrigger<'a>>),
    CreateFunction(Box<CreateFunction<'a>>),
    CreateDatabase(Box<CreateDatabase<'a>>),
    CreateSchema(Box<CreateSchema<'a>>),
    CreateSequence(Box<CreateSequence<'a>>),
    CreateServer(Box<CreateServer<'a>>),
    CreateRole(Box<CreateRole<'a>>),
    CreateOperator(Box<CreateOperator<'a>>),
    CreateTypeEnum(Box<CreateTypeEnum<'a>>),
    CreateOperatorClass(Box<CreateOperatorClass<'a>>),
    CreateOperatorFamily(Box<CreateOperatorFamily<'a>>),
    CreateExtension(Box<CreateExtension<'a>>),
    CreateDomain(Box<CreateDomain<'a>>),
    CreateConstraintTrigger(Box<CreateConstraintTrigger<'a>>),
    CreateTablePartitionOf(Box<CreateTablePartitionOf<'a>>),
    AlterOperator(Box<AlterOperator<'a>>),
    AlterOperatorClass(Box<AlterOperatorClass<'a>>),
    Select(Box<Select<'a>>),
    Delete(Box<Delete<'a>>),
    InsertReplace(Box<InsertReplace<'a>>),
    Update(Box<Update<'a>>),
    Unlock(Box<Unlock>),
    DropIndex(Box<DropIndex<'a>>),
    DropTable(Box<DropTable<'a>>),
    DropFunction(Box<DropFunction<'a>>),
    DropProcedure(Box<DropProcedure<'a>>),
    DropSequence(Box<DropSequence<'a>>),
    DropEvent(Box<DropEvent<'a>>),
    DropDatabase(Box<DropDatabase<'a>>),
    DropServer(Box<DropServer<'a>>),
    DropTrigger(Box<DropTrigger<'a>>),
    DropView(Box<DropView<'a>>),
    DropExtension(Box<DropExtension<'a>>),
    DropOperator(Box<DropOperator<'a>>),
    DropOperatorFamily(Box<DropOperatorFamily<'a>>),
    DropOperatorClass(Box<DropOperatorClass<'a>>),
    DropDomain(Box<DropDomain<'a>>),
    Set(Box<Set<'a>>),
    Signal(Box<Signal<'a>>),
    Kill(Box<Kill<'a>>),
    ShowTables(Box<ShowTables<'a>>),
    ShowDatabases(Box<ShowDatabases>),
    ShowProcessList(Box<ShowProcessList>),
    ShowVariables(Box<ShowVariables<'a>>),
    ShowStatus(Box<ShowStatus<'a>>),
    ShowColumns(Box<ShowColumns<'a>>),
    ShowCreateTable(Box<ShowCreateTable<'a>>),
    ShowCreateDatabase(Box<ShowCreateDatabase<'a>>),
    ShowCreateView(Box<ShowCreateView<'a>>),
    ShowCharacterSet(Box<ShowCharacterSet<'a>>),
    ShowCollation(Box<ShowCollation<'a>>),
    ShowEngines(Box<ShowEngines>),
    AlterTable(Box<AlterTable<'a>>),
    AlterRole(Box<AlterRole<'a>>),
    AlterType(Box<AlterType<'a>>),
    AlterOperatorFamily(Box<AlterOperatorFamily<'a>>),
    Block(Box<Block<'a>>),
    Begin(Box<Begin>),
    End(Box<End>),
    Commit(Box<Commit>),
    StartTransaction(Box<StartTransaction>),
    If(Box<If<'a>>),
    /// Invalid statement produced after recovering from parse error
    Invalid(Box<Invalid>),
    Lock(Box<Lock<'a>>),
    CompoundQuery(Box<CompoundQuery<'a>>),
    Case(Box<CaseStatement<'a>>),
    CopyFrom(Box<CopyFrom<'a>>),
    CopyTo(Box<CopyTo<'a>>),
    Stdin(Box<Stdin<'a>>),
    Do(Box<Do<'a>>),
    TruncateTable(Box<TruncateTable<'a>>),
    RenameTable(Box<RenameTable<'a>>),
    WithQuery(Box<WithQuery<'a>>),
    Return(Box<Return<'a>>),
    Flush(Box<Flush<'a>>),
    /// PostgreSQL VALUES statement
    Values(Box<crate::values::Values<'a>>),
    /// PostgreSQL EXPLAIN statement
    Explain(Box<Explain<'a>>),
    /// PostgreSQL DECLARE cursor statement
    DeclareCursor(Box<DeclareCursor<'a>>),
    /// PostgreSQL REFRESH MATERIALIZED VIEW statement
    RefreshMaterializedView(Box<RefreshMaterializedView<'a>>),
    /// PostgreSQL PREPARE statement
    Prepare(Box<Prepare<'a>>),
}

impl<'a> Spanned for Statement<'a> {
    fn span(&self) -> Span {
        match &self {
            Statement::AlterOperator(v) => v.span(),
            Statement::AlterSchema(v) => v.span(),
            Statement::CreateIndex(v) => v.span(),
            Statement::CreateTable(v) => v.span(),
            Statement::CreateView(v) => v.span(),
            Statement::CreateTrigger(v) => v.span(),
            Statement::CreateFunction(v) => v.span(),
            Statement::CreateDatabase(v) => v.span(),
            Statement::CreateSchema(v) => v.span(),
            Statement::CreateSequence(v) => v.span(),
            Statement::CreateServer(v) => v.span(),
            Statement::CreateRole(v) => v.span(),
            Statement::CreateOperator(v) => v.span(),
            Statement::Select(v) => v.span(),
            Statement::Delete(v) => v.span(),
            Statement::InsertReplace(v) => v.span(),
            Statement::Update(v) => v.span(),
            Statement::Unlock(v) => v.span(),
            Statement::DropDatabase(v) => v.span(),
            Statement::DropDomain(v) => v.span(),
            Statement::DropEvent(v) => v.span(),
            Statement::DropExtension(v) => v.span(),
            Statement::DropFunction(v) => v.span(),
            Statement::DropIndex(v) => v.span(),
            Statement::DropOperator(v) => v.span(),
            Statement::DropOperatorClass(v) => v.span(),
            Statement::AlterOperatorClass(v) => v.span(),
            Statement::DropProcedure(v) => v.span(),
            Statement::DropSequence(v) => v.span(),
            Statement::DropServer(v) => v.span(),
            Statement::DropTable(v) => v.span(),
            Statement::DropTrigger(v) => v.span(),
            Statement::DropView(v) => v.span(),
            Statement::Set(v) => v.span(),
            Statement::AlterOperatorFamily(v) => v.span(),
            Statement::DropOperatorFamily(v) => v.span(),
            Statement::AlterRole(v) => v.span(),
            Statement::AlterType(v) => v.span(),
            Statement::AlterTable(v) => v.span(),
            Statement::Block(v) => v.opt_span().expect("Span of block"),
            Statement::If(v) => v.span(),
            Statement::Invalid(v) => v.span(),
            Statement::Lock(v) => v.span(),
            Statement::CompoundQuery(v) => v.span(),
            Statement::Case(v) => v.span(),
            Statement::CopyFrom(v) => v.span(),
            Statement::CopyTo(v) => v.span(),
            Statement::Stdin(v) => v.span(),
            Statement::Begin(v) => v.span(),
            Statement::End(v) => v.span(),
            Statement::Commit(v) => v.span(),
            Statement::StartTransaction(v) => v.span(),
            Statement::CreateTypeEnum(v) => v.span(),
            Statement::CreateOperatorClass(v) => v.span(),
            Statement::Do(v) => v.opt_span().expect("Span of block"),
            Statement::TruncateTable(v) => v.span(),
            Statement::RenameTable(v) => v.span(),
            Statement::WithQuery(v) => v.span(),
            Statement::Return(v) => v.span(),
            Statement::Signal(v) => v.span(),
            Statement::Kill(v) => v.span(),
            Statement::ShowTables(v) => v.span(),
            Statement::ShowDatabases(v) => v.span(),
            Statement::ShowProcessList(v) => v.span(),
            Statement::ShowVariables(v) => v.span(),
            Statement::ShowStatus(v) => v.span(),
            Statement::ShowColumns(v) => v.span(),
            Statement::ShowCreateTable(v) => v.span(),
            Statement::ShowCreateDatabase(v) => v.span(),
            Statement::ShowCreateView(v) => v.span(),
            Statement::ShowCharacterSet(v) => v.span(),
            Statement::ShowCollation(v) => v.span(),
            Statement::ShowEngines(v) => v.span(),
            Statement::Flush(v) => v.span(),
            Statement::CreateOperatorFamily(v) => v.span(),
            Statement::Values(v) => v.span(),
            Statement::CreateExtension(v) => v.span(),
            Statement::CreateDomain(v) => v.span(),
            Statement::CreateConstraintTrigger(v) => v.span(),
            Statement::CreateTablePartitionOf(v) => v.span(),
            Statement::Explain(v) => v.span(),
            Statement::DeclareCursor(v) => v.span(),
            Statement::RefreshMaterializedView(v) => v.span(),
            Statement::Prepare(v) => v.span(),
        }
    }
}

impl Statement<'_> {
    fn reads_from_stdin(&self) -> bool {
        match self {
            Statement::CopyFrom(v) => v.reads_from_stdin(),
            _ => false,
        }
    }
}

pub(crate) fn parse_statement<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Option<Statement<'a>>, ParseError> {
    Ok(match &parser.token {
        Token::Ident(_, Keyword::CREATE) => Some(parse_create(parser)?),
        Token::Ident(_, Keyword::DROP) => Some(parse_drop(parser)?),
        Token::Ident(_, Keyword::SELECT) | Token::LParen => Some(parse_compound_query(parser)?),
        Token::Ident(_, Keyword::VALUES) => {
            Some(Statement::Values(Box::new(parse_values(parser)?)))
        }
        Token::Ident(_, Keyword::DELETE) => {
            Some(Statement::Delete(Box::new(parse_delete(parser)?)))
        }
        Token::Ident(_, Keyword::INSERT | Keyword::REPLACE) => Some(Statement::InsertReplace(
            Box::new(parse_insert_replace(parser)?),
        )),
        Token::Ident(_, Keyword::UPDATE) => {
            Some(Statement::Update(Box::new(parse_update(parser)?)))
        }
        Token::Ident(_, Keyword::SET) => Some(Statement::Set(Box::new(parse_set(parser)?))),
        Token::Ident(_, Keyword::SIGNAL) => {
            Some(Statement::Signal(Box::new(parse_signal(parser)?)))
        }
        Token::Ident(_, Keyword::KILL) => Some(Statement::Kill(Box::new(parse_kill(parser)?))),
        Token::Ident(_, Keyword::SHOW) => Some(parse_show(parser)?),
        Token::Ident(_, Keyword::BEGIN) => Some(if parser.permit_compound_statements {
            Statement::Block(Box::new(parse_block(parser)?))
        } else {
            Statement::Begin(Box::new(parse_begin(parser)?))
        }),
        Token::Ident(_, Keyword::END) if !parser.permit_compound_statements => {
            Some(Statement::End(Box::new(parse_end(parser)?)))
        }
        Token::Ident(_, Keyword::START) => Some(Statement::StartTransaction(Box::new(
            parse_start_transaction(parser)?,
        ))),
        Token::Ident(_, Keyword::COMMIT) => {
            Some(Statement::Commit(Box::new(parse_commit(parser)?)))
        }
        Token::Ident(_, Keyword::IF) => Some(Statement::If(Box::new(parse_if(parser)?))),
        Token::Ident(_, Keyword::RETURN) => {
            Some(Statement::Return(Box::new(parse_return(parser)?)))
        }
        Token::Ident(_, Keyword::ALTER) => Some(parse_alter(parser)?),
        Token::Ident(_, Keyword::CASE) => {
            Some(Statement::Case(Box::new(parse_case_statement(parser)?)))
        }
        Token::Ident(_, Keyword::COPY) => Some(parse_copy_statement(parser)?),
        Token::Ident(_, Keyword::DO) => Some(parse_do(parser)?),
        Token::Ident(_, Keyword::LOCK) => Some(Statement::Lock(Box::new(parse_lock(parser)?))),
        Token::Ident(_, Keyword::UNLOCK) => {
            Some(Statement::Unlock(Box::new(parse_unlock(parser)?)))
        }
        Token::Ident(_, Keyword::TRUNCATE) => Some(Statement::TruncateTable(Box::new(
            parse_truncate_table(parser)?,
        ))),
        Token::Ident(_, Keyword::RENAME) => Some(Statement::RenameTable(Box::new(
            parse_rename_table(parser)?,
        ))),
        Token::Ident(_, Keyword::WITH) => {
            Some(Statement::WithQuery(Box::new(parse_with_query(parser)?)))
        }
        Token::Ident(_, Keyword::FLUSH) => Some(Statement::Flush(Box::new(parse_flush(parser)?))),
        Token::Ident(_, Keyword::EXPLAIN) => {
            Some(Statement::Explain(Box::new(parse_explain(parser)?)))
        }
        Token::Ident(_, Keyword::DECLARE) => Some(Statement::DeclareCursor(Box::new(
            parse_declare_cursor(parser)?,
        ))),
        Token::Ident(_, Keyword::PREPARE) => {
            Some(Statement::Prepare(Box::new(parse_prepare(parser)?)))
        }
        Token::Ident(_, Keyword::REFRESH) => Some(Statement::RefreshMaterializedView(Box::new(
            parse_refresh_materialized_view(parser)?,
        ))),
        _ => None,
    })
}

pub(crate) fn parse_do<'a>(parser: &mut Parser<'a, '_>) -> Result<Statement<'a>, ParseError> {
    let do_span = parser.consume_keyword(Keyword::DO)?;
    if matches!(parser.token, Token::String(_, StringType::DollarQuoted)) {
        // PostgreSQL: DO $$...$$ — the body is a dollar-quoted string literal
        parser.consume();
        return Ok(Statement::Do(Box::new(Do {
            do_span,
            statements: Vec::new(),
        })));
    }
    parser.consume_token(Token::DoubleDollar)?;
    let block = parse_block(parser)?;
    parser.consume_token(Token::DoubleDollar)?;
    Ok(Statement::Do(Box::new(Do {
        do_span,
        statements: block.statements,
    })))
}

/// PostgreSQL EXPLAIN output format
#[derive(Clone, Debug)]
pub enum ExplainFormat {
    Text(Span),
    Xml(Span),
    Json(Span),
    Yaml(Span),
}

impl Spanned for ExplainFormat {
    fn span(&self) -> Span {
        match self {
            ExplainFormat::Text(s)
            | ExplainFormat::Xml(s)
            | ExplainFormat::Json(s)
            | ExplainFormat::Yaml(s) => s.clone(),
        }
    }
}

/// A single option in a parenthesized EXPLAIN (...) list
#[derive(Clone, Debug)]
pub enum ExplainOption {
    Analyze(Span, Option<(bool, Span)>),
    Verbose(Span, Option<(bool, Span)>),
    Costs(Span, Option<(bool, Span)>),
    Settings(Span, Option<(bool, Span)>),
    GenericPlan(Span, Option<(bool, Span)>),
    Buffers(Span, Option<(bool, Span)>),
    Wal(Span, Option<(bool, Span)>),
    Timing(Span, Option<(bool, Span)>),
    Summary(Span, Option<(bool, Span)>),
    Memory(Span, Option<(bool, Span)>),
    Format(Span, ExplainFormat),
}

impl Spanned for ExplainOption {
    fn span(&self) -> Span {
        match self {
            ExplainOption::Analyze(s, b)
            | ExplainOption::Verbose(s, b)
            | ExplainOption::Costs(s, b)
            | ExplainOption::Settings(s, b)
            | ExplainOption::GenericPlan(s, b)
            | ExplainOption::Buffers(s, b)
            | ExplainOption::Wal(s, b)
            | ExplainOption::Timing(s, b)
            | ExplainOption::Summary(s, b)
            | ExplainOption::Memory(s, b) => s.join_span(&b.as_ref().map(|(_, vs)| vs.clone())),
            ExplainOption::Format(s, fmt) => s.join_span(fmt),
        }
    }
}

/// PostgreSQL EXPLAIN statement
#[derive(Clone, Debug)]
pub struct Explain<'a> {
    pub explain_span: Span,
    pub options: Vec<ExplainOption>,
    pub statement: Box<Statement<'a>>,
}

impl<'a> Spanned for Explain<'a> {
    fn span(&self) -> Span {
        self.explain_span.join_span(&self.statement)
    }
}

fn parse_explain<'a>(parser: &mut Parser<'a, '_>) -> Result<Explain<'a>, ParseError> {
    let explain_span = parser.consume_keyword(Keyword::EXPLAIN)?;
    parser.postgres_only(&explain_span);
    let mut options = Vec::new();
    if matches!(parser.token, Token::LParen) {
        // Parenthesized option list: EXPLAIN (ANALYZE, BUFFERS, ...)
        parser.consume_token(Token::LParen)?;
        loop {
            let opt = match &parser.token {
                Token::Ident(_, Keyword::ANALYZE) => {
                    let s = parser.consume_keyword(Keyword::ANALYZE)?;
                    ExplainOption::Analyze(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::VERBOSE) => {
                    let s = parser.consume_keyword(Keyword::VERBOSE)?;
                    ExplainOption::Verbose(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::COSTS) => {
                    let s = parser.consume_keyword(Keyword::COSTS)?;
                    ExplainOption::Costs(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::SETTINGS) => {
                    let s = parser.consume_keyword(Keyword::SETTINGS)?;
                    ExplainOption::Settings(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::GENERIC_PLAN) => {
                    let s = parser.consume_keyword(Keyword::GENERIC_PLAN)?;
                    ExplainOption::GenericPlan(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::BUFFERS) => {
                    let s = parser.consume_keyword(Keyword::BUFFERS)?;
                    ExplainOption::Buffers(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::WAL) => {
                    let s = parser.consume_keyword(Keyword::WAL)?;
                    ExplainOption::Wal(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::TIMING) => {
                    let s = parser.consume_keyword(Keyword::TIMING)?;
                    ExplainOption::Timing(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::SUMMARY) => {
                    let s = parser.consume_keyword(Keyword::SUMMARY)?;
                    ExplainOption::Summary(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::MEMORY) => {
                    let s = parser.consume_keyword(Keyword::MEMORY)?;
                    ExplainOption::Memory(s, parser.try_parse_bool())
                }
                Token::Ident(_, Keyword::FORMAT) => {
                    let fmt_kw = parser.consume_keyword(Keyword::FORMAT)?;
                    let fmt = match &parser.token {
                        Token::Ident(_, Keyword::TEXT) => ExplainFormat::Text(parser.consume()),
                        Token::Ident(_, Keyword::XML) => ExplainFormat::Xml(parser.consume()),
                        Token::Ident(_, Keyword::JSON) => ExplainFormat::Json(parser.consume()),
                        Token::Ident(_, Keyword::YAML) => ExplainFormat::Yaml(parser.consume()),
                        _ => parser.expected_failure("TEXT, XML, JSON, or YAML")?,
                    };
                    ExplainOption::Format(fmt_kw, fmt)
                }
                _ => parser.expected_failure("EXPLAIN option")?,
            };
            options.push(opt);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        parser.consume_token(Token::RParen)?;
    } else {
        // Legacy: EXPLAIN [ANALYZE] [VERBOSE]
        if let Some(s) = parser.skip_keyword(Keyword::ANALYZE) {
            options.push(ExplainOption::Analyze(s.clone(), Some((true, s))));
        }
        if let Some(s) = parser.skip_keyword(Keyword::VERBOSE) {
            options.push(ExplainOption::Verbose(s.clone(), Some((true, s))));
        }
    }
    let inner = match parse_statement(parser)? {
        Some(s) => s,
        None => parser.expected_failure("Statement after EXPLAIN")?,
    };
    Ok(Explain {
        explain_span,
        options,
        statement: Box::new(inner),
    })
}

/// Sensitivity of a declared cursor
#[derive(Clone, Debug)]
pub enum CursorSensitivity {
    Asensitive(Span),
    Insensitive(Span),
}

impl Spanned for CursorSensitivity {
    fn span(&self) -> Span {
        match self {
            CursorSensitivity::Asensitive(s) | CursorSensitivity::Insensitive(s) => s.clone(),
        }
    }
}

/// Scroll behaviour of a declared cursor
#[derive(Clone, Debug)]
pub enum CursorScroll {
    Scroll(Span),
    NoScroll(Span),
}

impl Spanned for CursorScroll {
    fn span(&self) -> Span {
        match self {
            CursorScroll::Scroll(s) | CursorScroll::NoScroll(s) => s.clone(),
        }
    }
}

/// Hold behaviour of a declared cursor
#[derive(Clone, Debug)]
pub enum CursorHold {
    WithHold(Span),
    WithoutHold(Span),
}

impl Spanned for CursorHold {
    fn span(&self) -> Span {
        match self {
            CursorHold::WithHold(s) | CursorHold::WithoutHold(s) => s.clone(),
        }
    }
}

/// PostgreSQL DECLARE cursor statement
#[derive(Clone, Debug)]
pub struct DeclareCursor<'a> {
    pub declare_span: Span,
    pub name: crate::Identifier<'a>,
    pub binary: Option<Span>,
    pub sensitivity: Option<CursorSensitivity>,
    pub scroll: Option<CursorScroll>,
    pub cursor_span: Span,
    pub hold: Option<CursorHold>,
    pub for_span: Span,
    pub query: Box<Statement<'a>>,
}

impl<'a> Spanned for DeclareCursor<'a> {
    fn span(&self) -> Span {
        self.declare_span.join_span(&self.query)
    }
}

fn parse_declare_cursor<'a>(parser: &mut Parser<'a, '_>) -> Result<DeclareCursor<'a>, ParseError> {
    let declare_span = parser.consume_keyword(Keyword::DECLARE)?;
    parser.postgres_only(&declare_span);
    let name = parser.consume_plain_identifier_unreserved()?;
    // Optional BINARY
    let binary = parser.skip_keyword(Keyword::BINARY);
    // Optional ASENSITIVE | INSENSITIVE
    let sensitivity = match &parser.token {
        Token::Ident(_, Keyword::ASENSITIVE) => {
            Some(CursorSensitivity::Asensitive(parser.consume()))
        }
        Token::Ident(_, Keyword::INSENSITIVE) => {
            Some(CursorSensitivity::Insensitive(parser.consume()))
        }
        _ => None,
    };
    // Optional [NO] SCROLL
    let scroll = if let Some(no_span) = parser.skip_keyword(Keyword::NO) {
        let scroll_span = parser.consume_keyword(Keyword::SCROLL)?;
        Some(CursorScroll::NoScroll(no_span.join_span(&scroll_span)))
    } else {
        parser
            .skip_keyword(Keyword::SCROLL)
            .map(CursorScroll::Scroll)
    };
    let cursor_span = parser.consume_keyword(Keyword::CURSOR)?;
    // Optional WITH HOLD | WITHOUT HOLD
    let hold = if let Some(without_span) = parser.skip_keyword(Keyword::WITHOUT) {
        let hold_span = parser.consume_keyword(Keyword::HOLD)?;
        Some(CursorHold::WithoutHold(without_span.join_span(&hold_span)))
    } else if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
        let hold_span = parser.consume_keyword(Keyword::HOLD)?;
        Some(CursorHold::WithHold(with_span.join_span(&hold_span)))
    } else {
        None
    };
    let for_span = parser.consume_keyword(Keyword::FOR)?;
    let query = match parse_statement(parser)? {
        Some(s) => s,
        None => parser.expected_failure("Query after FOR")?,
    };
    Ok(DeclareCursor {
        declare_span,
        name,
        binary,
        sensitivity,
        scroll,
        cursor_span,
        hold,
        for_span,
        query: Box::new(query),
    })
}

/// PostgreSQL REFRESH MATERIALIZED VIEW statement
#[derive(Clone, Debug)]
pub struct RefreshMaterializedView<'a> {
    pub refresh_span: Span,
    pub concurrently: Option<Span>,
    pub view_name: crate::QualifiedName<'a>,
    /// WITH [ NO ] DATA: Some(true) = WITH DATA, Some(false) = WITH NO DATA, None = not specified
    pub with_data: Option<(Span, bool)>,
}

impl<'a> Spanned for RefreshMaterializedView<'a> {
    fn span(&self) -> Span {
        self.refresh_span
            .join_span(&self.view_name)
            .join_span(&self.with_data.as_ref().map(|(s, _)| s.clone()))
    }
}

fn parse_refresh_materialized_view<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<RefreshMaterializedView<'a>, ParseError> {
    let refresh_span = parser.consume_keyword(Keyword::REFRESH)?;
    parser.postgres_only(&refresh_span);
    parser.consume_keyword(Keyword::MATERIALIZED)?;
    parser.consume_keyword(Keyword::VIEW)?;
    let concurrently = parser.skip_keyword(Keyword::CONCURRENTLY);
    let view_name = parse_qualified_name_unreserved(parser)?;
    // Optional WITH [ NO ] DATA
    let with_data = if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
        if let Some(no_span) = parser.skip_keyword(Keyword::NO) {
            let data_span = parser.consume_keyword(Keyword::DATA)?;
            Some((with_span.join_span(&no_span).join_span(&data_span), false))
        } else {
            let data_span = parser.consume_keyword(Keyword::DATA)?;
            Some((with_span.join_span(&data_span), true))
        }
    } else {
        None
    };
    Ok(RefreshMaterializedView {
        refresh_span,
        concurrently,
        view_name,
        with_data,
    })
}

/// PostgreSQL PREPARE statement
#[derive(Clone, Debug)]
pub struct Prepare<'a> {
    pub prepare_span: Span,
    pub name: crate::Identifier<'a>,
    pub param_types: Vec<crate::DataType<'a>>,
    pub as_span: Span,
    pub statement: Box<Statement<'a>>,
}

impl<'a> Spanned for Prepare<'a> {
    fn span(&self) -> Span {
        self.prepare_span.join_span(&self.statement)
    }
}

fn parse_prepare<'a>(parser: &mut Parser<'a, '_>) -> Result<Prepare<'a>, ParseError> {
    use crate::data_type::{DataTypeContext, parse_data_type};
    let prepare_span = parser.consume_keyword(Keyword::PREPARE)?;
    parser.postgres_only(&prepare_span);
    let name = parser.consume_plain_identifier_unreserved()?;
    // Optional (type, ...) parameter type list
    let mut param_types = Vec::new();
    if matches!(parser.token, Token::LParen) {
        parser.consume_token(Token::LParen)?;
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    param_types.push(parse_data_type(parser, DataTypeContext::TypeRef)?);
                    Ok(())
                },
            )?;
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        parser.consume_token(Token::RParen)?;
    }
    let as_span = parser.consume_keyword(Keyword::AS)?;
    let statement = match parse_statement(parser)? {
        Some(s) => s,
        None => parser.expected_failure("Statement after AS")?,
    };
    Ok(Prepare {
        prepare_span,
        name,
        param_types,
        as_span,
        statement: Box::new(statement),
    })
}

/// When part of case statement
#[derive(Clone, Debug)]
pub struct WhenStatement<'a> {
    /// Span of "WHEN"
    pub when_span: Span,
    /// Expression who's match yields execution `then`
    pub when: Expression<'a>,
    /// Span of "THEN"
    pub then_span: Span,
    /// Statements to execute if `when` matches
    pub then: Vec<Statement<'a>>,
}

impl<'a> Spanned for WhenStatement<'a> {
    fn span(&self) -> Span {
        self.when_span
            .join_span(&self.when)
            .join_span(&self.then_span)
            .join_span(&self.then)
    }
}

/// Case statement
#[derive(Clone, Debug)]
pub struct CaseStatement<'a> {
    /// Span of "CASE"
    pub case_span: Span,
    /// Value to match against
    pub value: Option<Expression<'a>>,
    /// List of whens
    pub whens: Vec<WhenStatement<'a>>,
    /// Span of "ELSE" and statement to execute if specified
    pub else_: Option<(Span, Vec<Statement<'a>>)>,
    /// Span of "END"
    pub end_span: Span,
}

impl<'a> Spanned for CaseStatement<'a> {
    fn span(&self) -> Span {
        self.case_span
            .join_span(&self.value)
            .join_span(&self.whens)
            .join_span(&self.else_)
            .join_span(&self.end_span)
    }
}

pub(crate) fn parse_case_statement<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<CaseStatement<'a>, ParseError> {
    let case_span = parser.consume_keyword(Keyword::CASE)?;
    let value = if !matches!(parser.token, Token::Ident(_, Keyword::WHEN)) {
        Some(parse_expression_unreserved(parser, PRIORITY_MAX)?)
    } else {
        None
    };

    let mut whens = Vec::new();
    let mut else_ = None;
    parser.recovered(
        "'END'",
        &|t| matches!(t, Token::Ident(_, Keyword::END)),
        |parser| {
            loop {
                let when_span = parser.consume_keyword(Keyword::WHEN)?;
                let when = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                let then_span = parser.consume_keyword(Keyword::THEN)?;
                let mut then = Vec::new();
                parse_statement_list(parser, &mut then)?;
                whens.push(WhenStatement {
                    when_span,
                    when,
                    then_span,
                    then,
                });
                if !matches!(parser.token, Token::Ident(_, Keyword::WHEN)) {
                    break;
                }
            }
            if let Some(span) = parser.skip_keyword(Keyword::ELSE) {
                let mut e = Vec::new();
                parse_statement_list(parser, &mut e)?;
                else_ = Some((span, e))
            };
            Ok(())
        },
    )?;
    let end_span = parser.consume_keyword(Keyword::END)?;
    Ok(CaseStatement {
        case_span,
        value,
        whens,
        else_,
        end_span,
    })
}

pub(crate) fn parse_compound_query_bottom<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Statement<'a>, ParseError> {
    match &parser.token {
        Token::LParen => {
            let lp = parser.consume_token(Token::LParen)?;
            let s = parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                Ok(Some(parse_compound_query(parser)?))
            })?;
            parser.consume_token(Token::RParen)?;
            Ok(s.unwrap_or(Statement::Invalid(Box::new(Invalid { span: lp }))))
        }
        Token::Ident(_, Keyword::SELECT) => Ok(Statement::Select(Box::new(parse_select(parser)?))),
        Token::Ident(_, Keyword::VALUES) => Ok(Statement::Values(Box::new(parse_values(parser)?))),
        _ => parser.expected_failure("'SELECET' or '('")?,
    }
}

/// Quantifier for a compound-query operator
#[derive(Clone, Debug)]
pub enum CompoundQuantifier {
    All(Span),
    Distinct(Span),
    Default,
}

impl OptSpanned for CompoundQuantifier {
    fn opt_span(&self) -> Option<Span> {
        match &self {
            CompoundQuantifier::All(v) => v.opt_span(),
            CompoundQuantifier::Distinct(v) => v.opt_span(),
            CompoundQuantifier::Default => None,
        }
    }
}

/// Set operator used between compound query branches
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum CompoundOperator {
    Union,
    Intersect,
    Except,
}

/// Right hand side branch of a compound query
#[derive(Clone, Debug)]
pub struct CompoundQueryBranch<'a> {
    /// Operator used to combine this branch with the left side
    pub operator: CompoundOperator,
    /// Span of operator keyword (UNION/INTERSECT/EXCEPT)
    pub operator_span: Span,
    /// Optional quantifier (ALL / DISTINCT)
    pub quantifier: CompoundQuantifier,
    /// Statement for this branch
    pub statement: Box<Statement<'a>>,
}

impl<'a> Spanned for CompoundQueryBranch<'a> {
    fn span(&self) -> Span {
        self.operator_span
            .join_span(&self.quantifier)
            .join_span(&self.statement)
    }
}

/// Compound query statement
#[derive(Clone, Debug)]
pub struct CompoundQuery<'a> {
    /// Left side of compound query
    pub left: Box<Statement<'a>>,
    /// Branches combined with the left side
    pub with: Vec<CompoundQueryBranch<'a>>,
    /// Span of "ORDER BY", and list of ordering expressions and directions if specified
    pub order_by: Option<(Span, Vec<(Expression<'a>, OrderFlag)>)>,
    /// Span of "LIMIT", offset and count expressions if specified
    pub limit: Option<(Span, Option<Expression<'a>>, Expression<'a>)>,
}

impl<'a> Spanned for CompoundQuery<'a> {
    fn span(&self) -> Span {
        self.left
            .join_span(&self.with)
            .join_span(&self.order_by)
            .join_span(&self.limit)
    }
}

pub(crate) fn parse_compound_query<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Statement<'a>, ParseError> {
    let q = parse_compound_query_bottom(parser)?;
    if !matches!(
        parser.token,
        Token::Ident(_, Keyword::UNION | Keyword::INTERSECT | Keyword::EXCEPT)
    ) {
        return Ok(q);
    };
    let mut with = Vec::new();
    loop {
        let (operator, operator_span) = match &parser.token {
            Token::Ident(_, Keyword::UNION) => (
                CompoundOperator::Union,
                parser.consume_keyword(Keyword::UNION)?,
            ),
            Token::Ident(_, Keyword::INTERSECT) => (
                CompoundOperator::Intersect,
                parser.consume_keyword(Keyword::INTERSECT)?,
            ),
            Token::Ident(_, Keyword::EXCEPT) => (
                CompoundOperator::Except,
                parser.consume_keyword(Keyword::EXCEPT)?,
            ),
            _ => parser.expected_failure("'UNION' | 'INTERSECT' | 'EXCEPT'")?,
        };
        let quantifier = match &parser.token {
            Token::Ident(_, Keyword::ALL) => {
                CompoundQuantifier::All(parser.consume_keyword(Keyword::ALL)?)
            }
            Token::Ident(_, Keyword::DISTINCT) => {
                CompoundQuantifier::Distinct(parser.consume_keyword(Keyword::DISTINCT)?)
            }
            _ => CompoundQuantifier::Default,
        };
        let statement = Box::new(parse_compound_query_bottom(parser)?);
        with.push(CompoundQueryBranch {
            operator,
            operator_span,
            quantifier,
            statement,
        });
        if !matches!(
            parser.token,
            Token::Ident(_, Keyword::UNION | Keyword::INTERSECT | Keyword::EXCEPT)
        ) {
            break;
        }
    }

    let order_by = if let Some(span) = parser.skip_keyword(Keyword::ORDER) {
        let span = parser.consume_keyword(Keyword::BY)?.join_span(&span);
        let mut order = Vec::new();
        loop {
            let e = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            let f = match &parser.token {
                Token::Ident(_, Keyword::ASC) => OrderFlag::Asc(parser.consume()),
                Token::Ident(_, Keyword::DESC) => OrderFlag::Desc(parser.consume()),
                _ => OrderFlag::None,
            };
            order.push((e, f));
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Some((span, order))
    } else {
        None
    };

    let limit = if let Some(span) = parser.skip_keyword(Keyword::LIMIT) {
        let n = parse_expression_unreserved(parser, PRIORITY_MAX)?;
        match parser.token {
            Token::Comma => {
                parser.consume();
                Some((
                    span,
                    Some(n),
                    parse_expression_unreserved(parser, PRIORITY_MAX)?,
                ))
            }
            Token::Ident(_, Keyword::OFFSET) => {
                parser.consume();
                Some((
                    span,
                    Some(parse_expression_unreserved(parser, PRIORITY_MAX)?),
                    n,
                ))
            }
            _ => Some((span, None, n)),
        }
    } else {
        None
    };

    Ok(Statement::CompoundQuery(Box::new(CompoundQuery {
        left: Box::new(q),
        with,
        order_by,
        limit,
    })))
}

pub(crate) fn parse_statements<'a>(parser: &mut Parser<'a, '_>) -> Vec<Statement<'a>> {
    let mut ans = Vec::new();
    loop {
        match &parser.token {
            Token::Delimiter => {
                parser.consume();
                continue;
            }
            Token::Eof => return ans,
            _ => (),
        }

        if matches!(parser.token, Token::Ident(_, Keyword::DELIMITER)) {
            if let Err(e) = parser.lexer.update_mysql_delimitor() {
                parser.err("Invalid delimiter", &e);
            }
            parser.consume();
            continue;
        }

        let stmt = match parse_statement(parser) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => parser.expected_failure("Statement"),
            Err(e) => Err(e),
        };
        let err = stmt.is_err();
        let mut from_stdin = false;
        if let Ok(stmt) = stmt {
            from_stdin = stmt.reads_from_stdin();
            ans.push(stmt);
        }

        if !matches!(parser.token, Token::Delimiter) {
            if !err {
                parser.expected_error(parser.lexer.delimiter_name());
            }
            // We use a custom recovery here as ; is not allowed in sub expressions, it always terminates outer most statements
            loop {
                parser.next();
                match &parser.token {
                    Token::Delimiter => break,
                    Token::Eof => return ans,
                    _ => (),
                }
            }
        }
        if from_stdin {
            let (s, span) = parser.read_from_stdin_and_next();
            ans.push(Statement::Stdin(Box::new(Stdin { span, input: s })));
        } else {
            parser
                .consume_token(Token::Delimiter)
                .unwrap_or_else(|_| panic!("{}", parser.lexer.delimiter_name()));
        }
    }
}
