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

//! Parser for the `GRANT` statement.
//!
//! Covers both the privilege-grant form and the role-membership form as documented at
//! <https://www.postgresql.org/docs/current/sql-grant.html>.

use crate::{
    DataType, Identifier, QualifiedName, Span, Spanned,
    create_function::FunctionParamDirection,
    data_type::{DataTypeContext, parse_data_type},
    expression::{Expression, PRIORITY_MAX, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
    span::OptSpanned,
};
use alloc::vec::Vec;

/// A single privilege keyword in a GRANT statement.
#[derive(Clone, Debug)]
pub enum GrantPrivilege {
    Select(Span),
    Insert(Span),
    Update(Span),
    Delete(Span),
    Truncate(Span),
    References(Span),
    Trigger(Span),
    Maintain(Span),
    Usage(Span),
    Create(Span),
    Connect(Span),
    /// TEMPORARY or TEMP
    Temporary(Span),
    Execute(Span),
    Set(Span),
    /// ALTER SYSTEM - span covers both keywords
    AlterSystem(Span),
    /// ALL [ PRIVILEGES ] - second span is the optional PRIVILEGES keyword
    All(Span, Option<Span>),
}

impl Spanned for GrantPrivilege {
    fn span(&self) -> Span {
        match self {
            GrantPrivilege::Select(s)
            | GrantPrivilege::Insert(s)
            | GrantPrivilege::Update(s)
            | GrantPrivilege::Delete(s)
            | GrantPrivilege::Truncate(s)
            | GrantPrivilege::References(s)
            | GrantPrivilege::Trigger(s)
            | GrantPrivilege::Maintain(s)
            | GrantPrivilege::Usage(s)
            | GrantPrivilege::Create(s)
            | GrantPrivilege::Connect(s)
            | GrantPrivilege::Temporary(s)
            | GrantPrivilege::Execute(s)
            | GrantPrivilege::Set(s)
            | GrantPrivilege::AlterSystem(s) => s.clone(),
            GrantPrivilege::All(s, p) => s.join_span(p),
        }
    }
}

/// One entry in the privilege list: a privilege with an optional column list.
///
/// Column lists are valid for SELECT, INSERT, UPDATE, REFERENCES on tables.
#[derive(Clone, Debug)]
pub struct PrivilegeItem<'a> {
    pub privilege: GrantPrivilege,
    /// Empty unless the privilege was qualified with `( col [, ...] )`.
    pub columns: Vec<Identifier<'a>>,
}

impl<'a> Spanned for PrivilegeItem<'a> {
    fn span(&self) -> Span {
        self.privilege.join_span(&self.columns)
    }
}

/// How a routine was named in the GRANT ON { FUNCTION | PROCEDURE | ROUTINE } clause.
#[derive(Clone, Debug)]
pub enum RoutineKind {
    Function(Span),
    Procedure(Span),
    Routine(Span),
}

impl Spanned for RoutineKind {
    fn span(&self) -> Span {
        match self {
            RoutineKind::Function(s) | RoutineKind::Procedure(s) | RoutineKind::Routine(s) => {
                s.clone()
            }
        }
    }
}

/// ALL { FUNCTIONS | PROCEDURES | ROUTINES } - which plural form was used.
#[derive(Clone, Debug)]
pub enum AllRoutineKind {
    Functions(Span),
    Procedures(Span),
    Routines(Span),
}

impl Spanned for AllRoutineKind {
    fn span(&self) -> Span {
        match self {
            AllRoutineKind::Functions(s)
            | AllRoutineKind::Procedures(s)
            | AllRoutineKind::Routines(s) => s.clone(),
        }
    }
}

/// An argument type entry inside a routine reference's optional `( args )` list.
#[derive(Clone, Debug)]
pub struct RoutineArgType<'a> {
    /// Optional IN / OUT / INOUT modifier.
    pub mode: Option<FunctionParamDirection>,
    /// Optional parameter name (present when followed by a type token).
    pub name: Option<Identifier<'a>>,
    /// The argument data type.
    pub type_: DataType<'a>,
}

impl<'a> Spanned for RoutineArgType<'a> {
    fn span(&self) -> Span {
        self.type_.join_span(&self.mode).join_span(&self.name)
    }
}

/// A routine name together with an optional argument-type list.
///
/// The argument list is used only for disambiguation when overloaded routines exist.
#[derive(Clone, Debug)]
pub struct RoutineName<'a> {
    pub name: QualifiedName<'a>,
    /// `None` means no parentheses were written; `Some([])` means `name()`.
    pub args: Option<Vec<RoutineArgType<'a>>>,
}

impl<'a> Spanned for RoutineName<'a> {
    fn span(&self) -> Span {
        self.name.join_span(&self.args)
    }
}

/// The object (or set of objects) to which privileges are being granted.
#[derive(Clone, Debug)]
pub enum GrantObject<'a> {
    /// `[ TABLE ] table_name [, ...]`
    Tables {
        table_kw: Option<Span>,
        names: Vec<QualifiedName<'a>>,
    },
    /// `ALL TABLES IN SCHEMA schema_name [, ...]`
    AllTablesInSchema {
        /// Span covering `ALL TABLES IN SCHEMA`
        span: Span,
        schemas: Vec<QualifiedName<'a>>,
    },
    /// `SEQUENCE sequence_name [, ...]`
    Sequences {
        sequence_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `ALL SEQUENCES IN SCHEMA schema_name [, ...]`
    AllSequencesInSchema {
        /// Span covering `ALL SEQUENCES IN SCHEMA`
        span: Span,
        schemas: Vec<QualifiedName<'a>>,
    },
    /// `DATABASE database_name [, ...]`
    Databases {
        database_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `DOMAIN domain_name [, ...]`
    Domains {
        domain_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `FOREIGN DATA WRAPPER fdw_name [, ...]`
    ForeignDataWrappers {
        /// Span covering `FOREIGN DATA WRAPPER`
        span: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `FOREIGN SERVER server_name [, ...]`
    ForeignServers {
        /// Span covering `FOREIGN SERVER`
        span: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `{ FUNCTION | PROCEDURE | ROUTINE } name[(args)] [, ...]`
    Routines {
        kind: RoutineKind,
        names: Vec<RoutineName<'a>>,
    },
    /// `ALL { FUNCTIONS | PROCEDURES | ROUTINES } IN SCHEMA schema_name [, ...]`
    AllRoutinesInSchema {
        all_span: Span,
        kind: AllRoutineKind,
        in_schema_span: Span,
        schemas: Vec<QualifiedName<'a>>,
    },
    /// `LANGUAGE lang_name [, ...]`
    Languages {
        language_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `LARGE OBJECT loid [, ...]`
    LargeObjects {
        /// Span covering `LARGE OBJECT`
        span: Span,
        oids: Vec<Expression<'a>>,
    },
    /// `PARAMETER configuration_parameter [, ...]`
    Parameters {
        parameter_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `SCHEMA schema_name [, ...]`
    Schemas {
        schema_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `TABLESPACE tablespace_name [, ...]`
    Tablespaces {
        tablespace_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
    /// `TYPE type_name [, ...]`
    Types {
        type_kw: Span,
        names: Vec<QualifiedName<'a>>,
    },
}

impl<'a> Spanned for GrantObject<'a> {
    fn span(&self) -> Span {
        match self {
            GrantObject::Tables { table_kw, names } => {
                names.opt_span().unwrap().join_span(table_kw)
            }
            GrantObject::AllTablesInSchema { span, schemas } => span.join_span(schemas),
            GrantObject::Sequences { sequence_kw, names } => sequence_kw.join_span(names),
            GrantObject::AllSequencesInSchema { span, schemas } => span.join_span(schemas),
            GrantObject::Databases { database_kw, names } => database_kw.join_span(names),
            GrantObject::Domains { domain_kw, names } => domain_kw.join_span(names),
            GrantObject::ForeignDataWrappers { span, names } => span.join_span(names),
            GrantObject::ForeignServers { span, names } => span.join_span(names),
            GrantObject::Routines { kind, names } => kind.join_span(names),
            GrantObject::AllRoutinesInSchema {
                all_span, schemas, ..
            } => all_span.join_span(schemas),
            GrantObject::Languages { language_kw, names } => language_kw.join_span(names),
            GrantObject::LargeObjects { span, oids } => span.join_span(oids),
            GrantObject::Parameters {
                parameter_kw,
                names,
            } => parameter_kw.join_span(names),
            GrantObject::Schemas { schema_kw, names } => schema_kw.join_span(names),
            GrantObject::Tablespaces {
                tablespace_kw,
                names,
            } => tablespace_kw.join_span(names),
            GrantObject::Types { type_kw, names } => type_kw.join_span(names),
        }
    }
}

/// A grantee or grantor role specification.
#[derive(Clone, Debug)]
pub enum RoleSpec<'a> {
    /// `[ GROUP ] role_name`
    Named {
        group_kw: Option<Span>,
        name: Identifier<'a>,
    },
    /// `PUBLIC`
    Public(Span),
    /// `CURRENT_ROLE`
    CurrentRole(Span),
    /// `CURRENT_USER`
    CurrentUser(Span),
    /// `SESSION_USER`
    SessionUser(Span),
}

impl<'a> Spanned for RoleSpec<'a> {
    fn span(&self) -> Span {
        match self {
            RoleSpec::Named { group_kw, name } => name.span().join_span(group_kw),
            RoleSpec::Public(s)
            | RoleSpec::CurrentRole(s)
            | RoleSpec::CurrentUser(s)
            | RoleSpec::SessionUser(s) => s.clone(),
        }
    }
}

/// Which membership option is being set in `WITH { ADMIN | INHERIT | SET } ...`.
#[derive(Clone, Debug)]
pub enum MembershipOptionKind {
    Admin(Span),
    Inherit(Span),
    Set(Span),
}

impl Spanned for MembershipOptionKind {
    fn span(&self) -> Span {
        match self {
            MembershipOptionKind::Admin(s)
            | MembershipOptionKind::Inherit(s)
            | MembershipOptionKind::Set(s) => s.clone(),
        }
    }
}

/// The value part of a membership option: `OPTION` (= TRUE), `TRUE`, or `FALSE`.
#[derive(Clone, Debug)]
pub enum MembershipOptionValue {
    /// The `OPTION` noise word — equivalent to `TRUE`.
    Option(Span),
    True(Span),
    False(Span),
}

impl Spanned for MembershipOptionValue {
    fn span(&self) -> Span {
        match self {
            MembershipOptionValue::Option(s)
            | MembershipOptionValue::True(s)
            | MembershipOptionValue::False(s) => s.clone(),
        }
    }
}

/// A single `WITH { ADMIN | INHERIT | SET } { OPTION | TRUE | FALSE }` clause.
#[derive(Clone, Debug)]
pub struct MembershipOption {
    pub with_span: Span,
    pub kind: MembershipOptionKind,
    pub value: MembershipOptionValue,
}

impl Spanned for MembershipOption {
    fn span(&self) -> Span {
        self.with_span.join_span(&self.kind).join_span(&self.value)
    }
}

/// The two top-level forms that GRANT can take.
#[derive(Clone, Debug)]
pub enum GrantKind<'a> {
    /// `GRANT privileges ON object TO grantees [WITH GRANT OPTION] [GRANTED BY role]`
    Privilege {
        privileges: Vec<PrivilegeItem<'a>>,
        on_span: Span,
        object: GrantObject<'a>,
        to_span: Span,
        grantees: Vec<RoleSpec<'a>>,
        /// Span covering `WITH GRANT OPTION` if present.
        with_grant_option: Option<Span>,
        /// The `GRANTED BY role_spec` clause if present.
        granted_by: Option<(Span, RoleSpec<'a>)>,
    },
    /// `GRANT role_name [, ...] TO role_spec [, ...] [WITH opt] [GRANTED BY role]`
    Role {
        roles: Vec<QualifiedName<'a>>,
        to_span: Span,
        grantees: Vec<RoleSpec<'a>>,
        with_option: Option<MembershipOption>,
        granted_by: Option<(Span, RoleSpec<'a>)>,
    },
}

impl<'a> Spanned for GrantKind<'a> {
    fn span(&self) -> Span {
        match self {
            GrantKind::Privilege {
                privileges,
                on_span,
                object,
                grantees,
                granted_by,
                ..
            } => on_span
                .join_span(privileges)
                .join_span(object)
                .join_span(grantees)
                .join_span(granted_by),
            GrantKind::Role {
                roles,
                to_span,
                grantees,
                with_option,
                granted_by,
                ..
            } => to_span
                .join_span(roles)
                .join_span(grantees)
                .join_span(with_option)
                .join_span(granted_by),
        }
    }
}

/// A parsed `GRANT` statement.
#[derive(Clone, Debug)]
pub struct Grant<'a> {
    pub grant_span: Span,
    pub kind: GrantKind<'a>,
}

impl<'a> Spanned for Grant<'a> {
    fn span(&self) -> Span {
        self.grant_span.join_span(&self.kind)
    }
}

fn is_privilege_keyword(token: &Token) -> bool {
    matches!(
        token,
        Token::Ident(
            _,
            Keyword::SELECT
                | Keyword::INSERT
                | Keyword::UPDATE
                | Keyword::DELETE
                | Keyword::TRUNCATE
                | Keyword::REFERENCES
                | Keyword::TRIGGER
                | Keyword::MAINTAIN
                | Keyword::USAGE
                | Keyword::CREATE
                | Keyword::CONNECT
                | Keyword::TEMPORARY
                | Keyword::TEMP
                | Keyword::EXECUTE
                | Keyword::SET
                | Keyword::ALTER
                | Keyword::ALL
        )
    )
}

fn parse_privilege<'a>(parser: &mut Parser<'a, '_>) -> Result<GrantPrivilege, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::SELECT) => Ok(GrantPrivilege::Select(
            parser.consume_keyword(Keyword::SELECT)?,
        )),
        Token::Ident(_, Keyword::INSERT) => Ok(GrantPrivilege::Insert(
            parser.consume_keyword(Keyword::INSERT)?,
        )),
        Token::Ident(_, Keyword::UPDATE) => Ok(GrantPrivilege::Update(
            parser.consume_keyword(Keyword::UPDATE)?,
        )),
        Token::Ident(_, Keyword::DELETE) => Ok(GrantPrivilege::Delete(
            parser.consume_keyword(Keyword::DELETE)?,
        )),
        Token::Ident(_, Keyword::TRUNCATE) => Ok(GrantPrivilege::Truncate(
            parser.consume_keyword(Keyword::TRUNCATE)?,
        )),
        Token::Ident(_, Keyword::REFERENCES) => Ok(GrantPrivilege::References(
            parser.consume_keyword(Keyword::REFERENCES)?,
        )),
        Token::Ident(_, Keyword::TRIGGER) => Ok(GrantPrivilege::Trigger(
            parser.consume_keyword(Keyword::TRIGGER)?,
        )),
        Token::Ident(_, Keyword::MAINTAIN) => Ok(GrantPrivilege::Maintain(
            parser.consume_keyword(Keyword::MAINTAIN)?,
        )),
        Token::Ident(_, Keyword::USAGE) => Ok(GrantPrivilege::Usage(
            parser.consume_keyword(Keyword::USAGE)?,
        )),
        Token::Ident(_, Keyword::CREATE) => Ok(GrantPrivilege::Create(
            parser.consume_keyword(Keyword::CREATE)?,
        )),
        Token::Ident(_, Keyword::CONNECT) => Ok(GrantPrivilege::Connect(
            parser.consume_keyword(Keyword::CONNECT)?,
        )),
        Token::Ident(_, Keyword::TEMPORARY) => Ok(GrantPrivilege::Temporary(
            parser.consume_keyword(Keyword::TEMPORARY)?,
        )),
        Token::Ident(_, Keyword::TEMP) => Ok(GrantPrivilege::Temporary(
            parser.consume_keyword(Keyword::TEMP)?,
        )),
        Token::Ident(_, Keyword::EXECUTE) => Ok(GrantPrivilege::Execute(
            parser.consume_keyword(Keyword::EXECUTE)?,
        )),
        Token::Ident(_, Keyword::SET) => {
            Ok(GrantPrivilege::Set(parser.consume_keyword(Keyword::SET)?))
        }
        Token::Ident(_, Keyword::ALTER) => {
            let alter = parser.consume_keyword(Keyword::ALTER)?;
            let sys = parser.consume_keyword(Keyword::SYSTEM)?;
            Ok(GrantPrivilege::AlterSystem(alter.join_span(&sys)))
        }
        Token::Ident(_, Keyword::ALL) => {
            let all = parser.consume_keyword(Keyword::ALL)?;
            let privileges = parser.skip_keyword(Keyword::PRIVILEGES);
            Ok(GrantPrivilege::All(all, privileges))
        }
        _ => parser.expected_failure("privilege keyword"),
    }
}

fn parse_privilege_list<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<PrivilegeItem<'a>>, ParseError> {
    let mut items = Vec::new();
    loop {
        let privilege = parse_privilege(parser)?;
        let mut columns = Vec::new();
        if matches!(parser.token, Token::LParen) {
            parser.consume_token(Token::LParen)?;
            parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                loop {
                    columns.push(parser.consume_plain_identifier_unreserved()?);
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                Ok(())
            })?;
            parser.consume_token(Token::RParen)?;
        }
        items.push(PrivilegeItem { privilege, columns });
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
        // If after the comma we don't see a privilege keyword, stop - could be ON
        if !is_privilege_keyword(&parser.token) {
            break;
        }
    }
    Ok(items)
}

fn parse_role_spec<'a>(parser: &mut Parser<'a, '_>) -> Result<RoleSpec<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::PUBLIC) => {
            Ok(RoleSpec::Public(parser.consume_keyword(Keyword::PUBLIC)?))
        }
        Token::Ident(_, Keyword::CURRENT_ROLE) => Ok(RoleSpec::CurrentRole(
            parser.consume_keyword(Keyword::CURRENT_ROLE)?,
        )),
        Token::Ident(_, Keyword::CURRENT_USER) => Ok(RoleSpec::CurrentUser(
            parser.consume_keyword(Keyword::CURRENT_USER)?,
        )),
        Token::Ident(_, Keyword::SESSION_USER) => Ok(RoleSpec::SessionUser(
            parser.consume_keyword(Keyword::SESSION_USER)?,
        )),
        Token::Ident(_, Keyword::GROUP) => {
            let group_kw = Some(parser.consume_keyword(Keyword::GROUP)?);
            let name = parser.consume_plain_identifier_unreserved()?;
            Ok(RoleSpec::Named { group_kw, name })
        }
        _ => {
            let name = parser.consume_plain_identifier_unreserved()?;
            Ok(RoleSpec::Named {
                group_kw: None,
                name,
            })
        }
    }
}

fn parse_role_spec_list<'a>(parser: &mut Parser<'a, '_>) -> Result<Vec<RoleSpec<'a>>, ParseError> {
    let mut list = Vec::new();
    loop {
        list.push(parse_role_spec(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    Ok(list)
}

fn parse_granted_by<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Option<(Span, RoleSpec<'a>)>, ParseError> {
    if let Some(granted_span) = parser.skip_keyword(Keyword::GRANTED) {
        let by_span = parser.consume_keyword(Keyword::BY)?;
        let role = parse_role_spec(parser)?;
        Ok(Some((granted_span.join_span(&by_span), role)))
    } else {
        Ok(None)
    }
}

fn parse_routine_arg_list<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<RoutineArgType<'a>>, ParseError> {
    let mut args = Vec::new();
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            if matches!(parser.token, Token::RParen) {
                break;
            }
            let mode = match &parser.token {
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
            // Peek: if next token after this one looks like a type start, this is a name
            let name = {
                let is_unnamed = matches!(
                    parser.peek(),
                    Token::Comma | Token::RParen | Token::LBracket
                );
                if is_unnamed {
                    None
                } else {
                    Some(parser.consume_plain_identifier_unreserved()?)
                }
            };
            let type_ = parse_data_type(parser, DataTypeContext::FunctionParam)?;
            args.push(RoutineArgType { mode, name, type_ });
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    Ok(args)
}

fn parse_routine_name_list<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<RoutineName<'a>>, ParseError> {
    let mut names = Vec::new();
    loop {
        let name = parse_qualified_name_unreserved(parser)?;
        let args = if matches!(parser.token, Token::LParen) {
            parser.consume_token(Token::LParen)?;
            let args = parse_routine_arg_list(parser)?;
            parser.consume_token(Token::RParen)?;
            Some(args)
        } else {
            None
        };
        names.push(RoutineName { name, args });
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    Ok(names)
}

fn parse_qualified_name_list<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Vec<QualifiedName<'a>>, ParseError> {
    let mut names = Vec::new();
    loop {
        names.push(parse_qualified_name_unreserved(parser)?);
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }
    Ok(names)
}

fn parse_grant_object<'a>(parser: &mut Parser<'a, '_>) -> Result<GrantObject<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::TABLE) => {
            let table_kw = Some(parser.consume_keyword(Keyword::TABLE)?);
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Tables { table_kw, names })
        }
        Token::Ident(_, Keyword::ALL) => {
            let all_span = parser.consume_keyword(Keyword::ALL)?;
            match &parser.token {
                Token::Ident(_, Keyword::TABLES) => {
                    let table_in_schema_span = parser.consume_keywords(&[
                        Keyword::TABLES,
                        Keyword::IN,
                        Keyword::SCHEMA,
                    ])?;
                    let schemas = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::AllTablesInSchema {
                        span: all_span.join_span(&table_in_schema_span),
                        schemas,
                    })
                }
                Token::Ident(_, Keyword::SEQUENCES) => {
                    let in_sequences_schema_span = parser.consume_keywords(&[
                        Keyword::SEQUENCES,
                        Keyword::IN,
                        Keyword::SCHEMA,
                    ])?;
                    let schemas = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::AllSequencesInSchema {
                        span: all_span.join_span(&in_sequences_schema_span),
                        schemas,
                    })
                }
                Token::Ident(_, Keyword::FUNCTIONS) => {
                    let kind_span = parser.consume_keyword(Keyword::FUNCTIONS)?;
                    let in_schema_span =
                        parser.consume_keywords(&[Keyword::IN, Keyword::SCHEMA])?;
                    let schemas = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::AllRoutinesInSchema {
                        all_span,
                        kind: AllRoutineKind::Functions(kind_span),
                        in_schema_span,
                        schemas,
                    })
                }
                Token::Ident(_, Keyword::PROCEDURES) => {
                    let kind_span = parser.consume_keyword(Keyword::PROCEDURES)?;
                    let in_schema_span =
                        parser.consume_keywords(&[Keyword::IN, Keyword::SCHEMA])?;
                    let schemas = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::AllRoutinesInSchema {
                        all_span,
                        kind: AllRoutineKind::Procedures(kind_span),
                        in_schema_span,
                        schemas,
                    })
                }
                Token::Ident(_, Keyword::ROUTINES) => {
                    let kind_span = parser.consume_keyword(Keyword::ROUTINES)?;
                    let in_schema_span =
                        parser.consume_keywords(&[Keyword::IN, Keyword::SCHEMA])?;
                    let schemas = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::AllRoutinesInSchema {
                        all_span,
                        kind: AllRoutineKind::Routines(kind_span),
                        in_schema_span,
                        schemas,
                    })
                }
                _ => parser.expected_failure(
                    "TABLES, SEQUENCES, FUNCTIONS, PROCEDURES, or ROUTINES after ALL",
                ),
            }
        }
        Token::Ident(_, Keyword::SEQUENCE) => {
            let sequence_kw = parser.consume_keyword(Keyword::SEQUENCE)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Sequences { sequence_kw, names })
        }
        Token::Ident(_, Keyword::DATABASE) => {
            let database_kw = parser.consume_keyword(Keyword::DATABASE)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Databases { database_kw, names })
        }
        Token::Ident(_, Keyword::DOMAIN) => {
            let domain_kw = parser.consume_keyword(Keyword::DOMAIN)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Domains { domain_kw, names })
        }
        Token::Ident(_, Keyword::FOREIGN) => {
            let foreign_span = parser.consume_keyword(Keyword::FOREIGN)?;
            match &parser.token {
                Token::Ident(_, Keyword::DATA) => {
                    let data_span = parser.consume_keyword(Keyword::DATA)?;
                    let wrapper_span = parser.consume_keyword(Keyword::WRAPPER)?;
                    let names = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::ForeignDataWrappers {
                        span: foreign_span.join_span(&data_span).join_span(&wrapper_span),
                        names,
                    })
                }
                Token::Ident(_, Keyword::SERVER) => {
                    let server_span = parser.consume_keyword(Keyword::SERVER)?;
                    let names = parse_qualified_name_list(parser)?;
                    Ok(GrantObject::ForeignServers {
                        span: foreign_span.join_span(&server_span),
                        names,
                    })
                }
                _ => parser.expected_failure("DATA WRAPPER or SERVER after FOREIGN"),
            }
        }
        Token::Ident(_, Keyword::FUNCTION) => {
            let kw = parser.consume_keyword(Keyword::FUNCTION)?;
            let names = parse_routine_name_list(parser)?;
            Ok(GrantObject::Routines {
                kind: RoutineKind::Function(kw),
                names,
            })
        }
        Token::Ident(_, Keyword::PROCEDURE) => {
            let kw = parser.consume_keyword(Keyword::PROCEDURE)?;
            let names = parse_routine_name_list(parser)?;
            Ok(GrantObject::Routines {
                kind: RoutineKind::Procedure(kw),
                names,
            })
        }
        Token::Ident(_, Keyword::ROUTINE) => {
            let kw = parser.consume_keyword(Keyword::ROUTINE)?;
            let names = parse_routine_name_list(parser)?;
            Ok(GrantObject::Routines {
                kind: RoutineKind::Routine(kw),
                names,
            })
        }
        Token::Ident(_, Keyword::LANGUAGE) => {
            let language_kw = parser.consume_keyword(Keyword::LANGUAGE)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Languages { language_kw, names })
        }
        Token::Ident(_, Keyword::LARGE) => {
            let large_span = parser.consume_keyword(Keyword::LARGE)?;
            let object_span = parser.consume_keyword(Keyword::OBJECT)?;
            let mut oids = Vec::new();
            loop {
                oids.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
                if parser.skip_token(Token::Comma).is_none() {
                    break;
                }
            }
            Ok(GrantObject::LargeObjects {
                span: large_span.join_span(&object_span),
                oids,
            })
        }
        Token::Ident(_, Keyword::PARAMETER) => {
            let parameter_kw = parser.consume_keyword(Keyword::PARAMETER)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Parameters {
                parameter_kw,
                names,
            })
        }
        Token::Ident(_, Keyword::SCHEMA) => {
            let schema_kw = parser.consume_keyword(Keyword::SCHEMA)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Schemas { schema_kw, names })
        }
        Token::Ident(_, Keyword::TABLESPACE) => {
            let tablespace_kw = parser.consume_keyword(Keyword::TABLESPACE)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Tablespaces {
                tablespace_kw,
                names,
            })
        }
        Token::Ident(_, Keyword::TYPE) => {
            let type_kw = parser.consume_keyword(Keyword::TYPE)?;
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Types { type_kw, names })
        }
        _ => {
            // Bare table name without TABLE keyword
            let names = parse_qualified_name_list(parser)?;
            Ok(GrantObject::Tables {
                table_kw: None,
                names,
            })
        }
    }
}

pub(crate) fn parse_grant<'a>(parser: &mut Parser<'a, '_>) -> Result<Grant<'a>, ParseError> {
    let grant_span = parser.consume_keyword(Keyword::GRANT)?;

    // Disambiguate: if the first token is a privilege keyword, this is a privilege grant.
    // Otherwise it is a role-membership grant.
    let kind = if is_privilege_keyword(&parser.token) {
        let privileges = parse_privilege_list(parser)?;
        let on_span = parser.consume_keyword(Keyword::ON)?;
        let object = parse_grant_object(parser)?;
        let to_span = parser.consume_keyword(Keyword::TO)?;
        let grantees = parse_role_spec_list(parser)?;

        // WITH GRANT OPTION
        let with_grant_option = if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
            let grant_span = parser.consume_keyword(Keyword::GRANT)?;
            let option_span = parser.consume_keyword(Keyword::OPTION)?;
            Some(with_span.join_span(&grant_span).join_span(&option_span))
        } else {
            None
        };

        let granted_by = parse_granted_by(parser)?;

        GrantKind::Privilege {
            privileges,
            on_span,
            object,
            to_span,
            grantees,
            with_grant_option,
            granted_by,
        }
    } else {
        let roles = parse_qualified_name_list(parser)?;
        let to_span = parser.consume_keyword(Keyword::TO)?;
        let grantees = parse_role_spec_list(parser)?;

        // WITH { ADMIN | INHERIT | SET } { OPTION | TRUE | FALSE }
        let with_option = if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
            let kind = match &parser.token {
                Token::Ident(_, Keyword::ADMIN) => {
                    MembershipOptionKind::Admin(parser.consume_keyword(Keyword::ADMIN)?)
                }
                Token::Ident(_, Keyword::INHERIT) => {
                    MembershipOptionKind::Inherit(parser.consume_keyword(Keyword::INHERIT)?)
                }
                Token::Ident(_, Keyword::SET) => {
                    MembershipOptionKind::Set(parser.consume_keyword(Keyword::SET)?)
                }
                _ => parser.expected_failure("ADMIN, INHERIT, or SET after WITH")?,
            };
            let value = match &parser.token {
                Token::Ident(_, Keyword::OPTION) => {
                    MembershipOptionValue::Option(parser.consume_keyword(Keyword::OPTION)?)
                }
                Token::Ident(_, Keyword::TRUE) => {
                    MembershipOptionValue::True(parser.consume_keyword(Keyword::TRUE)?)
                }
                Token::Ident(_, Keyword::FALSE) => {
                    MembershipOptionValue::False(parser.consume_keyword(Keyword::FALSE)?)
                }
                _ => parser.expected_failure("OPTION, TRUE, or FALSE")?,
            };
            Some(MembershipOption {
                with_span,
                kind,
                value,
            })
        } else {
            None
        };

        let granted_by = parse_granted_by(parser)?;

        GrantKind::Role {
            roles,
            to_span,
            grantees,
            with_option,
            granted_by,
        }
    };

    Ok(Grant { grant_span, kind })
}
