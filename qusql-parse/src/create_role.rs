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
    Expression, Identifier, Span, Spanned,
    create_option::CreateOption,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
};
use alloc::vec::Vec;

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

/// Parse a single role option for CREATE/ALTER ROLE
pub (crate) fn parse_role_option<'a>(parser: &mut Parser<'a, '_>) -> Result<Option<RoleOption<'a>>, ParseError> {
    Ok(match &parser.token {
        Token::Ident(_, Keyword::SUPERUSER) => {
            let span = parser.consume_keyword(Keyword::SUPERUSER)?;
            Some(RoleOption::SuperUser(span))
        }
        Token::Ident(_, Keyword::NOSUPERUSER) => {
            let span = parser.consume_keyword(Keyword::NOSUPERUSER)?;
            Some(RoleOption::NoSuperUser(span))
        }
        Token::Ident(_, Keyword::CREATEDB) => {
            let span = parser.consume_keyword(Keyword::CREATEDB)?;
            Some(RoleOption::CreateDb(span))
        }
        Token::Ident(_, Keyword::NOCREATEDB) => {
            let span = parser.consume_keyword(Keyword::NOCREATEDB)?;
            Some(RoleOption::NoCreateDb(span))
        }
        Token::Ident(_, Keyword::CREATEROLE) => {
            let span = parser.consume_keyword(Keyword::CREATEROLE)?;
            Some(RoleOption::CreateRole(span))
        }
        Token::Ident(_, Keyword::NOCREATEROLE) => {
            let span = parser.consume_keyword(Keyword::NOCREATEROLE)?;
            Some(RoleOption::NoCreateRole(span))
        }
        Token::Ident(_, Keyword::INHERIT) => {
            let span = parser.consume_keyword(Keyword::INHERIT)?;
            Some(RoleOption::Inherit(span))
        }
        Token::Ident(_, Keyword::NOINHERIT) => {
            let span = parser.consume_keyword(Keyword::NOINHERIT)?;
            Some(RoleOption::NoInherit(span))
        }
        Token::Ident(_, Keyword::LOGIN) => {
            let span = parser.consume_keyword(Keyword::LOGIN)?;
            Some(RoleOption::Login(span))
        }
        Token::Ident(_, Keyword::NOLOGIN) => {
            let span = parser.consume_keyword(Keyword::NOLOGIN)?;
            Some(RoleOption::NoLogin(span))
        }
        Token::Ident(_, Keyword::REPLICATION) => {
            let span = parser.consume_keyword(Keyword::REPLICATION)?;
            Some(RoleOption::Replication(span))
        }
        Token::Ident(_, Keyword::NOREPLICATION) => {
            let span = parser.consume_keyword(Keyword::NOREPLICATION)?;
            Some(RoleOption::NoReplication(span))
        }
        Token::Ident(_, Keyword::BYPASSRLS) => {
            let span = parser.consume_keyword(Keyword::BYPASSRLS)?;
            Some(RoleOption::BypassRls(span))
        }
        Token::Ident(_, Keyword::NOBYPASSRLS) => {
            let span = parser.consume_keyword(Keyword::NOBYPASSRLS)?;
            Some(RoleOption::NoBypassRls(span))
        }
        Token::Ident(_, Keyword::CONNECTION) => {
            let span = parser.consume_keywords(&[Keyword::CONNECTION, Keyword::LIMIT])?;
            let expr = parse_expression(parser, false)?;
            Some(RoleOption::ConnectionLimit(span, expr))
        }
        Token::Ident(_, Keyword::ENCRYPTED) => {
            let span = parser.consume_keywords(&[Keyword::ENCRYPTED, Keyword::PASSWORD])?;
            let expr = parse_expression(parser, false)?;
            Some(RoleOption::EncryptedPassword(span, expr))
        }
        Token::Ident(_, Keyword::PASSWORD) => {
            let password_span = parser.consume_keyword(Keyword::PASSWORD)?;
            if parser.skip_keyword(Keyword::NULL).is_some() {
                Some(RoleOption::PasswordNull(password_span))
            } else {
                let expr = parse_expression(parser, false)?;
                Some(RoleOption::Password(password_span, expr))
            }
        }
        Token::Ident(_, Keyword::VALID) => {
            let span = parser.consume_keywords(&[Keyword::VALID, Keyword::UNTIL])?;
            let expr = parse_expression(parser, false)?;
            Some(RoleOption::ValidUntil(span, expr))
        }
        Token::Ident(_, Keyword::SYSID) => {
            let sysid_span = parser.consume_keyword(Keyword::SYSID)?;
            let expr = parse_expression(parser, false)?;
            Some(RoleOption::Sysid(sysid_span, expr))
        }
        _ => None,
    })
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

pub(crate) fn parse_create_role<'a>(
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
            _ => {
                // Not a membership clause, try parsing as an option
                if let Some(opt) = parse_role_option(parser)? {
                    options.push(opt);
                    continue;
                } else {
                    // Neither membership nor option, break the loop
                    break;
                }
            }
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
