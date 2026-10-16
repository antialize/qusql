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

use alloc::vec::Vec;

use crate::create::parse_sequence_options;
use crate::{
    Identifier, OptSpanned, QualifiedName, SequenceOption, Span, Spanned,
    alter_table::{AlterTableOwner, parse_alter_owner},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name_unreserved,
};

/// ALTER SEQUENCE action (PostgreSQL)
#[derive(Clone, Debug)]
pub enum AlterSequenceAction<'a> {
    /// One or more sequence option changes:
    /// AS / INCREMENT / MINVALUE / MAXVALUE / START / RESTART / CACHE / CYCLE / OWNED BY
    Options(Vec<SequenceOption<'a>>),
    /// RENAME TO new_name
    RenameTo {
        rename_to_span: Span,
        new_name: Identifier<'a>,
    },
    /// SET SCHEMA new_schema
    SetSchema {
        set_schema_span: Span,
        new_schema: Identifier<'a>,
    },
    /// OWNER TO { new_owner | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    OwnerTo {
        owner_to_span: Span,
        new_owner: AlterTableOwner<'a>,
    },
}

impl<'a> Spanned for AlterSequenceAction<'a> {
    fn span(&self) -> Span {
        match self {
            // Options is never empty: parse_alter_sequence rejects an empty option list
            AlterSequenceAction::Options(options) => options.opt_span().unwrap(),
            AlterSequenceAction::RenameTo {
                rename_to_span,
                new_name,
            } => rename_to_span.join_span(new_name),
            AlterSequenceAction::SetSchema {
                set_schema_span,
                new_schema,
            } => set_schema_span.join_span(new_schema),
            AlterSequenceAction::OwnerTo {
                owner_to_span,
                new_owner,
            } => owner_to_span.join_span(new_owner),
        }
    }
}

/// ALTER SEQUENCE statement (PostgreSQL)
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, AlterSequence, AlterSequenceAction, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::PostgreSQL);
/// #
/// let sql = "ALTER SEQUENCE myseq OWNER TO compute;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let s: AlterSequence = match stmts.pop() {
///     Some(Statement::AlterSequence(s)) => *s,
///     _ => panic!("We should get an alter sequence statement")
/// };
///
/// assert!(s.name.identifier.as_str() == "myseq");
/// assert!(matches!(s.action, AlterSequenceAction::OwnerTo { .. }));
/// ```
#[derive(Clone, Debug)]
pub struct AlterSequence<'a> {
    /// Span of "ALTER SEQUENCE"
    pub alter_sequence_span: Span,
    /// Span of "IF EXISTS" if specified
    pub if_exists: Option<Span>,
    /// Name of the sequence being altered
    pub name: QualifiedName<'a>,
    /// The action applied to the sequence
    pub action: AlterSequenceAction<'a>,
}

impl<'a> Spanned for AlterSequence<'a> {
    fn span(&self) -> Span {
        self.alter_sequence_span
            .join_span(&self.if_exists)
            .join_span(&self.name)
            .join_span(&self.action)
    }
}

/// Parse ALTER SEQUENCE statement (PostgreSQL).
/// `alter_sequence_span` is the span of the "ALTER SEQUENCE" keywords.
pub(crate) fn parse_alter_sequence<'a>(
    parser: &mut Parser<'a, '_>,
    alter_sequence_span: Span,
) -> Result<AlterSequence<'a>, ParseError> {
    parser.postgres_only(&alter_sequence_span);
    let if_exists = if let Some(if_span) = parser.skip_keyword(Keyword::IF) {
        Some(parser.consume_keyword(Keyword::EXISTS)?.join_span(&if_span))
    } else {
        None
    };
    let name = parse_qualified_name_unreserved(parser)?;
    let action = match &parser.token {
        Token::Ident(_, Keyword::RENAME) => {
            let rename_to_span = parser.consume_keywords(&[Keyword::RENAME, Keyword::TO])?;
            let new_name = parser.consume_plain_identifier_unreserved()?;
            AlterSequenceAction::RenameTo {
                rename_to_span,
                new_name,
            }
        }
        Token::Ident(_, Keyword::SET) => {
            let set_span = parser.consume_keyword(Keyword::SET)?;
            if let Token::Ident(_, Keyword::SCHEMA) = parser.token {
                let schema_span = parser.consume_keyword(Keyword::SCHEMA)?;
                let new_schema = parser.consume_plain_identifier_unreserved()?;
                AlterSequenceAction::SetSchema {
                    set_schema_span: set_span.join_span(&schema_span),
                    new_schema,
                }
            } else {
                parser.expected_failure("'SCHEMA' after 'SET' in ALTER SEQUENCE")?
            }
        }
        Token::Ident(_, Keyword::OWNER) => {
            let owner_to_span = parser.consume_keywords(&[Keyword::OWNER, Keyword::TO])?;
            let new_owner = parse_alter_owner(parser)?;
            AlterSequenceAction::OwnerTo {
                owner_to_span,
                new_owner,
            }
        }
        _ => {
            let options = parse_sequence_options(parser)?;
            if options.is_empty() {
                parser.expected_failure("sequence options, 'RENAME TO', 'SET SCHEMA' or 'OWNER TO' after ALTER SEQUENCE")?;
            }
            AlterSequenceAction::Options(options)
        }
    };
    Ok(AlterSequence {
        alter_sequence_span,
        if_exists,
        name,
        action,
    })
}
