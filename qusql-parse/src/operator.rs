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

use crate::{
    DataType, QualifiedName, Span, Spanned,
    data_type::parse_data_type,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

/// CREATE OPERATOR statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateOperator<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Span of "OPERATOR"
    pub operator_span: Span,
    /// The operator name (e.g., +, -, @@, myschema.@@)
    pub name: QualifiedName<'a>,
    /// Left parenthesis span
    pub lparen_span: Span,
    /// Operator options
    pub options: Vec<OperatorOption<'a>>,
    /// Right parenthesis span
    pub rparen_span: Span,
}

impl<'a> Spanned for CreateOperator<'a> {
    fn span(&self) -> Span {
        self.create_span.join_span(&self.rparen_span)
    }
}

/// Options for CREATE OPERATOR
#[derive(Clone, Debug)]
pub enum OperatorOption<'a> {
    /// FUNCTION = function_name
    Function {
        keyword_span: Span,
        eq_span: Span,
        function_name: QualifiedName<'a>,
    },
    /// PROCEDURE = procedure_name (synonym for FUNCTION)
    Procedure {
        keyword_span: Span,
        eq_span: Span,
        procedure_name: QualifiedName<'a>,
    },
    /// LEFTARG = type
    LeftArg {
        keyword_span: Span,
        eq_span: Span,
        arg_type: DataType<'a>,
    },
    /// RIGHTARG = type
    RightArg {
        keyword_span: Span,
        eq_span: Span,
        arg_type: DataType<'a>,
    },
    /// COMMUTATOR = operator or COMMUTATOR = OPERATOR(operator)
    Commutator {
        keyword_span: Span,
        eq_span: Span,
        operator: OperatorRef<'a>,
    },
    /// NEGATOR = operator or NEGATOR = OPERATOR(operator)
    Negator {
        keyword_span: Span,
        eq_span: Span,
        operator: OperatorRef<'a>,
    },
    /// RESTRICT = function
    Restrict {
        keyword_span: Span,
        eq_span: Span,
        function_name: QualifiedName<'a>,
    },
    /// JOIN = function
    Join {
        keyword_span: Span,
        eq_span: Span,
        function_name: QualifiedName<'a>,
    },
    /// HASHES
    Hashes(Span),
    /// MERGES
    Merges(Span),
}

impl<'a> Spanned for OperatorOption<'a> {
    fn span(&self) -> Span {
        match self {
            OperatorOption::Function {
                keyword_span,
                function_name,
                ..
            } => keyword_span.join_span(function_name),
            OperatorOption::Procedure {
                keyword_span,
                procedure_name,
                ..
            } => keyword_span.join_span(procedure_name),
            OperatorOption::LeftArg {
                keyword_span,
                arg_type,
                ..
            } => keyword_span.join_span(arg_type),
            OperatorOption::RightArg {
                keyword_span,
                arg_type,
                ..
            } => keyword_span.join_span(arg_type),
            OperatorOption::Commutator {
                keyword_span,
                operator,
                ..
            } => keyword_span.join_span(operator),
            OperatorOption::Negator {
                keyword_span,
                operator,
                ..
            } => keyword_span.join_span(operator),
            OperatorOption::Restrict {
                keyword_span,
                function_name,
                ..
            } => keyword_span.join_span(function_name),
            OperatorOption::Join {
                keyword_span,
                function_name,
                ..
            } => keyword_span.join_span(function_name),
            OperatorOption::Hashes(span) => span.clone(),
            OperatorOption::Merges(span) => span.clone(),
        }
    }
}

/// Reference to an operator
#[derive(Clone, Debug)]
pub enum OperatorRef<'a> {
    /// Simple operator reference (e.g., >)
    Simple(QualifiedName<'a>),
    /// OPERATOR(operator_name) syntax
    Wrapped {
        operator_span: Span,
        lparen_span: Span,
        operator_name: QualifiedName<'a>,
        rparen_span: Span,
    },
}

impl<'a> Spanned for OperatorRef<'a> {
    fn span(&self) -> Span {
        match self {
            OperatorRef::Simple(name) => name.span(),
            OperatorRef::Wrapped {
                operator_span,
                rparen_span,
                ..
            } => operator_span.join_span(rparen_span),
        }
    }
}

pub(crate) fn parse_create_operator<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<crate::create_option::CreateOption<'a>>,
) -> Result<CreateOperator<'a>, ParseError> {
    let operator_span = parser.consume_keyword(Keyword::OPERATOR)?;
    parser.postgres_only(&operator_span);

    // Report errors for unsupported create options
    for option in create_options {
        parser.err("Not supported for CREATE OPERATOR", &option.span());
    }

    // Parse operator name (can be a qualified name with schema or an operator symbol)
    // Operators can be symbols like +, -, @@, !=, or schema-qualified like myschema.@@
    let name = parse_operator_name(parser)?;

    // Parse options in parentheses
    let lparen_span = parser.consume_token(Token::LParen)?;

    let mut options = Vec::new();

    // Handle empty operator definition (should fail but we parse it)
    if let Some(rparen_span) = parser.skip_token(Token::RParen) {
        return Ok(CreateOperator {
            create_span,
            operator_span,
            name,
            lparen_span,
            options,
            rparen_span,
        });
    }

    // Parse operator options
    loop {
        let option = match &parser.token {
            Token::Ident(_, Keyword::FUNCTION) => {
                let keyword_span = parser.consume_keyword(Keyword::FUNCTION)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let function_name = parse_qualified_name(parser)?;
                OperatorOption::Function {
                    keyword_span,
                    eq_span,
                    function_name,
                }
            }
            Token::Ident(_, Keyword::PROCEDURE) => {
                let keyword_span = parser.consume_keyword(Keyword::PROCEDURE)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let procedure_name = parse_qualified_name(parser)?;
                OperatorOption::Procedure {
                    keyword_span,
                    eq_span,
                    procedure_name,
                }
            }
            Token::Ident(_, Keyword::LEFTARG) => {
                let keyword_span = parser.consume_keyword(Keyword::LEFTARG)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let arg_type = parse_data_type(parser, false)?;
                OperatorOption::LeftArg {
                    keyword_span,
                    eq_span,
                    arg_type,
                }
            }
            Token::Ident(_, Keyword::RIGHTARG) => {
                let keyword_span = parser.consume_keyword(Keyword::RIGHTARG)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let arg_type = parse_data_type(parser, false)?;
                OperatorOption::RightArg {
                    keyword_span,
                    eq_span,
                    arg_type,
                }
            }
            Token::Ident(_, Keyword::COMMUTATOR) => {
                let keyword_span = parser.consume_keyword(Keyword::COMMUTATOR)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let operator = parse_operator_ref(parser)?;
                OperatorOption::Commutator {
                    keyword_span,
                    eq_span,
                    operator,
                }
            }
            Token::Ident(_, Keyword::NEGATOR) => {
                let keyword_span = parser.consume_keyword(Keyword::NEGATOR)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let operator = parse_operator_ref(parser)?;
                OperatorOption::Negator {
                    keyword_span,
                    eq_span,
                    operator,
                }
            }
            Token::Ident(_, Keyword::RESTRICT) => {
                let keyword_span = parser.consume_keyword(Keyword::RESTRICT)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let function_name = parse_qualified_name(parser)?;
                OperatorOption::Restrict {
                    keyword_span,
                    eq_span,
                    function_name,
                }
            }
            Token::Ident(_, Keyword::JOIN) => {
                let keyword_span = parser.consume_keyword(Keyword::JOIN)?;
                let eq_span = parser.consume_token(Token::Eq)?;
                let function_name = parse_qualified_name(parser)?;
                OperatorOption::Join {
                    keyword_span,
                    eq_span,
                    function_name,
                }
            }
            Token::Ident(_, Keyword::HASHES) => {
                OperatorOption::Hashes(parser.consume_keyword(Keyword::HASHES)?)
            }
            Token::Ident(_, Keyword::MERGES) => {
                OperatorOption::Merges(parser.consume_keyword(Keyword::MERGES)?)
            }
            _ => {
                parser.expected_failure(
                    "'FUNCTION' | 'PROCEDURE' | 'LEFTARG' | 'RIGHTARG' | 'COMMUTATOR' | 'NEGATOR' | 'RESTRICT' | 'JOIN' | 'HASHES' | 'MERGES'"
                )?
            }
        };

        options.push(option);

        // Check for comma or end of options
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }

    let rparen_span = parser.consume_token(Token::RParen)?;

    Ok(CreateOperator {
        create_span,
        operator_span,
        name,
        lparen_span,
        options,
        rparen_span,
    })
}

/// Parse an operator reference (either simple or OPERATOR(...) syntax)
fn parse_operator_ref<'a>(parser: &mut Parser<'a, '_>) -> Result<OperatorRef<'a>, ParseError> {
    if let Token::Ident(_, Keyword::OPERATOR) = &parser.token {
        let operator_span = parser.consume_keyword(Keyword::OPERATOR)?;
        let lparen_span = parser.consume_token(Token::LParen)?;
        let operator_name = parse_operator_name(parser)?;
        let rparen_span = parser.consume_token(Token::RParen)?;
        Ok(OperatorRef::Wrapped {
            operator_span,
            lparen_span,
            operator_name,
            rparen_span,
        })
    } else {
        let name = parse_operator_name(parser)?;
        Ok(OperatorRef::Simple(name))
    }
}

/// Consume an operator symbol or identifier token
fn consume_operator_or_identifier<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<crate::Identifier<'a>, ParseError> {
    use crate::Identifier;

    match &parser.token {
        // Handle operator symbols as identifiers
        Token::Plus => Ok(Identifier::new("+", parser.consume())),
        Token::Minus => Ok(Identifier::new("-", parser.consume())),
        Token::Mul => Ok(Identifier::new("*", parser.consume())),
        Token::Div => Ok(Identifier::new("/", parser.consume())),
        Token::Mod => Ok(Identifier::new("%", parser.consume())),
        Token::Eq => Ok(Identifier::new("=", parser.consume())),
        Token::Neq => Ok(Identifier::new("!=", parser.consume())),
        Token::Lt => Ok(Identifier::new("<", parser.consume())),
        Token::LtEq => Ok(Identifier::new("<=", parser.consume())),
        Token::Gt => Ok(Identifier::new(">", parser.consume())),
        Token::GtEq => Ok(Identifier::new(">=", parser.consume())),
        Token::Ampersand => Ok(Identifier::new("&", parser.consume())),
        Token::Pipe => Ok(Identifier::new("|", parser.consume())),
        Token::Caret => Ok(Identifier::new("^", parser.consume())),
        Token::Tilde => Ok(Identifier::new("~", parser.consume())),
        Token::ExclamationMark => Ok(Identifier::new("!", parser.consume())),
        Token::At => Ok(Identifier::new("@", parser.consume())),
        Token::AtAt => Ok(Identifier::new("@@", parser.consume())),
        Token::Sharp => Ok(Identifier::new("#", parser.consume())),
        Token::ShiftLeft => Ok(Identifier::new("<<", parser.consume())),
        Token::ShiftRight => Ok(Identifier::new(">>", parser.consume())),
        Token::DoublePipe => Ok(Identifier::new("||", parser.consume())),
        Token::DoubleAmpersand => Ok(Identifier::new("&&", parser.consume())),
        Token::Spaceship => Ok(Identifier::new("<=>", parser.consume())),
        Token::RArrow => Ok(Identifier::new("->", parser.consume())),
        Token::RArrowJson => Ok(Identifier::new("->>", parser.consume())),
        Token::RDoubleArrowJson => Ok(Identifier::new("#>>", parser.consume())),
        Token::DoubleExclamationMark => Ok(Identifier::new("!!", parser.consume())),
        // Regular identifiers
        _ => parser.consume_plain_identifier(),
    }
}

/// Parse an operator name (can be an identifier, operator symbol, or schema-qualified)
pub(crate) fn parse_operator_name<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<QualifiedName<'a>, ParseError> {
    let mut identifier = consume_operator_or_identifier(parser)?;
    let mut prefix = Vec::new();

    // Handle schema qualification (e.g., myschema.@@)
    while let Some(dot) = parser.skip_token(Token::Period) {
        prefix.push((identifier, dot));
        identifier = consume_operator_or_identifier(parser)?;
    }

    Ok(QualifiedName { prefix, identifier })
}
