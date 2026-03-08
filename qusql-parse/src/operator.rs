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
    CreateOption, DataType, QualifiedName, Span, Spanned, UsingIndexMethod,
    data_type::parse_data_type,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};

/// CREATE OPERATOR statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateOperator<'a> {
    /// Span of "CREATE OPERATOR"
    pub create_operator_span: Span,
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
        self.create_operator_span
            .join_span(&self.name)
            .join_span(&self.lparen_span)
            .join_span(&self.options)
            .join_span(&self.rparen_span)
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
    create_operator_span: Span,
    create_options: Vec<crate::create_option::CreateOption<'a>>,
) -> Result<CreateOperator<'a>, ParseError> {
    parser.postgres_only(&create_operator_span);

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
            create_operator_span,
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
        create_operator_span,
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

/// CREATE OPERATOR CLASS statement (PostgreSQL)

#[derive(Clone, Debug)]
pub struct CreateOperatorClass<'a> {
    /// Span of "CREATE OPERATOR CLASS"
    pub create_operator_class_span: Span,
    /// Name of the operator class
    pub name: QualifiedName<'a>,
    /// DEFAULT keyword span (optional)
    pub default_span: Option<Span>,
    /// FOR TYPE span
    pub for_type_span: Span,
    /// Data type for the operator class
    pub data_type: DataType<'a>,
    /// Index method (btree, gist, etc)
    pub index_method: UsingIndexMethod,
    /// FAMILY clause (optional)
    pub family: Option<(Span, QualifiedName<'a>)>,
    /// AS span
    pub as_span: Span,
    /// Items (operators, functions, storage)
    pub items: Vec<OperatorClassItem<'a>>,
}

impl<'a> Spanned for CreateOperatorClass<'a> {
    fn span(&self) -> Span {
        self.create_operator_class_span
            .join_span(&self.name)
            .join_span(&self.default_span)
            .join_span(&self.for_type_span)
            .join_span(&self.data_type)
            .join_span(&self.index_method)
            .join_span(&self.family)
            .join_span(&self.as_span)
            .join_span(&self.items)
    }
}

#[derive(Clone, Debug)]
pub enum OperatorClassItem<'a> {
    Operator {
        operator_span: Span,
        number: (usize, Span),
        operator: QualifiedName<'a>,
        rest: Vec<OperatorClassOperatorOption<'a>>,
    },
    Function {
        function_span: Span,
        number: (usize, Span),
        function: QualifiedName<'a>,
        arg_types: Vec<DataType<'a>>,
    },
    Storage {
        storage_span: Span,
        data_type: DataType<'a>,
    },
}

impl<'a> Spanned for OperatorClassItem<'a> {
    fn span(&self) -> Span {
        match self {
            OperatorClassItem::Operator {
                operator_span,
                number: (_, nspan),
                operator,
                rest,
            } => operator_span
                .join_span(nspan)
                .join_span(operator)
                .join_span(rest),
            OperatorClassItem::Function {
                function_span,
                number: (_, nspan),
                function,
                arg_types,
            } => function_span
                .join_span(nspan)
                .join_span(function)
                .join_span(arg_types),
            OperatorClassItem::Storage {
                storage_span,
                data_type,
            } => storage_span.join_span(data_type),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OperatorClassOperatorOption<'a> {
    ForSearch(Span),
    ForOrderBy(Span, QualifiedName<'a>),
}

impl<'a> Spanned for OperatorClassOperatorOption<'a> {
    fn span(&self) -> Span {
        match self {
            OperatorClassOperatorOption::ForSearch(span) => span.clone(),
            OperatorClassOperatorOption::ForOrderBy(span, qn) => span.join_span(qn),
        }
    }
}

pub(crate) fn parse_create_operator_class<'a>(
    parser: &mut Parser<'a, '_>,
    create_operator_class_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateOperatorClass<'a>, ParseError> {
    parser.postgres_only(&create_operator_class_span);

    for create_option in create_options {
        parser.err(
            "CREATE OPERATOR CLASS does not support any options",
            &create_option.span(),
        );
    }

    // Parse name
    let name = parse_qualified_name(parser)?;

    // Optional DEFAULT
    let default_span = parser.skip_keyword(Keyword::DEFAULT);

    // FOR TYPE
    let for_type_span = parser.consume_keywords(&[Keyword::FOR, Keyword::TYPE])?;
    let data_type = parse_data_type(parser, false)?;

    // USING index_method
    let using_span = parser.consume_keyword(Keyword::USING)?;
    let index_method = crate::create_index::parse_using_index_method(parser, using_span)?;

    // Optional FAMILY clause
    let family = if let Some(family_span) = parser.skip_keyword(Keyword::FAMILY) {
        let family_name = parse_qualified_name(parser)?;
        Some((family_span, family_name))
    } else {
        None
    };

    // AS
    let as_span = parser.consume_keyword(Keyword::AS)?;

    // Parse items (operators, functions, storage)
    let mut items = Vec::new();
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::OPERATOR) => {
                let operator_span = parser.consume_keyword(Keyword::OPERATOR)?;
                let (num, num_span) = parser.consume_int()?;
                let operator = parse_operator_name(parser)?;
                let mut rest = Vec::new();
                // Optional FOR SEARCH / FOR ORDER BY
                if let Some(for_span) = parser.skip_keyword(Keyword::FOR) {
                    if let Some(search_span) = parser.skip_keyword(Keyword::SEARCH) {
                        rest.push(OperatorClassOperatorOption::ForSearch(
                            for_span.join_span(&search_span),
                        ));
                    } else if let Some(order_span) = parser.skip_keyword(Keyword::ORDER) {
                        parser.consume_keyword(Keyword::BY)?;
                        let opclass = parse_operator_name(parser)?;
                        rest.push(OperatorClassOperatorOption::ForOrderBy(
                            for_span.join_span(&order_span),
                            opclass,
                        ));
                    }
                }
                items.push(OperatorClassItem::Operator {
                    operator_span,
                    number: (num, num_span),
                    operator,
                    rest,
                });
            }
            Token::Ident(_, Keyword::FUNCTION) => {
                let function_span = parser.consume_keyword(Keyword::FUNCTION)?;
                let (num, num_span) = parser.consume_int()?;
                // Optional arg types in parens
                let mut arg_types = Vec::new();
                if parser.skip_token(Token::LParen).is_some() {
                    loop {
                        arg_types.push(parse_data_type(parser, false)?);
                        if parser.skip_token(Token::Comma).is_none() {
                            break;
                        }
                    }
                    parser.consume_token(Token::RParen)?;
                }
                let function = parse_qualified_name(parser)?;
                items.push(OperatorClassItem::Function {
                    function_span,
                    number: (num, num_span),
                    function,
                    arg_types,
                });
            }
            Token::Ident(_, Keyword::STORAGE) => {
                let storage_span = parser.consume_keyword(Keyword::STORAGE)?;
                let data_type = parse_data_type(parser, false)?;
                items.push(OperatorClassItem::Storage {
                    storage_span,
                    data_type,
                });
            }
            _ => break,
        }
        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }

    Ok(CreateOperatorClass {
        create_operator_class_span,
        name,
        default_span,
        for_type_span,
        data_type,
        index_method,
        family,
        as_span,
        items,
    })
}

/// CREATE OPERATOR FAMILY statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct CreateOperatorFamily<'a> {
    /// Span of "CREATE OPERATOR FAMILY"
    pub create_operator_family_span: Span,
    /// The operator family name
    pub name: QualifiedName<'a>,
    /// The index method (btree, hash, gist, gin)
    pub index_method: UsingIndexMethod,
    /// Left parenthesis span
    pub lparen_span: Span,
    /// Right parenthesis span
    pub rparen_span: Span,
}

impl<'a> Spanned for CreateOperatorFamily<'a> {
    fn span(&self) -> Span {
        self.create_operator_family_span.clone()
    }
}

/// Parse CREATE OPERATOR FAMILY statement
pub(crate) fn parse_create_operator_family<'a>(
    parser: &mut Parser<'a, '_>,
    create_operator_family_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateOperatorFamily<'a>, ParseError> {
    parser.postgres_only(&create_operator_family_span);
    for create_option in create_options {
        parser.err(
            "CREATE OPERATOR FAMILY does not support any options",
            &create_option.span(),
        );
    }
    let name = parse_qualified_name(parser)?;
    let using_span = parser.consume_keyword(Keyword::USING)?;
    let using_index_method = crate::create_index::parse_using_index_method(parser, using_span)?;
    let lparen_span = parser.consume_token(Token::LParen)?;
    let rparen_span = parser.consume_token(Token::RParen)?;
    Ok(CreateOperatorFamily {
        create_operator_family_span,
        name,
        index_method: using_index_method,
        lparen_span,
        rparen_span,
    })
}

/// ALTER OPERATOR FAMILY statement (PostgreSQL)
#[derive(Clone, Debug)]
pub struct AlterOperatorFamily<'a> {
    /// Span of "ALTER OPERATOR FAMILY"
    pub alter_operator_family_span: Span,
    /// The operator family name
    pub name: QualifiedName<'a>,
    /// The index method (btree, hash, gist, gin)
    pub index_method: UsingIndexMethod,
    /// Left parenthesis span
    pub lparen_span: Span,
    /// Right parenthesis span
    pub rparen_span: Span,
}

impl<'a> Spanned for AlterOperatorFamily<'a> {
    fn span(&self) -> Span {
        self.alter_operator_family_span
            .join_span(&self.name)
            .join_span(&self.index_method)
            .join_span(&self.lparen_span)
            .join_span(&self.rparen_span)
    }
}

/// Parse ALTER OPERATOR FAMILY statement
pub(crate) fn parse_alter_operator_family<'a>(
    parser: &mut Parser<'a, '_>,
    alter_operator_family_span: Span,
) -> Result<AlterOperatorFamily<'a>, ParseError> {
    parser.postgres_only(&alter_operator_family_span);
    let name = parse_qualified_name(parser)?;
    let using_span = parser.consume_keyword(Keyword::USING)?;
    let index_method = crate::create_index::parse_using_index_method(parser, using_span)?;
    let lparen_span = parser.consume_token(Token::LParen)?;
    let rparen_span = parser.consume_token(Token::RParen)?;
    Ok(AlterOperatorFamily {
        alter_operator_family_span,
        name,
        index_method,
        lparen_span,
        rparen_span,
    })
}
