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
    DataType, Expression, Identifier, SString, Span, Spanned, Statement,
    create_option::CreateOption,
    data_type::{DataTypeContext, parse_data_type},
    expression::{PRIORITY_MAX, parse_expression_unreserved},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    statement::parse_statement,
};
use alloc::vec::Vec;

/// Language of a function
#[derive(Clone, Debug)]
pub enum FunctionLanguage<'a> {
    Sql(Span),
    Plpgsql(Span),
    Other(Identifier<'a>),
}

impl<'a> Spanned for FunctionLanguage<'a> {
    fn span(&self) -> Span {
        match &self {
            FunctionLanguage::Sql(v) => v.span(),
            FunctionLanguage::Plpgsql(v) => v.span(),
            FunctionLanguage::Other(v) => v.span(),
        }
    }
}

/// Parallel safety level of a function
#[derive(Clone, Debug)]
pub enum FunctionParallel {
    Safe(Span),
    Unsafe(Span),
    Restricted(Span),
}

impl Spanned for FunctionParallel {
    fn span(&self) -> Span {
        match self {
            FunctionParallel::Safe(s) => s.clone(),
            FunctionParallel::Unsafe(s) => s.clone(),
            FunctionParallel::Restricted(s) => s.clone(),
        }
    }
}

/// Characteristic of a function
#[derive(Clone, Debug)]
pub enum FunctionCharacteristic<'a> {
    Language(Span, FunctionLanguage<'a>),
    Immutable(Span),
    Stable(Span),
    Volatile(Span),
    Strict(Span),
    CalledOnNullInput(Span),
    ReturnsNullOnNullInput(Span),
    Parallel(Span, FunctionParallel),
    NotDeterministic(Span),
    Deterministic(Span),
    ContainsSql(Span),
    NoSql(Span),
    ReadsSqlData(Span),
    ModifiesSqlData(Span),
    SqlSecurityDefiner(Span),
    SqlSecurityUser(Span),
    Comment(SString<'a>),
}

impl<'a> Spanned for FunctionCharacteristic<'a> {
    fn span(&self) -> Span {
        match &self {
            FunctionCharacteristic::Language(s, v) => s.join_span(v),
            FunctionCharacteristic::NotDeterministic(v) => v.span(),
            FunctionCharacteristic::Deterministic(v) => v.span(),
            FunctionCharacteristic::ContainsSql(v) => v.span(),
            FunctionCharacteristic::NoSql(v) => v.span(),
            FunctionCharacteristic::ReadsSqlData(v) => v.span(),
            FunctionCharacteristic::ModifiesSqlData(v) => v.span(),
            FunctionCharacteristic::SqlSecurityDefiner(v) => v.span(),
            FunctionCharacteristic::SqlSecurityUser(v) => v.span(),
            FunctionCharacteristic::Comment(v) => v.span(),
            FunctionCharacteristic::Immutable(v) => v.span(),
            FunctionCharacteristic::Stable(v) => v.span(),
            FunctionCharacteristic::Volatile(v) => v.span(),
            FunctionCharacteristic::Strict(v) => v.span(),
            FunctionCharacteristic::CalledOnNullInput(v) => v.span(),
            FunctionCharacteristic::ReturnsNullOnNullInput(v) => v.span(),
            FunctionCharacteristic::Parallel(s, v) => s.join_span(v),
        }
    }
}

/// Direction of a function argument
#[derive(Clone, Debug)]
pub enum FunctionParamDirection {
    In(Span),
    Out(Span),
    InOut(Span),
}

impl Spanned for FunctionParamDirection {
    fn span(&self) -> Span {
        match &self {
            FunctionParamDirection::In(v) => v.span(),
            FunctionParamDirection::Out(v) => v.span(),
            FunctionParamDirection::InOut(v) => v.span(),
        }
    }
}

/// A single function parameter
#[derive(Clone, Debug)]
pub struct FunctionParam<'a> {
    /// Optional direction modifier (IN, OUT, INOUT)
    pub direction: Option<FunctionParamDirection>,
    /// Optional parameter name
    pub name: Option<Identifier<'a>>,
    /// Parameter type
    pub type_: DataType<'a>,
    /// Optional default value: (= or DEFAULT span, expression)
    pub default: Option<(Span, Expression<'a>)>,
}

impl<'a> Spanned for FunctionParam<'a> {
    fn span(&self) -> Span {
        self.type_
            .join_span(&self.direction)
            .join_span(&self.name)
            .join_span(&self.default.as_ref().map(|(s, e)| s.join_span(e)))
    }
}

/// Body of a CREATE FUNCTION AS clause
#[derive(Clone, Debug)]
pub struct FunctionBody<'a> {
    /// Span of the AS keyword
    pub as_span: Span,
    /// The body string(s) — typically one dollar-quoted string, or two strings for C functions
    pub strings: Vec<SString<'a>>,
}

impl<'a> Spanned for FunctionBody<'a> {
    fn span(&self) -> Span {
        self.as_span.join_span(&self.strings)
    }
}

/// Representation of Create Function Statement
///
/// This is not fully implemented yet
///
/// ```ignore
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, CreateFunction, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "DELIMITER $$
/// CREATE FUNCTION add_func3(IN a INT, IN b INT, OUT c INT) RETURNS INT
/// BEGIN
///     SET c = 100;
///     RETURN a + b;
/// END;
/// $$
/// DELIMITER ;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// assert!(issues.is_empty());
/// #
/// let create: CreateFunction = match stmts.pop() {
///     Some(Statement::CreateFunction(c)) => c,
///     _ => panic!("We should get an create function statement")
/// };
///
/// assert!(create.name.as_str() == "add_func3");
/// println!("{:#?}", create.return_)
/// ```
#[derive(Clone, Debug)]
pub struct CreateFunction<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "FUNCTION"
    pub function_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name o created function
    pub name: Identifier<'a>,
    /// Names and types of function arguments
    pub params: Vec<FunctionParam<'a>>,
    /// Span of "RETURNS"
    pub returns_span: Span,
    /// Type of return value
    pub return_type: DataType<'a>,
    /// Characteristics of created function
    pub characteristics: Vec<FunctionCharacteristic<'a>>,
    /// Optional AS body (PostgreSQL)
    pub body: Option<FunctionBody<'a>>,
    /// Statement computing return value
    pub return_: Option<Statement<'a>>,
}

impl<'a> Spanned for CreateFunction<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.function_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.params)
            .join_span(&self.returns_span)
            .join_span(&self.return_type)
            .join_span(&self.characteristics)
            .join_span(&self.body)
            .join_span(&self.return_)
    }
}

pub(crate) fn parse_create_function<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateFunction<'a>, ParseError> {
    let function_span = parser.consume_keyword(Keyword::FUNCTION)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let name = parser.consume_plain_identifier_unreserved()?;
    let mut params = Vec::new();
    parser.consume_token(Token::LParen)?;
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            let direction = match &parser.token {
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

            let name = if parser.options.dialect.is_postgresql() {
                // In PostgreSQL, params can be unnamed (type only).
                // Peek at the next token to decide: if it's a boundary/separator,
                // the current token is the type start (unnamed param).
                let is_unnamed = matches!(
                    parser.peek(),
                    Token::Comma
                        | Token::RParen
                        | Token::LParen
                        | Token::Eq
                        | Token::Ident(_, Keyword::DEFAULT)
                        | Token::LBracket
                );
                if is_unnamed {
                    None
                } else {
                    Some(parser.consume_plain_identifier_unreserved()?)
                }
            } else {
                Some(parser.consume_plain_identifier_unreserved()?)
            };
            let type_ = parse_data_type(parser, DataTypeContext::FunctionParam)?;
            // Optional default value: '= expr' or 'DEFAULT expr'
            let default = if let Some(eq_span) = parser.skip_token(Token::Eq) {
                Some((eq_span, parse_expression_unreserved(parser, PRIORITY_MAX)?))
            } else if let Some(default_span) = parser.skip_keyword(Keyword::DEFAULT) {
                Some((
                    default_span,
                    parse_expression_unreserved(parser, PRIORITY_MAX)?,
                ))
            } else {
                None
            };
            params.push(FunctionParam {
                direction,
                name,
                type_,
                default,
            });
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    parser.consume_token(Token::RParen)?;
    let returns_span = parser.consume_keyword(Keyword::RETURNS)?;
    let return_type = parse_data_type(parser, DataTypeContext::FunctionReturn)?;
    let mut body: Option<FunctionBody<'_>> = None;
    let mut characteristics = Vec::new();
    loop {
        let f = match &parser.token {
            Token::Ident(_, Keyword::LANGUAGE) => {
                let lg = parser.consume();
                match &parser.token {
                    Token::Ident(_, Keyword::SQL) => FunctionCharacteristic::Language(
                        lg,
                        FunctionLanguage::Sql(parser.consume()),
                    ),
                    Token::Ident(_, Keyword::PLPGSQL) => FunctionCharacteristic::Language(
                        lg,
                        FunctionLanguage::Plpgsql(parser.consume()),
                    ),
                    Token::Ident(_, _) if parser.options.dialect.is_postgresql() => {
                        FunctionCharacteristic::Language(
                            lg,
                            FunctionLanguage::Other(parser.consume_plain_identifier_unreserved()?),
                        )
                    }
                    _ => parser.expected_failure("language name")?,
                }
            }
            Token::Ident(_, Keyword::NOT) => FunctionCharacteristic::NotDeterministic(
                parser.consume_keywords(&[Keyword::NOT, Keyword::DETERMINISTIC])?,
            ),
            Token::Ident(_, Keyword::DETERMINISTIC) => FunctionCharacteristic::Deterministic(
                parser.consume_keyword(Keyword::DETERMINISTIC)?,
            ),
            Token::Ident(_, Keyword::CONTAINS) => FunctionCharacteristic::ContainsSql(
                parser.consume_keywords(&[Keyword::CONTAINS, Keyword::SQL])?,
            ),
            Token::Ident(_, Keyword::NO) => FunctionCharacteristic::NoSql(
                parser.consume_keywords(&[Keyword::NO, Keyword::SQL])?,
            ),
            Token::Ident(_, Keyword::READS) => {
                FunctionCharacteristic::ReadsSqlData(parser.consume_keywords(&[
                    Keyword::READS,
                    Keyword::SQL,
                    Keyword::DATA,
                ])?)
            }
            Token::Ident(_, Keyword::MODIFIES) => {
                FunctionCharacteristic::ModifiesSqlData(parser.consume_keywords(&[
                    Keyword::MODIFIES,
                    Keyword::SQL,
                    Keyword::DATA,
                ])?)
            }
            Token::Ident(_, Keyword::COMMENT) => {
                parser.consume_keyword(Keyword::COMMENT)?;
                FunctionCharacteristic::Comment(parser.consume_string()?)
            }
            Token::Ident(_, Keyword::SQL) => {
                let span = parser.consume_keywords(&[Keyword::SQL, Keyword::SECURITY])?;
                match &parser.token {
                    Token::Ident(_, Keyword::DEFINER) => {
                        FunctionCharacteristic::SqlSecurityDefiner(
                            parser.consume_keyword(Keyword::DEFINER)?.join_span(&span),
                        )
                    }
                    Token::Ident(_, Keyword::USER) => FunctionCharacteristic::SqlSecurityUser(
                        parser.consume_keyword(Keyword::USER)?.join_span(&span),
                    ),
                    _ => parser.expected_failure("'DEFINER' or 'USER'")?,
                }
            }
            Token::Ident(_, Keyword::IMMUTABLE) => {
                FunctionCharacteristic::Immutable(parser.consume_keyword(Keyword::IMMUTABLE)?)
            }
            Token::Ident(_, Keyword::STABLE) => {
                FunctionCharacteristic::Stable(parser.consume_keyword(Keyword::STABLE)?)
            }
            Token::Ident(_, Keyword::VOLATILE) => {
                FunctionCharacteristic::Volatile(parser.consume_keyword(Keyword::VOLATILE)?)
            }
            Token::Ident(_, Keyword::STRICT) => {
                FunctionCharacteristic::Strict(parser.consume_keyword(Keyword::STRICT)?)
            }
            Token::Ident(_, Keyword::CALLED) if parser.options.dialect.is_postgresql() => {
                FunctionCharacteristic::CalledOnNullInput(parser.consume_keywords(&[
                    Keyword::CALLED,
                    Keyword::ON,
                    Keyword::NULL,
                    Keyword::INPUT,
                ])?)
            }
            Token::Ident(_, Keyword::RETURNS) if parser.options.dialect.is_postgresql() => {
                FunctionCharacteristic::ReturnsNullOnNullInput(parser.consume_keywords(&[
                    Keyword::RETURNS,
                    Keyword::NULL,
                    Keyword::ON,
                    Keyword::NULL,
                    Keyword::INPUT,
                ])?)
            }
            Token::Ident(_, Keyword::PARALLEL) => {
                let parallel_span = parser.consume_keyword(Keyword::PARALLEL)?;
                let level = match parser.consume_plain_identifier_unreserved()?.value {
                    v if v.eq_ignore_ascii_case("safe") => {
                        FunctionParallel::Safe(parallel_span.clone())
                    }
                    v if v.eq_ignore_ascii_case("unsafe") => {
                        FunctionParallel::Unsafe(parallel_span.clone())
                    }
                    v if v.eq_ignore_ascii_case("restricted") => {
                        FunctionParallel::Restricted(parallel_span.clone())
                    }
                    _ => {
                        parser.expected_error("SAFE, UNSAFE, or RESTRICTED");
                        FunctionParallel::Unsafe(parallel_span.clone())
                    }
                };
                FunctionCharacteristic::Parallel(parallel_span, level)
            }
            Token::Ident(_, Keyword::AS) if parser.options.dialect.is_postgresql() => {
                let as_span = parser.consume_keyword(Keyword::AS)?;
                let mut strings = Vec::new();
                match &parser.token {
                    Token::String(_, _) => {
                        strings.push(parser.consume_string()?);
                        // Handle comma-separated strings (e.g. C functions: 'MODULE_PATHNAME', 'func_name')
                        while parser.skip_token(Token::Comma).is_some() {
                            if matches!(&parser.token, Token::String(_, _)) {
                                strings.push(parser.consume_string()?);
                            } else {
                                break;
                            }
                        }
                    }
                    _ => {
                        parser.expected_error("'$$' or string");
                    }
                }
                body = Some(FunctionBody { as_span, strings });
                break;
            }
            _ => break,
        };
        characteristics.push(f);
    }

    if parser.options.dialect.is_postgresql()
        && !characteristics
            .iter()
            .any(|c| matches!(c, FunctionCharacteristic::Language(_, _)))
    {
        parser.expected_failure("LANGUAGE")?;
    }

    let return_ = if parser.options.dialect.is_maria() {
        let old = core::mem::replace(&mut parser.permit_compound_statements, true);
        let r = match parse_statement(parser)? {
            Some(v) => Some(v),
            None => parser.expected_failure("statement")?,
        };
        parser.permit_compound_statements = old;
        r
    } else if matches!(&parser.token, Token::Ident(_, Keyword::RETURN)) {
        // PostgreSQL SQL/PSM inline function body: `RETURN <expr>`
        match parse_statement(parser)? {
            Some(v) => Some(v),
            None => parser.expected_failure("statement after RETURN")?,
        }
    } else {
        None
    };

    Ok(CreateFunction {
        create_span,
        create_options,
        function_span,
        if_not_exists,
        name,
        params,
        return_type,
        characteristics,
        body,
        return_,
        returns_span,
    })
}

/// Representation of a CREATE PROCEDURE statement
///
/// Like functions but without a RETURNS clause.
#[derive(Clone, Debug)]
pub struct CreateProcedure<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE" (e.g. DEFINER=)
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "PROCEDURE"
    pub procedure_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of created procedure
    pub name: Identifier<'a>,
    /// Names and types of procedure parameters
    pub params: Vec<FunctionParam<'a>>,
    /// Characteristics (DETERMINISTIC, NO SQL, etc.)
    pub characteristics: Vec<FunctionCharacteristic<'a>>,
    /// Body statement (typically a BEGIN...END block)
    pub body: Option<Statement<'a>>,
}

impl<'a> Spanned for CreateProcedure<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.procedure_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.params)
            .join_span(&self.characteristics)
            .join_span(&self.body)
    }
}

pub(crate) fn parse_create_procedure<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateProcedure<'a>, ParseError> {
    let procedure_span = parser.consume_keyword(Keyword::PROCEDURE)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let name = parser.consume_plain_identifier_unreserved()?;
    let mut params = Vec::new();
    parser.consume_token(Token::LParen)?;
    parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
        loop {
            if matches!(parser.token, Token::RParen) {
                break;
            }
            let direction = match &parser.token {
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
            let name = Some(parser.consume_plain_identifier_unreserved()?);
            let type_ = parse_data_type(parser, DataTypeContext::FunctionParam)?;
            params.push(FunctionParam {
                direction,
                name,
                type_,
                default: None,
            });
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    parser.consume_token(Token::RParen)?;

    let mut characteristics = Vec::new();
    loop {
        let f = match &parser.token {
            Token::Ident(_, Keyword::NOT) => FunctionCharacteristic::NotDeterministic(
                parser.consume_keywords(&[Keyword::NOT, Keyword::DETERMINISTIC])?,
            ),
            Token::Ident(_, Keyword::DETERMINISTIC) => FunctionCharacteristic::Deterministic(
                parser.consume_keyword(Keyword::DETERMINISTIC)?,
            ),
            Token::Ident(_, Keyword::CONTAINS) => FunctionCharacteristic::ContainsSql(
                parser.consume_keywords(&[Keyword::CONTAINS, Keyword::SQL])?,
            ),
            Token::Ident(_, Keyword::NO) => FunctionCharacteristic::NoSql(
                parser.consume_keywords(&[Keyword::NO, Keyword::SQL])?,
            ),
            Token::Ident(_, Keyword::READS) => {
                FunctionCharacteristic::ReadsSqlData(parser.consume_keywords(&[
                    Keyword::READS,
                    Keyword::SQL,
                    Keyword::DATA,
                ])?)
            }
            Token::Ident(_, Keyword::MODIFIES) => {
                FunctionCharacteristic::ModifiesSqlData(parser.consume_keywords(&[
                    Keyword::MODIFIES,
                    Keyword::SQL,
                    Keyword::DATA,
                ])?)
            }
            Token::Ident(_, Keyword::COMMENT) => {
                parser.consume_keyword(Keyword::COMMENT)?;
                FunctionCharacteristic::Comment(parser.consume_string()?)
            }
            Token::Ident(_, Keyword::SQL) => {
                let span = parser.consume_keywords(&[Keyword::SQL, Keyword::SECURITY])?;
                match &parser.token {
                    Token::Ident(_, Keyword::DEFINER) => {
                        FunctionCharacteristic::SqlSecurityDefiner(
                            parser.consume_keyword(Keyword::DEFINER)?.join_span(&span),
                        )
                    }
                    Token::Ident(_, Keyword::USER) => FunctionCharacteristic::SqlSecurityUser(
                        parser.consume_keyword(Keyword::USER)?.join_span(&span),
                    ),
                    _ => parser.expected_failure("'DEFINER' or 'USER'")?,
                }
            }
            _ => break,
        };
        characteristics.push(f);
    }

    let old = core::mem::replace(&mut parser.permit_compound_statements, true);
    let body = parse_statement(parser)?;
    parser.permit_compound_statements = old;

    Ok(CreateProcedure {
        create_span,
        create_options,
        procedure_span,
        if_not_exists,
        name,
        params,
        characteristics,
        body,
    })
}
