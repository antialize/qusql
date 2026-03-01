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
    DataType, Identifier, SString, Span, Spanned, Statement,
    create_option::CreateOption,
    data_type::parse_data_type,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    statement::parse_statement,
};
use alloc::vec::Vec;

/// Characteristic of a function
#[derive(Clone, Debug)]
pub enum FunctionCharacteristic<'a> {
    LanguageSql(Span),
    LanguagePlpgsql(Span),
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
            FunctionCharacteristic::LanguageSql(v) => v.span(),
            FunctionCharacteristic::NotDeterministic(v) => v.span(),
            FunctionCharacteristic::Deterministic(v) => v.span(),
            FunctionCharacteristic::ContainsSql(v) => v.span(),
            FunctionCharacteristic::NoSql(v) => v.span(),
            FunctionCharacteristic::ReadsSqlData(v) => v.span(),
            FunctionCharacteristic::ModifiesSqlData(v) => v.span(),
            FunctionCharacteristic::SqlSecurityDefiner(v) => v.span(),
            FunctionCharacteristic::SqlSecurityUser(v) => v.span(),
            FunctionCharacteristic::Comment(v) => v.span(),
            FunctionCharacteristic::LanguagePlpgsql(v) => v.span(),
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
    pub params: Vec<(Option<FunctionParamDirection>, Identifier<'a>, DataType<'a>)>,
    /// Span of "RETURNS"
    pub returns_span: Span,
    /// Type of return value
    pub return_type: DataType<'a>,
    /// Characteristics of created function
    pub characteristics: Vec<FunctionCharacteristic<'a>>,
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
            .join_span(&self.return_type)
            .join_span(&self.characteristics)
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

    let name = parser.consume_plain_identifier_unrestricted()?;
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

            let name = parser.consume_plain_identifier_unrestricted()?;
            let type_ = parse_data_type(parser, false)?;
            params.push((direction, name, type_));
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(())
    })?;
    parser.consume_token(Token::RParen)?;
    let returns_span = parser.consume_keyword(Keyword::RETURNS)?;
    let return_type = parse_data_type(parser, true)?;
    if parser.options.dialect.is_postgresql() && parser.skip_keyword(Keyword::AS).is_some() {
        parser.consume_token(Token::DoubleDollar)?;
        loop {
            match &parser.token {
                Token::Eof | Token::DoubleDollar => {
                    parser.consume_token(Token::DoubleDollar)?;
                    break;
                }
                _ => {
                    parser.consume();
                }
            }
        }
    }

    let mut characteristics = Vec::new();
    loop {
        let f = match &parser.token {
            Token::Ident(_, Keyword::LANGUAGE) => {
                let lg = parser.consume();
                match &parser.token {
                    Token::Ident(_, Keyword::SQL) => {
                        FunctionCharacteristic::LanguageSql(lg.join_span(&parser.consume()))
                    }
                    Token::Ident(_, Keyword::PLPGSQL) => {
                        FunctionCharacteristic::LanguagePlpgsql(lg.join_span(&parser.consume()))
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
            _ => break,
        };
        characteristics.push(f);
    }

    let return_ = if parser.options.dialect.is_maria() {
        match parse_statement(parser)? {
            Some(v) => Some(v),
            None => parser.expected_failure("statement")?,
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
        return_,
        returns_span,
    })
}
