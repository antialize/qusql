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
    Identifier, InvalidExpression, SString, Span, Spanned,
    alter_table::{ForeignKeyMatch, ForeignKeyOn, ForeignKeyOnAction, ForeignKeyOnType},
    create::parse_sequence_options,
    expression::{Expression, PRIORITY_MAX, parse_expression_unreserved},
    keywords::Keyword,
    lexer::{StringType, Token},
    parser::{ParseError, Parser},
    span::OptSpanned,
};

/// A property on a datatype
#[derive(Debug, Clone)]
pub enum DataTypeProperty<'a> {
    Signed(Span),
    Unsigned(Span),
    Zerofill(Span),
    Null(Span),
    NotNull(Span),
    Default(Expression<'a>),
    Comment(SString<'a>),
    Charset(Identifier<'a>),
    Collate(Identifier<'a>),
    Virtual(Span),
    Persistent(Span),
    Stored(Span),
    Unique(Span),
    UniqueKey(Span),
    GeneratedAlways(Span),
    /// PostgreSQL GENERATED ALWAYS AS (expr) STORED — computed column
    GeneratedAlwaysAsExpr {
        span: Span,
        expr: Expression<'a>,
        stored_span: Option<Span>,
    },
    AutoIncrement(Span),
    PrimaryKey(Span),
    As((Span, Expression<'a>)),
    Check((Span, Expression<'a>)),
    OnUpdate((Span, Expression<'a>)),
    References {
        /// Span of the `REFERENCES` keyword
        span: Span,
        /// Referenced table
        table: Identifier<'a>,
        /// Referenced columns (may be empty if omitted)
        columns: Vec<Identifier<'a>>,
        /// Optional MATCH FULL / MATCH SIMPLE / MATCH PARTIAL
        match_type: Option<ForeignKeyMatch>,
        /// Optional ON DELETE / ON UPDATE actions
        ons: Vec<ForeignKeyOn>,
    },
}

impl<'a> Spanned for DataTypeProperty<'a> {
    fn span(&self) -> Span {
        match &self {
            DataTypeProperty::Signed(v) => v.span(),
            DataTypeProperty::Unsigned(v) => v.span(),
            DataTypeProperty::Zerofill(v) => v.span(),
            DataTypeProperty::Null(v) => v.span(),
            DataTypeProperty::NotNull(v) => v.span(),
            DataTypeProperty::Default(v) => v.span(),
            DataTypeProperty::Comment(v) => v.span(),
            DataTypeProperty::Charset(v) => v.span(),
            DataTypeProperty::Collate(v) => v.span(),
            DataTypeProperty::Virtual(v) => v.span(),
            DataTypeProperty::Persistent(v) => v.span(),
            DataTypeProperty::Stored(v) => v.span(),
            DataTypeProperty::Unique(v) => v.span(),
            DataTypeProperty::UniqueKey(v) => v.span(),
            DataTypeProperty::GeneratedAlways(v) => v.span(),
            DataTypeProperty::GeneratedAlwaysAsExpr {
                span,
                expr,
                stored_span,
            } => span.join_span(expr).join_span(stored_span),
            DataTypeProperty::AutoIncrement(v) => v.span(),
            DataTypeProperty::As((s, v)) => s.join_span(v),
            DataTypeProperty::Check((s, v)) => s.join_span(v),
            DataTypeProperty::PrimaryKey(v) => v.span(),
            DataTypeProperty::OnUpdate((s, v)) => s.join_span(v),
            DataTypeProperty::References {
                span,
                table,
                columns,
                match_type,
                ons,
            } => span
                .join_span(table)
                .join_span(columns)
                .join_span(match_type)
                .join_span(ons),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Timestamp {
    pub width: Option<(usize, Span)>,
    pub with_time_zone: Option<Span>,
}

impl OptSpanned for Timestamp {
    fn opt_span(&self) -> Option<Span> {
        self.width.opt_span().opt_join_span(&self.with_time_zone)
    }
}

/// Subtype for a built-in PostgreSQL range or multirange type.
#[derive(Debug, Clone)]
pub enum RangeSubtype {
    Int4,
    Int8,
    Num,
    Ts,
    Tstz,
    Date,
}

/// A single field qualifier for an `INTERVAL` type.
#[derive(Debug, Clone)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// Interior of `Type::Interval`.
#[derive(Debug, Clone)]
pub struct Interval {
    /// The first (or only) field qualifier, e.g. `YEAR` in `INTERVAL YEAR TO MONTH`.
    pub start_field: Option<(IntervalField, Span)>,
    /// The upper-bound field, e.g. `MONTH` in `INTERVAL YEAR TO MONTH`.
    pub end_field: Option<(IntervalField, Span)>,
    /// Fractional-seconds precision; only meaningful when the rightmost unit is `SECOND`.
    pub precision: Option<(usize, Span)>,
}

impl OptSpanned for Interval {
    fn opt_span(&self) -> Option<Span> {
        let s = self.start_field.as_ref().map(|(_, s)| s.clone());
        let e = self.end_field.as_ref().map(|(_, s)| s.clone());
        s.opt_join_span(&e).opt_join_span(&self.precision)
    }
}

/// Type of datatype
#[derive(Debug, Clone)]
pub enum Type<'a> {
    Array(Box<Type<'a>>, Span),
    BigInt(Option<(usize, Span)>),
    BigSerial,
    Binary(Option<(usize, Span)>),
    Bit(usize, Span),
    Blob(Option<(usize, Span)>),
    Boolean,
    Box,
    Bytea,
    Char(Option<(usize, Span)>),
    Cidr,
    Circle,
    Date,
    DateTime(Option<(usize, Span)>),
    Decimal(Option<(usize, usize, Span)>),
    Double(Option<(usize, usize, Span)>),
    Enum(Vec<SString<'a>>),
    Float(Option<(usize, usize, Span)>),
    Float8,
    Inet4,
    Inet6,
    InetAddr,
    Int(Option<(usize, Span)>),
    Integer(Option<(usize, Span)>),
    Interval(Interval),
    Json,
    Jsonb,
    Line,
    LongBlob(Option<(usize, Span)>),
    LongText(Option<(usize, Span)>),
    Lseg,
    Macaddr,
    Macaddr8,
    MediumBlob(Option<(usize, Span)>),
    MediumInt(Option<(usize, Span)>),
    MediumText(Option<(usize, Span)>),
    Money,
    Named(Span),
    Path,
    Numeric(Option<(usize, usize, Span)>),
    Range(RangeSubtype),
    MultiRange(RangeSubtype),
    Serial,
    Set(Vec<SString<'a>>),
    Point,
    Polygon,
    SmallInt(Option<(usize, Span)>),
    SmallSerial,
    Table(Span, Vec<(Identifier<'a>, DataType<'a>)>),
    Text(Option<(usize, Span)>),
    Time(Option<(usize, Span)>),
    Timestamp(Timestamp),
    Timestamptz,
    Timetz(Option<(usize, Span)>),
    TsQuery,
    TsVector,
    TinyBlob(Option<(usize, Span)>),
    TinyInt(Option<(usize, Span)>),
    TinyText(Option<(usize, Span)>),
    VarBinary((usize, Span)),
    VarBit(Option<(usize, Span)>),
    VarChar(Option<(usize, Span)>),
    Uuid,
    Xml,
}

impl<'a> OptSpanned for Type<'a> {
    fn opt_span(&self) -> Option<Span> {
        match &self {
            Type::Array(inner, span) => Some(span.join_span(inner.as_ref())),
            Type::BigInt(v) => v.opt_span(),
            Type::BigSerial => None,
            Type::Binary(v) => v.opt_span(),
            Type::Bit(_, b) => b.opt_span(),
            Type::Blob(v) => v.opt_span(),
            Type::Boolean => None,
            Type::Box => None,
            Type::Bytea => None,
            Type::Char(v) => v.opt_span(),
            Type::Cidr => None,
            Type::Circle => None,
            Type::Date => None,
            Type::DateTime(v) => v.opt_span(),
            Type::Decimal(v) => v.opt_span(),
            Type::Double(v) => v.opt_span(),
            Type::Enum(v) => v.opt_span(),
            Type::Float(v) => v.opt_span(),
            Type::Float8 => None,
            Type::Inet4 => None,
            Type::Inet6 => None,
            Type::InetAddr => None,
            Type::Int(v) => v.opt_span(),
            Type::Integer(v) => v.opt_span(),
            Type::Interval(v) => v.opt_span(),
            Type::Json => None,
            Type::Jsonb => None,
            Type::Line => None,
            Type::LongBlob(v) => v.opt_span(),
            Type::LongText(v) => v.opt_span(),
            Type::Lseg => None,
            Type::Macaddr => None,
            Type::Macaddr8 => None,
            Type::MediumBlob(v) => v.opt_span(),
            Type::MediumInt(v) => v.opt_span(),
            Type::MediumText(v) => v.opt_span(),
            Type::Money => None,
            Type::Named(v) => v.opt_span(),
            Type::Path => None,
            Type::Numeric(v) => v.opt_span(),
            Type::Range(_) => None,
            Type::MultiRange(_) => None,
            Type::Serial => None,
            Type::Set(v) => v.opt_span(),
            Type::Point => None,
            Type::Polygon => None,
            Type::SmallInt(v) => v.opt_span(),
            Type::SmallSerial => None,
            Type::Table(span, _) => Some(span.clone()),
            Type::Text(v) => v.opt_span(),
            Type::Time(v) => v.opt_span(),
            Type::Timestamp(v) => v.opt_span(),
            Type::Timestamptz => None,
            Type::Timetz(v) => v.opt_span(),
            Type::TsQuery => None,
            Type::TsVector => None,
            Type::TinyBlob(v) => v.opt_span(),
            Type::TinyInt(v) => v.opt_span(),
            Type::TinyText(v) => v.opt_span(),
            Type::VarBinary(v) => v.opt_span(),
            Type::VarBit(v) => v.opt_span(),
            Type::VarChar(v) => v.opt_span(),
            Type::Uuid => None,
            Type::Xml => None,
        }
    }
}

/// Type of data
#[derive(Debug, Clone)]
pub struct DataType<'a> {
    /// Span of type_ identifier
    pub identifier: Span,
    /// Type with width
    pub type_: Type<'a>,
    /// Properties on type
    pub properties: Vec<DataTypeProperty<'a>>,
}

impl<'a> Spanned for DataType<'a> {
    fn span(&self) -> Span {
        self.identifier
            .join_span(&self.type_)
            .join_span(&self.properties)
    }
}
fn parse_width(parser: &mut Parser<'_, '_>) -> Result<Option<(usize, Span)>, ParseError> {
    if !matches!(parser.token, Token::LParen) {
        return Ok(None);
    }
    parser.consume_token(Token::LParen)?;
    let value = parser.recovered(")", &|t| t == &Token::RParen, |parser| parser.consume_int())?;
    parser.consume_token(Token::RParen)?;
    Ok(Some(value))
}

fn parse_width_req(parser: &mut Parser<'_, '_>) -> Result<(usize, Span), ParseError> {
    if !matches!(parser.token, Token::LParen) {
        return parser.expected_failure("'('");
    }
    Ok(parse_width(parser)?.expect("width"))
}

fn parse_precision_scale(
    parser: &mut Parser<'_, '_>,
) -> Result<Option<(usize, usize, Span)>, ParseError> {
    if !matches!(parser.token, Token::LParen) {
        return Ok(None);
    }
    let left = parser.consume_token(Token::LParen)?;
    let (precision, s1) = parser.consume_int()?;
    let scale = if parser.skip_token(Token::Comma).is_some() {
        let (v, _) = parser.consume_int()?;
        v
    } else {
        0
    };
    let right = parser.consume_token(Token::RParen)?;
    let span = left.join_span(&s1).join_span(&right);
    Ok(Some((precision, scale, span)))
}

fn parse_enum_set_values<'a>(parser: &mut Parser<'a, '_>) -> Result<Vec<SString<'a>>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut ans = Vec::new();
    parser.recovered(")", &|t| t == &Token::RParen, |parser| {
        loop {
            ans.push(parser.consume_string()?);
            match &parser.token {
                Token::Comma => {
                    parser.consume_token(Token::Comma)?;
                }
                Token::RParen => break,
                _ => parser.expected_failure("',' or ')'")?,
            }
        }
        Ok(())
    })?;
    parser.consume_token(Token::RParen)?;
    Ok(ans)
}

fn parse_interval_field(
    parser: &mut Parser<'_, '_>,
) -> Result<Option<(IntervalField, Span)>, ParseError> {
    let (field, kw) = match &parser.token {
        Token::Ident(_, Keyword::YEAR) => (IntervalField::Year, Keyword::YEAR),
        Token::Ident(_, Keyword::MONTH) => (IntervalField::Month, Keyword::MONTH),
        Token::Ident(_, Keyword::DAY) => (IntervalField::Day, Keyword::DAY),
        Token::Ident(_, Keyword::HOUR) => (IntervalField::Hour, Keyword::HOUR),
        Token::Ident(_, Keyword::MINUTE) => (IntervalField::Minute, Keyword::MINUTE),
        Token::Ident(_, Keyword::SECOND) => (IntervalField::Second, Keyword::SECOND),
        _ => return Ok(None),
    };
    let span = parser.consume_keyword(kw)?;
    Ok(Some((field, span)))
}

/// The context in which a data type is being parsed.
///
/// This controls which [`DataTypeProperty`] variants are syntactically accepted
/// and enables future per-context validation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataTypeContext {
    /// Column definition in `CREATE TABLE`, `ALTER TABLE ADD/MODIFY COLUMN`,
    /// or composite type attributes (`ALTER TYPE … ADD/ALTER ATTRIBUTE`).
    /// All properties are allowed, including the `AS (expr)` generated-column syntax.
    Column,
    /// Function or procedure parameter type (`CREATE FUNCTION f(a INT …)`).
    /// The `AS (expr)` generated-column syntax is not accepted here.
    FunctionParam,
    /// Function return type (`RETURNS …`), or a `JSON_TABLE` column path type.
    /// The `AS (expr)` generated-column syntax is not accepted here.
    FunctionReturn,
    /// Type reference in an expression context: `CAST(… AS type)`, `expr::type`,
    /// `CONVERT(expr, type)`, or operator/function argument types in `DROP`.
    /// The `AS (expr)` generated-column syntax is not accepted here.
    TypeRef,
}

pub(crate) fn parse_data_type<'a>(
    parser: &mut Parser<'a, '_>,
    ctx: DataTypeContext,
) -> Result<DataType<'a>, ParseError> {
    let (identifier, type_) = match &parser.token {
        Token::Ident(_, Keyword::BOOLEAN) => {
            (parser.consume_keyword(Keyword::BOOLEAN)?, Type::Boolean)
        }
        Token::Ident(_, Keyword::BOOL) => (parser.consume_keyword(Keyword::BOOL)?, Type::Boolean),
        Token::Ident(_, Keyword::TINYINT) => (
            parser.consume_keyword(Keyword::TINYINT)?,
            Type::TinyInt(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::SMALLINT) => (
            parser.consume_keyword(Keyword::SMALLINT)?,
            Type::SmallInt(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::MEDIUMINT) => (
            parser.consume_keyword(Keyword::MEDIUMINT)?,
            Type::MediumInt(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::INTEGER) => (
            parser.consume_keyword(Keyword::INTEGER)?,
            Type::Integer(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::INT) => (
            parser.consume_keyword(Keyword::INT)?,
            Type::Int(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::BIGINT) => (
            parser.consume_keyword(Keyword::BIGINT)?,
            Type::BigInt(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::INET4) => (parser.consume_keyword(Keyword::INET4)?, Type::Inet4),
        Token::Ident(_, Keyword::INET6) => (parser.consume_keyword(Keyword::INET6)?, Type::Inet6),
        Token::Ident(_, Keyword::INET) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::INET)?, Type::InetAddr)
        }
        Token::Ident(_, Keyword::CIDR) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::CIDR)?, Type::Cidr)
        }
        Token::Ident(_, Keyword::MACADDR) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::MACADDR)?, Type::Macaddr)
        }
        Token::Ident(_, Keyword::MACADDR8) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::MACADDR8)?, Type::Macaddr8)
        }
        Token::Ident(_, Keyword::INT2) => {
            (parser.consume_keyword(Keyword::INT2)?, Type::SmallInt(None))
        }
        Token::Ident(_, Keyword::INT4) => (parser.consume_keyword(Keyword::INT4)?, Type::Int(None)),
        Token::Ident(_, Keyword::INT8) => {
            (parser.consume_keyword(Keyword::INT8)?, Type::BigInt(None))
        }
        Token::Ident(_, Keyword::FLOAT4) => {
            (parser.consume_keyword(Keyword::FLOAT4)?, Type::Float(None))
        }
        Token::Ident(_, Keyword::SERIAL) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::SERIAL)?, Type::Serial)
        }
        Token::Ident(_, Keyword::SMALLSERIAL) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::SMALLSERIAL)?,
            Type::SmallSerial,
        ),
        Token::Ident(_, Keyword::BIGSERIAL) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::BIGSERIAL)?, Type::BigSerial)
        }
        Token::Ident(_, Keyword::MONEY) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::MONEY)?, Type::Money)
        }
        Token::Ident(_, Keyword::TINYTEXT) => (
            parser.consume_keyword(Keyword::TINYTEXT)?,
            Type::TinyText(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::CHAR) => (
            parser.consume_keyword(Keyword::CHAR)?,
            Type::Char(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::CHARACTER) => {
            let char_span = parser.consume_keyword(Keyword::CHARACTER)?;
            if let Some(varying_span) = parser.skip_keyword(Keyword::VARYING) {
                (
                    char_span.join_span(&varying_span),
                    Type::VarChar(parse_width(parser)?),
                )
            } else {
                (char_span, Type::Char(parse_width(parser)?))
            }
        }
        Token::Ident(_, Keyword::BPCHAR) => (
            parser.consume_keyword(Keyword::BPCHAR)?,
            Type::Char(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::NCHAR) => (
            parser.consume_keyword(Keyword::NCHAR)?,
            Type::Char(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::TEXT) => (
            parser.consume_keyword(Keyword::TEXT)?,
            Type::Text(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::MEDIUMTEXT) => (
            parser.consume_keyword(Keyword::MEDIUMTEXT)?,
            Type::MediumText(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::LONGTEXT) => (
            parser.consume_keyword(Keyword::LONGTEXT)?,
            Type::LongText(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::VARCHAR) => (
            parser.consume_keyword(Keyword::VARCHAR)?,
            Type::VarChar(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::VARCHARACTER) => (
            parser.consume_keyword(Keyword::VARCHARACTER)?,
            Type::VarChar(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::NVARCHAR) => (
            parser.consume_keyword(Keyword::NVARCHAR)?,
            Type::VarChar(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::TINYBLOB) => (
            parser.consume_keyword(Keyword::TINYBLOB)?,
            Type::TinyBlob(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::BLOB) => (
            parser.consume_keyword(Keyword::BLOB)?,
            Type::Blob(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::MEDIUMBLOB) => (
            parser.consume_keyword(Keyword::MEDIUMBLOB)?,
            Type::MediumBlob(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::LONGBLOB) => (
            parser.consume_keyword(Keyword::LONGBLOB)?,
            Type::LongBlob(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::VARBINARY) => (
            parser.consume_keyword(Keyword::VARBINARY)?,
            Type::VarBinary(parse_width_req(parser)?),
        ),
        Token::Ident(_, Keyword::BINARY) => (
            parser.consume_keyword(Keyword::BINARY)?,
            Type::Binary(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::FLOAT8) => {
            (parser.consume_keyword(Keyword::FLOAT8)?, Type::Float8)
        }
        Token::Ident(_, Keyword::REAL) => {
            let i = parser.consume_keyword(Keyword::REAL)?;
            if parser.options.dialect.is_sqlite() {
                (i, Type::Double(None))
            } else {
                (i, Type::Float(None))
            }
        }
        Token::Ident(_, Keyword::FLOAT) => {
            let i = parser.consume_keyword(Keyword::FLOAT)?;
            (i, Type::Float(parse_precision_scale(parser)?))
        }
        Token::Ident(_, Keyword::DOUBLE) => {
            let i = if parser.options.dialect.is_postgresql() {
                parser.consume_keywords(&[Keyword::DOUBLE, Keyword::PRECISION])?
            } else {
                let double_span = parser.consume_keyword(Keyword::DOUBLE)?;
                // MySQL also supports optional PRECISION keyword
                if let Some(precision_span) = parser.skip_keyword(Keyword::PRECISION) {
                    double_span.join_span(&precision_span)
                } else {
                    double_span
                }
            };
            (i, Type::Double(parse_precision_scale(parser)?))
        }
        Token::Ident(_, Keyword::NUMERIC) => {
            let numeric = parser.consume_keyword(Keyword::NUMERIC)?;
            (numeric, Type::Numeric(parse_precision_scale(parser)?))
        }
        Token::Ident(_, Keyword::DECIMAL) => {
            let decimal = parser.consume_keyword(Keyword::DECIMAL)?;
            (decimal, Type::Decimal(parse_precision_scale(parser)?))
        }
        Token::Ident(_, Keyword::DEC) => {
            let dec = parser.consume_keyword(Keyword::DEC)?;
            (dec, Type::Decimal(parse_precision_scale(parser)?))
        }
        Token::Ident(_, Keyword::DATETIME) => (
            parser.consume_keyword(Keyword::DATETIME)?,
            Type::DateTime(parse_width(parser)?),
        ),
        Token::Ident(_, Keyword::TIMETZ) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::TIMETZ)?, Type::Timetz(None))
        }
        Token::Ident(_, Keyword::TIME) => {
            let time_span = parser.consume_keyword(Keyword::TIME)?;
            let width = parse_width(parser)?;
            if parser.options.dialect.is_postgresql() {
                if parser.skip_keyword(Keyword::WITH).is_some() {
                    parser.consume_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    (time_span, Type::Timetz(width))
                } else if parser.skip_keyword(Keyword::WITHOUT).is_some() {
                    parser.consume_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    (time_span, Type::Time(width))
                } else {
                    (time_span, Type::Time(width))
                }
            } else {
                (time_span, Type::Time(width))
            }
        }
        Token::Ident(_, Keyword::TIMESTAMPTZ) => (
            parser.consume_keyword(Keyword::TIMESTAMPTZ)?,
            Type::Timestamptz,
        ),
        Token::Ident(_, Keyword::TIMESTAMP) => {
            let timestamp_span = parser.consume_keyword(Keyword::TIMESTAMP)?;
            let width = parse_width(parser)?;
            let with_time_zone = if let Some(with_span) = parser.skip_keyword(Keyword::WITH) {
                Some(
                    with_span.join_span(&parser.consume_keywords(&[Keyword::TIME, Keyword::ZONE])?),
                )
            } else {
                if parser.skip_keyword(Keyword::WITHOUT).is_some() {
                    parser.consume_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                }
                None
            };
            let timestamp = Timestamp {
                width,
                with_time_zone,
            };
            (timestamp_span, Type::Timestamp(timestamp))
        }
        Token::Ident(_, Keyword::DATE) => (parser.consume_keyword(Keyword::DATE)?, Type::Date),
        Token::Ident(_, Keyword::BOX) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::BOX)?, Type::Box)
        }
        Token::Ident(_, Keyword::CIRCLE) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::CIRCLE)?, Type::Circle)
        }
        Token::Ident(_, Keyword::LINE) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::LINE)?, Type::Line)
        }
        Token::Ident(_, Keyword::LSEG) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::LSEG)?, Type::Lseg)
        }
        Token::Ident(_, Keyword::PATH) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::PATH)?, Type::Path)
        }
        Token::Ident(_, Keyword::POINT) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::POINT)?, Type::Point)
        }
        Token::Ident(_, Keyword::POLYGON) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::POLYGON)?, Type::Polygon)
        }
        Token::Ident(_, Keyword::INTERVAL) if parser.options.dialect.is_postgresql() => {
            let interval_span = parser.consume_keyword(Keyword::INTERVAL)?;
            let start_field = parse_interval_field(parser)?;
            let end_field = if start_field.is_some() && parser.skip_keyword(Keyword::TO).is_some() {
                parse_interval_field(parser)?
            } else {
                None
            };
            let precision = parse_width(parser)?;
            (
                interval_span,
                Type::Interval(Interval {
                    start_field,
                    end_field,
                    precision,
                }),
            )
        }
        Token::Ident(_, Keyword::ENUM) => (
            parser.consume_keyword(Keyword::ENUM)?,
            Type::Enum(parse_enum_set_values(parser)?),
        ),
        Token::Ident(_, Keyword::SET) => (
            parser.consume_keyword(Keyword::SET)?,
            Type::Set(parse_enum_set_values(parser)?),
        ),
        Token::Ident(_, Keyword::JSON) => (parser.consume_keyword(Keyword::JSON)?, Type::Json),
        Token::Ident(_, Keyword::JSONB) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::JSONB)?, Type::Jsonb)
        }
        Token::Ident(_, Keyword::BYTEA) => (parser.consume_keyword(Keyword::BYTEA)?, Type::Bytea),
        Token::Ident(_, Keyword::INT4RANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::INT4RANGE)?,
            Type::Range(RangeSubtype::Int4),
        ),
        Token::Ident(_, Keyword::INT8RANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::INT8RANGE)?,
            Type::Range(RangeSubtype::Int8),
        ),
        Token::Ident(_, Keyword::NUMRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::NUMRANGE)?,
            Type::Range(RangeSubtype::Num),
        ),
        Token::Ident(_, Keyword::TSRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::TSRANGE)?,
            Type::Range(RangeSubtype::Ts),
        ),
        Token::Ident(_, Keyword::TSTZRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::TSTZRANGE)?,
            Type::Range(RangeSubtype::Tstz),
        ),
        Token::Ident(_, Keyword::DATERANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::DATERANGE)?,
            Type::Range(RangeSubtype::Date),
        ),
        Token::Ident(_, Keyword::INT4MULTIRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::INT4MULTIRANGE)?,
            Type::MultiRange(RangeSubtype::Int4),
        ),
        Token::Ident(_, Keyword::INT8MULTIRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::INT8MULTIRANGE)?,
            Type::MultiRange(RangeSubtype::Int8),
        ),
        Token::Ident(_, Keyword::NUMMULTIRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::NUMMULTIRANGE)?,
            Type::MultiRange(RangeSubtype::Num),
        ),
        Token::Ident(_, Keyword::TSMULTIRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::TSMULTIRANGE)?,
            Type::MultiRange(RangeSubtype::Ts),
        ),
        Token::Ident(_, Keyword::TSTZMULTIRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::TSTZMULTIRANGE)?,
            Type::MultiRange(RangeSubtype::Tstz),
        ),
        Token::Ident(_, Keyword::DATEMULTIRANGE) if parser.options.dialect.is_postgresql() => (
            parser.consume_keyword(Keyword::DATEMULTIRANGE)?,
            Type::MultiRange(RangeSubtype::Date),
        ),
        Token::Ident(_, Keyword::UUID) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::UUID)?, Type::Uuid)
        }
        Token::Ident(_, Keyword::XML) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::XML)?, Type::Xml)
        }
        Token::Ident(_, Keyword::TSQUERY) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::TSQUERY)?, Type::TsQuery)
        }
        Token::Ident(_, Keyword::TSVECTOR) if parser.options.dialect.is_postgresql() => {
            (parser.consume_keyword(Keyword::TSVECTOR)?, Type::TsVector)
        }
        Token::Ident(_, Keyword::BIT) => {
            let t = parser.consume_keyword(Keyword::BIT)?;
            if parser.options.dialect.is_postgresql() {
                if parser.skip_keyword(Keyword::VARYING).is_some() {
                    (t, Type::VarBit(parse_width(parser)?))
                } else {
                    let width = parse_width(parser)?;
                    let (w, ws) = width.unwrap_or((1, t.clone()));
                    (t, Type::Bit(w, ws))
                }
            } else {
                let (w, ws) = parse_width_req(parser)?;
                (t, Type::Bit(w, ws))
            }
        }
        Token::Ident(_, Keyword::VARBIT) => {
            let t = parser.consume_keyword(Keyword::VARBIT)?;
            parser.postgres_only(&t);
            (t, Type::VarBit(parse_width(parser)?))
        }
        Token::Ident(_, Keyword::TABLE)
            if parser.options.dialect.is_postgresql() && ctx == DataTypeContext::FunctionReturn =>
        {
            let table_span = parser.consume_keyword(Keyword::TABLE)?;
            let lparen = parser.consume_token(Token::LParen)?;
            let mut columns = Vec::new();
            parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                loop {
                    let name = parser.consume_plain_identifier_unreserved()?;
                    let col_type = parse_data_type(parser, DataTypeContext::FunctionParam)?;
                    columns.push((name, col_type));
                    if parser.skip_token(Token::Comma).is_none() {
                        break;
                    }
                }
                Ok(())
            })?;
            let rparen = parser.consume_token(Token::RParen)?;
            let paren_span = lparen.join_span(&rparen);
            (table_span, Type::Table(paren_span, columns))
        }
        Token::String(_, StringType::DoubleQuoted) if parser.options.dialect.is_postgresql() => {
            let name = parser.consume();
            (name.clone(), Type::Named(name))
        }
        Token::Ident(_, _) if parser.options.dialect.is_postgresql() => {
            let name = parser.consume();
            (name.clone(), Type::Named(name))
        }
        _ => parser.expected_failure("type")?,
    };

    // Check for PostgreSQL array type syntax: TYPE[] or TYPE[][] etc.
    let (identifier, type_) = {
        let mut identifier = identifier;
        let mut type_ = type_;
        while parser.options.dialect.is_postgresql() && matches!(parser.token, Token::LBracket) {
            let lbracket = parser.consume_token(Token::LBracket)?;
            let rbracket = parser.consume_token(Token::RBracket)?;
            let array_span = lbracket.join_span(&rbracket);
            identifier = identifier.join_span(&array_span);
            type_ = Type::Array(Box::new(type_), array_span);
        }
        (identifier, type_)
    };

    let mut properties = Vec::new();
    loop {
        // Each arm tuple is (&parser.token, ctx).  The wildcard arm stops
        // property parsing for both "unknown token" and "property not allowed
        // in this context", so callers never silently swallow tokens.
        use DataTypeContext::*;
        match (&parser.token, ctx) {
            // ── Properties valid in column definitions and function signatures ──
            (Token::Ident(_, Keyword::SIGNED), Column | FunctionParam | FunctionReturn) => {
                properties.push(DataTypeProperty::Signed(
                    parser.consume_keyword(Keyword::SIGNED)?,
                ));
            }
            (Token::Ident(_, Keyword::UNSIGNED), Column | FunctionParam | FunctionReturn) => {
                properties.push(DataTypeProperty::Unsigned(
                    parser.consume_keyword(Keyword::UNSIGNED)?,
                ));
            }
            (Token::Ident(_, Keyword::ZEROFILL), Column | FunctionParam | FunctionReturn) => {
                properties.push(DataTypeProperty::Zerofill(
                    parser.consume_keyword(Keyword::ZEROFILL)?,
                ));
            }
            (
                Token::Ident(_, Keyword::CHARACTER),
                Column | FunctionParam | FunctionReturn | TypeRef,
            ) => {
                parser.consume_keywords(&[Keyword::CHARACTER, Keyword::SET])?;
                properties.push(DataTypeProperty::Charset(
                    parser.consume_plain_identifier_unreserved()?,
                ));
            }
            (
                Token::Ident(_, Keyword::CHARSET),
                Column | FunctionParam | FunctionReturn | TypeRef,
            ) => {
                parser.consume_keyword(Keyword::CHARSET)?;
                properties.push(DataTypeProperty::Charset(
                    parser.consume_plain_identifier_unreserved()?,
                ));
            }
            (
                Token::Ident(_, Keyword::COLLATE),
                Column | FunctionParam | FunctionReturn | TypeRef,
            ) => {
                parser.consume_keyword(Keyword::COLLATE)?;
                properties.push(DataTypeProperty::Collate(
                    parser.consume_plain_identifier_unreserved()?,
                ));
            }

            // ── Column-only properties ──
            (Token::Ident(_, Keyword::NULL), Column) => {
                properties.push(DataTypeProperty::Null(
                    parser.consume_keyword(Keyword::NULL)?,
                ));
            }
            (Token::Ident(_, Keyword::NOT), Column) => {
                let start = parser.consume_keyword(Keyword::NOT)?.start;
                properties.push(DataTypeProperty::NotNull(
                    start..parser.consume_keyword(Keyword::NULL)?.end,
                ));
            }
            (Token::Ident(_, Keyword::COMMENT), Column) => {
                parser.consume_keyword(Keyword::COMMENT)?;
                properties.push(DataTypeProperty::Comment(parser.consume_string()?));
            }
            (Token::Ident(_, Keyword::DEFAULT), Column) => {
                parser.consume_keyword(Keyword::DEFAULT)?;
                properties.push(DataTypeProperty::Default(parse_expression_unreserved(
                    parser,
                    PRIORITY_MAX,
                )?));
            }
            (Token::Ident(_, Keyword::AUTO_INCREMENT), Column) => {
                properties.push(DataTypeProperty::AutoIncrement(
                    parser.consume_keyword(Keyword::AUTO_INCREMENT)?,
                ));
            }
            (Token::Ident(_, Keyword::VIRTUAL), Column) => {
                properties.push(DataTypeProperty::Virtual(
                    parser.consume_keyword(Keyword::VIRTUAL)?,
                ));
            }
            (Token::Ident(_, Keyword::PERSISTENT), Column) => {
                properties.push(DataTypeProperty::Persistent(
                    parser.consume_keyword(Keyword::PERSISTENT)?,
                ));
            }
            (Token::Ident(_, Keyword::STORED), Column) => {
                properties.push(DataTypeProperty::Stored(
                    parser.consume_keyword(Keyword::STORED)?,
                ));
            }
            (Token::Ident(_, Keyword::UNIQUE), Column) => {
                let span = parser.consume_keyword(Keyword::UNIQUE)?;
                if let Some(s2) = parser.skip_keyword(Keyword::KEY) {
                    properties.push(DataTypeProperty::UniqueKey(s2.join_span(&span)));
                } else {
                    properties.push(DataTypeProperty::Unique(span));
                }
            }
            (Token::Ident(_, Keyword::GENERATED), Column) => {
                if parser.options.dialect.is_postgresql() {
                    let generated_span = parser.consume_keyword(Keyword::GENERATED)?;
                    parser.consume_keyword(Keyword::ALWAYS)?;
                    let as_span = parser.consume_keyword(Keyword::AS)?;
                    if matches!(parser.token, Token::LParen) {
                        // GENERATED ALWAYS AS (expr) STORED — computed column
                        let l_paren = parser.consume_token(Token::LParen)?;
                        let e = parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                            Ok(Some(parse_expression_unreserved(parser, PRIORITY_MAX)?))
                        })?;
                        let r_paren = parser.consume_token(Token::RParen)?;
                        let expr = e.unwrap_or_else(|| {
                            Expression::Invalid(Box::new(InvalidExpression {
                                span: l_paren.join_span(&r_paren),
                            }))
                        });
                        let stored_span = parser.skip_keyword(Keyword::STORED);
                        let span = generated_span
                            .join_span(&as_span)
                            .join_span(&expr)
                            .join_span(&stored_span);
                        properties.push(DataTypeProperty::GeneratedAlwaysAsExpr {
                            span,
                            expr,
                            stored_span,
                        });
                    } else {
                        let identity_span = parser.consume_keyword(Keyword::IDENTITY)?;
                        // Parse optional sequence options in parentheses
                        if parser.skip_token(Token::LParen).is_some() {
                            let _ = parse_sequence_options(parser);
                            parser.consume_token(Token::RParen)?;
                        }
                        properties.push(DataTypeProperty::GeneratedAlways(
                            generated_span.join_span(&identity_span),
                        ));
                    }
                } else {
                    properties.push(DataTypeProperty::GeneratedAlways(
                        parser.consume_keywords(&[Keyword::GENERATED, Keyword::ALWAYS])?,
                    ));
                }
            }
            (Token::Ident(_, Keyword::AS), Column) => {
                let span = parser.consume_keyword(Keyword::AS)?;
                let s1 = parser.consume_token(Token::LParen)?;
                let e = parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                    Ok(Some(parse_expression_unreserved(parser, PRIORITY_MAX)?))
                })?;
                let s2 = parser.consume_token(Token::RParen)?;
                let e = e.unwrap_or_else(|| {
                    Expression::Invalid(Box::new(InvalidExpression {
                        span: s1.join_span(&s2),
                    }))
                });
                properties.push(DataTypeProperty::As((span, e)));
            }
            (Token::Ident(_, Keyword::PRIMARY), Column) => {
                properties.push(DataTypeProperty::PrimaryKey(
                    parser.consume_keywords(&[Keyword::PRIMARY, Keyword::KEY])?,
                ));
            }
            (Token::Ident(_, Keyword::CHECK), Column) => {
                let span = parser.consume_keyword(Keyword::CHECK)?;
                let s1 = parser.consume_token(Token::LParen)?;
                let e = parser.recovered(")", &|t| t == &Token::RParen, |parser| {
                    Ok(Some(parse_expression_unreserved(parser, PRIORITY_MAX)?))
                })?;
                let s2 = parser.consume_token(Token::RParen)?;
                let e = e.unwrap_or_else(|| {
                    Expression::Invalid(Box::new(InvalidExpression {
                        span: s1.join_span(&s2),
                    }))
                });
                properties.push(DataTypeProperty::Check((span, e)));
            }
            (Token::Ident(_, Keyword::ON), Column) => {
                let span = parser.consume_keywords(&[Keyword::ON, Keyword::UPDATE])?;
                let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
                properties.push(DataTypeProperty::OnUpdate((span, expr)));
            }
            (Token::Ident(_, Keyword::REFERENCES), Column) => {
                let span = parser.consume_keyword(Keyword::REFERENCES)?;
                let table = parser.consume_plain_identifier_unreserved()?;
                let mut columns = Vec::new();
                if matches!(parser.token, Token::LParen) {
                    parser.consume_token(Token::LParen)?;
                    loop {
                        columns.push(parser.consume_plain_identifier_unreserved()?);
                        if parser.skip_token(Token::Comma).is_none() {
                            break;
                        }
                    }
                    parser.consume_token(Token::RParen)?;
                }
                let match_type = if parser.skip_keyword(Keyword::MATCH).is_some() {
                    match &parser.token {
                        Token::Ident(_, Keyword::FULL) => {
                            Some(ForeignKeyMatch::Full(parser.consume()))
                        }
                        Token::Ident(_, Keyword::SIMPLE) => {
                            Some(ForeignKeyMatch::Simple(parser.consume()))
                        }
                        Token::Ident(_, Keyword::PARTIAL) => {
                            Some(ForeignKeyMatch::Partial(parser.consume()))
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                let mut ons = Vec::new();
                while parser.skip_keyword(Keyword::ON).is_some() {
                    let on_type = match &parser.token {
                        Token::Ident(_, Keyword::UPDATE) => {
                            ForeignKeyOnType::Update(parser.consume_keyword(Keyword::UPDATE)?)
                        }
                        Token::Ident(_, Keyword::DELETE) => {
                            ForeignKeyOnType::Delete(parser.consume_keyword(Keyword::DELETE)?)
                        }
                        _ => parser.expected_failure("UPDATE or DELETE")?,
                    };
                    let on_action = match &parser.token {
                        Token::Ident(_, Keyword::CASCADE) => {
                            ForeignKeyOnAction::Cascade(parser.consume_keyword(Keyword::CASCADE)?)
                        }
                        Token::Ident(_, Keyword::RESTRICT) => {
                            ForeignKeyOnAction::Restrict(parser.consume_keyword(Keyword::RESTRICT)?)
                        }
                        Token::Ident(_, Keyword::SET) => {
                            let set_span = parser.consume_keyword(Keyword::SET)?;
                            if parser.skip_keyword(Keyword::NULL).is_some() {
                                ForeignKeyOnAction::SetNull(set_span)
                            } else if parser.skip_keyword(Keyword::DEFAULT).is_some() {
                                ForeignKeyOnAction::SetDefault(set_span)
                            } else {
                                parser.expected_failure("NULL or DEFAULT after SET")?
                            }
                        }
                        Token::Ident(_, Keyword::NO) => {
                            let no_span = parser.consume_keyword(Keyword::NO)?;
                            parser.consume_keyword(Keyword::ACTION)?;
                            ForeignKeyOnAction::NoAction(no_span)
                        }
                        _ => parser.expected_failure(
                            "CASCADE, RESTRICT, SET NULL, SET DEFAULT, or NO ACTION",
                        )?,
                    };
                    ons.push(ForeignKeyOn {
                        type_: on_type,
                        action: on_action,
                    });
                }
                properties.push(DataTypeProperty::References {
                    span,
                    table,
                    columns,
                    match_type,
                    ons,
                });
            }

            // End of properties (or property not valid in this context)
            _ => break,
        }
    }
    Ok(DataType {
        identifier,
        type_,
        properties,
    })
}
