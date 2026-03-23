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
    Expression, Span, Spanned,
    expression::{parse_expression, parse_expression_outer},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    select::OrderFlag,
};
use alloc::{boxed::Box, vec::Vec};

/// Function to execute
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Function<'a> {
    Abs,
    Acos,
    AddDate,
    AddMonths,
    AddTime,
    Ascii,
    Asin,
    Atan,
    Atan2,
    Bin,
    BitLength,
    Ceil,
    CharacterLength,
    Chr,
    Concat,
    ConcatWs,
    Conv,
    ConvertTz,
    Cos,
    Cot,
    Crc32,
    Crc32c,
    CurDate,
    CurrentTimestamp,
    CurTime,
    Date,
    DateDiff,
    DateFormat,
    DateSub,
    Datetime,
    DayName,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    Degrees,
    Elt,
    Exists,
    Exp,
    ExportSet,
    ExtractValue,
    Field,
    FindInSet,
    Floor,
    Format,
    FromBase64,
    FromDays,
    FromUnixTime,
    Greatest,
    Hex,
    Hour,
    If,
    IfNull,
    Insert,
    InStr,
    JsonArray,
    JsonArrayAgg,
    JsonArrayAppend,
    JsonArrayInsert,
    JsonArrayIntersect,
    JsonCompact,
    JsonContains,
    JsonContainsPath,
    JsonDepth,
    JsonDetailed,
    JsonEquals,
    JsonExists,
    JsonExtract,
    JsonInsert,
    JsonKeys,
    JsonLength,
    JsonLoose,
    JsonMerge,
    JsonMergePath,
    JsonMergePerserve,
    JsonNormalize,
    JsonObject,
    JsonObjectAgg,
    JsonObjectFilterKeys,
    JsonObjectToArray,
    JsonOverlaps,
    JsonPretty,
    JsonQuery,
    JsonQuote,
    JsonRemove,
    JsonReplace,
    JsonSchemaValid,
    JsonSearch,
    JsonSet,
    JsonTable,
    JsonType,
    JsonUnquote,
    JsonValid,
    JsonValue,
    Lag,
    LastDay,
    LCase,
    Lead,
    Least,
    Left,
    Length,
    LengthB,
    Ln,
    LoadFile,
    Locate,
    Log,
    Log10,
    Log2,
    Lower,
    LPad,
    LTrim,
    MakeDate,
    MakeSet,
    MakeTime,
    Max,
    MicroSecond,
    Mid,
    Min,
    Minute,
    Month,
    MonthName,
    NaturalSortkey,
    Now,
    NullIf,
    NVL2,
    Oct,
    OctetLength,
    Ord,
    PeriodAdd,
    PeriodDiff,
    Pi,
    Position,
    Pow,
    Quarter,
    Quote,
    Radians,
    Rand,
    Repeat,
    Replace,
    Reverse,
    Right,
    Round,
    RPad,
    RTrim,
    Second,
    SecToTime,
    SFormat,
    Sign,
    Sin,
    Sleep,
    SoundEx,
    Space,
    Sqrt,
    StartsWith,
    StrCmp,
    Strftime,
    StrToDate,
    SubStr,
    SubStringIndex,
    SubTime,
    Sum,
    SysDate,
    Tan,
    Time,
    TimeDiff,
    TimeFormat,
    Timestamp,
    TimeToSec,
    ToBase64,
    ToChar,
    ToDays,
    ToSeconds,
    Truncate,
    UCase,
    UncompressedLength,
    UnHex,
    UnixTimestamp,
    Unknown,
    UpdateXml,
    Upper,
    UtcDate,
    UtcTime,
    UtcTimeStamp,
    Value,
    Week,
    Weekday,
    WeekOfYear,
    Year,
    YearWeek,
    Other(&'a str),
}

/// Function call expression,
#[derive(Debug, Clone)]
pub struct FunctionCallExpression<'a> {
    pub function: Function<'a>,
    pub args: Vec<Expression<'a>>,
    pub function_span: Span,
}

impl Spanned for FunctionCallExpression<'_> {
    fn span(&self) -> Span {
        self.function_span.join_span(&self.args)
    }
}

/// When part of CASE
#[derive(Debug, Clone)]
pub struct WindowSpec<'a> {
    /// Span of "ORDER BY" and list of order expression and directions, if specified
    pub order_by: (Span, Vec<(Expression<'a>, OrderFlag)>),
}

impl<'a> Spanned for WindowSpec<'a> {
    fn span(&self) -> Span {
        self.order_by.span()
    }
}

/// A window function call expression
#[derive(Debug, Clone)]
pub struct WindowFunctionCallExpression<'a> {
    pub function: Function<'a>,
    pub args: Vec<Expression<'a>>,
    pub function_span: Span,
    pub over_span: Span,
    pub window_spec: WindowSpec<'a>,
}

impl Spanned for WindowFunctionCallExpression<'_> {
    fn span(&self) -> Span {
        self.function_span
            .join_span(&self.args)
            .join_span(&self.over_span)
            .join_span(&self.window_spec)
    }
}

pub(crate) fn parse_function<'a>(
    parser: &mut Parser<'a, '_>,
    t: Token<'a>,
    span: Span,
) -> Result<Expression<'a>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let func = match &t {
        // https://mariadb.com/kb/en/string-functions/
        Token::Ident(_, Keyword::ASCII) => Function::Ascii,
        Token::Ident(_, Keyword::BIN) => Function::Bin,
        Token::Ident(_, Keyword::BIT_LENGTH) => Function::BitLength,
        Token::Ident(_, Keyword::CHAR_LENGTH) => Function::CharacterLength,
        Token::Ident(_, Keyword::CHARACTER_LENGTH) => Function::CharacterLength,
        Token::Ident(_, Keyword::CHR) => Function::Chr,
        Token::Ident(_, Keyword::CONCAT) => Function::Concat,
        Token::Ident(_, Keyword::CONCAT_WS) => Function::ConcatWs,
        Token::Ident(_, Keyword::ELT) => Function::Elt,
        Token::Ident(_, Keyword::EXPORT_SET) => Function::ExportSet,
        Token::Ident(_, Keyword::EXTRACTVALUE) => Function::ExtractValue,
        Token::Ident(_, Keyword::FIELD) => Function::Field,
        Token::Ident(_, Keyword::FIND_IN_SET) => Function::FindInSet,
        Token::Ident(_, Keyword::FORMAT) => Function::Format,
        Token::Ident(_, Keyword::FROM_BASE64) => Function::FromBase64,
        Token::Ident(_, Keyword::HEX) => Function::Hex,
        Token::Ident(_, Keyword::INSERT) => Function::Insert,
        Token::Ident(_, Keyword::INSTR) => Function::InStr,
        Token::Ident(_, Keyword::LCASE) => Function::LCase,
        Token::Ident(_, Keyword::LEFT) => Function::Left,
        Token::Ident(_, Keyword::LENGTH) => Function::Length,
        Token::Ident(_, Keyword::LENGTHB) => Function::LengthB,
        Token::Ident(_, Keyword::LOAD_FILE) => Function::LoadFile,
        Token::Ident(_, Keyword::LOCATE) => Function::Locate,
        Token::Ident(_, Keyword::LOWER) => Function::Lower,
        Token::Ident(_, Keyword::LPAD) => Function::LPad,
        Token::Ident(_, Keyword::LTRIM) => Function::LTrim,
        Token::Ident(_, Keyword::MAKE_SET) => Function::MakeSet,
        Token::Ident(_, Keyword::MID) => Function::Mid,
        Token::Ident(_, Keyword::NATURAL_SORT_KEY) => Function::NaturalSortkey,
        Token::Ident(_, Keyword::OCTET_LENGTH) => Function::OctetLength,
        Token::Ident(_, Keyword::ORD) => Function::Ord,
        Token::Ident(_, Keyword::POSITION) => Function::Position,
        Token::Ident(_, Keyword::QUOTE) => Function::Quote,
        Token::Ident(_, Keyword::REPEAT) => Function::Repeat,
        Token::Ident(_, Keyword::REPLACE) => Function::Replace,
        Token::Ident(_, Keyword::REVERSE) => Function::Reverse,
        Token::Ident(_, Keyword::RIGHT) => Function::Right,
        Token::Ident(_, Keyword::RPAD) => Function::RPad,
        Token::Ident(_, Keyword::RTRIM) => Function::RTrim,
        Token::Ident(_, Keyword::SOUNDEX) => Function::SoundEx,
        Token::Ident(_, Keyword::SLEEP) => Function::Sleep,
        Token::Ident(_, Keyword::SPACE) => Function::Space,
        Token::Ident(_, Keyword::STRCMP) => Function::StrCmp,
        Token::Ident(_, Keyword::SUBSTR) => Function::SubStr,
        Token::Ident(_, Keyword::SUBSTRING) => Function::SubStr,
        Token::Ident(_, Keyword::SUBSTRING_INDEX) => Function::SubStringIndex,
        Token::Ident(_, Keyword::TO_BASE64) => Function::ToBase64,
        Token::Ident(_, Keyword::TO_CHAR) => Function::ToChar,
        Token::Ident(_, Keyword::UCASE) => Function::UCase,
        Token::Ident(_, Keyword::UNCOMPRESSED_LENGTH) => Function::UncompressedLength,
        Token::Ident(_, Keyword::UNHEX) => Function::UnHex,
        Token::Ident(_, Keyword::UPDATEXML) => Function::UpdateXml,
        Token::Ident(_, Keyword::UPPER) => Function::Upper,
        Token::Ident(_, Keyword::SFORMAT) => Function::SFormat,

        // TODO uncat
        Token::Ident(_, Keyword::EXISTS) => Function::Exists,
        Token::Ident(_, Keyword::MIN) => Function::Min,
        Token::Ident(_, Keyword::MAX) => Function::Max,
        Token::Ident(_, Keyword::SUM) => Function::Sum,
        Token::Ident(_, Keyword::VALUE) => Function::Value,
        Token::Ident(_, Keyword::VALUES) => Function::Value,
        Token::Ident(_, Keyword::LEAD) => Function::Lead,
        Token::Ident(_, Keyword::LAG) => Function::Lag,
        Token::Ident(_, Keyword::STARTS_WITH) => Function::StartsWith,

        //https://mariadb.com/kb/en/control-flow-functions/
        Token::Ident(_, Keyword::IFNULL) => Function::IfNull,
        Token::Ident(_, Keyword::NULLIF) => Function::NullIf,
        Token::Ident(_, Keyword::NVL) => Function::IfNull,
        Token::Ident(_, Keyword::NVL2) => Function::NVL2,
        Token::Ident(_, Keyword::IF) => Function::If,

        //https://mariadb.com/kb/en/numeric-functions/
        Token::Ident(_, Keyword::ABS) => Function::Abs,
        Token::Ident(_, Keyword::ACOS) => Function::Acos,
        Token::Ident(_, Keyword::ASIN) => Function::Asin,
        Token::Ident(_, Keyword::ATAN) => Function::Atan,
        Token::Ident(_, Keyword::ATAN2) => Function::Atan2,
        Token::Ident(_, Keyword::CEIL | Keyword::CEILING) => Function::Ceil,
        Token::Ident(_, Keyword::CONV) => Function::Conv,
        Token::Ident(_, Keyword::COS) => Function::Cos,
        Token::Ident(_, Keyword::COT) => Function::Cot,
        Token::Ident(_, Keyword::CRC32) => Function::Crc32,
        Token::Ident(_, Keyword::DEGREES) => Function::Degrees,
        Token::Ident(_, Keyword::EXP) => Function::Exp,
        Token::Ident(_, Keyword::FLOOR) => Function::Floor,
        Token::Ident(_, Keyword::GREATEST) => Function::Greatest,
        Token::Ident(_, Keyword::LN) => Function::Ln,
        Token::Ident(_, Keyword::LOG) => Function::Log,
        Token::Ident(_, Keyword::LOG10) => Function::Log10,
        Token::Ident(_, Keyword::LOG2) => Function::Log2,
        Token::Ident(_, Keyword::OCT) => Function::Oct,
        Token::Ident(_, Keyword::PI) => Function::Pi,
        Token::Ident(_, Keyword::POW | Keyword::POWER) => Function::Pow,
        Token::Ident(_, Keyword::RADIANS) => Function::Radians,
        Token::Ident(_, Keyword::RAND) => Function::Rand,
        Token::Ident(_, Keyword::ROUND) => Function::Round,
        Token::Ident(_, Keyword::SIGN) => Function::Sign,
        Token::Ident(_, Keyword::SIN) => Function::Sin,
        Token::Ident(_, Keyword::SQRT) => Function::Sqrt,
        Token::Ident(_, Keyword::TAN) => Function::Tan,
        Token::Ident(_, Keyword::TRUNCATE) => Function::Truncate,
        Token::Ident(_, Keyword::CRC32C) => Function::Crc32c,
        Token::Ident(_, Keyword::LEAST) => Function::Least,

        // https://mariadb.com/kb/en/date-time-functions/
        Token::Ident(_, Keyword::ADDDATE) => Function::AddDate,
        Token::Ident(_, Keyword::ADDTIME) => Function::AddTime,
        Token::Ident(_, Keyword::CONVERT_TZ) => Function::ConvertTz,
        Token::Ident(_, Keyword::CURDATE) => Function::CurDate,
        Token::Ident(_, Keyword::CURRENT_DATE) => Function::CurDate,
        Token::Ident(_, Keyword::CURRENT_TIME) => Function::CurTime,
        Token::Ident(_, Keyword::CURTIME) => Function::CurTime,
        Token::Ident(_, Keyword::DATE) => Function::Date,
        Token::Ident(_, Keyword::HOUR) => Function::Hour,
        Token::Ident(_, Keyword::DATEDIFF) => Function::DateDiff,
        Token::Ident(_, Keyword::DATE_ADD) => Function::AddDate,
        Token::Ident(_, Keyword::DATE_FORMAT) => Function::DateFormat,
        Token::Ident(_, Keyword::DATE_SUB) => Function::DateSub,
        Token::Ident(_, Keyword::DAY | Keyword::DAYOFMONTH) => Function::DayOfMonth,
        Token::Ident(_, Keyword::DAYNAME) => Function::DayName,
        Token::Ident(_, Keyword::DAYOFWEEK) => Function::DayOfWeek,
        Token::Ident(_, Keyword::DAYOFYEAR) => Function::DayOfYear,
        Token::Ident(_, Keyword::FROM_DAYS) => Function::FromDays,
        Token::Ident(_, Keyword::CURRENT_TIMESTAMP) => Function::CurrentTimestamp,
        Token::Ident(_, Keyword::LOCALTIME | Keyword::LOCALTIMESTAMP | Keyword::NOW) => {
            Function::Now
        }
        Token::Ident(_, Keyword::MAKEDATE) => Function::MakeDate,
        Token::Ident(_, Keyword::MAKETIME) => Function::MakeTime,
        Token::Ident(_, Keyword::MICROSECOND) => Function::MicroSecond,
        Token::Ident(_, Keyword::MINUTE) => Function::Minute,
        Token::Ident(_, Keyword::MONTH) => Function::Month,
        Token::Ident(_, Keyword::MONTHNAME) => Function::MonthName,
        Token::Ident(_, Keyword::PERIOD_ADD) => Function::PeriodAdd,
        Token::Ident(_, Keyword::PERIOD_DIFF) => Function::PeriodDiff,
        Token::Ident(_, Keyword::QUARTER) => Function::Quarter,
        Token::Ident(_, Keyword::SECOND) => Function::Second,
        Token::Ident(_, Keyword::SEC_TO_TIME) => Function::SecToTime,
        Token::Ident(_, Keyword::STR_TO_DATE) => Function::StrToDate,
        Token::Ident(_, Keyword::SUBDATE) => Function::DateSub,
        Token::Ident(_, Keyword::SUBTIME) => Function::SubTime,
        Token::Ident(_, Keyword::TIME) => Function::Time,
        Token::Ident(_, Keyword::LAST_DAY) => Function::LastDay,
        Token::Ident(_, Keyword::TIMEDIFF) => Function::TimeDiff,
        Token::Ident(_, Keyword::TIMESTAMP) => Function::Timestamp,
        Token::Ident(_, Keyword::TIME_FORMAT) => Function::TimeFormat,
        Token::Ident(_, Keyword::TIME_TO_SEC) => Function::TimeToSec,
        Token::Ident(_, Keyword::TO_DAYS) => Function::ToDays,
        Token::Ident(_, Keyword::TO_SECONDS) => Function::ToSeconds,
        Token::Ident(_, Keyword::UNIX_TIMESTAMP) => Function::UnixTimestamp,
        Token::Ident(_, Keyword::UTC_DATE) => Function::UtcDate,
        Token::Ident(_, Keyword::UTC_TIME) => Function::UtcTime,
        Token::Ident(_, Keyword::UTC_TIMESTAMP) => Function::UtcTimeStamp,
        Token::Ident(_, Keyword::WEEK) => Function::Week,
        Token::Ident(_, Keyword::WEEKDAY) => Function::Weekday,
        Token::Ident(_, Keyword::WEEKOFYEAR) => Function::WeekOfYear,
        Token::Ident(_, Keyword::ADD_MONTHS) => Function::AddMonths,
        Token::Ident(_, Keyword::FROM_UNIXTIME) => Function::FromUnixTime,
        Token::Ident(_, Keyword::YEAR) => Function::Year,
        Token::Ident(_, Keyword::YEARWEEK) => Function::YearWeek,
        Token::Ident(_, Keyword::SYSDATE) => Function::SysDate,

        // https://mariadb.com/kb/en/json-functions/
        Token::Ident(_, Keyword::JSON_ARRAY) => Function::JsonArray,
        Token::Ident(_, Keyword::JSON_ARRAYAGG) => Function::JsonArrayAgg,
        Token::Ident(_, Keyword::JSON_ARRAY_APPEND) => Function::JsonArrayAppend,
        Token::Ident(_, Keyword::JSON_ARRAY_INSERT) => Function::JsonArrayInsert,
        Token::Ident(_, Keyword::JSON_ARRAY_INTERSECT) => Function::JsonArrayIntersect,
        Token::Ident(_, Keyword::JSON_COMPACT) => Function::JsonCompact,
        Token::Ident(_, Keyword::JSON_CONTAINS) => Function::JsonContains,
        Token::Ident(_, Keyword::JSON_CONTAINS_PATH) => Function::JsonContainsPath,
        Token::Ident(_, Keyword::JSON_DEPTH) => Function::JsonDepth,
        Token::Ident(_, Keyword::JSON_DETAILED) => Function::JsonDetailed,
        Token::Ident(_, Keyword::JSON_EQUALS) => Function::JsonEquals,
        Token::Ident(_, Keyword::JSON_EXISTS) => Function::JsonExists,
        Token::Ident(_, Keyword::JSON_EXTRACT) => Function::JsonExtract,
        Token::Ident(_, Keyword::JSON_INSERT) => Function::JsonInsert,
        Token::Ident(_, Keyword::JSON_KEYS) => Function::JsonKeys,
        Token::Ident(_, Keyword::JSON_LENGTH) => Function::JsonLength,
        Token::Ident(_, Keyword::JSON_LOOSE) => Function::JsonLoose,
        Token::Ident(_, Keyword::JSON_MERGE) => Function::JsonMerge,
        Token::Ident(_, Keyword::JSON_MERGE_PATCH) => Function::JsonMergePath,
        Token::Ident(_, Keyword::JSON_MERGE_PRESERVE) => Function::JsonMergePerserve,
        Token::Ident(_, Keyword::JSON_NORMALIZE) => Function::JsonNormalize,
        Token::Ident(_, Keyword::JSON_OBJECT) => Function::JsonObject,
        Token::Ident(_, Keyword::JSON_OBJECT_FILTER_KEYS) => Function::JsonObjectFilterKeys,
        Token::Ident(_, Keyword::JSON_OBJECT_TO_ARRAY) => Function::JsonObjectToArray,
        Token::Ident(_, Keyword::JSON_OBJECTAGG) => Function::JsonObjectAgg,
        Token::Ident(_, Keyword::JSON_OVERLAPS) => Function::JsonOverlaps,
        Token::Ident(_, Keyword::JSON_PRETTY) => Function::JsonPretty,
        Token::Ident(_, Keyword::JSON_QUERY) => Function::JsonQuery,
        Token::Ident(_, Keyword::JSON_QUOTE) => Function::JsonQuote,
        Token::Ident(_, Keyword::JSON_REMOVE) => Function::JsonRemove,
        Token::Ident(_, Keyword::JSON_REPLACE) => Function::JsonReplace,
        Token::Ident(_, Keyword::JSON_SCHEMA_VALID) => Function::JsonSchemaValid,
        Token::Ident(_, Keyword::JSON_SEARCH) => Function::JsonSearch,
        Token::Ident(_, Keyword::JSON_SET) => Function::JsonSet,
        Token::Ident(_, Keyword::JSON_TABLE) => Function::JsonTable,
        Token::Ident(_, Keyword::JSON_TYPE) => Function::JsonType,
        Token::Ident(_, Keyword::JSON_UNQUOTE) => Function::JsonUnquote,
        Token::Ident(_, Keyword::JSON_VALID) => Function::JsonValid,
        Token::Ident(_, Keyword::JSON_VALUE) => Function::JsonValue,

        // Sqlite
        Token::Ident(_, Keyword::STRFTIME) => Function::Strftime,
        Token::Ident(_, Keyword::DATETIME) => Function::Datetime,
        Token::Ident(v, k) if !k.reserved() => Function::Other(v),
        _ => {
            parser.err("Unknown function", &span);
            Function::Unknown
        }
    };

    let mut args = Vec::new();
    if !matches!(parser.token, Token::RParen) {
        loop {
            parser.recovered(
                "')' or ','",
                &|t| matches!(t, Token::RParen | Token::Comma),
                |parser| {
                    args.push(parse_expression_outer(parser)?);
                    Ok(())
                },
            )?;
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
    }
    parser.consume_token(Token::RParen)?;

    if let Some(over_span) = parser.skip_keyword(Keyword::OVER) {
        parser.consume_token(Token::LParen)?;
        let order_span = parser.consume_keywords(&[Keyword::ORDER, Keyword::BY])?;
        let mut order = Vec::new();
        loop {
            let e = parse_expression(parser, false)?;
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
        parser.consume_token(Token::RParen)?;
        Ok(Expression::WindowFunction(Box::new(
            WindowFunctionCallExpression {
                function: func,
                args,
                function_span: span,
                over_span,
                window_spec: WindowSpec {
                    order_by: (order_span, order),
                },
            },
        )))
    } else {
        Ok(Expression::Function(Box::new(FunctionCallExpression {
            function: func,
            args,
            function_span: span,
        })))
    }
}

#[cfg(test)]
mod tests {
    use core::ops::Deref;

    use alloc::string::{String, ToString};

    use crate::{
        Function, FunctionCallExpression, ParseOptions, SQLDialect, expression::Expression,
        issue::Issues, parser::Parser,
    };

    use super::parse_expression;

    fn test_expr(src: &'static str, f: impl FnOnce(&Expression<'_>) -> Result<(), String>) {
        let mut issues = Issues::new(src);
        let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
        let mut parser = Parser::new(src, &mut issues, &options);
        let res = parse_expression(&mut parser, false).expect("Expression in test expr");
        if let Err(e) = f(&res) {
            panic!("Error parsing {}: {}\nGot {:#?}", src, e, res);
        }
    }

    #[test]
    fn mariadb_datetime_functions() {
        fn test_func(src: &'static str, f: Function, cnt: usize) {
            let mut issues = Issues::new(src);
            let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
            let mut parser = Parser::new(src, &mut issues, &options);
            let res = match parse_expression(&mut parser, false) {
                Ok(res) => res,
                Err(e) => panic!("Unable to parse {}: {:?}", src, e),
            };
            let Expression::Function(r) = res else {
                panic!("Should be parsed as function {}", src);
            };
            let FunctionCallExpression {
                function: pf, args, ..
            } = r.deref();
            assert_eq!(pf, &f, "Failure en expr {}", src);
            assert_eq!(args.len(), cnt, "Failure en expr {}", src);
        }
        test_func("ADD_MONTHS('2012-01-31', 2)", Function::AddMonths, 2);
        test_func(
            "ADDTIME('2007-12-31 23:59:59.999999', '1 1:1:1.000002')",
            Function::AddTime,
            2,
        );
        test_func(
            "DATE_ADD('2008-01-02', INTERVAL 31 DAY)",
            Function::AddDate,
            2,
        );
        test_func(
            "ADDDATE('2008-01-02', INTERVAL 31 DAY)",
            Function::AddDate,
            2,
        );
        test_func("ADDDATE('2008-01-02', 31)", Function::AddDate, 2);
        test_func(
            "CONVERT_TZ('2016-01-01 12:00:00','+00:00','+10:00')",
            Function::ConvertTz,
            3,
        );
        test_func("CURDATE()", Function::CurDate, 0);
        test_func("CURRENT_DATE", Function::CurDate, 0);
        test_func("CURRENT_DATE()", Function::CurDate, 0);
        test_func("CURRENT_TIME", Function::CurTime, 0);
        test_func("CURRENT_TIME()", Function::CurTime, 0);
        test_func("CURTIME()", Function::CurTime, 0);
        test_func("CURTIME(2)", Function::CurTime, 1);
        test_func("CURRENT_DATE", Function::CurDate, 0);
        test_func("CURRENT_DATE()", Function::CurDate, 0);
        test_func("CURDATE()", Function::CurDate, 0);
        test_func("CURRENT_TIMESTAMP", Function::CurrentTimestamp, 0);
        test_func("CURRENT_TIMESTAMP()", Function::CurrentTimestamp, 0);
        test_func("CURRENT_TIMESTAMP(10)", Function::CurrentTimestamp, 1);
        test_func("LOCALTIME", Function::Now, 0);
        test_func("LOCALTIME()", Function::Now, 0);
        test_func("LOCALTIME(10)", Function::Now, 1);
        test_func("LOCALTIMESTAMP", Function::Now, 0);
        test_func("LOCALTIMESTAMP()", Function::Now, 0);
        test_func("LOCALTIMESTAMP(10)", Function::Now, 1);
        test_func("DATE('2013-07-18 12:21:32')", Function::Date, 1);
        test_func(
            "DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')",
            Function::DateFormat,
            2,
        );
        test_func(
            "DATE_SUB('1998-01-02', INTERVAL 31 DAY)",
            Function::DateSub,
            2,
        );
        test_func("DAY('2007-02-03')", Function::DayOfMonth, 1);
        test_func("DAYOFMONTH('2007-02-03')", Function::DayOfMonth, 1);
        test_func(
            "DATEDIFF('2007-12-31 23:59:59','2007-12-30')",
            Function::DateDiff,
            2,
        );
        test_func("DAYNAME('2007-02-03')", Function::DayName, 1);
        test_func("DAYOFYEAR('2018-02-16')", Function::DayOfYear, 1);
        test_func("DAYOFWEEK('2007-02-03')", Function::DayOfWeek, 1);
        test_expr("EXTRACT(YEAR_MONTH FROM '2009-07-02 01:02:03')", |e| {
            let Expression::Extract { .. } = e else {
                return Err("Wrong type".to_string());
            };
            Ok(())
        });
        //test_func("FORMAT_PICO_TIME(4321123443212345) AS h", Function::DayOfWeek, 1);
        test_func("FROM_DAYS(730669)", Function::FromDays, 1);
        test_func("FROM_UNIXTIME(1196440219)", Function::FromUnixTime, 1);
        test_func(
            "FROM_UNIXTIME(UNIX_TIMESTAMP(), '%Y %D %M %h:%i:%s %x')",
            Function::FromUnixTime,
            2,
        );
        //test_func("GET_FORMAT(DATE, 'EUR')", Function::GetFormat, 2);
        test_func("HOUR('10:05:03')", Function::Hour, 1);
        test_func("LAST_DAY('2004-01-01 01:01:01')", Function::LastDay, 1);
        test_func("MAKEDATE(2011,31)", Function::MakeDate, 2);
        test_func("MAKETIME(-13,57,33)", Function::MakeTime, 3);
        test_func("MICROSECOND('12:00:00.123456')", Function::MicroSecond, 1);
        test_func("MINUTE('2013-08-03 11:04:03')", Function::Minute, 1);
        test_func("MONTH('2019-01-03')", Function::Month, 1);
        test_func("MONTHNAME('2019-02-03')", Function::MonthName, 1);
        test_func("PERIOD_ADD(200801,2)", Function::PeriodAdd, 2);
        test_func("PERIOD_DIFF(200802,200703)", Function::PeriodDiff, 2);
        test_func("QUARTER('2008-04-01')", Function::Quarter, 1);
        test_func("SEC_TO_TIME(12414)", Function::SecToTime, 1);
        test_func("SECOND('10:05:03')", Function::Second, 1);
        test_func(
            "STR_TO_DATE('Wednesday, June 2, 2014', '%W, %M %e, %Y')",
            Function::StrToDate,
            2,
        );
        test_func(
            "DATE_SUB('2008-01-02', INTERVAL 31 DAY)",
            Function::DateSub,
            2,
        );
        test_func("SUBDATE('2008-01-02 12:00:00', 31)", Function::DateSub, 2);
        test_func(
            "SUBDATE('2008-01-02', INTERVAL 31 DAY)",
            Function::DateSub,
            2,
        );
        test_func(
            "SUBTIME('2007-12-31 23:59:59.999999','1 1:1:1.000002')",
            Function::SubTime,
            2,
        );
        test_func("SYSDATE()", Function::SysDate, 0);
        test_func("SYSDATE(4)", Function::SysDate, 1);
        test_func("TIME('2003-12-31 01:02:03')", Function::Time, 1);
        test_func(
            "TIME_FORMAT('100:00:00', '%H %k %h %I %l')",
            Function::TimeFormat,
            2,
        );
        test_func("TIME_TO_SEC('22:23:00')", Function::TimeToSec, 1);
        test_func(
            "TIMEDIFF('2008-12-31 23:59:59.000001', '2008-12-30 01:01:01.000002')",
            Function::TimeDiff,
            2,
        );
        test_func("TIMESTAMP('2003-12-31')", Function::Timestamp, 1);
        test_func(
            "TIMESTAMP('2003-12-31 12:00:00','6:30:00')",
            Function::Timestamp,
            2,
        );
        test_expr("TIMESTAMPADD(MINUTE,1,'2003-01-02')", |e| {
            let Expression::TimestampAdd { .. } = e else {
                return Err("Wrong type".to_string());
            };
            Ok(())
        });
        test_expr("TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');", |e| {
            let Expression::TimestampDiff { .. } = e else {
                return Err("Wrong type".to_string());
            };
            Ok(())
        });
        test_func("TO_DAYS('2007-10-07')", Function::ToDays, 1);
        test_func("UNIX_TIMESTAMP()", Function::UnixTimestamp, 0);
        test_func(
            "UNIX_TIMESTAMP('2007-11-30 10:30:19')",
            Function::UnixTimestamp,
            1,
        );
        test_func("UTC_DATE", Function::UtcDate, 0);
        test_func("UTC_DATE()", Function::UtcDate, 0);
        test_func("UTC_TIME", Function::UtcTime, 0);
        test_func("UTC_TIME()", Function::UtcTime, 0);
        test_func("UTC_TIME(5)", Function::UtcTime, 1);
        test_func("UTC_TIMESTAMP", Function::UtcTimeStamp, 0);
        test_func("UTC_TIMESTAMP()", Function::UtcTimeStamp, 0);
        test_func("UTC_TIMESTAMP(4)", Function::UtcTimeStamp, 1);
        test_func("WEEK('2008-02-20')", Function::Week, 1);
        test_func("WEEK('2008-02-20',0)", Function::Week, 2);
        test_func("WEEKDAY('2008-02-03 22:23:00')", Function::Weekday, 1);
        test_func("WEEKOFYEAR('2008-02-20')", Function::WeekOfYear, 1);
        test_func("YEAR('1987-01-01')", Function::Year, 1);
        test_func("YEARWEEK('1987-01-01')", Function::YearWeek, 1);
        test_func("YEARWEEK('1987-01-01',0)", Function::YearWeek, 2);
    }
}
