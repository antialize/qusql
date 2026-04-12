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
    Expression, Identifier, OptSpanned, Span, Spanned,
    expression::{PRIORITY_MAX, parse_expression_outer, parse_expression_unreserved},
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
    Acosd,
    Acosh,
    AddDate,
    AddMonths,
    AddTime,
    Area,
    Abbrev,
    Ascii,
    Btrim,
    Asin,
    Asind,
    Asinh,
    Atan,
    Atan2,
    Atan2d,
    Atand,
    Atanh,
    Bin,
    BitLength,
    Broadcast,
    Casefold,
    Cbrt,
    Ceil,
    Center,
    Char,
    CharacterLength,
    Chr,
    Concat,
    ConcatWs,
    Conv,
    ConvertTz,
    Cos,
    Cosd,
    Cosh,
    Cot,
    Cotd,
    Crc32,
    Crc32c,
    CurrentCatalog,
    CurrentRole,
    CurrentUser,
    CurDate,
    CurrentTimestamp,
    CurTime,
    SessionUser,
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
    Diagonal,
    Diameter,
    Elt,
    Exists,
    Erf,
    Erfc,
    Exp,
    EnumFirst,
    EnumLast,
    EnumRange,
    ExportSet,
    ExtractValue,
    Family,
    Field,
    Factorial,
    FindInSet,
    Floor,
    Format,
    FromBase64,
    FromDays,
    FromUnixTime,
    Gamma,
    Gcd,
    Greatest,
    Hex,
    Height,
    Host,
    Hour,
    If,
    IfNull,
    Initcap,
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
    Avg,
    Count,
    LCase,
    Lead,
    Lcm,
    Least,
    Left,
    Length,
    LengthB,
    Lgamma,
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
    MinScale,
    Mod,
    Month,
    MonthName,
    Normalize,
    NaturalSortkey,
    Now,
    NullIf,
    NVL2,
    Npoints,
    Oct,
    OctetLength,
    Ord,
    ParseIdent,
    Pclose,
    PeriodAdd,
    PeriodDiff,
    PgClientEncoding,
    Pi,
    Popen,
    Position,
    Pow,
    Quarter,
    Quote,
    QuoteIdent,
    QuoteLiteral,
    QuoteNullable,
    Radians,
    Radius,
    Rand,
    RandomNormal,
    Repeat,
    Replace,
    Reverse,
    Right,
    Round,
    RPad,
    RTrim,
    Scale,
    Second,
    SecToTime,
    SFormat,
    SetBit,
    SetSeed,
    Sign,
    Sin,
    Sind,
    Sinh,
    Sleep,
    Slope,
    SoundEx,
    Space,
    SplitPart,
    Sqrt,
    StartsWith,
    StrCmp,
    Strpos,
    Strftime,
    StrToDate,
    SubStr,
    SubStringIndex,
    SubTime,
    Sum,
    SysDate,
    Tan,
    Tand,
    Tanh,
    Time,
    TimeDiff,
    TimeFormat,
    Timestamp,
    TimeToSec,
    ToAscii,
    ToBase64,
    ToBin,
    ToChar,
    ToDate,
    ToDays,
    ToHex,
    ToNumber,
    ToOct,
    ToSeconds,
    ToTimestamp,
    Translate,
    TrimScale,
    Truncate,
    UCase,
    UncompressedLength,
    UnHex,
    UnixTimestamp,
    Unknown,
    UpdateXml,
    Upper,
    UnicodeAssigned,
    Unistr,
    UtcDate,
    UtcTime,
    UtcTimeStamp,
    Value,
    Week,
    Weekday,
    WeekOfYear,
    Width,
    WidthBucket,
    Year,
    YearWeek,
    // MySQL 8.4 explicit functions
    AesDecrypt,
    AesEncrypt,
    AnyValue,
    Benchmark,
    BinToUuid,
    BitCount,
    Charset,
    Coercibility,
    Collation,
    Compress,
    ConnectionId,
    DatabaseFunc,
    FirstValue,
    FormatBytes,
    FormatPicoTime,
    FoundRows,
    GetBit,
    GetFormat,
    GetLock,
    Grouping,
    IcuVersion,
    Inet6Aton,
    Inet6Ntoa,
    InetAton,
    InetNtoa,
    IsFreeLock,
    IsIPv4,
    IsIPv4Compat,
    IsIPv4Mapped,
    IsIPv6,
    InetMerge,
    InetSameFamily,
    Macaddr8Set7bit,
    MaskLen,
    NetMask,
    Network,
    SetMaskLen,
    IsUsedLock,
    IsUuid,
    LastInsertId,
    LastValue,
    Md5,
    NameConst,
    NthValue,
    Ntile,
    PsCurrentThreadId,
    PsThreadId,
    RandomBytes,
    RegexpCount,
    RegexpInstr,
    RegexpLike,
    RegexpMatch,
    RegexpMatches,
    RegexpReplace,
    RegexpSplitToArray,
    RegexpSplitToTable,
    RegexpSubstr,
    ReleaseAllLocks,
    ReleaseLock,
    RolesGraphml,
    RowCount,
    RowNumber,
    SchemaFunc,
    SessionUserFunc,
    Sha,
    Sha1,
    Sha2,
    StatementDigest,
    StatementDigestText,
    SystemUser,
    Uncompress,
    UserFunc,
    Uuid,
    UuidShort,
    UuidToBin,
    ValidatePasswordStrength,
    Version,
    WeightString,
    // PostgreSQL system functions
    InetServerAddr,
    InetServerPort,
    HostMask,
    PgPostmasterStartTime,
    PostgisFullVersion,
    ArrayAgg,
    BitAnd,
    BitOr,
    BitXor,
    BoolAnd,
    BoolOr,
    Corr,
    CovarPop,
    CovarSamp,
    CumeDist,
    DenseRank,
    JsonAgg,
    JsonbAgg,
    JsonbObjectAgg,
    JsonbSet,
    JsonBuildObject,
    PercentRank,
    PercentileCont,
    PercentileDisc,
    Rank,
    RegrAvgx,
    RegrAvgy,
    RegrCount,
    RegrIntercept,
    RegrR2,
    RegrSlope,
    RegrSxx,
    RegrSxy,
    RegrSyy,
    Mode,
    Std,
    Stddev,
    StddevPop,
    StddevSamp,
    StringAgg,
    StringToArray,
    StringToTable,
    Variance,
    VarPop,
    VarSamp,
    Xmlagg,
    Coalesce,
    // PostgreSQL geometric functions (non-PostGIS)
    BoundBox,
    Isclosed,
    IsOpen,
    // PostGIS / geometry functions
    Box2D,
    Box3D,
    GeometryType,
    StAddMeasure,
    StAddPoint,
    StAffine,
    StArea,
    StAsBinary,
    StAsEwkb,
    StAsEwkt,
    StAsGeoJson,
    StAsGml,
    StAsHexEwkb,
    StAsKml,
    StAsSvg,
    StAsText,
    StAzimuth,
    StBoundary,
    StBuffer,
    StBuildArea,
    StCentroid,
    StClosestPoint,
    StCollect,
    StCollectionExtract,
    StContains,
    StContainsProperly,
    StConvexHull,
    StCoordDim,
    StCoveredBy,
    StCovers,
    StCrosses,
    StCurveToLine,
    StDFullyWithin,
    StDifference,
    StDimension,
    StDisjoint,
    StDistance,
    StDistanceSphere,
    StDistanceSpheroidal,
    StDWithin,
    StEndPoint,
    StEnvelope,
    StEquals,
    StExteriorRing,
    StForce2D,
    StForce3D,
    StForce3DM,
    StForce3DZ,
    StForce4D,
    StForceCollection,
    StForceRHR,
    StGeoHash,
    StGeomCollFromText,
    StGeomFromEwkb,
    StGeomFromEwkt,
    StGeomFromGeoJson,
    StGeomFromGml,
    StGeomFromKml,
    StGeomFromText,
    StGeomFromWkb,
    StGeometryFromText,
    StGeometryN,
    StGeometryType,
    StGmlToSQL,
    StHasArc,
    StHausdorffDistance,
    StInteriorRingN,
    StIntersection,
    StIntersects,
    StIsClosed,
    StIsEmpty,
    StIsRing,
    StIsSimple,
    StIsValid,
    StIsValidReason,
    StLength,
    StLength2D,
    StLength3D,
    StLineCrossingDirection,
    StLineFromMultiPoint,
    StLineFromText,
    StLineFromWkb,
    StLineMerge,
    StLinestringFromWkb,
    StLineToCurve,
    StLineInterpolatePoint,
    StLineLocatePoint,
    StLineSubstring,
    StLongestLine,
    StM,
    StMakeEnvelope,
    StMakeLine,
    StMakePoint,
    StMakePointM,
    StMakePolygon,
    StMaxDistance,
    StMemSize,
    StMinimumBoundingCircle,
    StMulti,
    StNDims,
    StNPoints,
    StNRings,
    StNumGeometries,
    StNumInteriorRing,
    StNumInteriorRings,
    StNumPoints,
    StOrderingEquals,
    StOverlaps,
    StPerimeter,
    StPerimeter2D,
    StPerimeter3D,
    StPoint,
    StPointFromText,
    StPointFromWkb,
    StPointN,
    StPointOnSurface,
    StPointInsideCircle,
    StPolygon,
    StPolygonFromText,
    StPolygonize,
    StRelate,
    StRemovePoint,
    StReverse,
    StRotate,
    StRotateX,
    StRotateY,
    StRotateZ,
    StScale,
    StSegmentize,
    StSetPoint,
    StSetSrid,
    StShiftLongitude,
    StShortestLine,
    StSimplify,
    StSimplifyPreserveTopology,
    StSnapToGrid,
    StSRID,
    StStartPoint,
    StSummary,
    StSymDifference,
    StTouches,
    StTransform,
    StTranslate,
    StTransScale,
    StUnion,
    StWithin,
    StWkbToSQL,
    StWktToSQL,
    StX,
    StXMax,
    StXMin,
    StY,
    StYMax,
    StYMin,
    StZ,
    StZMax,
    StZMin,
    StZmflag,
    // PostgreSQL UUID functions
    GenRandomUuid,
    UuidExtractTimestamp,
    UuidExtractVersion,
    Uuidv4,
    Uuidv7,
    // PostgreSQL XML functions
    CursorToXml,
    CursorToXmlschema,
    DatabaseToXml,
    DatabaseToXmlAndXmlschema,
    DatabaseToXmlschema,
    QueryToXml,
    QueryToXmlAndXmlschema,
    QueryToXmlschema,
    SchemaToXml,
    SchemaToXmlAndXmlschema,
    SchemaToXmlschema,
    TableToXml,
    TableToXmlAndXmlschema,
    TableToXmlschema,
    XmlComment,
    XmlConcat,
    XmlIsWellFormed,
    XmlIsWellFormedContent,
    XmlIsWellFormedDocument,
    XmlText,
    Xpath,
    XpathExists,
    // PostgreSQL JSON functions
    ArrayToJson,
    JsonArrayElements,
    JsonArrayElementsText,
    JsonArrayLength,
    JsonBuildArray,
    JsonEach,
    JsonEachText,
    JsonExtractPath,
    JsonExtractPathText,
    JsonObjectKeys,
    JsonPopulateRecord,
    JsonPopulateRecordset,
    JsonScalar,
    JsonSerialize,
    JsonStripNulls,
    JsonToRecord,
    JsonToRecordset,
    JsonTypeof,
    JsonbArrayElements,
    JsonbArrayElementsText,
    JsonbArrayLength,
    JsonbBuildArray,
    JsonbBuildObject,
    JsonbEach,
    JsonbEachText,
    JsonbExtractPath,
    JsonbExtractPathText,
    JsonbInsert,
    JsonbObject,
    JsonbObjectKeys,
    JsonbPathExists,
    JsonbPathExistsTz,
    JsonbPathMatch,
    JsonbPathMatchTz,
    JsonbPathQuery,
    JsonbPathQueryArray,
    JsonbPathQueryArrayTz,
    JsonbPathQueryFirst,
    JsonbPathQueryFirstTz,
    JsonbPathQueryTz,
    JsonbPopulateRecord,
    JsonbPopulateRecordValid,
    JsonbPopulateRecordset,
    JsonbPretty,
    JsonbSetLax,
    JsonbStripNulls,
    JsonbToRecord,
    JsonbToRecordset,
    JsonbTypeof,
    RowToJson,
    ToJson,
    ToJsonb,
    // PostgreSQL sequence functions
    Currval,
    Lastval,
    Nextval,
    Setval,
    // PostgreSQL text search functions
    ArrayToTsvector,
    GetCurrentTsConfig,
    JsonToTsvector,
    JsonbToTsvector,
    Numnode,
    PhraseToTsquery,
    PlainToTsquery,
    Querytree,
    Setweight,
    Strip,
    ToTsquery,
    ToTsvector,
    TsDebug,
    TsDelete,
    TsFilter,
    TsHeadline,
    TsLexize,
    TsParse,
    TsRank,
    TsRankCd,
    TsRewrite,
    TsStat,
    TsTokenType,
    TsqueryPhrase,
    TsvectorToArray,
    Unnest,
    WebsearchToTsquery,
    Other(Vec<Identifier<'a>>),
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

/// CHAR(N,... [USING charset_name]) expression
#[derive(Debug, Clone)]
pub struct CharFunctionExpression<'a> {
    /// Span of "CHAR"
    pub char_span: Span,
    /// Arguments to CHAR()
    pub args: Vec<Expression<'a>>,
    /// Optional USING charset_name clause
    pub using_charset: Option<(Span, Identifier<'a>)>,
}

impl<'a> Spanned for CharFunctionExpression<'a> {
    fn span(&self) -> Span {
        self.char_span
            .join_span(&self.args)
            .join_span(&self.using_charset)
    }
}

/// Window frame mode: ROWS, RANGE, or GROUPS
#[derive(Debug, Clone)]
pub enum WindowFrameMode {
    Rows(Span),
    Range(Span),
    Groups(Span),
}

impl Spanned for WindowFrameMode {
    fn span(&self) -> Span {
        match self {
            WindowFrameMode::Rows(s) | WindowFrameMode::Range(s) | WindowFrameMode::Groups(s) => {
                s.clone()
            }
        }
    }
}

/// One bound in a window frame clause
#[derive(Debug, Clone)]
pub enum WindowFrameBound<'a> {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding(Span),
    /// <expr> PRECEDING
    Preceding(Expression<'a>, Span),
    /// CURRENT ROW
    CurrentRow(Span),
    /// <expr> FOLLOWING
    Following(Expression<'a>, Span),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing(Span),
}

impl<'a> Spanned for WindowFrameBound<'a> {
    fn span(&self) -> Span {
        match self {
            WindowFrameBound::UnboundedPreceding(s) => s.clone(),
            WindowFrameBound::Preceding(e, s) => e.span().join_span(s),
            WindowFrameBound::CurrentRow(s) => s.clone(),
            WindowFrameBound::Following(e, s) => e.span().join_span(s),
            WindowFrameBound::UnboundedFollowing(s) => s.clone(),
        }
    }
}

/// Window frame clause: ROWS/RANGE/GROUPS { frame_start | BETWEEN frame_start AND frame_end }
#[derive(Debug, Clone)]
pub struct WindowFrame<'a> {
    /// ROWS, RANGE, or GROUPS
    pub mode: WindowFrameMode,
    /// The start bound (or sole bound when no BETWEEN)
    pub start: WindowFrameBound<'a>,
    /// When BETWEEN was used: span covering "BETWEEN ... AND" and the end bound
    pub between: Option<(Span, WindowFrameBound<'a>)>,
}

impl<'a> Spanned for WindowFrame<'a> {
    fn span(&self) -> Span {
        let s = self.mode.span().join_span(&self.start);
        if let Some((and_span, end)) = &self.between {
            s.join_span(and_span).join_span(end)
        } else {
            s
        }
    }
}

/// When part of CASE
#[derive(Debug, Clone)]
pub struct WindowSpec<'a> {
    /// Span of "PARTITION BY" and list of partition expressions, if specified
    pub partition_by: Option<(Span, Vec<Expression<'a>>)>,
    /// Span of "ORDER BY" and list of order expression and directions, if specified
    pub order_by: Option<(Span, Vec<(Expression<'a>, OrderFlag)>)>,
    /// Window frame clause (ROWS/RANGE BETWEEN ... AND ...), if specified
    pub frame: Option<WindowFrame<'a>>,
}

impl<'a> Spanned for WindowSpec<'a> {
    fn span(&self) -> Span {
        self.partition_by
            .opt_join_span(&self.order_by)
            .opt_join_span(&self.frame)
            .expect("Either partition_by, order_by, or frame must be specified")
    }
}

#[derive(Debug, Clone)]
pub struct WindowClause<'a> {
    pub over_span: Span,
    pub window_spec: WindowSpec<'a>,
}

impl Spanned for WindowClause<'_> {
    fn span(&self) -> Span {
        self.over_span.join_span(&self.window_spec)
    }
}

/// A window function call expression
#[derive(Debug, Clone)]
pub struct WindowFunctionCallExpression<'a> {
    pub function: Function<'a>,
    pub args: Vec<Expression<'a>>,
    pub function_span: Span,
    pub over: WindowClause<'a>,
}

impl Spanned for WindowFunctionCallExpression<'_> {
    fn span(&self) -> Span {
        self.function_span
            .join_span(&self.args)
            .join_span(&self.over)
    }
}

#[derive(Debug, Clone)]
pub struct AggregateFunctionCallExpression<'a> {
    pub function: Function<'a>,
    pub args: Vec<Expression<'a>>,
    pub function_span: Span,
    pub distinct_span: Option<Span>,
    pub within_group: Option<(Span, Vec<(Expression<'a>, OrderFlag)>)>,
    pub filter: Option<(Span, Expression<'a>)>,
    pub over: Option<WindowClause<'a>>,
}

impl Spanned for AggregateFunctionCallExpression<'_> {
    fn span(&self) -> Span {
        self.function_span
            .join_span(&self.args)
            .join_span(&self.distinct_span)
            .join_span(&self.within_group)
            .join_span(&self.filter)
            .join_span(&self.over)
    }
}

pub(crate) fn is_aggregate_function_ident(keyword: &Keyword) -> bool {
    matches!(
        keyword,
        Keyword::COUNT
            | Keyword::AVG
            | Keyword::SUM
            | Keyword::MIN
            | Keyword::MAX
            | Keyword::JSON_ARRAYAGG
            | Keyword::JSON_OBJECTAGG
            | Keyword::ARRAY_AGG
            | Keyword::BIT_AND
            | Keyword::BIT_OR
            | Keyword::BIT_XOR
            | Keyword::BOOL_AND
            | Keyword::BOOL_OR
            | Keyword::CORR
            | Keyword::COVAR_POP
            | Keyword::COVAR_SAMP
            | Keyword::CUME_DIST
            | Keyword::DENSE_RANK
            | Keyword::EVERY
            | Keyword::JSON_AGG
            | Keyword::JSONB_AGG
            | Keyword::JSONB_OBJECT_AGG
            | Keyword::PERCENT_RANK
            | Keyword::PERCENTILE_CONT
            | Keyword::PERCENTILE_DISC
            | Keyword::RANK
            | Keyword::REGR_AVGX
            | Keyword::REGR_AVGY
            | Keyword::REGR_COUNT
            | Keyword::REGR_INTERCEPT
            | Keyword::REGR_R2
            | Keyword::REGR_SLOPE
            | Keyword::REGR_SXX
            | Keyword::REGR_SXY
            | Keyword::REGR_SYY
            | Keyword::STD
            | Keyword::STDDEV
            | Keyword::STDDEV_POP
            | Keyword::STDDEV_SAMP
            | Keyword::STRING_AGG
            | Keyword::VARIANCE
            | Keyword::VAR_POP
            | Keyword::VAR_SAMP
            | Keyword::XMLAGG
            | Keyword::MODE
    )
}

fn parse_window_frame_bound<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<WindowFrameBound<'a>, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::UNBOUNDED) => {
            let kw_span = parser.consume_keyword(Keyword::UNBOUNDED)?;
            if let Some(span) = parser.skip_keyword(Keyword::PRECEDING) {
                Ok(WindowFrameBound::UnboundedPreceding(
                    kw_span.join_span(&span),
                ))
            } else {
                Ok(WindowFrameBound::UnboundedFollowing(
                    parser
                        .consume_keyword(Keyword::FOLLOWING)?
                        .join_span(&kw_span),
                ))
            }
        }
        Token::Ident(_, Keyword::CURRENT) => {
            let current_row_span = parser.consume_keywords(&[Keyword::CURRENT, Keyword::ROW])?;
            Ok(WindowFrameBound::CurrentRow(current_row_span))
        }
        _ => {
            let expr = parse_expression_unreserved(parser, PRIORITY_MAX)?;
            if let Some(s) = parser.skip_keyword(Keyword::PRECEDING) {
                Ok(WindowFrameBound::Preceding(expr, s))
            } else {
                let s = parser.consume_keyword(Keyword::FOLLOWING)?;
                Ok(WindowFrameBound::Following(expr, s))
            }
        }
    }
}

fn parse_window_frame<'a>(
    parser: &mut Parser<'a, '_>,
    mode: WindowFrameMode,
) -> Result<WindowFrame<'a>, ParseError> {
    if let Some(between_span) = parser.skip_keyword(Keyword::BETWEEN) {
        let start = parse_window_frame_bound(parser)?;
        let and_span = parser.consume_keyword(Keyword::AND)?;
        let end = parse_window_frame_bound(parser)?;
        Ok(WindowFrame {
            mode,
            start,
            between: Some((between_span.join_span(&and_span), end)),
        })
    } else {
        let start = parse_window_frame_bound(parser)?;
        Ok(WindowFrame {
            mode,
            start,
            between: None,
        })
    }
}

fn parse_over_clause<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<Option<WindowClause<'a>>, ParseError> {
    let Some(over_span) = parser.skip_keyword(Keyword::OVER) else {
        return Ok(None);
    };

    parser.consume_token(Token::LParen)?;

    let partition_by = if let Some(partition_span) = parser.skip_keyword(Keyword::PARTITION) {
        let partition_by_span = partition_span.join_span(&parser.consume_keyword(Keyword::BY)?);
        let mut partition_exprs = Vec::new();
        loop {
            partition_exprs.push(parse_expression_unreserved(parser, PRIORITY_MAX)?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        Some((partition_by_span, partition_exprs))
    } else {
        None
    };

    let order_by = if let Some(span) = parser.skip_keyword(Keyword::ORDER) {
        let order_span = span.join_span(&parser.consume_keyword(Keyword::BY)?);
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
        Some((order_span, order))
    } else {
        None
    };

    // Window frame clause: ROWS/RANGE { frame_start | BETWEEN frame_start AND frame_end }
    let frame = if let Some(s) = parser.skip_keyword(Keyword::ROWS) {
        Some(parse_window_frame(parser, WindowFrameMode::Rows(s))?)
    } else if let Some(s) = parser.skip_keyword(Keyword::RANGE) {
        Some(parse_window_frame(parser, WindowFrameMode::Range(s))?)
    } else {
        None
    };

    parser.consume_token(Token::RParen)?;

    Ok(Some(WindowClause {
        over_span,
        window_spec: WindowSpec {
            partition_by,
            order_by,
            frame,
        },
    }))
}

pub(crate) fn parse_aggregate_function<'a>(
    parser: &mut Parser<'a, '_>,
    t: Token<'a>,
    span: Span,
) -> Result<Expression<'a>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let func = match &t {
        Token::Ident(_, Keyword::COUNT) => Function::Count,
        Token::Ident(_, Keyword::AVG) => Function::Avg,
        Token::Ident(_, Keyword::SUM) => Function::Sum,
        Token::Ident(_, Keyword::MIN) => Function::Min,
        Token::Ident(_, Keyword::MAX) => Function::Max,
        Token::Ident(_, Keyword::JSON_ARRAYAGG) => Function::JsonArrayAgg,
        Token::Ident(_, Keyword::JSON_OBJECTAGG) => Function::JsonObjectAgg,
        Token::Ident(_, Keyword::ARRAY_AGG) => Function::ArrayAgg,
        Token::Ident(_, Keyword::BIT_AND) => Function::BitAnd,
        Token::Ident(_, Keyword::BIT_OR) => Function::BitOr,
        Token::Ident(_, Keyword::BIT_XOR) => Function::BitXor,
        Token::Ident(_, Keyword::BOOL_AND) => Function::BoolAnd,
        Token::Ident(_, Keyword::BOOL_OR) => Function::BoolOr,
        Token::Ident(_, Keyword::CORR) => Function::Corr,
        Token::Ident(_, Keyword::COVAR_POP) => Function::CovarPop,
        Token::Ident(_, Keyword::COVAR_SAMP) => Function::CovarSamp,
        Token::Ident(_, Keyword::CUME_DIST) => Function::CumeDist,
        Token::Ident(_, Keyword::DENSE_RANK) => Function::DenseRank,
        Token::Ident(_, Keyword::EVERY) => Function::BoolAnd,
        Token::Ident(_, Keyword::JSON_AGG) => Function::JsonAgg,
        Token::Ident(_, Keyword::JSONB_AGG) => Function::JsonbAgg,
        Token::Ident(_, Keyword::JSONB_OBJECT_AGG) => Function::JsonbObjectAgg,
        Token::Ident(_, Keyword::JSONB_SET) => Function::JsonbSet,
        Token::Ident(_, Keyword::PERCENT_RANK) => Function::PercentRank,
        Token::Ident(_, Keyword::PERCENTILE_CONT) => Function::PercentileCont,
        Token::Ident(_, Keyword::PERCENTILE_DISC) => Function::PercentileDisc,
        Token::Ident(_, Keyword::RANK) => Function::Rank,
        Token::Ident(_, Keyword::REGR_AVGX) => Function::RegrAvgx,
        Token::Ident(_, Keyword::REGR_AVGY) => Function::RegrAvgy,
        Token::Ident(_, Keyword::REGR_COUNT) => Function::RegrCount,
        Token::Ident(_, Keyword::REGR_INTERCEPT) => Function::RegrIntercept,
        Token::Ident(_, Keyword::REGR_R2) => Function::RegrR2,
        Token::Ident(_, Keyword::REGR_SLOPE) => Function::RegrSlope,
        Token::Ident(_, Keyword::REGR_SXX) => Function::RegrSxx,
        Token::Ident(_, Keyword::REGR_SXY) => Function::RegrSxy,
        Token::Ident(_, Keyword::REGR_SYY) => Function::RegrSyy,
        Token::Ident(_, Keyword::STD) => Function::Std,
        Token::Ident(_, Keyword::STDDEV) => Function::Stddev,
        Token::Ident(_, Keyword::STDDEV_POP) => Function::StddevPop,
        Token::Ident(_, Keyword::STDDEV_SAMP) => Function::StddevSamp,
        Token::Ident(_, Keyword::STRING_AGG) => Function::StringAgg,
        Token::Ident(_, Keyword::VARIANCE) => Function::Variance,
        Token::Ident(_, Keyword::VAR_POP) => Function::VarPop,
        Token::Ident(_, Keyword::VAR_SAMP) => Function::VarSamp,
        Token::Ident(_, Keyword::XMLAGG) => Function::Xmlagg,
        Token::Ident(_, Keyword::MODE) => Function::Mode,
        _ => {
            parser.err("Unknown aggregate function", &span);
            Function::Unknown
        }
    };

    let distinct_span = parser.skip_keyword(Keyword::DISTINCT);
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

    let within_group = if let Some(within_span) = parser.skip_keyword(Keyword::WITHIN) {
        let within_group_span = within_span.join_span(&parser.consume_keyword(Keyword::GROUP)?);
        parser.consume_token(Token::LParen)?;
        let order_span = parser.consume_keyword(Keyword::ORDER)?;
        let order_by_span = order_span.join_span(&parser.consume_keyword(Keyword::BY)?);
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
        parser.consume_token(Token::RParen)?;
        Some((within_group_span.join_span(&order_by_span), order))
    } else {
        None
    };

    let filter = if let Some(filter_span) = parser.skip_keyword(Keyword::FILTER) {
        parser.postgres_only(&filter_span);
        parser.consume_token(Token::LParen)?;
        parser.consume_keyword(Keyword::WHERE)?;
        let condition = parse_expression_unreserved(parser, PRIORITY_MAX)?;
        parser.consume_token(Token::RParen)?;
        Some((filter_span, condition))
    } else {
        None
    };

    let over = parse_over_clause(parser)?;

    Ok(Expression::AggregateFunction(Box::new(
        AggregateFunctionCallExpression {
            function: func,
            args,
            function_span: span,
            distinct_span,
            within_group,
            filter,
            over,
        },
    )))
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
        Token::Ident(_, Keyword::TO_DATE) => Function::ToDate,
        Token::Ident(_, Keyword::TO_NUMBER) => Function::ToNumber,
        Token::Ident(_, Keyword::TO_TIMESTAMP) => Function::ToTimestamp,
        Token::Ident(_, Keyword::UCASE) => Function::UCase,
        Token::Ident(_, Keyword::UNCOMPRESSED_LENGTH) => Function::UncompressedLength,
        Token::Ident(_, Keyword::UNHEX) => Function::UnHex,
        Token::Ident(_, Keyword::UPDATEXML) => Function::UpdateXml,
        Token::Ident(_, Keyword::UPPER) => Function::Upper,
        Token::Ident(_, Keyword::SFORMAT) => Function::SFormat,

        // PostgreSQL string functions
        Token::Ident(_, Keyword::BTRIM) => Function::Btrim,
        Token::Ident(_, Keyword::CASEFOLD) => Function::Casefold,
        Token::Ident(_, Keyword::INITCAP) => Function::Initcap,
        Token::Ident(_, Keyword::NORMALIZE) => Function::Normalize,
        Token::Ident(_, Keyword::PARSE_IDENT) => Function::ParseIdent,
        Token::Ident(_, Keyword::PG_CLIENT_ENCODING) => Function::PgClientEncoding,
        Token::Ident(_, Keyword::QUOTE_IDENT) => Function::QuoteIdent,
        Token::Ident(_, Keyword::QUOTE_LITERAL) => Function::QuoteLiteral,
        Token::Ident(_, Keyword::QUOTE_NULLABLE) => Function::QuoteNullable,
        Token::Ident(_, Keyword::SPLIT_PART) => Function::SplitPart,
        Token::Ident(_, Keyword::STRING_TO_ARRAY) => Function::StringToArray,
        Token::Ident(_, Keyword::STRING_TO_TABLE) => Function::StringToTable,
        Token::Ident(_, Keyword::STRPOS) => Function::Strpos,
        Token::Ident(_, Keyword::TO_ASCII) => Function::ToAscii,
        Token::Ident(_, Keyword::TO_BIN) => Function::ToBin,
        Token::Ident(_, Keyword::TO_HEX) => Function::ToHex,
        Token::Ident(_, Keyword::TO_OCT) => Function::ToOct,
        Token::Ident(_, Keyword::TRANSLATE) => Function::Translate,
        Token::Ident(_, Keyword::UNICODE_ASSIGNED) => Function::UnicodeAssigned,
        Token::Ident(_, Keyword::UNISTR) => Function::Unistr,

        // TODO uncat
        Token::Ident(_, Keyword::EXISTS) => Function::Exists,
        Token::Ident(_, Keyword::COUNT) => Function::Count,
        Token::Ident(_, Keyword::AVG) => Function::Avg,
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
        Token::Ident(_, Keyword::COALESCE) => Function::Coalesce,

        //https://mariadb.com/kb/en/numeric-functions/
        Token::Ident(_, Keyword::ABS) => Function::Abs,
        Token::Ident(_, Keyword::ACOS) => Function::Acos,
        Token::Ident(_, Keyword::ACOSD) => Function::Acosd,
        Token::Ident(_, Keyword::ACOSH) => Function::Acosh,
        Token::Ident(_, Keyword::ASIN) => Function::Asin,
        Token::Ident(_, Keyword::ASIND) => Function::Asind,
        Token::Ident(_, Keyword::ASINH) => Function::Asinh,
        Token::Ident(_, Keyword::ATAN) => Function::Atan,
        Token::Ident(_, Keyword::ATAN2) => Function::Atan2,
        Token::Ident(_, Keyword::ATAN2D) => Function::Atan2d,
        Token::Ident(_, Keyword::ATAND) => Function::Atand,
        Token::Ident(_, Keyword::ATANH) => Function::Atanh,
        Token::Ident(_, Keyword::CBRT) => Function::Cbrt,
        Token::Ident(_, Keyword::CEIL | Keyword::CEILING) => Function::Ceil,
        Token::Ident(_, Keyword::CONV) => Function::Conv,
        Token::Ident(_, Keyword::COS) => Function::Cos,
        Token::Ident(_, Keyword::COSD) => Function::Cosd,
        Token::Ident(_, Keyword::COSH) => Function::Cosh,
        Token::Ident(_, Keyword::COT) => Function::Cot,
        Token::Ident(_, Keyword::COTD) => Function::Cotd,
        Token::Ident(_, Keyword::CRC32) => Function::Crc32,
        Token::Ident(_, Keyword::DEGREES) => Function::Degrees,
        Token::Ident(_, Keyword::ERF) => Function::Erf,
        Token::Ident(_, Keyword::ERFC) => Function::Erfc,
        Token::Ident(_, Keyword::EXP) => Function::Exp,
        Token::Ident(_, Keyword::FACTORIAL) => Function::Factorial,
        Token::Ident(_, Keyword::FLOOR) => Function::Floor,
        Token::Ident(_, Keyword::GAMMA) => Function::Gamma,
        Token::Ident(_, Keyword::GCD) => Function::Gcd,
        Token::Ident(_, Keyword::GREATEST) => Function::Greatest,
        Token::Ident(_, Keyword::LCM) => Function::Lcm,
        Token::Ident(_, Keyword::LGAMMA) => Function::Lgamma,
        Token::Ident(_, Keyword::LN) => Function::Ln,
        Token::Ident(_, Keyword::LOG) => Function::Log,
        Token::Ident(_, Keyword::LOG10) => Function::Log10,
        Token::Ident(_, Keyword::LOG2) => Function::Log2,
        Token::Ident(_, Keyword::MIN_SCALE) => Function::MinScale,
        Token::Ident(_, Keyword::MOD) => Function::Mod,
        Token::Ident(_, Keyword::OCT) => Function::Oct,
        Token::Ident(_, Keyword::PI) => Function::Pi,
        Token::Ident(_, Keyword::POW | Keyword::POWER) => Function::Pow,
        Token::Ident(_, Keyword::RADIANS) => Function::Radians,
        Token::Ident(_, Keyword::RAND) => Function::Rand,
        Token::Ident(_, Keyword::RANDOM_NORMAL) => Function::RandomNormal,
        Token::Ident(_, Keyword::ROUND) => Function::Round,
        Token::Ident(_, Keyword::SCALE) => Function::Scale,
        Token::Ident(_, Keyword::SET_BIT) => Function::SetBit,
        Token::Ident(_, Keyword::SETSEED) => Function::SetSeed,
        Token::Ident(_, Keyword::SIGN) => Function::Sign,
        Token::Ident(_, Keyword::SIN) => Function::Sin,
        Token::Ident(_, Keyword::SIND) => Function::Sind,
        Token::Ident(_, Keyword::SINH) => Function::Sinh,
        Token::Ident(_, Keyword::SQRT) => Function::Sqrt,
        Token::Ident(_, Keyword::TAN) => Function::Tan,
        Token::Ident(_, Keyword::TAND) => Function::Tand,
        Token::Ident(_, Keyword::TANH) => Function::Tanh,
        Token::Ident(_, Keyword::TRIM_SCALE) => Function::TrimScale,
        Token::Ident(_, Keyword::TRUNCATE) => Function::Truncate,
        Token::Ident(_, Keyword::WIDTH_BUCKET) => Function::WidthBucket,
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

        // MySQL 8.4 encryption / compression
        Token::Ident(_, Keyword::AES_DECRYPT) => Function::AesDecrypt,
        Token::Ident(_, Keyword::AES_ENCRYPT) => Function::AesEncrypt,
        Token::Ident(_, Keyword::COMPRESS) => Function::Compress,
        Token::Ident(_, Keyword::MD5) => Function::Md5,
        Token::Ident(_, Keyword::RANDOM_BYTES) => Function::RandomBytes,
        Token::Ident(_, Keyword::SHA) => Function::Sha,
        Token::Ident(_, Keyword::SHA1) => Function::Sha1,
        Token::Ident(_, Keyword::SHA2) => Function::Sha2,
        Token::Ident(_, Keyword::STATEMENT_DIGEST) => Function::StatementDigest,
        Token::Ident(_, Keyword::STATEMENT_DIGEST_TEXT) => Function::StatementDigestText,
        Token::Ident(_, Keyword::UNCOMPRESS) => Function::Uncompress,
        Token::Ident(_, Keyword::VALIDATE_PASSWORD_STRENGTH) => Function::ValidatePasswordStrength,

        // MySQL 8.4 locking
        Token::Ident(_, Keyword::GET_LOCK) => Function::GetLock,
        Token::Ident(_, Keyword::IS_FREE_LOCK) => Function::IsFreeLock,
        Token::Ident(_, Keyword::IS_USED_LOCK) => Function::IsUsedLock,
        Token::Ident(_, Keyword::RELEASE_ALL_LOCKS) => Function::ReleaseAllLocks,
        Token::Ident(_, Keyword::RELEASE_LOCK) => Function::ReleaseLock,

        // MySQL 8.4 information
        Token::Ident(_, Keyword::BENCHMARK) => Function::Benchmark,
        Token::Ident(_, Keyword::CHARSET) => Function::Charset,
        Token::Ident(_, Keyword::COERCIBILITY) => Function::Coercibility,
        Token::Ident(_, Keyword::COLLATION) => Function::Collation,
        Token::Ident(_, Keyword::CONNECTION_ID) => Function::ConnectionId,
        Token::Ident(_, Keyword::CURRENT_ROLE) => Function::CurrentRole,
        Token::Ident(_, Keyword::CURRENT_USER) => Function::CurrentUser,
        Token::Ident(_, Keyword::DATABASE) => Function::DatabaseFunc,
        Token::Ident(_, Keyword::FOUND_ROWS) => Function::FoundRows,
        Token::Ident(_, Keyword::ICU_VERSION) => Function::IcuVersion,
        Token::Ident(_, Keyword::LAST_INSERT_ID) => Function::LastInsertId,
        Token::Ident(_, Keyword::ROLES_GRAPHML) => Function::RolesGraphml,
        Token::Ident(_, Keyword::ROW_COUNT) => Function::RowCount,
        Token::Ident(_, Keyword::SCHEMA) => Function::SchemaFunc,
        Token::Ident(_, Keyword::SESSION_USER) => Function::SessionUserFunc,
        Token::Ident(_, Keyword::SYSTEM_USER) => Function::SystemUser,
        Token::Ident(_, Keyword::USER) => Function::UserFunc,
        Token::Ident(_, Keyword::VERSION) => Function::Version,
        // PostgreSQL system functions
        Token::Ident(_, Keyword::INET_SERVER_ADDR) => Function::InetServerAddr,
        Token::Ident(_, Keyword::INET_SERVER_PORT) => Function::InetServerPort,
        Token::Ident(_, Keyword::JSON_BUILD_OBJECT) => Function::JsonBuildObject,
        Token::Ident(_, Keyword::JSONB_SET) => Function::JsonbSet,
        Token::Ident(_, Keyword::PG_POSTMASTER_START_TIME) => Function::PgPostmasterStartTime,
        Token::Ident(_, Keyword::POSTGIS_FULL_VERSION) => Function::PostgisFullVersion,

        // PostgreSQL network address functions
        Token::Ident(_, Keyword::ABBREV) => Function::Abbrev,
        Token::Ident(_, Keyword::BROADCAST) => Function::Broadcast,
        Token::Ident(_, Keyword::FAMILY) => Function::Family,
        Token::Ident(_, Keyword::HOST) => Function::Host,
        Token::Ident(_, Keyword::HOSTMASK) => Function::HostMask,
        Token::Ident(_, Keyword::INET_MERGE) => Function::InetMerge,
        Token::Ident(_, Keyword::INET_SAME_FAMILY) => Function::InetSameFamily,
        Token::Ident(_, Keyword::MACADDR8_SET7BIT) => Function::Macaddr8Set7bit,
        Token::Ident(_, Keyword::MASKLEN) => Function::MaskLen,
        Token::Ident(_, Keyword::NETMASK) => Function::NetMask,
        Token::Ident(_, Keyword::NETWORK) => Function::Network,
        Token::Ident(_, Keyword::SET_MASKLEN) => Function::SetMaskLen,

        // PostgreSQL UUID functions
        Token::Ident(_, Keyword::GEN_RANDOM_UUID) => Function::GenRandomUuid,
        Token::Ident(_, Keyword::UUID_EXTRACT_TIMESTAMP) => Function::UuidExtractTimestamp,
        Token::Ident(_, Keyword::UUID_EXTRACT_VERSION) => Function::UuidExtractVersion,
        Token::Ident(_, Keyword::UUIDV4) => Function::Uuidv4,
        Token::Ident(_, Keyword::UUIDV7) => Function::Uuidv7,

        // PostgreSQL XML functions
        Token::Ident(_, Keyword::CURSOR_TO_XML) => Function::CursorToXml,
        Token::Ident(_, Keyword::CURSOR_TO_XMLSCHEMA) => Function::CursorToXmlschema,
        Token::Ident(_, Keyword::DATABASE_TO_XML) => Function::DatabaseToXml,
        Token::Ident(_, Keyword::DATABASE_TO_XML_AND_XMLSCHEMA) => {
            Function::DatabaseToXmlAndXmlschema
        }
        Token::Ident(_, Keyword::DATABASE_TO_XMLSCHEMA) => Function::DatabaseToXmlschema,
        Token::Ident(_, Keyword::QUERY_TO_XML) => Function::QueryToXml,
        Token::Ident(_, Keyword::QUERY_TO_XML_AND_XMLSCHEMA) => Function::QueryToXmlAndXmlschema,
        Token::Ident(_, Keyword::QUERY_TO_XMLSCHEMA) => Function::QueryToXmlschema,
        Token::Ident(_, Keyword::SCHEMA_TO_XML) => Function::SchemaToXml,
        Token::Ident(_, Keyword::SCHEMA_TO_XML_AND_XMLSCHEMA) => Function::SchemaToXmlAndXmlschema,
        Token::Ident(_, Keyword::SCHEMA_TO_XMLSCHEMA) => Function::SchemaToXmlschema,
        Token::Ident(_, Keyword::TABLE_TO_XML) => Function::TableToXml,
        Token::Ident(_, Keyword::TABLE_TO_XML_AND_XMLSCHEMA) => Function::TableToXmlAndXmlschema,
        Token::Ident(_, Keyword::TABLE_TO_XMLSCHEMA) => Function::TableToXmlschema,
        Token::Ident(_, Keyword::XML_IS_WELL_FORMED) => Function::XmlIsWellFormed,
        Token::Ident(_, Keyword::XML_IS_WELL_FORMED_CONTENT) => Function::XmlIsWellFormedContent,
        Token::Ident(_, Keyword::XML_IS_WELL_FORMED_DOCUMENT) => Function::XmlIsWellFormedDocument,
        Token::Ident(_, Keyword::XMLCOMMENT) => Function::XmlComment,
        Token::Ident(_, Keyword::XMLCONCAT) => Function::XmlConcat,
        Token::Ident(_, Keyword::XMLTEXT) => Function::XmlText,
        Token::Ident(_, Keyword::XPATH) => Function::Xpath,
        Token::Ident(_, Keyword::XPATH_EXISTS) => Function::XpathExists,

        // PostgreSQL JSON functions
        Token::Ident(_, Keyword::ARRAY_TO_JSON) => Function::ArrayToJson,
        Token::Ident(_, Keyword::JSON_ARRAY_ELEMENTS) => Function::JsonArrayElements,
        Token::Ident(_, Keyword::JSON_ARRAY_ELEMENTS_TEXT) => Function::JsonArrayElementsText,
        Token::Ident(_, Keyword::JSON_ARRAY_LENGTH) => Function::JsonArrayLength,
        Token::Ident(_, Keyword::JSON_BUILD_ARRAY) => Function::JsonBuildArray,
        Token::Ident(_, Keyword::JSON_EACH) => Function::JsonEach,
        Token::Ident(_, Keyword::JSON_EACH_TEXT) => Function::JsonEachText,
        Token::Ident(_, Keyword::JSON_EXTRACT_PATH) => Function::JsonExtractPath,
        Token::Ident(_, Keyword::JSON_EXTRACT_PATH_TEXT) => Function::JsonExtractPathText,
        Token::Ident(_, Keyword::JSON_OBJECT_KEYS) => Function::JsonObjectKeys,
        Token::Ident(_, Keyword::JSON_POPULATE_RECORD) => Function::JsonPopulateRecord,
        Token::Ident(_, Keyword::JSON_POPULATE_RECORDSET) => Function::JsonPopulateRecordset,
        Token::Ident(_, Keyword::JSON_SCALAR) => Function::JsonScalar,
        Token::Ident(_, Keyword::JSON_SERIALIZE) => Function::JsonSerialize,
        Token::Ident(_, Keyword::JSON_STRIP_NULLS) => Function::JsonStripNulls,
        Token::Ident(_, Keyword::JSON_TO_RECORD) => Function::JsonToRecord,
        Token::Ident(_, Keyword::JSON_TO_RECORDSET) => Function::JsonToRecordset,
        Token::Ident(_, Keyword::JSON_TYPEOF) => Function::JsonTypeof,
        Token::Ident(_, Keyword::JSONB_ARRAY_ELEMENTS) => Function::JsonbArrayElements,
        Token::Ident(_, Keyword::JSONB_ARRAY_ELEMENTS_TEXT) => Function::JsonbArrayElementsText,
        Token::Ident(_, Keyword::JSONB_ARRAY_LENGTH) => Function::JsonbArrayLength,
        Token::Ident(_, Keyword::JSONB_BUILD_ARRAY) => Function::JsonbBuildArray,
        Token::Ident(_, Keyword::JSONB_BUILD_OBJECT) => Function::JsonbBuildObject,
        Token::Ident(_, Keyword::JSONB_EACH) => Function::JsonbEach,
        Token::Ident(_, Keyword::JSONB_EACH_TEXT) => Function::JsonbEachText,
        Token::Ident(_, Keyword::JSONB_EXTRACT_PATH) => Function::JsonbExtractPath,
        Token::Ident(_, Keyword::JSONB_EXTRACT_PATH_TEXT) => Function::JsonbExtractPathText,
        Token::Ident(_, Keyword::JSONB_INSERT) => Function::JsonbInsert,
        Token::Ident(_, Keyword::JSONB_OBJECT) => Function::JsonbObject,
        Token::Ident(_, Keyword::JSONB_OBJECT_KEYS) => Function::JsonbObjectKeys,
        Token::Ident(_, Keyword::JSONB_PATH_EXISTS) => Function::JsonbPathExists,
        Token::Ident(_, Keyword::JSONB_PATH_EXISTS_TZ) => Function::JsonbPathExistsTz,
        Token::Ident(_, Keyword::JSONB_PATH_MATCH) => Function::JsonbPathMatch,
        Token::Ident(_, Keyword::JSONB_PATH_MATCH_TZ) => Function::JsonbPathMatchTz,
        Token::Ident(_, Keyword::JSONB_PATH_QUERY) => Function::JsonbPathQuery,
        Token::Ident(_, Keyword::JSONB_PATH_QUERY_ARRAY) => Function::JsonbPathQueryArray,
        Token::Ident(_, Keyword::JSONB_PATH_QUERY_ARRAY_TZ) => Function::JsonbPathQueryArrayTz,
        Token::Ident(_, Keyword::JSONB_PATH_QUERY_FIRST) => Function::JsonbPathQueryFirst,
        Token::Ident(_, Keyword::JSONB_PATH_QUERY_FIRST_TZ) => Function::JsonbPathQueryFirstTz,
        Token::Ident(_, Keyword::JSONB_PATH_QUERY_TZ) => Function::JsonbPathQueryTz,
        Token::Ident(_, Keyword::JSONB_POPULATE_RECORD) => Function::JsonbPopulateRecord,
        Token::Ident(_, Keyword::JSONB_POPULATE_RECORD_VALID) => Function::JsonbPopulateRecordValid,
        Token::Ident(_, Keyword::JSONB_POPULATE_RECORDSET) => Function::JsonbPopulateRecordset,
        Token::Ident(_, Keyword::JSONB_PRETTY) => Function::JsonbPretty,
        Token::Ident(_, Keyword::JSONB_SET_LAX) => Function::JsonbSetLax,
        Token::Ident(_, Keyword::JSONB_STRIP_NULLS) => Function::JsonbStripNulls,
        Token::Ident(_, Keyword::JSONB_TO_RECORD) => Function::JsonbToRecord,
        Token::Ident(_, Keyword::JSONB_TO_RECORDSET) => Function::JsonbToRecordset,
        Token::Ident(_, Keyword::JSONB_TYPEOF) => Function::JsonbTypeof,
        Token::Ident(_, Keyword::ROW_TO_JSON) => Function::RowToJson,
        Token::Ident(_, Keyword::TO_JSON) => Function::ToJson,
        Token::Ident(_, Keyword::TO_JSONB) => Function::ToJsonb,

        // PostgreSQL sequence functions
        Token::Ident(_, Keyword::CURRVAL) => Function::Currval,
        Token::Ident(_, Keyword::LASTVAL) => Function::Lastval,
        Token::Ident(_, Keyword::NEXTVAL) => Function::Nextval,
        Token::Ident(_, Keyword::SETVAL) => Function::Setval,

        // PostgreSQL text search functions
        Token::Ident(_, Keyword::ARRAY_TO_TSVECTOR) => Function::ArrayToTsvector,
        Token::Ident(_, Keyword::GET_CURRENT_TS_CONFIG) => Function::GetCurrentTsConfig,
        Token::Ident(_, Keyword::JSON_TO_TSVECTOR) => Function::JsonToTsvector,
        Token::Ident(_, Keyword::JSONB_TO_TSVECTOR) => Function::JsonbToTsvector,
        Token::Ident(_, Keyword::NUMNODE) => Function::Numnode,
        Token::Ident(_, Keyword::PHRASETO_TSQUERY) => Function::PhraseToTsquery,
        Token::Ident(_, Keyword::PLAINTO_TSQUERY) => Function::PlainToTsquery,
        Token::Ident(_, Keyword::QUERYTREE) => Function::Querytree,
        Token::Ident(_, Keyword::SETWEIGHT) => Function::Setweight,
        Token::Ident(_, Keyword::STRIP) => Function::Strip,
        Token::Ident(_, Keyword::TO_TSQUERY) => Function::ToTsquery,
        Token::Ident(_, Keyword::TO_TSVECTOR) => Function::ToTsvector,
        Token::Ident(_, Keyword::TS_DEBUG) => Function::TsDebug,
        Token::Ident(_, Keyword::TS_DELETE) => Function::TsDelete,
        Token::Ident(_, Keyword::TS_FILTER) => Function::TsFilter,
        Token::Ident(_, Keyword::TS_HEADLINE) => Function::TsHeadline,
        Token::Ident(_, Keyword::TS_LEXIZE) => Function::TsLexize,
        Token::Ident(_, Keyword::TS_PARSE) => Function::TsParse,
        Token::Ident(_, Keyword::TS_RANK) => Function::TsRank,
        Token::Ident(_, Keyword::TS_RANK_CD) => Function::TsRankCd,
        Token::Ident(_, Keyword::TS_REWRITE) => Function::TsRewrite,
        Token::Ident(_, Keyword::TS_STAT) => Function::TsStat,
        Token::Ident(_, Keyword::TS_TOKEN_TYPE) => Function::TsTokenType,
        Token::Ident(_, Keyword::TSQUERY_PHRASE) => Function::TsqueryPhrase,
        Token::Ident(_, Keyword::TSVECTOR_TO_ARRAY) => Function::TsvectorToArray,
        Token::Ident(_, Keyword::UNNEST) => Function::Unnest,
        Token::Ident(_, Keyword::WEBSEARCH_TO_TSQUERY) => Function::WebsearchToTsquery,

        // MySQL 8.4 regexp
        Token::Ident(_, Keyword::REGEXP_COUNT) => Function::RegexpCount,
        Token::Ident(_, Keyword::REGEXP_INSTR) => Function::RegexpInstr,
        Token::Ident(_, Keyword::REGEXP_LIKE) => Function::RegexpLike,
        Token::Ident(_, Keyword::REGEXP_MATCH) => Function::RegexpMatch,
        Token::Ident(_, Keyword::REGEXP_MATCHES) => Function::RegexpMatches,
        Token::Ident(_, Keyword::REGEXP_REPLACE) => Function::RegexpReplace,
        Token::Ident(_, Keyword::REGEXP_SPLIT_TO_ARRAY) => Function::RegexpSplitToArray,
        Token::Ident(_, Keyword::REGEXP_SPLIT_TO_TABLE) => Function::RegexpSplitToTable,
        Token::Ident(_, Keyword::REGEXP_SUBSTR) => Function::RegexpSubstr,
        Token::Ident(_, Keyword::WEIGHT_STRING) => Function::WeightString,

        // MySQL 8.4 datetime
        Token::Ident(_, Keyword::GET_BIT) => Function::GetBit,
        Token::Ident(_, Keyword::GET_FORMAT) => Function::GetFormat,

        // MySQL 8.4 window / analytics
        Token::Ident(_, Keyword::FIRST_VALUE) => Function::FirstValue,
        Token::Ident(_, Keyword::LAST_VALUE) => Function::LastValue,
        Token::Ident(_, Keyword::NTH_VALUE) => Function::NthValue,
        Token::Ident(_, Keyword::NTILE) => Function::Ntile,
        Token::Ident(_, Keyword::ROW_NUMBER) => Function::RowNumber,

        // MySQL 8.4 performance schema
        Token::Ident(_, Keyword::FORMAT_BYTES) => Function::FormatBytes,
        Token::Ident(_, Keyword::FORMAT_PICO_TIME) => Function::FormatPicoTime,
        Token::Ident(_, Keyword::PS_CURRENT_THREAD_ID) => Function::PsCurrentThreadId,
        Token::Ident(_, Keyword::PS_THREAD_ID) => Function::PsThreadId,

        // MySQL 8.4 miscellaneous
        Token::Ident(_, Keyword::ANY_VALUE) => Function::AnyValue,
        Token::Ident(_, Keyword::BIN_TO_UUID) => Function::BinToUuid,
        Token::Ident(_, Keyword::BIT_COUNT) => Function::BitCount,
        Token::Ident(_, Keyword::GROUPING) => Function::Grouping,
        Token::Ident(_, Keyword::INET6_ATON) => Function::Inet6Aton,
        Token::Ident(_, Keyword::INET6_NTOA) => Function::Inet6Ntoa,
        Token::Ident(_, Keyword::INET_ATON) => Function::InetAton,
        Token::Ident(_, Keyword::INET_NTOA) => Function::InetNtoa,
        Token::Ident(_, Keyword::IS_IPV4) => Function::IsIPv4,
        Token::Ident(_, Keyword::IS_IPV4_COMPAT) => Function::IsIPv4Compat,
        Token::Ident(_, Keyword::IS_IPV4_MAPPED) => Function::IsIPv4Mapped,
        Token::Ident(_, Keyword::IS_IPV6) => Function::IsIPv6,
        Token::Ident(_, Keyword::IS_UUID) => Function::IsUuid,
        Token::Ident(_, Keyword::NAME_CONST) => Function::NameConst,
        Token::Ident(_, Keyword::UUID) => Function::Uuid,
        Token::Ident(_, Keyword::UUID_SHORT) => Function::UuidShort,
        Token::Ident(_, Keyword::UUID_TO_BIN) => Function::UuidToBin,

        // PostgreSQL geometric functions (non-PostGIS) - Table 9.37
        Token::Ident(_, Keyword::AREA) => Function::Area,
        Token::Ident(_, Keyword::BOUND_BOX) => Function::BoundBox,
        Token::Ident(_, Keyword::CENTER) => Function::Center,
        Token::Ident(_, Keyword::DIAGONAL) => Function::Diagonal,
        Token::Ident(_, Keyword::DIAMETER) => Function::Diameter,
        Token::Ident(_, Keyword::HEIGHT) => Function::Height,
        Token::Ident(_, Keyword::ISCLOSED) => Function::Isclosed,
        Token::Ident(_, Keyword::ISOPEN) => Function::IsOpen,
        Token::Ident(_, Keyword::NPOINTS) => Function::Npoints,
        Token::Ident(_, Keyword::PCLOSE) => Function::Pclose,
        Token::Ident(_, Keyword::POPEN) => Function::Popen,
        Token::Ident(_, Keyword::RADIUS) => Function::Radius,
        Token::Ident(_, Keyword::SLOPE) => Function::Slope,
        Token::Ident(_, Keyword::WIDTH) => Function::Width,

        // PostgreSQL enum support functions
        Token::Ident(_, Keyword::ENUM_FIRST) => Function::EnumFirst,
        Token::Ident(_, Keyword::ENUM_LAST) => Function::EnumLast,
        Token::Ident(_, Keyword::ENUM_RANGE) => Function::EnumRange,

        // PostGIS / geometry functions
        Token::Ident(_, Keyword::BOX2D) => Function::Box2D,
        Token::Ident(_, Keyword::BOX3D) => Function::Box3D,
        Token::Ident(_, Keyword::GEOMETRYTYPE) => Function::GeometryType,
        Token::Ident(_, Keyword::ST_ADDMEASURE) => Function::StAddMeasure,
        Token::Ident(_, Keyword::ST_ADDPOINT) => Function::StAddPoint,
        Token::Ident(_, Keyword::ST_AFFINE) => Function::StAffine,
        Token::Ident(_, Keyword::ST_AREA) => Function::StArea,
        Token::Ident(_, Keyword::ST_ASBINARY) => Function::StAsBinary,
        Token::Ident(_, Keyword::ST_ASEWKB) => Function::StAsEwkb,
        Token::Ident(_, Keyword::ST_ASEWKT) => Function::StAsEwkt,
        Token::Ident(_, Keyword::ST_ASGEOJSON) => Function::StAsGeoJson,
        Token::Ident(_, Keyword::ST_ASGML) => Function::StAsGml,
        Token::Ident(_, Keyword::ST_ASHEXEWKB) => Function::StAsHexEwkb,
        Token::Ident(_, Keyword::ST_ASKML) => Function::StAsKml,
        Token::Ident(_, Keyword::ST_ASSVG) => Function::StAsSvg,
        Token::Ident(_, Keyword::ST_ASTEXT) => Function::StAsText,
        Token::Ident(_, Keyword::ST_AZIMUTH) => Function::StAzimuth,
        Token::Ident(_, Keyword::ST_BOUNDARY) => Function::StBoundary,
        Token::Ident(_, Keyword::ST_BUFFER) => Function::StBuffer,
        Token::Ident(_, Keyword::ST_BUILDAREA) => Function::StBuildArea,
        Token::Ident(_, Keyword::ST_CENTROID) => Function::StCentroid,
        Token::Ident(_, Keyword::ST_CLOSESTPOINT) => Function::StClosestPoint,
        Token::Ident(_, Keyword::ST_COLLECT) => Function::StCollect,
        Token::Ident(_, Keyword::ST_COLLECTIONEXTRACT) => Function::StCollectionExtract,
        Token::Ident(_, Keyword::ST_CONTAINS) => Function::StContains,
        Token::Ident(_, Keyword::ST_CONTAINSPROPERLY) => Function::StContainsProperly,
        Token::Ident(_, Keyword::ST_CONVEXHULL) => Function::StConvexHull,
        Token::Ident(_, Keyword::ST_COORDDIM) => Function::StCoordDim,
        Token::Ident(_, Keyword::ST_COVEREDBY) => Function::StCoveredBy,
        Token::Ident(_, Keyword::ST_COVERS) => Function::StCovers,
        Token::Ident(_, Keyword::ST_CROSSES) => Function::StCrosses,
        Token::Ident(_, Keyword::ST_CURVETOLINE) => Function::StCurveToLine,
        Token::Ident(_, Keyword::ST_DFULLWITHIN) => Function::StDFullyWithin,
        Token::Ident(_, Keyword::ST_DIFFERENCE) => Function::StDifference,
        Token::Ident(_, Keyword::ST_DIMENSION) => Function::StDimension,
        Token::Ident(_, Keyword::ST_DISJOINT) => Function::StDisjoint,
        Token::Ident(_, Keyword::ST_DISTANCE) => Function::StDistance,
        Token::Ident(_, Keyword::ST_DISTANCE_SPHERE) => Function::StDistanceSphere,
        Token::Ident(_, Keyword::ST_DISTANCE_SPHEROID) => Function::StDistanceSpheroidal,
        Token::Ident(_, Keyword::ST_DWITHIN) => Function::StDWithin,
        Token::Ident(_, Keyword::ST_ENDPOINT) => Function::StEndPoint,
        Token::Ident(_, Keyword::ST_ENVELOPE) => Function::StEnvelope,
        Token::Ident(_, Keyword::ST_EQUALS) => Function::StEquals,
        Token::Ident(_, Keyword::ST_EXTERIORRING) => Function::StExteriorRing,
        Token::Ident(_, Keyword::ST_FORCE_2D) => Function::StForce2D,
        Token::Ident(_, Keyword::ST_FORCE_3D) => Function::StForce3D,
        Token::Ident(_, Keyword::ST_FORCE_3DM) => Function::StForce3DM,
        Token::Ident(_, Keyword::ST_FORCE_3DZ) => Function::StForce3DZ,
        Token::Ident(_, Keyword::ST_FORCE_4D) => Function::StForce4D,
        Token::Ident(_, Keyword::ST_FORCE_COLLECTION) => Function::StForceCollection,
        Token::Ident(_, Keyword::ST_FORCERHR) => Function::StForceRHR,
        Token::Ident(_, Keyword::ST_GEOHASH) => Function::StGeoHash,
        Token::Ident(_, Keyword::ST_GEOMCOLLFROMTEXT) => Function::StGeomCollFromText,
        Token::Ident(_, Keyword::ST_GEOMFROMEWKB) => Function::StGeomFromEwkb,
        Token::Ident(_, Keyword::ST_GEOMFROMEWKT) => Function::StGeomFromEwkt,
        Token::Ident(_, Keyword::ST_GEOMFROMGEOJSON) => Function::StGeomFromGeoJson,
        Token::Ident(_, Keyword::ST_GEOMFROMGML) => Function::StGeomFromGml,
        Token::Ident(_, Keyword::ST_GEOMFROMKML) => Function::StGeomFromKml,
        Token::Ident(_, Keyword::ST_GEOMFROMTEXT) => Function::StGeomFromText,
        Token::Ident(_, Keyword::ST_GEOMFROMWKB) => Function::StGeomFromWkb,
        Token::Ident(_, Keyword::ST_GEOMETRYFROMTEXT) => Function::StGeometryFromText,
        Token::Ident(_, Keyword::ST_GEOMETRYN) => Function::StGeometryN,
        Token::Ident(_, Keyword::ST_GEOMETRYTYPE) => Function::StGeometryType,
        Token::Ident(_, Keyword::ST_GMLTOSQL) => Function::StGmlToSQL,
        Token::Ident(_, Keyword::ST_HASARC) => Function::StHasArc,
        Token::Ident(_, Keyword::ST_HAUSDORFFDISTANCE) => Function::StHausdorffDistance,
        Token::Ident(_, Keyword::ST_INTERIORRINGN) => Function::StInteriorRingN,
        Token::Ident(_, Keyword::ST_INTERSECTION) => Function::StIntersection,
        Token::Ident(_, Keyword::ST_INTERSECTS) => Function::StIntersects,
        Token::Ident(_, Keyword::ST_ISCLOSED) => Function::StIsClosed,
        Token::Ident(_, Keyword::ST_ISEMPTY) => Function::StIsEmpty,
        Token::Ident(_, Keyword::ST_ISRING) => Function::StIsRing,
        Token::Ident(_, Keyword::ST_ISSIMPLE) => Function::StIsSimple,
        Token::Ident(_, Keyword::ST_ISVALID) => Function::StIsValid,
        Token::Ident(_, Keyword::ST_ISVALIDREASON) => Function::StIsValidReason,
        Token::Ident(_, Keyword::ST_LENGTH) => Function::StLength,
        Token::Ident(_, Keyword::ST_LENGTH2D) => Function::StLength2D,
        Token::Ident(_, Keyword::ST_LENGTH3D) => Function::StLength3D,
        Token::Ident(_, Keyword::ST_LINECROSSINGDIRECTION) => Function::StLineCrossingDirection,
        Token::Ident(_, Keyword::ST_LINEFROMMULTIPOINT) => Function::StLineFromMultiPoint,
        Token::Ident(_, Keyword::ST_LINEFROMTEXT) => Function::StLineFromText,
        Token::Ident(_, Keyword::ST_LINEFROMWKB) => Function::StLineFromWkb,
        Token::Ident(_, Keyword::ST_LINEMERGE) => Function::StLineMerge,
        Token::Ident(_, Keyword::ST_LINESTRINGFROMWKB) => Function::StLinestringFromWkb,
        Token::Ident(_, Keyword::ST_LINETOCURVE) => Function::StLineToCurve,
        Token::Ident(_, Keyword::ST_LINE_INTERPOLATE_POINT) => Function::StLineInterpolatePoint,
        Token::Ident(_, Keyword::ST_LINE_LOCATE_POINT) => Function::StLineLocatePoint,
        Token::Ident(_, Keyword::ST_LINE_SUBSTRING) => Function::StLineSubstring,
        Token::Ident(_, Keyword::ST_LONGESTLINE) => Function::StLongestLine,
        Token::Ident(_, Keyword::ST_M) => Function::StM,
        Token::Ident(_, Keyword::ST_MAKEENVELOPE) => Function::StMakeEnvelope,
        Token::Ident(_, Keyword::ST_MAKELINE) => Function::StMakeLine,
        Token::Ident(_, Keyword::ST_MAKEPOINT) => Function::StMakePoint,
        Token::Ident(_, Keyword::ST_MAKEPOINTM) => Function::StMakePointM,
        Token::Ident(_, Keyword::ST_MAKEPOLYGON) => Function::StMakePolygon,
        Token::Ident(_, Keyword::ST_MAXDISTANCE) => Function::StMaxDistance,
        Token::Ident(_, Keyword::ST_MEM_SIZE) => Function::StMemSize,
        Token::Ident(_, Keyword::ST_MINIMUMBOUNDINGCIRCLE) => Function::StMinimumBoundingCircle,
        Token::Ident(_, Keyword::ST_MULTI) => Function::StMulti,
        Token::Ident(_, Keyword::ST_NDIMS) => Function::StNDims,
        Token::Ident(_, Keyword::ST_NPOINTS) => Function::StNPoints,
        Token::Ident(_, Keyword::ST_NRINGS) => Function::StNRings,
        Token::Ident(_, Keyword::ST_NUMGEOMETRIES) => Function::StNumGeometries,
        Token::Ident(_, Keyword::ST_NUMINTERIORRING) => Function::StNumInteriorRing,
        Token::Ident(_, Keyword::ST_NUMINTERIORRINGS) => Function::StNumInteriorRings,
        Token::Ident(_, Keyword::ST_NUMPOINTS) => Function::StNumPoints,
        Token::Ident(_, Keyword::ST_ORDERINGEQUALS) => Function::StOrderingEquals,
        Token::Ident(_, Keyword::ST_OVERLAPS) => Function::StOverlaps,
        Token::Ident(_, Keyword::ST_PERIMETER) => Function::StPerimeter,
        Token::Ident(_, Keyword::ST_PERIMETER2D) => Function::StPerimeter2D,
        Token::Ident(_, Keyword::ST_PERIMETER3D) => Function::StPerimeter3D,
        Token::Ident(_, Keyword::ST_POINT) => Function::StPoint,
        Token::Ident(_, Keyword::ST_POINTFROMTEXT) => Function::StPointFromText,
        Token::Ident(_, Keyword::ST_POINTFROMWKB) => Function::StPointFromWkb,
        Token::Ident(_, Keyword::ST_POINTN) => Function::StPointN,
        Token::Ident(_, Keyword::ST_POINTONSURFACE) => Function::StPointOnSurface,
        Token::Ident(_, Keyword::ST_POINT_INSIDE_CIRCLE) => Function::StPointInsideCircle,
        Token::Ident(_, Keyword::ST_POLYGON) => Function::StPolygon,
        Token::Ident(_, Keyword::ST_POLYGONFROMTEXT) => Function::StPolygonFromText,
        Token::Ident(_, Keyword::ST_POLYGONIZE) => Function::StPolygonize,
        Token::Ident(_, Keyword::ST_RELATE) => Function::StRelate,
        Token::Ident(_, Keyword::ST_REMOVEPOINT) => Function::StRemovePoint,
        Token::Ident(_, Keyword::ST_REVERSE) => Function::StReverse,
        Token::Ident(_, Keyword::ST_ROTATE) => Function::StRotate,
        Token::Ident(_, Keyword::ST_ROTATEX) => Function::StRotateX,
        Token::Ident(_, Keyword::ST_ROTATEY) => Function::StRotateY,
        Token::Ident(_, Keyword::ST_ROTATEZ) => Function::StRotateZ,
        Token::Ident(_, Keyword::ST_SCALE) => Function::StScale,
        Token::Ident(_, Keyword::ST_SEGMENTIZE) => Function::StSegmentize,
        Token::Ident(_, Keyword::ST_SETPOINT) => Function::StSetPoint,
        Token::Ident(_, Keyword::ST_SETSRID) => Function::StSetSrid,
        Token::Ident(_, Keyword::ST_SHIFT_LONGITUDE) => Function::StShiftLongitude,
        Token::Ident(_, Keyword::ST_SHORTESTLINE) => Function::StShortestLine,
        Token::Ident(_, Keyword::ST_SIMPLIFY) => Function::StSimplify,
        Token::Ident(_, Keyword::ST_SIMPLIFYPRESERVETOPOLOGY) => {
            Function::StSimplifyPreserveTopology
        }
        Token::Ident(_, Keyword::ST_SNAPTOGRID) => Function::StSnapToGrid,
        Token::Ident(_, Keyword::ST_SRID) => Function::StSRID,
        Token::Ident(_, Keyword::ST_STARTPOINT) => Function::StStartPoint,
        Token::Ident(_, Keyword::ST_SUMMARY) => Function::StSummary,
        Token::Ident(_, Keyword::ST_SYMDIFFERENCE) => Function::StSymDifference,
        Token::Ident(_, Keyword::ST_TOUCHES) => Function::StTouches,
        Token::Ident(_, Keyword::ST_TRANSFORM) => Function::StTransform,
        Token::Ident(_, Keyword::ST_TRANSLATE) => Function::StTranslate,
        Token::Ident(_, Keyword::ST_TRANSSCALE) => Function::StTransScale,
        Token::Ident(_, Keyword::ST_UNION) => Function::StUnion,
        Token::Ident(_, Keyword::ST_WITHIN) => Function::StWithin,
        Token::Ident(_, Keyword::ST_WKBTOSQL) => Function::StWkbToSQL,
        Token::Ident(_, Keyword::ST_WKTTOSQL) => Function::StWktToSQL,
        Token::Ident(_, Keyword::ST_X) => Function::StX,
        Token::Ident(_, Keyword::ST_XMAX) => Function::StXMax,
        Token::Ident(_, Keyword::ST_XMIN) => Function::StXMin,
        Token::Ident(_, Keyword::ST_Y) => Function::StY,
        Token::Ident(_, Keyword::ST_YMAX) => Function::StYMax,
        Token::Ident(_, Keyword::ST_YMIN) => Function::StYMin,
        Token::Ident(_, Keyword::ST_Z) => Function::StZ,
        Token::Ident(_, Keyword::ST_ZMAX) => Function::StZMax,
        Token::Ident(_, Keyword::ST_ZMIN) => Function::StZMin,
        Token::Ident(_, Keyword::ST_ZMFLAG) => Function::StZmflag,

        Token::Ident(v, k) if !k.restricted(parser.reserved()) => {
            Function::Other(alloc::vec![Identifier {
                value: v,
                span: span.clone()
            }])
        }
        _ => {
            parser.err("Unknown function", &span);
            Function::Unknown
        }
    };

    let mut args = Vec::new();

    // SQL-standard SUBSTRING(str FROM pos [FOR len]) — PostgreSQL and ANSI SQL.
    if matches!(func, Function::SubStr) && !matches!(parser.token, Token::RParen) {
        let expr = parse_expression_outer(parser)?;
        if let Some(_from) = parser.skip_keyword(Keyword::FROM) {
            let pos = parse_expression_outer(parser)?;
            args.push(expr);
            args.push(pos);
            if parser.skip_keyword(Keyword::FOR).is_some() {
                args.push(parse_expression_outer(parser)?);
            }
            parser.consume_token(Token::RParen)?;
            return Ok(Expression::Function(Box::new(FunctionCallExpression {
                function: func,
                args,
                function_span: span,
            })));
        } else {
            // Comma-style: push first arg and continue normally
            args.push(expr);
            while parser.skip_token(Token::Comma).is_some() {
                parser.recovered(
                    "')' or ','",
                    &|t| matches!(t, Token::RParen | Token::Comma),
                    |parser| {
                        args.push(parse_expression_outer(parser)?);
                        Ok(())
                    },
                )?;
            }
            parser.consume_token(Token::RParen)?;
            if let Some(over) = parse_over_clause(parser)? {
                return Ok(Expression::WindowFunction(Box::new(
                    WindowFunctionCallExpression {
                        function: func,
                        args,
                        function_span: span,
                        over,
                    },
                )));
            }
            return Ok(Expression::Function(Box::new(FunctionCallExpression {
                function: func,
                args,
                function_span: span,
            })));
        }
    }

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

    if let Some(over) = parse_over_clause(parser)? {
        Ok(Expression::WindowFunction(Box::new(
            WindowFunctionCallExpression {
                function: func,
                args,
                function_span: span,
                over,
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

/// Parse the argument list and optional OVER clause for a schema-qualified function call.
/// The caller has already resolved `func = Function::Other(qualified_parts)` and
/// computed `function_span` covering the full qualified name.
pub(crate) fn parse_char_function<'a>(
    parser: &mut Parser<'a, '_>,
    char_span: Span,
) -> Result<Expression<'a>, ParseError> {
    parser.consume_token(Token::LParen)?;
    let mut args = Vec::new();
    if !matches!(parser.token, Token::RParen) {
        loop {
            parser.recovered(
                "')' or ','",
                &|t| {
                    matches!(
                        t,
                        Token::RParen | Token::Comma | Token::Ident(_, Keyword::USING)
                    )
                },
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
    // Optional USING charset_name
    let using_charset = if let Some(using_span) = parser.skip_keyword(Keyword::USING) {
        let charset = parser.consume_plain_identifier_unreserved()?;
        Some((using_span, charset))
    } else {
        None
    };
    parser.consume_token(Token::RParen)?;
    Ok(Expression::Char(Box::new(CharFunctionExpression {
        char_span,
        args,
        using_charset,
    })))
}

pub(crate) fn parse_function_call<'a>(
    parser: &mut Parser<'a, '_>,
    func: Function<'a>,
    function_span: Span,
) -> Result<Expression<'a>, ParseError> {
    parser.consume_token(Token::LParen)?;
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
    if let Some(over) = parse_over_clause(parser)? {
        Ok(Expression::WindowFunction(Box::new(
            WindowFunctionCallExpression {
                function: func,
                args,
                function_span,
                over,
            },
        )))
    } else {
        Ok(Expression::Function(Box::new(FunctionCallExpression {
            function: func,
            args,
            function_span,
        })))
    }
}

#[cfg(test)]
mod tests {
    use core::ops::Deref;

    use alloc::string::{String, ToString};

    use crate::{
        Function, FunctionCallExpression, ParseOptions, SQLDialect,
        expression::{Expression, PRIORITY_MAX},
        issue::Issues,
        parser::Parser,
    };

    use super::parse_expression_unreserved;

    fn test_expr(src: &'static str, f: impl FnOnce(&Expression<'_>) -> Result<(), String>) {
        let mut issues = Issues::new(src);
        let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
        let mut parser = Parser::new(src, &mut issues, &options);
        let res = parse_expression_unreserved(&mut parser, PRIORITY_MAX)
            .expect("Expression in test expr");
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
            let res = match parse_expression_unreserved(&mut parser, PRIORITY_MAX) {
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
