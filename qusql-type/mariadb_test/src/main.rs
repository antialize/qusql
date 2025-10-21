mod connection;
mod error;
mod tester;
use std::process::exit;

use crate::tester::ArgType::*;
use crate::{
    connection::Connection,
    tester::{NO_FLAGS, STRICTER_TYPE, Tester, emit_issues},
};
use error::Result;
use sql_type::{Issues, SQLArguments, SQLDialect, TypeOptions, schema::parse_schemas};

fn main() -> Result<()> {
    let schema_src = include_str!("schema.sql");
    let options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark);
    let mut issues = Issues::new(schema_src);
    let schemas = parse_schemas(schema_src, &mut issues, &options);
    emit_issues("schema", schema_src, issues.get());

    let mut tester = Tester {
        conn: Connection::connect()?,
        schemas,
        options,
        errors: 0,
        tests: 0,
    };

    for t in [
        U8,
        U8NotNull,
        U8Literal,
        U16,
        U16NotNull,
        U16Literal,
        U32,
        U32NotNull,
        U32Literal,
        U64,
        U64NotNull,
        U64Literal,
        I8,
        I8NotNull,
        I8Literal,
        I16,
        I16NotNull,
        I16Literal,
        I32,
        I32NotNull,
        I32Literal,
        I64,
        I64NotNull,
        I64Literal,
        F32,
        F32NotNull,
        F32Literal,
        F64,
        F64NotNull,
        F64Literal,
        Time,
        TimeNotNull,
        TimeLiteral,
        Date,
        DateNotNull,
        DateLiteral,
        DateTime,
        DateTimeNotNull,
        DateTimeLiteral,
        Timestamp,
        TimestampNotNull,
        TimestampLiteral,
        IntervalLiteralDate,
        IntervalLiteralTime,
        Bool,
        BoolNotNull,
        BoolLiteral,
        NullLiteral,
        String,
        StringNotNull,
        StringLiteral,
        QueryArg] {
        tester.test_statement(&format!("SELECT {} as `q`", t.v()), NO_FLAGS)?;
    }

    let date_type = [
        TimestampLiteral,
        TimestampNotNull,
        Timestamp,
        DateTime,
        DateTimeLiteral,
        DateTimeNotNull,
        Date,
        DateLiteral,
        DateNotNull,
        Time,
        TimeLiteral,
        TimeNotNull,
        String,
        U16,
        QueryArg,
    ];

    let interval_types = [
        IntervalLiteralTime,
        IntervalLiteralDate,
        U8Literal,
        QueryArg,
    ];

    tester.test_function_mult("ADDDATE", &[&date_type, &interval_types], STRICTER_TYPE)?;
    tester.test_function_mult("SUBDATE", &[&date_type, &interval_types], STRICTER_TYPE)?;

    // t("ADD_MONTHS(`d`, 2)", "date!");
    // t("DATE_ADD(`d`, INTERVAL 31 DAY)", "date!");
    // t("ADDDATE(`d`, 31)", "date!");
    // t("ADDTIME(`dt`, '1 1:1:1.000002')", "dt!");
    // t("ADDTIME(`t`, '02:00:00.999998')", "time!");
    // t("CONVERT_TZ(`dt`, '+00:00','+10:00')", "dt!");
    // //t("CURDATE() + 0", "i!");
    // t("CURDATE()", "date!");
    // t("CURDATE() - INTERVAL 5 DAY", "date!");
    // // t("CURTIME() + 0.0", "f!");
    // t("CURTIME()", "time!");
    // t("CURTIME()", "time!");
    // t("DATE('2013-07-18 12:21:32')", "date!");
    // t("`dt` + INTERVAL 1 SECOND", "dt!");
    // t("INTERVAL 1 DAY + `d`", "date!");
    // t("DATE_ADD(`dt`, INTERVAL 1 SECOND)", "dt!");
    // t("DATE_ADD(`dt`, INTERVAL '1:1' MINUTE_SECOND)", "dt!");
    // t("DATE_FORMAT(`dt`, '%D %y %a %d %m %b %j')", "str!");
    // t("DATE_SUB(`d`, INTERVAL 31 DAY)", "date!");
    // t("DATE_SUB(`dt`, INTERVAL '1 1:1:1' DAY_SECOND)", "dt!");
    // t("DATEDIFF(`dt`, `d`)", "i!");

    tester.test_function_mult("DAYNAME", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("DAYOFMONTH", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("DAYOFWEEK", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("DAYOFYEAR", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("MICROSECOND", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("MINUTE", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("MONTH", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("HOUR", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("MONTHNAME", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("SECOND", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("TO_DAYS", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("TO_SECONDS", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("WEEK", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("WEEKDAY", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult("YEAR", &[&date_type], STRICTER_TYPE)?;
    tester.test_function_mult(
        "YEARWEEK",
        &[&date_type, &[U8NotNull, String]],
        STRICTER_TYPE,
    )?;
    tester.test_function_mult("WEEK", &[&date_type, &[U8NotNull, String]], STRICTER_TYPE)?;

    // t("EXTRACT(DAY_MINUTE FROM `dt`)", "i!");
    // t("FROM_DAYS(730669)", "date!");
    // t("FROM_UNIXTIME(1196440219.0)", "dt!");
    // // t("FROM_UNIXTIME(1196440219.0) + 0.0", "f!");
    // t(
    //     "FROM_UNIXTIME(1196440219.0, '%Y %D %M %h:%i:%s %x')",
    //     "str!",
    // );
    // t("LAST_DAY('2004-01-01 01:01:01')", "date!");
    // t("MAKEDATE(2011,31)", "date");
    // t("MAKETIME(13,57,33)", "time");
    // t("NOW()", "dt!");
    // // t("NOW() + 0.0", "f!");
    // t("PERIOD_ADD(200801,2)", "i!");
    // t("PERIOD_DIFF(200802,200703)", "i!");
    // t("QUARTER(`dt`)", "i!");
    // // t("SEC_TO_TIME(12414)+0", "i!");
    // t("SEC_TO_TIME(12414)", "time!");
    // t(
    //     "STR_TO_DATE('Wednesday23423, June 2, 2014', '%W, %M %e, %Y')",
    //     "dt!",
    // );
    // t("SUBTIME(`dt`,'1 1:1:1.000002')", "dt!");
    // t("SUBTIME(`t`, '02:00:00.999998')", "time!");
    // t("SYSDATE()", "dt!");
    // t("TIME('2013-07-18 12:21:32')", "time!");
    // t("TIME_FORMAT(`t`, '%H %k %h %I %l')", "str!");
    // t("TIME_TO_SEC(`t`)", "f!");
    // t(
    //     "TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001')",
    //     "time!",
    // );
    // t("TIMESTAMP('2003-12-31')", "dt!");
    // t("TIMESTAMP('2003-12-31 12:00:00','6:30:00')", "dt!");
    // t("TIMESTAMPADD(MINUTE,1,`d`)", "dt!");
    // t("TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01')", "i!");
    // t("UNIX_TIMESTAMP(`dt`)", "i64!");
    // t("UNIX_TIMESTAMP()", "i64!");
    // // t("UTC_DATE() + 0", "i!");
    // t("UTC_DATE()", "date!");
    // // t("UTC_TIME() + 0", "f!");
    // t("UTC_TIME()", "time!");
    // // t("UTC_TIMESTAMP() + 0", "f!");
    // t("UTC_TIMESTAMP()", "dt!");

    if tester.errors != 0 {
        println!("{} of {} tests failed", tester.errors, tester.tests);
        exit(1);
    }
    println!("All {} tests successful", tester.tests);
    Ok(())
}
