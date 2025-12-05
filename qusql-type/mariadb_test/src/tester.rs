use std::io::{Write, stdout};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFiles,
    term::{
        self,
        termcolor::{ColorChoice, StandardStream},
    },
};
use sql_type::{BaseType, Issue, Issues, Level, StatementType, Type, TypeOptions, schema::Schemas};

use crate::error::Result;
use crate::{
    connection::{Connection, MariaDBType, TypeStatementResult},
    error::{Context, Error},
};

pub struct Tester<'a> {
    pub conn: Connection,
    pub schemas: Schemas<'a>,
    pub options: TypeOptions,
    pub errors: usize,
    pub tests: usize,
}

#[allow(unused)]
#[derive(Clone, Copy)]
pub enum ArgType {
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
    QueryArg,
}

impl ArgType {
    pub fn v(self) -> &'static str {
        match self {
            ArgType::U8 => "`u8`",
            ArgType::U8NotNull => "`u8nn`",
            ArgType::U8Literal => "243",
            ArgType::U16 => "`u16`",
            ArgType::U16NotNull => "`u16nn`",
            ArgType::U16Literal => "64000",
            ArgType::U32 => "`u32`",
            ArgType::U32NotNull => "`u32nn`",
            ArgType::U32Literal => "4294967234",
            ArgType::U64 => "`u64`",
            ArgType::U64NotNull => "`u64nn`",
            ArgType::U64Literal => "18446744073709551554",
            ArgType::I8 => "`i8`",
            ArgType::I8NotNull => "`i8nn`",
            ArgType::I8Literal => "-100",
            ArgType::I16 => "`i16`",
            ArgType::I16NotNull => "`i16nn`",
            ArgType::I16Literal => "32668",
            ArgType::I32 => "`i32`",
            ArgType::I32NotNull => "`i32nn`",
            ArgType::I32Literal => "-2147483548",
            ArgType::I64 => "`i64`",
            ArgType::I64NotNull => "`i64nn`",
            ArgType::I64Literal => "9223372036854775708",
            ArgType::F32 => "`f32`",
            ArgType::F32NotNull => "`f32nn`",
            ArgType::F32Literal => "0.2",
            ArgType::F64 => "`f64`",
            ArgType::F64NotNull => "`f64nn`",
            ArgType::F64Literal => "1e40",
            ArgType::Time => "`time`",
            ArgType::TimeNotNull => "`time_nn`",
            ArgType::TimeLiteral => "'11:42:55.00",
            ArgType::Date => "`date`",
            ArgType::DateNotNull => "`date_nn`",
            ArgType::DateLiteral => "'2025-12-31'",
            ArgType::DateTime => "`datetime`",
            ArgType::DateTimeNotNull => "`datetime_nn`",
            ArgType::DateTimeLiteral => "'2013-07-18 13:44:22.123456'",
            ArgType::Timestamp => "`timestamp`",
            ArgType::TimestampNotNull => "`timestamp_nn`",
            ArgType::TimestampLiteral => "'2013-07-22 12:50:05'",
            ArgType::IntervalLiteralDate => "INTERVAL 31 DAY",
            ArgType::IntervalLiteralTime => "INTERVAL 1 HOUR",
            ArgType::Bool => "`u8nn`",
            ArgType::BoolNotNull => "`bool_nn`",
            ArgType::BoolLiteral => "true",
            ArgType::NullLiteral => "null",
            ArgType::String => "`u8nn`",
            ArgType::StringNotNull => "`str_nn`",
            ArgType::StringLiteral => "'hello world'",
            ArgType::QueryArg => "?",
        }
    }
}

pub fn emit_issues(name: &str, src: &str, issues: &[Issue]) {
    let mut files = SimpleFiles::new();
    let file_id = files.add(name, &src);
    let writer = StandardStream::stderr(ColorChoice::Always);
    let config = codespan_reporting::term::Config::default();
    for issue in issues {
        let mut labels = vec![Label::primary(file_id, issue.span.clone())];
        for fragment in &issue.fragments {
            labels.push(
                Label::secondary(file_id, fragment.span.clone())
                    .with_message(fragment.message.to_string()),
            );
        }
        let d = match issue.level {
            Level::Error => Diagnostic::error(),
            Level::Warning => Diagnostic::warning(),
        };
        let d = d
            .with_message(issue.message.to_string())
            .with_labels(labels);
        term::emit_to_write_style(&mut writer.lock(), &config, &files, &d).unwrap();
    }
}

pub const NO_FLAGS: usize = 0;
pub const STRICTER_TYPE: usize = 1;
pub const STRICTER_NULL: usize = 2;
//pub const EXPECT_FAIL: usize = 4;

impl<'a> Tester<'a> {
    /// Check that we type the given stmt the same way as mysql
    pub fn test_statement(&mut self, stmt: &str, flags: usize) -> Result<()> {
        self.tests += 1;
        print!("\x1b[2K\rtesting {}: {}", self.tests, stmt);
        stdout().flush()?;

        fn print_header(errors: &mut usize, is_error: &mut bool, stmt: &str) {
            if !*is_error {
                println!(
                    "\x1b[2K\r==============================> Failure <============================="
                );
                println!("Stmt: {stmt}");
                *errors += 1;
                *is_error = true;
            }
            println!();
        }

        let r1 = self
            .conn
            .type_statement(stmt)
            .context("Typing statement")?;

        let mut is_error = false;
        let mut issues = Issues::new(stmt);
        let r2 = sql_type::type_statement(&self.schemas, stmt, &mut issues, &self.options);
        match (r1, issues.is_ok()) {
            (TypeStatementResult::Typed { params, columns }, true) => {
                let (our_cols, our_params) = match r2 {
                    StatementType::Select { columns, arguments } => (columns, arguments),
                    StatementType::Delete {
                        arguments,
                        returning,
                    } => (returning.unwrap_or_default(), arguments),
                    StatementType::Insert {
                        arguments,
                        returning,
                        ..
                    } => (returning.unwrap_or_default(), arguments),
                    StatementType::Update {
                        arguments,
                        returning,
                    } => (returning.unwrap_or_default(), arguments),
                    StatementType::Replace {
                        arguments,
                        returning,
                    } => (returning.unwrap_or_default(), arguments),
                    StatementType::Invalid => return Err(Error::bail("Unexpected invalid")),
                };
                for i in 0..usize::max(our_cols.len(), columns.len()) {
                    match (our_cols.get(i), columns.get(i)) {
                        (None, None) => return Err(Error::bail("Unexpected invalid")),
                        (None, Some(m)) => {
                            print_header(&mut self.errors, &mut is_error, stmt);
                            println!("Mariadb generated column {i} ({}) we did not", m.column);
                        }
                        (Some(o), None) => {
                            print_header(&mut self.errors, &mut is_error, stmt);
                            println!("We generated column {i} ({:?}) mariadb did not", o.name);
                        }
                        (Some(o), Some(m)) => {
                            if let Some(name) = &o.name
                                && name.as_str() != m.column.as_str()
                            {
                                print_header(&mut self.errors, &mut is_error, stmt);
                                println!(
                                    "Wrong column name {i} expected {} got {}",
                                    m.column,
                                    name.as_str()
                                );
                            }
                            match (o.type_.not_null, m.not_null) {
                                (true, true) | (false, false) => (),
                                (true, false) => {
                                    if flags & STRICTER_NULL == 0 {
                                        print_header(&mut self.errors, &mut is_error, stmt);
                                        println!(
                                            "Column {i} ({}) is not null but should not be",
                                            m.column
                                        );
                                    }
                                }
                                (false, true) => {
                                    print_header(&mut self.errors, &mut is_error, stmt);
                                    println!(
                                        "Column {i} ({}) is not not null but should be",
                                        m.column
                                    );
                                }
                            }
                            match (&m.field_type, m.unsigned, &o.type_.t) {
                                (MariaDBType::Long, false, Type::Base(BaseType::Integer)) => (),
                                (MariaDBType::Timestamp, _, Type::Base(BaseType::TimeStamp)) => (),
                                (MariaDBType::Time, _, Type::Base(BaseType::Time)) => (),
                                (MariaDBType::Date, _, Type::Base(BaseType::Date)) => (),
                                (MariaDBType::Datetime, _, Type::Base(BaseType::DateTime)) => (),
                                (
                                    MariaDBType::String
                                    | MariaDBType::VarChar
                                    | MariaDBType::VarString,
                                    _,
                                    Type::Base(BaseType::String),
                                ) => (),
                                (m_type, true, ot) => {
                                    print_header(&mut self.errors, &mut is_error, stmt);
                                    println!(
                                        "Column {i} ({}) type failure: expected type unsigned {m_type:?} got {ot:?}",
                                        m.column
                                    );
                                }
                                (m_type, false, ot) => {
                                    print_header(&mut self.errors, &mut is_error, stmt);
                                    println!(
                                        "Column {i} ({}) type failure: expected type {m_type:?} got {ot:?}",
                                        m.column
                                    );
                                }
                            }
                        }
                    }
                }
                for i in 0..usize::max(our_params.len(), params.len()) {
                    match (our_params.get(i), params.get(i)) {
                        (None, None) => return Err(Error::bail("Unexpected invalid")),
                        (None, Some(_)) => {
                            print_header(&mut self.errors, &mut is_error, stmt);
                            println!("Mariadb generated param {i} we did not");
                        }
                        (Some(_), None) => {
                            print_header(&mut self.errors, &mut is_error, stmt);
                            println!("We generated param {i} mariadb did not");
                        }
                        (Some(_), Some(_)) => {
                            //println!("Check args")
                        }
                    }
                }
            }
            (TypeStatementResult::Typed { params, columns }, false) => {
                if flags & STRICTER_TYPE == 0 {
                    print_header(&mut self.errors, &mut is_error, stmt);
                    println!("Mariadb typed the query, we produced errors");
                    emit_issues("stmt", stmt, issues.get());
                    println!("Columns:");
                    for c in columns {
                        println!(
                            "  {}: {}{}{:?}",
                            c.column,
                            if c.not_null { "not null " } else { "" },
                            if c.unsigned { "unsigned " } else { "" },
                            c.field_type
                        )
                    }
                    println!("Params:");
                    for c in params {
                        println!(
                            "  {}: {}{}{:?}",
                            c.column,
                            if c.not_null { "not null " } else { "" },
                            if c.unsigned { "unsigned " } else { "" },
                            c.field_type
                        )
                    }
                }
            }
            (TypeStatementResult::Error { code, message }, true) => {
                print_header(&mut self.errors, &mut is_error, stmt);
                println!("Mariadb failed to type query with {code}: {message}");
                println!("We claimed that the query was good");
            }
            (TypeStatementResult::Error { .. }, false) => {}
        }
        Ok(())
    }

    pub fn test_function(
        &mut self,
        function: &str,
        arg_types: &[ArgType],
        flags: usize,
    ) -> Result<()> {
        let mut q = String::new();
        q.push_str("SELECT ");
        q.push_str(function);
        q.push('(');
        for (i, t) in arg_types.iter().enumerate() {
            if i != 0 {
                q.push_str(", ");
            }
            q.push_str(t.v());
        }
        q.push_str(") AS `q` FROM `tt`");
        self.test_statement(&q, flags)?;

        Ok(())
    }

    pub fn test_function_mult(
        &mut self,
        function: &str,
        arg_types: &[&[ArgType]],
        flags: usize,
    ) -> Result<()> {
        if let Some(idx) = arg_types.iter().position(|v| v.len() > 1) {
            let mut arg_types2 = arg_types.to_vec();
            for arg_type in arg_types[idx].chunks(1) {
                arg_types2[idx] = arg_type;
                self.test_function_mult(function, &arg_types2, flags)?;
            }
        } else {
            let arg_types: Vec<_> = arg_types.iter().map(|v| *v.first().unwrap()).collect();
            self.test_function(function, &arg_types, flags)?
        }
        Ok(())
    }
}
