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
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

//! Crate for typing SQL statements.
//!
//! ```
//! use sql_type::{schema::parse_schemas, type_statement, TypeOptions,
//!     SQLDialect, SQLArguments, StatementType};
//! let schemas = "
//!     CREATE TABLE `events` (
//!       `id` bigint(20) NOT NULL,
//!       `user` int(11) NOT NULL,
//!       `message` text NOT NULL
//!     );";
//!
//! let mut issues = Vec::new();
//!
//! // Compute terse representation of the schemas
//! let schemas = parse_schemas(schemas,
//!     &mut issues,
//!     &TypeOptions::new().dialect(SQLDialect::MariaDB));
//! assert!(issues.is_empty());
//!
//! let sql = "SELECT `id`, `user`, `message` FROM `events` WHERE `id` = ?";
//! let stmt = type_statement(&schemas, sql, &mut issues,
//!     &TypeOptions::new().dialect(SQLDialect::MariaDB).arguments(SQLArguments::QuestionMark));
//! assert!(issues.is_empty());
//!
//! let stmt = match stmt {
//!     StatementType::Select{columns, arguments} => {
//!         assert_eq!(columns.len(), 3);
//!         assert_eq!(arguments.len(), 1);
//!     }
//!     _ => panic!("Expected select statement")
//! };
//! ```

extern crate alloc;

use alloc::vec::Vec;
use schema::Schemas;
use sql_parse::{parse_statement, ParseOptions};
pub use sql_parse::{Issue, Level};

mod type_;
mod type_binary_expression;
mod type_delete;
mod type_expression;
mod type_function;
mod type_insert_replace;
mod type_reference;
mod type_select;
mod type_statement;
mod type_update;
mod typer;

mod ref_or_val;
pub mod schema;
pub use ref_or_val::RefOrVal;
pub use type_::{BaseType, FullType, Type};
pub use type_insert_replace::AutoIncrementId;
pub use type_select::SelectTypeColumn;
use typer::Typer;

pub use sql_parse::{SQLArguments, SQLDialect};

/// Options used when typing sql or parsing a schema
#[derive(Debug, Default, Clone)]
pub struct TypeOptions {
    parse_options: ParseOptions,
    warn_unnamed_column_in_select: bool,
}

impl TypeOptions {
    /// Produce new default options
    pub fn new() -> Self {
        Default::default()
    }

    /// Change what sql dialect is used
    pub fn dialect(self, dialect: SQLDialect) -> Self {
        Self {
            parse_options: self.parse_options.dialect(dialect),
            ..self
        }
    }

    /// Change how sql arguments are supplied
    pub fn arguments(self, arguments: SQLArguments) -> Self {
        Self {
            parse_options: self.parse_options.arguments(arguments),
            ..self
        }
    }

    /// Should we warn about unquoted identifiers
    pub fn warn_unquoted_identifiers(self, warn_unquoted_identifiers: bool) -> Self {
        Self {
            parse_options: self
                .parse_options
                .warn_unquoted_identifiers(warn_unquoted_identifiers),
            ..self
        }
    }

    /// Should we warn about keywords not in ALL CAPS
    pub fn warn_none_capital_keywords(self, warn_none_capital_keywords: bool) -> Self {
        Self {
            parse_options: self
                .parse_options
                .warn_none_capital_keywords(warn_none_capital_keywords),
            ..self
        }
    }

    /// Should we warn about unnamed columns in selects
    pub fn warn_unnamed_column_in_select(self, warn_unnamed_column_in_select: bool) -> Self {
        Self {
            warn_unnamed_column_in_select,
            ..self
        }
    }
}

/// Key of argument
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ArgumentKey<'a> {
    /// Index of unnamed argument
    Index(usize),
    /// Name of named argument
    Identifier(&'a str),
}

/// Type information of typed statement
#[derive(Debug, Clone)]
pub enum StatementType<'a> {
    /// The statement was a select statement
    Select {
        /// The types and named of the columns return from the select
        columns: Vec<SelectTypeColumn<'a>>,
        /// The key and type of arguments to the query
        arguments: Vec<(ArgumentKey<'a>, FullType<'a>)>,
    },
    /// The statement is a delete statement
    Delete {
        /// The key and type of arguments to the query
        arguments: Vec<(ArgumentKey<'a>, FullType<'a>)>,
    },
    /// The statement is an insert statement
    Insert {
        /// The insert happend in a table with a auto increment id row
        yield_autoincrement: AutoIncrementId,
        /// The key and type of arguments to the query
        arguments: Vec<(ArgumentKey<'a>, FullType<'a>)>,
    },
    /// The statement is a update statement
    Update {
        /// The key and type of arguments to the query
        arguments: Vec<(ArgumentKey<'a>, FullType<'a>)>,
    },
    /// The statement is a replace statement
    Replace {
        /// The key and type of arguments to the query
        arguments: Vec<(ArgumentKey<'a>, FullType<'a>)>,
    },
    /// The query was not valid, errors are preset in issues
    Invalid,
}

/// Type an sql statement with respect to a given schema
pub fn type_statement<'a>(
    schemas: &'a Schemas<'a>,
    statement: &'a str,
    issues: &mut Vec<Issue>,
    options: &TypeOptions,
) -> StatementType<'a> {
    if let Some(stmt) = parse_statement(statement, issues, &options.parse_options) {
        let mut typer = Typer {
            schemas,
            issues,
            reference_types: Vec::new(),
            arg_types: Default::default(),
            options,
        };
        let t = type_statement::type_statement(&mut typer, &stmt);
        let arguments = typer.arg_types;
        match t {
            type_statement::InnerStatementType::Select(s) => StatementType::Select {
                columns: s.columns,
                arguments,
            },
            type_statement::InnerStatementType::Delete => StatementType::Delete { arguments },
            type_statement::InnerStatementType::Insert { auto_increment_id } => {
                StatementType::Insert {
                    yield_autoincrement: auto_increment_id,
                    arguments,
                }
            }
            type_statement::InnerStatementType::Update => StatementType::Update { arguments },
            type_statement::InnerStatementType::Replace => StatementType::Replace { arguments },
            type_statement::InnerStatementType::Invalid => StatementType::Invalid,
        }
    } else {
        StatementType::Invalid
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use codespan_reporting::{
        diagnostic::{Diagnostic, Label},
        files::SimpleFiles,
        term::{
            self,
            termcolor::{ColorChoice, StandardStream},
        },
    };
    use sql_parse::{Issue, Level, SQLArguments, SQLDialect};

    use crate::{
        schema::parse_schemas, type_statement, ArgumentKey, AutoIncrementId, BaseType, FullType,
        SelectTypeColumn, StatementType, Type, TypeOptions,
    };

    struct N<'a>(Option<&'a str>);
    impl<'a> alloc::fmt::Display for N<'a> {
        fn fmt(&self, f: &mut alloc::fmt::Formatter<'_>) -> alloc::fmt::Result {
            if let Some(v) = self.0 {
                v.fmt(f)
            } else {
                f.write_str("None")
            }
        }
    }

    fn check_no_errors(name: &str, src: &str, issues: &[Issue], errors: &mut usize) {
        let mut files = SimpleFiles::new();
        let file_id = files.add(name, &src);
        let writer = StandardStream::stderr(ColorChoice::Always);
        let config = codespan_reporting::term::Config::default();
        for issue in issues {
            let mut labels = vec![Label::primary(file_id, issue.span.clone())];
            for (message, span) in &issue.fragments {
                labels.push(Label::secondary(file_id, span.clone()).with_message(message));
            }
            let d = match issue.level {
                Level::Error => {
                    *errors += 1;
                    Diagnostic::error()
                }
                Level::Warning => Diagnostic::warning(),
            };
            let d = d.with_message(&issue.message).with_labels(labels);
            term::emit(&mut writer.lock(), &config, &files, &d).unwrap();
        }
    }

    fn str_to_type(t: &str) -> FullType<'static> {
        let (t, not_null) = if let Some(t) = t.strip_suffix('!') {
            (t, true)
        } else {
            (t, false)
        };
        let t = match t {
            "b" => BaseType::Bool.into(),
            "u8" => Type::U8,
            "u16" => Type::U16,
            "u32" => Type::U32,
            "u64" => Type::U64,
            "i8" => Type::I8,
            "i16" => Type::I16,
            "i32" => Type::I32,
            "i64" => Type::I64,
            "f32" => Type::F32,
            "f64" => Type::F64,
            "i" => BaseType::Integer.into(),
            "f" => BaseType::Float.into(),
            "str" => BaseType::String.into(),
            "bytes" => BaseType::Bytes.into(),
            "dt" => BaseType::DateTime.into(),
            _ => panic!("Unknown type {}", t),
        };
        FullType::new(t, not_null)
    }

    fn check_arguments(
        name: &str,
        got: &[(ArgumentKey<'_>, FullType<'_>)],
        expected: &str,
        errors: &mut usize,
    ) {
        if expected.is_empty() {
            for (cnt, value) in got.iter().enumerate() {
                println!("{}: Unexpected argument {} type {:?}", name, cnt, value);
                *errors += 1;
            }
            return;
        }
        let mut got2 = Vec::new();
        let inv = FullType::invalid();
        for (k, v) in got {
            match k {
                ArgumentKey::Index(i) => {
                    while got2.len() <= *i {
                        got2.push(&inv);
                    }
                    got2[*i] = v;
                }
                ArgumentKey::Identifier(k) => {
                    println!("{}: Got named argument {}", name, k);
                    *errors += 1;
                }
            }
        }
        let mut cnt = 0;
        for (i, t) in expected.split(',').enumerate() {
            let t = t.trim();
            let t = str_to_type(t);
            if let Some(v) = got2.get(i) {
                if *v != &t {
                    println!("{}: Expected type {} for argument {} got {}", name, t, i, v);
                    *errors += 1;
                }
            } else {
                println!("{}: Expected type {} for argument {} got None", name, t, i);
                *errors += 1;
            }
            cnt += 1;
        }
        while cnt < got.len() {
            println!("{}: Unexpected argument {} type {:?}", name, cnt, got[cnt]);
            cnt += 1;
            *errors += 1;
        }
    }

    fn check_columns(name: &str, got: &[SelectTypeColumn<'_>], expected: &str, errors: &mut usize) {
        let mut cnt = 0;
        for (i, t) in expected.split(',').enumerate() {
            let t = t.trim();
            let (cname, t) = t.split_once(":").unwrap();
            let t = str_to_type(t);
            let cname = if cname.is_empty() { None } else { Some(cname) };
            if let Some(v) = got.get(i) {
                if v.name != cname || v.type_ != t {
                    println!(
                        "{}: Expected column {} with name {} of type {} got {} of type {}",
                        name,
                        i,
                        N(cname),
                        t,
                        N(v.name),
                        v.type_
                    );
                    *errors += 1;
                }
            } else {
                println!(
                    "{}: Expected column {} with name {} of type {} got None",
                    name,
                    i,
                    N(cname),
                    t
                );
                *errors += 1;
            }
            cnt += 1;
        }
        while cnt < got.len() {
            println!(
                "{}: Unexpected column {} with name {} of type {}",
                name,
                cnt,
                N(got[cnt].name),
                got[cnt].type_
            );
            cnt += 1;
            *errors += 1;
        }
    }

    #[test]
    fn mariadb() {
        let schema_src = "

        DROP TABLE IF EXISTS `t1`;
        CREATE TABLE `t1` (
          `id` int(11) NOT NULL,
          `cbool` tinyint(1) NOT NULL,
          `cu8` tinyint UNSIGNED NOT NULL,
          `cu16` smallint UNSIGNED NOT NULL,
          `cu32` int UNSIGNED NOT NULL,
          `cu64` bigint UNSIGNED NOT NULL,
          `ci8` tinyint,
          `ci16` smallint,
          `ci32` int,
          `ci64` bigint,
          `ctext` varchar(100) NOT NULL,
          `cbytes` blob,
          `cf32` float,
          `cf64` double
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

        ALTER TABLE `t1`
          MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

        CREATE TABLE `t2` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `t1_id` int(11) NOT NULL);
        ";

        let options = TypeOptions::new().dialect(SQLDialect::MariaDB);
        let mut issues = Vec::new();
        let schema = parse_schemas(schema_src, &mut issues, &options);
        let mut errors = 0;
        check_no_errors("schema", schema_src, &issues, &mut errors);

        issues.clear();
        let options = TypeOptions::new()
            .dialect(SQLDialect::MariaDB)
            .arguments(SQLArguments::QuestionMark);

        {
            let name = "q1";
            let src =
                "SELECT `id`, `cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ci8`, `ci16`, `ci32`, `ci64`,
                `ctext`, `cbytes`, `cf32`, `cf64` FROM `t1` WHERE ci8 IS NOT NULL
                AND `cbool`=? AND `cu8`=? AND `cu16`=? AND `cu32`=? AND `cu64`=?
                AND `ci8`=? AND `ci16`=? AND `ci32`=? AND `ci64`=?
                AND `ctext`=? AND `cbytes`=? AND `cf32`=? AND `cf64`=?";

            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Select { arguments, columns } = q {
                check_arguments(
                    name,
                    &arguments,
                    "b,i,i,i,i,i,i,i,i,str,bytes,f,f",
                    &mut errors,
                );
                check_columns(
                    name,
                    &columns,
                    "id:i32!,cbool:b!,cu8:u8!,cu16:u16!,cu32:u32!,cu64:u64!,
                    ci8:i8!,ci16:i16,ci32:i32,ci64:i64,ctext:str!,cbytes:bytes,cf32:f32,cf64:f64",
                    &mut errors,
                );
            } else {
                println!("{} should be select", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q2";
            let src =
            "INSERT INTO `t1` (`cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ci8`, `ci16`, `ci32`, `ci64`,
            `ctext`, `cbytes`, `cf32`, `cf64`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Insert {
                arguments,
                yield_autoincrement,
            } = q
            {
                check_arguments(
                    name,
                    &arguments,
                    "b!,u8!,u16!,u32!,u64!,i8,i16,i32,i64,str!,bytes,f32,f64",
                    &mut errors,
                );
                if yield_autoincrement != AutoIncrementId::Yes {
                    println!("{} should yield autoincrement", name);
                    errors += 1;
                }
            } else {
                println!("{} should be insert", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q3";
            let src =
                "DELETE `t1` FROM `t1`, `t2` WHERE `t1`.`id` = `t2`.`t1_id` AND `t2`.`id` = ?";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Delete { arguments } = q {
                check_arguments(name, &arguments, "i", &mut errors);
            } else {
                println!("{} should be delete", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q4";
            let src = "INSERT INTO `t2` (`t1_id`) VALUES (?) ON DUPLICATE KEY UPDATE `t1_id`=?";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Insert {
                arguments,
                yield_autoincrement,
            } = q
            {
                check_arguments(name, &arguments, "i32!,i32!", &mut errors);
                if yield_autoincrement != AutoIncrementId::Optional {
                    println!("{} should yield optional auto increment", name);
                    errors += 1;
                }
            } else {
                println!("{} should be insert", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q5";
            let src = "INSERT IGNORE INTO `t2` SET `t1_id`=?";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Insert {
                arguments,
                yield_autoincrement,
            } = q
            {
                check_arguments(name, &arguments, "i32!", &mut errors);
                if yield_autoincrement != AutoIncrementId::Optional {
                    println!("{} should yield optional auto increment", name);
                    errors += 1;
                }
            } else {
                println!("{} should be insert", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q6";
            let src = "SELECT IF(`ci32` IS NULL, `cbool`, ?) AS `cc` FROM `t1`";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Select { arguments, columns } = q {
                check_arguments(name, &arguments, "b", &mut errors);
                check_columns(name, &columns, "cc:b", &mut errors);
            } else {
                println!("{} should be select", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q7";
            let src = "SELECT FROM_UNIXTIME(CAST(UNIX_TIMESTAMP() AS DOUBLE)) AS `cc` FROM `t1` WHERE `id`=?";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Select { arguments, columns } = q {
                check_arguments(name, &arguments, "i", &mut errors);
                check_columns(name, &columns, "cc:dt!", &mut errors);
            } else {
                println!("{} should be select", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q8";
            let src = "REPLACE INTO `t2` SET `id` = ?, `t1_id`=?";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Replace { arguments } = q {
                check_arguments(name, &arguments, "i32!,i32!", &mut errors);
            } else {
                println!("{} should be replace", name);
                errors += 1;
            }
        }

        {
            issues.clear();
            let name = "q9";
            let src = "INSERT INTO `t2` (`t1_id`) VALUES (32) ON DUPLICATE KEY UPDATE `t1_id` = `t1_id` + VALUES(`t1_id`)";
            let q = type_statement(&schema, src, &mut issues, &options);
            check_no_errors(name, src, &issues, &mut errors);
            if let StatementType::Insert { arguments, .. } = q {
                check_arguments(name, &arguments, "", &mut errors);
            } else {
                println!("{} should be insert", name);
                errors += 1;
            }
        }

        if errors != 0 {
            panic!("{} errors in test", errors);
        }
    }
}
