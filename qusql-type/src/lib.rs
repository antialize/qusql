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

use schema::Schemas;
use sql_ast::{parse_statement, ParseOptions};
pub use sql_ast::{Issue, Level};

mod type_;
mod type_binary_expression;
mod type_delete;
mod type_expression;
mod type_function;
mod type_insert;
mod type_reference;
mod type_select;
mod type_statement;
mod type_update;
mod typer;

mod ref_or_val;
pub mod schema;
pub use ref_or_val::RefOrVal;
pub use type_::FullType;
pub use type_::Type;
pub use type_select::SelectTypeColumn;
use typer::Typer;

#[derive(Debug, Clone)]
pub enum StatementType<'a> {
    Select {
        columns: Vec<SelectTypeColumn<'a>>,
        arguments: Vec<FullType<'a>>,
    },
    Delete {
        arguments: Vec<FullType<'a>>,
    },
    Insert {
        yield_autoincrement: bool,
        arguments: Vec<FullType<'a>>,
    },
    Update {
        arguments: Vec<FullType<'a>>,
    },
    Replace {
        arguments: Vec<FullType<'a>>,
    },
    Invalid,
}

pub fn type_statement<'a>(
    schemas: &'a Schemas<'a>,
    statement: &'a str,
    issues: &mut Vec<Issue>,
    options: &ParseOptions,
) -> StatementType<'a> {
    if let Some(stmt) = parse_statement(statement, issues, options) {
        let mut typer = Typer {
            schemas: &schemas,
            issues,
            reference_types: Vec::new(),
            arg_types: Vec::new(),
        };
        let t = type_statement::type_statement(&mut typer, &stmt);
        let arguments = typer.arg_types;
        match t {
            type_statement::InnerStatementType::Select(s) => StatementType::Select {
                columns: s.columns,
                arguments,
            },
            type_statement::InnerStatementType::Delete => StatementType::Delete { arguments },
            type_statement::InnerStatementType::Insert { auto_increment } => {
                StatementType::Insert {
                    yield_autoincrement: auto_increment,
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
    use codespan_reporting::{
        diagnostic::{Diagnostic, Label},
        files::SimpleFiles,
        term::{
            self,
            termcolor::{ColorChoice, StandardStream},
        },
    };
    use sql_ast::{Issue, Level, ParseOptions, SQLArguments, SQLDialect};

    use crate::{
        schema::parse_schemas, type_statement, FullType, SelectTypeColumn, StatementType, Type,
    };

    struct N<'a>(Option<&'a str>);
    impl<'a> std::fmt::Display for N<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            "b" => Type::Bool,
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
            "text" => Type::Text,
            "bytes" => Type::Bytes,
            _ => panic!("Unknown type {}", t),
        };
        FullType::new(t, not_null)
    }

    fn check_arguments(name: &str, got: &[FullType<'_>], expected: &str, errors: &mut usize) {
        let mut cnt = 0;
        for (i, t) in expected.split(",").enumerate() {
            let t = t.trim();
            let t = str_to_type(t);
            if let Some(v) = got.get(i) {
                if v != &t {
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
        for (i, t) in expected.split(",").enumerate() {
            let t = t.trim();
            let (cname, t) = t.split_once(":").unwrap();
            let t = str_to_type(t);
            let cname = if cname == "" { None } else { Some(cname) };
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
        ";

        let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
        let mut issues = Vec::new();
        let schema = parse_schemas(schema_src, &mut issues, &options);
        let mut errors = 0;
        check_no_errors("schema", &schema_src, &issues, &mut errors);

        let options = ParseOptions::new()
            .dialect(SQLDialect::MariaDB)
            .arguments(SQLArguments::QuestionMark);

        let q1_src =
            "SELECT `id`, `cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ci8`, `ci16`, `ci32`, `ci64`, 
            `ctext`, `cbytes`, `cf32`, `cf64` FROM `t1` WHERE ci8 IS NOT NULL
            AND `cbool`=? AND `cu8`=? AND `cu16`=? AND `cu32`=? AND `cu64`=?
            AND `ci8`=? AND `ci16`=? AND `ci32`=? AND `ci64`=?
            AND `ctext`=? AND `cbytes`=? AND `cf32`=? AND `cf64`=?";

        let q1 = type_statement(&schema, q1_src, &mut issues, &options);
        check_no_errors("q1", &q1_src, &issues, &mut errors);
        if let StatementType::Select { arguments, columns } = q1 {
            check_arguments(
                "q1",
                &arguments,
                "b!,u8!,u16!,u32!,u64!,i8!,i16,i32,i64,text!,bytes,f32,f64",
                &mut errors,
            );
            check_columns(
                "q1",
                &columns,
                "id:i32!,cbool:b!,cu8:u8!,cu16:u16!,cu32:u32!,cu64:u64!,
                ci8:i8!,ci16:i16,ci32:i32,ci64:i64,ctext:text!,cbytes:bytes,cf32:f32,cf64:f64",
                &mut errors,
            );
        } else {
            println!("q1 should be select");
            errors += 1;
        }

        if errors != 0 {
            panic!("{} errors in test", errors);
        }
    }
}
