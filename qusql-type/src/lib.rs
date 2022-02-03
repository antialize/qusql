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
mod typer;

mod ref_or_val;
pub mod schema;
pub use ref_or_val::RefOrVal;
pub use type_::Type;

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
    use sql_ast::{parse_statements, Level};

    use crate::{schema::parse_schemas, type_statement::type_statement, typer::Typer};

    #[test]
    fn it_works() {
        let src = std::fs::read_to_string("schema.sql").expect("Failed to read file");
        let (schemas, issues) = parse_schemas(&src);

        if !issues.is_empty() {
            let mut files = SimpleFiles::new();
            let file_id = files.add("schema.sql", &src);
            let writer = StandardStream::stderr(ColorChoice::Always);
            let err = issues.iter().any(|i| i.level == Level::Error);
            let config = codespan_reporting::term::Config::default();
            for issue in issues {
                let mut labels = vec![Label::primary(file_id, issue.span)];
                for (message, span) in issue.fragments {
                    labels.push(Label::secondary(file_id, span).with_message(message));
                }
                let d = match issue.level {
                    Level::Error => Diagnostic::error(),
                    Level::Warning => Diagnostic::warning(),
                };
                let d = d.with_message(issue.message).with_labels(labels);
                term::emit(&mut writer.lock(), &config, &files, &d).unwrap();
            }
            if err {
                panic!("Errors parsing schema");
            }
        }

        let src = std::fs::read_to_string("qs.sql").expect("Failed to read file");

        let (statements, mut issues) = parse_statements(&src);

        for statement in statements {
            let mut typer = Typer {
                schemas: &schemas,
                issues: &mut issues,
                reference_types: Vec::new(),
            };
            type_statement(&mut typer, &statement);
        }

        if !issues.is_empty() {
            let mut files = SimpleFiles::new();
            let file_id = files.add("qs.sql", &src);
            let writer = StandardStream::stderr(ColorChoice::Always);
            let err = issues.iter().any(|i| i.level == Level::Error);
            let config = codespan_reporting::term::Config::default();
            for issue in issues {
                let mut labels = vec![Label::primary(file_id, issue.span)];
                for (message, span) in issue.fragments {
                    labels.push(Label::secondary(file_id, span).with_message(message));
                }
                let d = match issue.level {
                    Level::Error => Diagnostic::error(),
                    Level::Warning => Diagnostic::warning(),
                };
                let d = d.with_message(issue.message).with_labels(labels);
                term::emit(&mut writer.lock(), &config, &files, &d).unwrap();
            }
            if err {
                panic!("Error in statements");
            }
        }

        //println!("HI {:#?}", schema);
        panic!("HERE");
    }
}
