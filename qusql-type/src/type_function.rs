use sql_ast::{Expression, Function, Issue, Span};

use crate::{type_::FullType, type_expression::type_expression, typer::Typer, Type};

pub(crate) fn type_function<'a>(
    typer: &mut Typer<'a>,
    func: &Function<'a>,
    args: &Vec<Expression<'a>>,
    span: &Span,
) -> FullType<'a> {
    let mut arg_types = Vec::new();
    for arg in args {
        arg_types.push(type_expression(typer, arg, false));
    }
    match func {
        Function::UnixTimestamp => {
            if let Some(args) = args.get(1..) {
                for arg in args {
                    typer.issues.push(Issue::err("Unexpected argument", arg));
                }
            }
            if let Some(arg) = args.get(0) {
                typer.issues.push(Issue::todo(arg));
            }
            FullType::new(Type::U64, true)
        }
        Function::Rand => {
            if let Some(args) = args.get(1..) {
                for arg in args {
                    typer.issues.push(Issue::err("Unexpected argument", arg));
                }
            }
            if let Some(arg) = args.get(0) {
                typer.issues.push(Issue::todo(arg));
            }
            FullType::new(Type::F64, true)
        }
        _ => {
            typer.issues.push(Issue::todo(span));
            FullType::invalid()
        }
    }
}
