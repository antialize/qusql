use sql_ast::{Expression, Function, Issue, Span};

use crate::{type_::FullType, type_expression::type_expression, typer::Typer, Type};

fn arg_cnt<'a>(
    typer: &mut Typer<'a>,
    rng: std::ops::Range<usize>,
    args: &[Expression<'a>],
    span: &Span,
) {
    if args.len() >= rng.start && args.len() <= rng.end {
        return;
    }

    let mut issue = if rng.len() == 0 {
        Issue::err(
            format!("Expected {} arguments got {}", rng.start, args.len()),
            span,
        )
    } else {
        Issue::err(
            format!(
                "Expected between {} and {} arguments got {}",
                rng.start,
                rng.end,
                args.len()
            ),
            span,
        )
    };

    if let Some(args) = args.get(rng.end..) {
        for (cnt, arg) in args.iter().enumerate() {
            issue = issue.frag(format!("Argument {}", rng.end + cnt), arg);
        }
    }
    typer.issues.push(issue);
}

pub(crate) fn type_function<'a>(
    typer: &mut Typer<'a>,
    func: &Function<'a>,
    args: &Vec<Expression<'a>>,
    span: &Span,
) -> FullType<'a> {
    let mut typed = Vec::new();
    for arg in args {
        typed.push((arg, type_expression(typer, arg, false)));
    }
    match func {
        Function::UnixTimestamp => {
            arg_cnt(typer, 0..1, args, span);
            if let Some(arg) = args.get(0) {
                typer.issues.push(Issue::todo(arg));
            }
            FullType::new(Type::U64, true)
        }
        Function::Rand => {
            arg_cnt(typer, 0..1, args, span);
            if let Some(arg) = args.get(0) {
                typer.issues.push(Issue::todo(arg));
            }
            FullType::new(Type::F64, true)
        }
        Function::IfNull => {
            arg_cnt(typer, 2..2, args, span);
            let t = if let Some((e, t)) = typed.get(0) {
                if t.not_null {
                    typer.issues.push(Issue::warn("Cannot be null", *e));
                }
                t.clone()
            } else {
                FullType::invalid()
            };
            if let Some((e, t2)) = typed.get(0) {
                typer.ensure_type(*e, t2, &t);
            }
            t
        }
        _ => {
            typer.issues.push(Issue::todo(span));
            FullType::invalid()
        }
    }
}
