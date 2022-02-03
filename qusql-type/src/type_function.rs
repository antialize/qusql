use sql_ast::{Expression, Function, Issue, Span};

use crate::{
    type_::FullType,
    type_expression::{self, type_expression},
    type_select::resolve_kleene_identifier,
    typer::Typer,
    Type,
};

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
    if matches!(func, Function::Count) {
        arg_cnt(typer, 1..1, args, span);
        match &args[0] {
            Expression::Identifier(parts) => {
                resolve_kleene_identifier(typer, parts, &None, |_, _, _, _| {})
            }
            arg => {
                type_expression(typer, arg, false);
            }
        }
        return FullType::new(Type::Integer, true);
    }

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
        Function::JsonExtract => {
            arg_cnt(typer, 2..999, args, span);
            for (a, t) in &typed {
                typer.ensure_text(*a, t);
            }
            // TODO this can have any type, we do not currently have an any type so we return
            // invalid
            FullType::new(Type::Invalid, false)
        }
        Function::JsonUnquote => {
            arg_cnt(typer, 1..1, args, span);
            for (a, t) in &typed {
                typer.ensure_text(*a, t);
            }
            FullType::new(Type::Text, false)
        }
        Function::Right | Function::Left => {
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((a, t)) = typed.get(0) {
                not_null = not_null && t.not_null;
                typer.ensure_text(*a, t);
            }
            if let Some((a, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_type(*a, t, &FullType::new(Type::Integer, false));
            }
            FullType::new(Type::Text, not_null)
        }
        _ => {
            typer.issues.push(Issue::todo(span));
            FullType::invalid()
        }
    }
}
