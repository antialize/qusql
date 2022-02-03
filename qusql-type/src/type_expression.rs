use sql_ast::{Expression, Issue};

use crate::{
    type_::FullType, type_binary_expression::type_binary_expression, type_function::type_function,
    typer::Typer, Type,
};

pub(crate) fn type_expression<'a>(
    typer: &mut Typer<'a>,
    expression: &Expression<'a>,
    outer_where: bool,
) -> FullType<'a> {
    match expression {
        Expression::Binary {
            op,
            op_span,
            lhs,
            rhs,
        } => type_binary_expression(typer, op, op_span, lhs, rhs, outer_where),
        Expression::Unary {
            op,
            op_span,
            operand,
        } => {
            typer.issues.push(Issue::todo(expression));
            FullType::invalid()
        }
        Expression::Subquery(_) => {
            typer.issues.push(Issue::todo(expression));
            FullType::invalid()
        }
        Expression::Null(_) => FullType::new(Type::Null, false),
        Expression::Bool(_, _) => FullType::new(Type::Bool, true),
        Expression::String(_) => FullType::new(Type::Text, true),
        Expression::Integer(_) => FullType::new(Type::Integer, true),
        Expression::Float(_) => {
            typer.issues.push(Issue::todo(expression));
            FullType::new(Type::Float, true)
        }
        Expression::Function(func, args, span) => type_function(typer, func, args, span),
        Expression::Identifier(i) => {
            let mut t = None;
            match i.len() {
                1 => {
                    let col = match &i[0] {
                        sql_ast::IdentifierPart::Name(n) => n,
                        sql_ast::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    let mut cnt = 0;
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            if c.0 == col.0 {
                                cnt += 1;
                                t = Some(c);
                            }
                        }
                    }
                    if cnt > 1 {
                        let mut issue = Issue::err("Ambigious reference", col);
                        for r in &typer.reference_types {
                            for c in &r.columns {
                                if c.0 == col.0 {
                                    issue = issue.frag("Defined here", &r.name);
                                }
                            }
                        }
                        typer.issues.push(issue);
                        return FullType::invalid();
                    }
                }
                2 => {
                    let tbl = match &i[0] {
                        sql_ast::IdentifierPart::Name(n) => n,
                        sql_ast::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    let col = match &i[1] {
                        sql_ast::IdentifierPart::Name(n) => n,
                        sql_ast::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    for r in &typer.reference_types {
                        if r.name.0 == tbl.0 {
                            for c in &r.columns {
                                if c.0 == col.0 {
                                    t = Some(c);
                                }
                            }
                        }
                    }
                }
                _ => {
                    typer
                        .issues
                        .push(Issue::err("Bad identifier length", expression));
                    return FullType::invalid();
                }
            }
            match t {
                None => {
                    typer
                        .issues
                        .push(Issue::err("Unknown identifier", expression));
                    FullType::invalid()
                }
                Some((_, type_)) => type_.clone(),
            }
        }
        Expression::Arg((idx, span)) => FullType::new(Type::Arg(*idx, span.clone()), false),
        Expression::Exists(_) => {
            typer.issues.push(Issue::todo(expression));
            FullType::invalid()
        }
        Expression::In {
            lhs,
            rhs,
            in_span,
            not_in,
        } => {
            typer.issues.push(Issue::todo(expression));
            FullType::invalid()
        }
        Expression::Is(e, is, _) => {
            let t = type_expression(typer, e, false);
            match is {
                sql_ast::Is::Null => {
                    if t.not_null {
                        typer.issues.push(Issue::warn("Is newer null", e));
                    }
                    FullType::new(Type::Bool, true)
                }
                sql_ast::Is::NotNull => {
                    if t.not_null {
                        typer.issues.push(Issue::warn("Is newer null", e));
                    }
                    if outer_where {
                        // If were are in the outer part of a where expression possibly behind ands,
                        // and the expression is an identifier, we can mark the columns not_null
                        // the reference_types
                        if let Expression::Identifier(parts) = e.as_ref() {
                            if let Some(sql_ast::IdentifierPart::Name((n0, _))) = parts.get(0) {
                                if parts.len() == 1 {
                                    for r in &mut typer.reference_types {
                                        for c in &mut r.columns {
                                            if &c.0 == n0 {
                                                c.1.not_null = true;
                                            }
                                        }
                                    }
                                } else if let Some(sql_ast::IdentifierPart::Name((n1, _))) =
                                    parts.get(1)
                                {
                                    for r in &mut typer.reference_types {
                                        if &r.name.0 == n0 {
                                            for c in &mut r.columns {
                                                if &c.0 == n1 {
                                                    c.1.not_null = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    FullType::new(Type::Bool, true)
                }
                sql_ast::Is::True
                | sql_ast::Is::NotTrue
                | sql_ast::Is::False
                | sql_ast::Is::NotFalse
                | sql_ast::Is::Unknown
                | sql_ast::Is::NotUnknown => {
                    typer.issues.push(Issue::todo(expression));
                    FullType::invalid()
                }
            }
        }
        Expression::Invalid => FullType::invalid(),
        Expression::Case {
            case_span,
            value,
            whens,
            else_,
            end_span,
        } => {
            typer.issues.push(Issue::todo(expression));
            FullType::invalid()
        }
    }
}
