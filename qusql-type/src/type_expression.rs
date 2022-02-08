use sql_ast::{issue_todo, Expression, Issue, Span, UnaryOperator};

use crate::{
    type_::FullType, type_binary_expression::type_binary_expression, type_function::type_function,
    type_select::type_select, typer::Typer, Type,
};

fn type_unary_expression<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    op: &UnaryOperator,
    op_span: &Span,
    operand: &Expression<'a>,
) -> FullType<'a> {
    let op_type = type_expression(typer, operand, false);
    match op {
        UnaryOperator::Binary
        | UnaryOperator::Collate
        | UnaryOperator::LogicalNot
        | UnaryOperator::Minus => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        UnaryOperator::Not => {
            typer.ensure_bool(operand, &op_type);
            op_type
        }
    }
}

pub(crate) fn type_expression<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
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
        } => type_unary_expression(typer, op, op_span, operand),
        Expression::Subquery(select) => {
            let select_type = type_select(typer, select, false);
            if let [v] = select_type.columns.as_slice() {
                v.type_.clone()
            } else {
                typer
                    .issues
                    .push(Issue::err("Subquery should yield one column", select));
                FullType::invalid()
            }
        }
        Expression::Null(_) => FullType::new(Type::Null, false),
        Expression::Bool(_, _) => FullType::new(Type::Bool, true),
        Expression::String(_) => FullType::new(Type::Text, true),
        Expression::Integer(_) => FullType::new(Type::Integer, true),
        Expression::Float(_) => {
            typer.issues.push(issue_todo!(expression));
            FullType::new(Type::Float, true)
        }
        Expression::Function(func, args, span) => type_function(typer, func, args, span),
        Expression::Identifier(i) => {
            let mut t = None;
            match i.as_slice() {
                [part] => {
                    let col = match part {
                        sql_ast::IdentifierPart::Name(n) => n,
                        sql_ast::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    let mut cnt = 0;
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            if c.0 == col.value {
                                cnt += 1;
                                t = Some(c);
                            }
                        }
                    }
                    if cnt > 1 {
                        let mut issue = Issue::err("Ambigious reference", col);
                        for r in &typer.reference_types {
                            for c in &r.columns {
                                if c.0 == col.value {
                                    issue = issue.frag("Defined here", &r.span);
                                }
                            }
                        }
                        typer.issues.push(issue);
                        return FullType::invalid();
                    }
                }
                [p1, p2] => {
                    let tbl = match p1 {
                        sql_ast::IdentifierPart::Name(n) => n,
                        sql_ast::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    let col = match p2 {
                        sql_ast::IdentifierPart::Name(n) => n,
                        sql_ast::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    for r in &typer.reference_types {
                        if r.name == Some(tbl.value) {
                            for c in &r.columns {
                                if c.0 == col.value {
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
        Expression::Exists(s) => {
            type_select(typer, s, false);
            FullType::new(Type::Bool, true)
        }
        Expression::In {
            lhs, rhs, in_span, ..
        } => {
            let mut lhs_type = type_expression(typer, lhs, false);
            let mut not_null = lhs_type.not_null;
            // Hack to allow null arguments on the right hand side of an in expression
            // where the lhs is not null
            lhs_type.not_null = false;
            for rhs in rhs {
                let rhs_type = if let Expression::Subquery(q) = rhs {
                    let rhs_type = type_select(typer, q, false);
                    if rhs_type.columns.len() != 1 {
                        typer.issues.push(Issue::err(
                            format!(
                                "Subquery in IN should yield one column but gave {}",
                                rhs_type.columns.len()
                            ),
                            q,
                        ))
                    }
                    if let Some(c) = rhs_type.columns.get(0) {
                        c.type_.clone()
                    } else {
                        FullType::invalid()
                    }
                } else {
                    type_expression(typer, rhs, false)
                };
                not_null = not_null & rhs_type.not_null;
                if typer.common_type(&lhs_type, &rhs_type).is_none() {
                    typer.issues.push(
                        Issue::err("Incompatible types", in_span)
                            .frag(lhs_type.t.to_string(), lhs)
                            .frag(rhs_type.to_string(), rhs),
                    );
                }
            }
            FullType::new(Type::Bool, not_null)
        }
        Expression::Is(e, is, _) => {
            let t = type_expression(typer, e, false);
            match is {
                sql_ast::Is::Null => {
                    if t.not_null {
                        typer.issues.push(Issue::warn("Cannot be null", e));
                    }
                    FullType::new(Type::Bool, true)
                }
                sql_ast::Is::NotNull => {
                    if t.not_null {
                        typer.issues.push(Issue::warn("Cannot be null", e));
                    }
                    if outer_where {
                        // If were are in the outer part of a where expression possibly behind ands,
                        // and the expression is an identifier, we can mark the columns not_null
                        // the reference_types
                        if let Expression::Identifier(parts) = e.as_ref() {
                            if let Some(sql_ast::IdentifierPart::Name(n0)) = parts.get(0) {
                                if parts.len() == 1 {
                                    for r in &mut typer.reference_types {
                                        for c in &mut r.columns {
                                            if c.0 == n0.value {
                                                c.1.not_null = true;
                                            }
                                        }
                                    }
                                } else if let Some(sql_ast::IdentifierPart::Name(n1)) = parts.get(1)
                                {
                                    for r in &mut typer.reference_types {
                                        if r.name == Some(n0.value) {
                                            for c in &mut r.columns {
                                                if c.0 == n1.value {
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
                    typer.issues.push(issue_todo!(expression));
                    FullType::invalid()
                }
            }
        }
        Expression::Invalid => FullType::invalid(),
        Expression::Case { .. } => {
            typer.issues.push(issue_todo!(expression));
            FullType::invalid()
        }
    }
}
