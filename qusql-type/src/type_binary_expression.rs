use sql_ast::{BinaryOperator, Expression, Issue, Span};

use crate::{type_::FullType, type_expression::type_expression, typer::Typer, Type};

pub(crate) fn type_binary_expression<'a>(
    typer: &mut Typer<'a>,
    op: &BinaryOperator,
    op_span: &Span,
    lhs: &Expression<'a>,
    rhs: &Expression<'a>,
    outer_where: bool,
) -> FullType<'a> {
    let outer_where = matches!(op, BinaryOperator::And) && outer_where;
    let lhs_type = type_expression(typer, lhs, outer_where);
    let rhs_type = type_expression(typer, rhs, outer_where);
    match op {
        BinaryOperator::Or | BinaryOperator::Xor | BinaryOperator::And => {
            typer.ensure_bool(lhs, &lhs_type);
            typer.ensure_bool(rhs, &rhs_type);
            FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Eq
        | BinaryOperator::Neq
        | BinaryOperator::GtEq
        | BinaryOperator::Gt
        | BinaryOperator::LtEq
        | BinaryOperator::Lt => {
            if lhs_type.t == Type::Null {
                typer.issues.push(Issue::warn("Comparison with null", lhs));
            }
            if rhs_type.t == Type::Null {
                typer.issues.push(Issue::warn("Comparison with null", rhs));
            }
            if typer.common_type(&lhs_type, &rhs_type).is_none() {
                typer.issues.push(
                    Issue::err("Type error in comparison", op_span)
                        .frag(format!("Of type {}", lhs_type.t), lhs)
                        .frag(format!("Of type {}", rhs_type.t), rhs),
                );
            }
            FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::NullSafeEq => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::ShiftLeft | BinaryOperator::ShiftRight => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Add => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Subtract => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Divide => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Div => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Mod => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Mult => {
            typer.issues.push(Issue::todo(op_span));
            FullType::invalid()
        }
        BinaryOperator::Like | BinaryOperator::NotLike => {
            typer.ensure_text(lhs, &lhs_type);
            typer.ensure_text(rhs, &rhs_type);
            FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
        }
    }
}
