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

use sql_ast::{issue_todo, BinaryOperator, Expression, Issue, Span};

use crate::{type_::FullType, type_expression::type_expression, typer::Typer, Type};

pub(crate) fn type_binary_expression<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
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
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::ShiftLeft | BinaryOperator::ShiftRight => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::Add | BinaryOperator::Subtract => {
            //TODO This is not the right type
            if let Some(t) = typer.common_type(&lhs_type, &rhs_type) {
                t
            } else {
                typer.issues.push(
                    Issue::err("Type error in addition/subtraction", op_span)
                        .frag(format!("type {}", lhs_type.t), lhs)
                        .frag(format!("type {}", rhs_type.t), rhs),
                );
                FullType::invalid()
            }
        }
        BinaryOperator::Divide => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::Div => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::Mod => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::Mult => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::Like | BinaryOperator::NotLike => {
            typer.ensure_text(lhs, &lhs_type);
            typer.ensure_text(rhs, &rhs_type);
            FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
        }
    }
}
