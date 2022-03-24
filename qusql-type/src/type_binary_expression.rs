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

use alloc::format;
use sql_parse::{issue_todo, BinaryOperator, Expression, Issue, Span};

use crate::{
    type_::{BaseType, FullType},
    type_expression::{type_expression, ExpressionFlags},
    typer::Typer,
    Type,
};

pub(crate) fn type_binary_expression<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    op: &BinaryOperator,
    op_span: &Span,
    lhs: &Expression<'a>,
    rhs: &Expression<'a>,
    flags: ExpressionFlags,
) -> FullType<'a> {
    let flags = match op {
        BinaryOperator::And =>
            if flags.true_ {
                flags.with_not_null(true)
            } else {
                flags
            }
        BinaryOperator::Or | BinaryOperator::Xor | BinaryOperator::NullSafeEq => flags.without_values(),
        BinaryOperator::Eq |
        BinaryOperator::GtEq |
        BinaryOperator::Gt |
        BinaryOperator::LtEq |
        BinaryOperator::Lt |
        BinaryOperator::Neq |
        BinaryOperator::ShiftLeft |
        BinaryOperator::ShiftRight |
        BinaryOperator::BitAnd |
        BinaryOperator::BitOr |
        BinaryOperator::BitXor |
        BinaryOperator::Add |
        BinaryOperator::Subtract |
        BinaryOperator::Divide |
        BinaryOperator::Div |
        BinaryOperator::Mod |
        BinaryOperator::Mult |
        BinaryOperator::Like |
        BinaryOperator::NotLike => {
            if flags.true_ {
                flags.with_not_null(true).with_true(false)
            } else {
                flags
            }
        }
    };


    let lhs_type = type_expression(typer, lhs, flags);
    let rhs_type = type_expression(typer, rhs, flags);
    match op {
        BinaryOperator::Or | BinaryOperator::Xor | BinaryOperator::And => {
            typer.ensure_base(lhs, &lhs_type, BaseType::Bool);
            typer.ensure_base(rhs, &rhs_type, BaseType::Bool);
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
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
            if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                typer.issues.push(
                    Issue::err("Type error in comparison", op_span)
                        .frag(format!("Of type {}", lhs_type.t), lhs)
                        .frag(format!("Of type {}", rhs_type.t), rhs),
                );
            }
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::NullSafeEq => {
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        BinaryOperator::ShiftLeft
        | BinaryOperator::ShiftRight
        | BinaryOperator::BitAnd
        | BinaryOperator::BitOr
        | BinaryOperator::BitXor => {
            typer.ensure_base(lhs, &lhs_type, BaseType::Integer);
            typer.ensure_base(rhs, &rhs_type, BaseType::Integer);
            FullType::new(BaseType::Integer, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Add
        | BinaryOperator::Subtract
        | BinaryOperator::Divide
        | BinaryOperator::Div
        | BinaryOperator::Mod
        | BinaryOperator::Mult => {
            if let Some(t) = typer.matched_type(&lhs_type, &rhs_type) {
                match t.base() {
                    BaseType::Any | BaseType::Float | BaseType::Integer => {
                        FullType::new(t, lhs_type.not_null && rhs_type.not_null)
                    }
                    _ => {
                        typer.issues.push(
                            Issue::err("Type error in addition/subtraction", op_span)
                                .frag(format!("type {}", lhs_type.t), lhs)
                                .frag(format!("type {}", rhs_type.t), rhs),
                        );
                        FullType::invalid()
                    }
                }
            } else {
                typer.issues.push(
                    Issue::err("Type error in addition/subtraction", op_span)
                        .frag(format!("type {}", lhs_type.t), lhs)
                        .frag(format!("type {}", rhs_type.t), rhs),
                );
                FullType::invalid()
            }
        }
        BinaryOperator::Like | BinaryOperator::NotLike => {
            typer.ensure_base(lhs, &lhs_type, BaseType::String);
            typer.ensure_base(rhs, &rhs_type, BaseType::String);
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
    }
}
