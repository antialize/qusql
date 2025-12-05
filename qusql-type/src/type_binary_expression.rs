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
use sql_parse::{BinaryOperator, Expression, Span};

use crate::{
    Type,
    type_::{BaseType, FullType},
    type_expression::{ExpressionFlags, type_expression},
    typer::{Restrict, Typer},
};

pub(crate) fn type_binary_expression<'a>(
    typer: &mut Typer<'a, '_>,
    op: &BinaryOperator,
    op_span: &Span,
    lhs: &Expression<'a>,
    rhs: &Expression<'a>,
    flags: ExpressionFlags,
) -> FullType<'a> {
    let (flags, context) = match op {
        BinaryOperator::And => {
            if flags.true_ {
                (flags.with_not_null(true), BaseType::Bool)
            } else {
                (flags, BaseType::Bool)
            }
        }
        BinaryOperator::Or | BinaryOperator::Xor => (flags.without_values(), BaseType::Bool),
        BinaryOperator::NullSafeEq => (flags.without_values(), BaseType::Any),
        BinaryOperator::Eq
        | BinaryOperator::GtEq
        | BinaryOperator::Gt
        | BinaryOperator::LtEq
        | BinaryOperator::Lt
        | BinaryOperator::Neq
        | BinaryOperator::Add
        | BinaryOperator::Subtract
        | BinaryOperator::Divide
        | BinaryOperator::Div
        | BinaryOperator::Mod
        | BinaryOperator::Mult => {
            if flags.true_ {
                (flags.with_not_null(true).with_true(false), BaseType::Any)
            } else {
                (flags, BaseType::Any)
            }
        }
        BinaryOperator::Like | BinaryOperator::NotLike => {
            if flags.true_ {
                (flags.with_not_null(true).with_true(false), BaseType::String)
            } else {
                (flags, BaseType::String)
            }
        }
        BinaryOperator::ShiftLeft
        | BinaryOperator::ShiftRight
        | BinaryOperator::BitAnd
        | BinaryOperator::BitOr
        | BinaryOperator::BitXor => {
            if flags.true_ {
                (
                    flags.with_not_null(true).with_true(false),
                    BaseType::Integer,
                )
            } else {
                (flags, BaseType::Integer)
            }
        }
    };

    let lhs_type = type_expression(typer, lhs, flags, context);
    let rhs_type = type_expression(typer, rhs, flags, context);
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
                typer.warn("Comparison with null", lhs);
            }
            if rhs_type.t == Type::Null {
                typer.warn("Comparison with null", rhs);
            }
            if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                typer
                    .err("Type error in comparison", op_span)
                    .frag(format!("Of type {}", lhs_type.t), lhs)
                    .frag(format!("Of type {}", rhs_type.t), rhs);
            }
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::NullSafeEq => {
            if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                typer
                    .err("Type error in comparison", op_span)
                    .frag(format!("Of type {}", lhs_type.t), lhs)
                    .frag(format!("Of type {}", rhs_type.t), rhs);
            }
            FullType::new(BaseType::Bool, true)
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
        BinaryOperator::Add | BinaryOperator::Subtract => {
            if matches!(lhs_type.base(), BaseType::TimeInterval) {
                let t = typer.ensure_datetime(op_span, &rhs_type, Restrict::Allow, Restrict::Allow);
                FullType::new(t, lhs_type.not_null && rhs_type.not_null)
            } else if matches!(rhs_type.base(), BaseType::TimeInterval) {
                let t = typer.ensure_datetime(op_span, &lhs_type, Restrict::Allow, Restrict::Allow);
                FullType::new(t, lhs_type.not_null && rhs_type.not_null)
            } else if let Some(t) = typer.matched_type(&lhs_type, &rhs_type) {
                match t.base() {
                    BaseType::Any | BaseType::Float | BaseType::Integer => {
                        FullType::new(t, lhs_type.not_null && rhs_type.not_null)
                    }
                    _ => {
                        typer
                            .err("Type error in addition/subtraction", op_span)
                            .frag(format!("type {}", lhs_type.t), lhs)
                            .frag(format!("type {}", rhs_type.t), rhs);
                        FullType::invalid()
                    }
                }
            } else {
                typer
                    .err("Type error in addition/subtraction", op_span)
                    .frag(format!("type {}", lhs_type.t), lhs)
                    .frag(format!("type {}", rhs_type.t), rhs);
                FullType::invalid()
            }
        }
        BinaryOperator::Divide
        | BinaryOperator::Div
        | BinaryOperator::Mod
        | BinaryOperator::Mult => {
            if let Some(t) = typer.matched_type(&lhs_type, &rhs_type) {
                match t.base() {
                    BaseType::Any | BaseType::Float | BaseType::Integer => {
                        FullType::new(t, lhs_type.not_null && rhs_type.not_null)
                    }
                    _ => {
                        typer
                            .err("Type error in multiplication/division", op_span)
                            .frag(format!("type {}", lhs_type.t), lhs)
                            .frag(format!("type {}", rhs_type.t), rhs);
                        FullType::invalid()
                    }
                }
            } else {
                typer
                    .err("Type error in multiplication/division", op_span)
                    .frag(format!("type {}", lhs_type.t), lhs)
                    .frag(format!("type {}", rhs_type.t), rhs);
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
