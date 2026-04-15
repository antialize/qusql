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

use alloc::{format, vec::Vec};
use qusql_parse::{BinaryOperator, Expression, Spanned};

use crate::{
    Type,
    type_::{BaseType, FullType},
    type_expression::{ExpressionFlags, type_expression},
    typer::{Restrict, Typer},
};

pub(crate) fn type_binary_expression<'a>(
    typer: &mut Typer<'a, '_>,
    op: &BinaryOperator<'a>,
    lhs: &Expression<'a>,
    rhs: &Expression<'a>,
    flags: ExpressionFlags,
) -> FullType<'a> {
    let op_span = op.span();

    let (flags, context) = match op {
        BinaryOperator::Assignment(_) => (flags, BaseType::Any),
        BinaryOperator::And(_) => {
            if flags.true_ {
                (flags.with_not_null(true), BaseType::Bool)
            } else {
                (flags, BaseType::Bool)
            }
        }
        BinaryOperator::Or(_) if flags.true_ => {
            // Special case for OR in an assert-true context: a column is only not_null if
            // *both* branches independently imply it is not_null. Process each branch with
            // the true_ context, snapshot/restore reference_types, then intersect.
            let child_flags = flags.with_not_null(true);

            let snapshot_not_null: Vec<Vec<bool>> = typer
                .reference_types
                .iter()
                .map(|rt| rt.columns.iter().map(|c| c.1.not_null).collect())
                .collect();

            let lhs_type = type_expression(typer, lhs, child_flags, BaseType::Bool);

            let after_lhs_not_null: Vec<Vec<bool>> = typer
                .reference_types
                .iter()
                .map(|rt| rt.columns.iter().map(|c| c.1.not_null).collect())
                .collect();

            // Restore only not_null flags (keeping any other mutations from lhs) before rhs.
            for (rt, snap_nn) in typer
                .reference_types
                .iter_mut()
                .zip(snapshot_not_null.iter())
            {
                for (col, &nn) in rt.columns.iter_mut().zip(snap_nn.iter()) {
                    col.1.not_null = nn;
                }
            }

            let rhs_type = type_expression(typer, rhs, child_flags, BaseType::Bool);

            // Intersection: a column is only not_null if both branches independently set it.
            for (cur, lhs_nn) in typer
                .reference_types
                .iter_mut()
                .zip(after_lhs_not_null.iter())
            {
                for (cur_col, &lhs_col_nn) in cur.columns.iter_mut().zip(lhs_nn.iter()) {
                    cur_col.1.not_null = cur_col.1.not_null && lhs_col_nn;
                }
            }

            typer.ensure_base(lhs, &lhs_type, BaseType::Bool);
            typer.ensure_base(rhs, &rhs_type, BaseType::Bool);
            return FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null);
        }
        BinaryOperator::Or(_) => (flags.without_values(), BaseType::Bool),
        BinaryOperator::Xor(_) => (flags.without_values(), BaseType::Bool),
        BinaryOperator::NullSafeEq(_) => (flags.without_values(), BaseType::Any),
        BinaryOperator::Eq(_)
        | BinaryOperator::GtEq(_)
        | BinaryOperator::Gt(_)
        | BinaryOperator::LtEq(_)
        | BinaryOperator::Lt(_)
        | BinaryOperator::Neq(_)
        | BinaryOperator::Add(_)
        | BinaryOperator::Subtract(_)
        | BinaryOperator::Divide(_)
        | BinaryOperator::Div(_)
        | BinaryOperator::Mod(_)
        | BinaryOperator::Mult(_) => {
            if flags.true_ {
                (flags.with_not_null(true).with_true(false), BaseType::Any)
            } else {
                (flags, BaseType::Any)
            }
        }
        BinaryOperator::Like(_)
        | BinaryOperator::NotLike(_)
        | BinaryOperator::Regexp(_)
        | BinaryOperator::NotRegexp(_)
        | BinaryOperator::Rlike(_)
        | BinaryOperator::NotRlike(_) => {
            if flags.true_ {
                (flags.with_not_null(true).with_true(false), BaseType::String)
            } else {
                (flags, BaseType::String)
            }
        }
        BinaryOperator::ShiftLeft(_)
        | BinaryOperator::ShiftRight(_)
        | BinaryOperator::BitAnd(_)
        | BinaryOperator::BitOr(_)
        | BinaryOperator::BitXor(_) => {
            if flags.true_ {
                (
                    flags.with_not_null(true).with_true(false),
                    BaseType::Integer,
                )
            } else {
                (flags, BaseType::Integer)
            }
        }
        BinaryOperator::Collate(_) => (flags, BaseType::String),
        BinaryOperator::Concat(_) => (flags.without_values(), BaseType::String),
        BinaryOperator::JsonExtract(_) => (flags, BaseType::String), // JSON value returned
        BinaryOperator::JsonExtractUnquote(_) => (flags, BaseType::String), // Unquoted string
        BinaryOperator::User(_, _) => (flags, BaseType::Any),
        BinaryOperator::Contains(_)
        | BinaryOperator::ContainedBy(_)
        | BinaryOperator::JsonPathMatch(_)
        | BinaryOperator::JsonPathExists(_)
        | BinaryOperator::JsonbKeyExists(_)
        | BinaryOperator::JsonbAnyKeyExists(_)
        | BinaryOperator::JsonbAllKeyExists(_) => (flags.without_values(), BaseType::Any),
        BinaryOperator::JsonGetPath(_)
        | BinaryOperator::JsonGetPathText(_)
        | BinaryOperator::JsonDeletePath(_) => (flags, BaseType::Any),
        BinaryOperator::RegexMatch(_)
        | BinaryOperator::RegexIMatch(_)
        | BinaryOperator::NotRegexMatch(_)
        | BinaryOperator::NotRegexIMatch(_) => (flags.without_values(), BaseType::String),
        BinaryOperator::Operator(_, _) => (flags, BaseType::Any),
    };

    let lhs_type = type_expression(typer, lhs, flags, context);
    let rhs_type = type_expression(typer, rhs, flags, context);
    match op {
        BinaryOperator::Or(_) => {
            typer.ensure_base(lhs, &lhs_type, BaseType::Bool);
            typer.ensure_base(rhs, &rhs_type, BaseType::Bool);
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Concat(_) => {
            // `||` is string/array/jsonb concatenation (PostgreSQL and ANSI SQL).
            if let Some(t) = typer.matched_type(&lhs_type, &rhs_type) {
                return FullType::new(t, lhs_type.not_null && rhs_type.not_null);
            }
            typer.ensure_base(lhs, &lhs_type, BaseType::String);
            typer.ensure_base(rhs, &rhs_type, BaseType::String);
            FullType::new(BaseType::String, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Xor(_) | BinaryOperator::And(_) => {
            typer.ensure_base(lhs, &lhs_type, BaseType::Bool);
            typer.ensure_base(rhs, &rhs_type, BaseType::Bool);
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Eq(_)
        | BinaryOperator::Neq(_)
        | BinaryOperator::GtEq(_)
        | BinaryOperator::Gt(_)
        | BinaryOperator::LtEq(_)
        | BinaryOperator::Lt(_) => {
            if lhs_type.t == Type::Null {
                typer.warn("Comparison with null", lhs);
            }
            if rhs_type.t == Type::Null {
                typer.warn("Comparison with null", rhs);
            }
            if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                typer
                    .err("Type error in comparison", &op_span)
                    .frag(format!("Of type {}", lhs_type.t), lhs)
                    .frag(format!("Of type {}", rhs_type.t), rhs);
            }
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::NullSafeEq(_) => {
            if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                typer
                    .err("Type error in comparison", &op_span)
                    .frag(format!("Of type {}", lhs_type.t), lhs)
                    .frag(format!("Of type {}", rhs_type.t), rhs);
            }
            FullType::new(BaseType::Bool, true)
        }
        BinaryOperator::ShiftLeft(_)
        | BinaryOperator::ShiftRight(_)
        | BinaryOperator::BitAnd(_)
        | BinaryOperator::BitOr(_)
        | BinaryOperator::BitXor(_) => {
            typer.ensure_base(lhs, &lhs_type, BaseType::Integer);
            typer.ensure_base(rhs, &rhs_type, BaseType::Integer);
            FullType::new(BaseType::Integer, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Add(_) | BinaryOperator::Subtract(_) => {
            if matches!(lhs_type.base(), BaseType::TimeInterval) {
                let t =
                    typer.ensure_datetime(&op_span, &rhs_type, Restrict::Allow, Restrict::Allow);
                FullType::new(t, lhs_type.not_null && rhs_type.not_null)
            } else if matches!(rhs_type.base(), BaseType::TimeInterval) {
                let t =
                    typer.ensure_datetime(&op_span, &lhs_type, Restrict::Allow, Restrict::Allow);
                FullType::new(t, lhs_type.not_null && rhs_type.not_null)
            } else if let Some(t) = typer.matched_type(&lhs_type, &rhs_type) {
                match t.base() {
                    BaseType::Any | BaseType::Float | BaseType::Integer => {
                        FullType::new(t, lhs_type.not_null && rhs_type.not_null)
                    }
                    _ => {
                        typer
                            .err("Type error in addition/subtraction", &op_span)
                            .frag(format!("type {}", lhs_type.t), lhs)
                            .frag(format!("type {}", rhs_type.t), rhs);
                        FullType::invalid()
                    }
                }
            } else {
                typer
                    .err("Type error in addition/subtraction", &op_span)
                    .frag(format!("type {}", lhs_type.t), lhs)
                    .frag(format!("type {}", rhs_type.t), rhs);
                FullType::invalid()
            }
        }
        BinaryOperator::Divide(_)
        | BinaryOperator::Div(_)
        | BinaryOperator::Mod(_)
        | BinaryOperator::Mult(_) => {
            if let Some(t) = typer.matched_type(&lhs_type, &rhs_type) {
                match t.base() {
                    BaseType::Any | BaseType::Float | BaseType::Integer => {
                        FullType::new(t, lhs_type.not_null && rhs_type.not_null)
                    }
                    _ => {
                        typer
                            .err("Type error in multiplication/division", &op_span)
                            .frag(format!("type {}", lhs_type.t), lhs)
                            .frag(format!("type {}", rhs_type.t), rhs);
                        FullType::invalid()
                    }
                }
            } else {
                typer
                    .err("Type error in multiplication/division", &op_span)
                    .frag(format!("type {}", lhs_type.t), lhs)
                    .frag(format!("type {}", rhs_type.t), rhs);
                FullType::invalid()
            }
        }
        BinaryOperator::Like(_)
        | BinaryOperator::NotLike(_)
        | BinaryOperator::Regexp(_)
        | BinaryOperator::NotRegexp(_)
        | BinaryOperator::Rlike(_)
        | BinaryOperator::NotRlike(_) => {
            typer.ensure_base(lhs, &lhs_type, BaseType::String);
            typer.ensure_base(rhs, &rhs_type, BaseType::String);
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Collate(_) => {
            // COLLATE: LHS is the expression, RHS is the collation name (identifier)
            // Just return the LHS type as the collation doesn't change the type
            typer.ensure_base(lhs, &lhs_type, BaseType::String);
            lhs_type
        }
        BinaryOperator::JsonExtract(_) | BinaryOperator::JsonExtractUnquote(_) => {
            // JSON operators: -> returns JSON, ->> returns unquoted string
            // LHS is the JSON document, RHS is the path (string)
            typer.ensure_base(rhs, &rhs_type, BaseType::String);
            FullType::new(BaseType::String, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Contains(_)
        | BinaryOperator::ContainedBy(_)
        | BinaryOperator::JsonPathMatch(_)
        | BinaryOperator::JsonPathExists(_)
        | BinaryOperator::JsonbKeyExists(_)
        | BinaryOperator::JsonbAnyKeyExists(_)
        | BinaryOperator::JsonbAllKeyExists(_) => {
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::JsonGetPath(_) | BinaryOperator::JsonGetPathText(_) => {
            FullType::new(BaseType::String, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::JsonDeletePath(_) => {
            // Returns the modified jsonb value
            FullType::new(BaseType::Any, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::RegexMatch(_)
        | BinaryOperator::RegexIMatch(_)
        | BinaryOperator::NotRegexMatch(_)
        | BinaryOperator::NotRegexIMatch(_) => {
            typer.ensure_base(lhs, &lhs_type, BaseType::String);
            typer.ensure_base(rhs, &rhs_type, BaseType::String);
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        BinaryOperator::Assignment(_) => {
            // Assignment: @var := value
            // Returns the type of the value being assigned (rhs)
            rhs_type
        }
        BinaryOperator::User(_, _) => {
            FullType::new(BaseType::Any, lhs_type.not_null && rhs_type.not_null)
        }
        o @ BinaryOperator::Operator(_, _) => {
            typer.err("Not supported", o);
            FullType::invalid()
        }
    }
}
