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

use alloc::{format, string::ToString, vec};
use core::ops::Deref;
use sql_parse::{issue_todo, Expression, Issue, Span, UnaryOperator, Variable};

use crate::{
    schema::parse_column,
    type_::{ArgType, BaseType, FullType},
    type_binary_expression::type_binary_expression,
    type_function::type_function,
    type_select::{resolve_kleene_identifier, type_union_select},
    typer::Typer,
    Type,
};

#[derive(Clone, Copy, Default)]
pub struct ExpressionFlags {
    pub true_: bool,
    pub not_null: bool,
    pub in_on_duplicate_key_update: bool,
}

impl ExpressionFlags {
    pub fn with_true(self, true_: bool) -> Self {
        Self { true_, ..self }
    }

    pub fn with_not_null(self, not_null: bool) -> Self {
        Self { not_null, ..self }
    }

    pub fn with_in_on_duplicate_key_update(self, in_on_duplicate_key_update: bool) -> Self {
        Self {
            in_on_duplicate_key_update,
            ..self
        }
    }

    pub fn without_values(self) -> Self {
        Self {
            not_null: false,
            true_: false,
            ..self
        }
    }
}

fn type_unary_expression<'a>(
    typer: &mut Typer<'a, '_>,
    op: &UnaryOperator,
    op_span: &Span,
    operand: &Expression<'a>,
    flags: ExpressionFlags,
) -> FullType<'a> {
    match op {
        UnaryOperator::Binary
        | UnaryOperator::Collate
        | UnaryOperator::LogicalNot
        | UnaryOperator::Minus => {
            let _op_type = type_expression(typer, operand, flags.with_true(false), BaseType::Any);
            typer.issues.push(issue_todo!(op_span));
            FullType::invalid()
        }
        UnaryOperator::Not => {
            let op_type = type_expression(typer, operand, flags.with_true(false), BaseType::Bool);
            typer.ensure_base(operand, &op_type, BaseType::Bool);
            op_type
        }
    }
}

pub(crate) fn type_expression<'a>(
    typer: &mut Typer<'a, '_>,
    expression: &Expression<'a>,
    flags: ExpressionFlags,
    _context: BaseType,
) -> FullType<'a> {
    match expression {
        Expression::Binary {
            op,
            op_span,
            lhs,
            rhs,
        } => type_binary_expression(typer, op, op_span, lhs, rhs, flags),
        Expression::Unary {
            op,
            op_span,
            operand,
        } => type_unary_expression(typer, op, op_span, operand, flags),
        Expression::Subquery(select) => {
            let select_type = type_union_select(typer, select, false);
            if let [v] = select_type.columns.as_slice() {
                let mut r = v.type_.clone();
                r.not_null = false;
                r
            } else {
                typer
                    .issues
                    .push(Issue::err("Subquery should yield one column", select));
                FullType::invalid()
            }
        }
        Expression::ListHack(v) => {
            typer
                .issues
                .push(Issue::err("_LIST_ only allowed in IN ()", v));
            FullType::invalid()
        }
        Expression::Null(_) => FullType::new(Type::Null, false),
        Expression::Bool(_, _) => FullType::new(BaseType::Bool, true),
        Expression::String(_) => FullType::new(BaseType::String, true),
        Expression::Integer(_) => FullType::new(BaseType::Integer, true),
        Expression::Float(_) => FullType::new(BaseType::Float, true),
        Expression::Function(func, args, span) => type_function(typer, func, args, span, flags),
        Expression::WindowFunction {
            function,
            args,
            function_span,
            over_span: _,
            window_spec,
        } => {
            for (e, _) in &window_spec.order_by.1 {
                type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
            }
            type_function(typer, function, args, function_span, flags)
        }
        Expression::Identifier(i) => {
            let mut t = None;
            match i.as_slice() {
                [part] => {
                    let col = match part {
                        sql_parse::IdentifierPart::Name(n) => n,
                        sql_parse::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    let mut cnt = 0;
                    for r in &mut typer.reference_types {
                        for c in &mut r.columns {
                            if c.0 == col.value {
                                cnt += 1;
                                if flags.not_null {
                                    c.1.not_null = true;
                                }
                                t = Some(c);
                            }
                        }
                    }
                    if cnt > 1 {
                        let mut issue = Issue::err("Ambiguous reference", col);
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
                        sql_parse::IdentifierPart::Name(n) => n,
                        sql_parse::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    let col = match p2 {
                        sql_parse::IdentifierPart::Name(n) => n,
                        sql_parse::IdentifierPart::Star(v) => {
                            typer.issues.push(Issue::err("Not supported here", v));
                            return FullType::invalid();
                        }
                    };
                    for r in &mut typer.reference_types {
                        if r.name == Some(tbl.value) {
                            for c in &mut r.columns {
                                if c.0 == col.value {
                                    if flags.not_null {
                                        c.1.not_null = true;
                                    }
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
        Expression::Arg((idx, span)) => FullType::new(
            Type::Args(BaseType::Any, vec![(*idx, ArgType::Normal, span.clone())]),
            false,
        ),
        Expression::Exists(s) => {
            type_union_select(typer, s, false);
            FullType::new(BaseType::Bool, true)
        }
        Expression::In {
            lhs, rhs, in_span, ..
        } => {
            let f2 = if flags.true_ {
                flags.with_not_null(true).with_true(false)
            } else {
                flags
            };

            let mut lhs_type = type_expression(typer, lhs, f2, BaseType::Any);
            let mut not_null = lhs_type.not_null;
            // Hack to allow null arguments on the right hand side of an in expression
            // where the lhs is not null
            lhs_type.not_null = false;
            for rhs in rhs {
                let rhs_type = match rhs {
                    Expression::Subquery(q) => {
                        let rhs_type = type_union_select(typer, q, false);
                        if rhs_type.columns.len() != 1 {
                            typer.issues.push(Issue::err(
                                format!(
                                    "Subquery in IN should yield one column but gave {}",
                                    rhs_type.columns.len()
                                ),
                                q,
                            ))
                        }
                        if let Some(c) = rhs_type.columns.first() {
                            c.type_.clone()
                        } else {
                            FullType::invalid()
                        }
                    }
                    Expression::ListHack((idx, span)) => FullType::new(
                        Type::Args(BaseType::Any, vec![(*idx, ArgType::ListHack, span.clone())]),
                        false,
                    ),
                    _ => type_expression(typer, rhs, flags.without_values(), BaseType::Any),
                };
                not_null &= rhs_type.not_null;
                if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                    typer.issues.push(
                        Issue::err("Incompatible types", in_span)
                            .frag(lhs_type.t.to_string(), lhs)
                            .frag(rhs_type.to_string(), rhs),
                    );
                }
            }
            FullType::new(BaseType::Bool, not_null)
        }
        Expression::Is(e, is, _) => {
            let (flags, base_type) = match is {
                sql_parse::Is::Null => (flags.without_values(), BaseType::Any),
                sql_parse::Is::NotNull => {
                    if flags.true_ {
                        (flags.with_not_null(true).with_true(false), BaseType::Any)
                    } else {
                        (flags.with_not_null(false), BaseType::Any)
                    }
                }
                sql_parse::Is::True
                | sql_parse::Is::NotTrue
                | sql_parse::Is::False
                | sql_parse::Is::NotFalse => (flags.without_values(), BaseType::Bool),
                sql_parse::Is::Unknown | sql_parse::Is::NotUnknown => {
                    (flags.without_values(), BaseType::Any)
                }
            };
            let t = type_expression(typer, e, flags, base_type);
            match is {
                sql_parse::Is::Null => {
                    if t.not_null {
                        typer.issues.push(Issue::warn("Cannot be null", e));
                    }
                    FullType::new(BaseType::Bool, true)
                }
                sql_parse::Is::NotNull
                | sql_parse::Is::True
                | sql_parse::Is::NotTrue
                | sql_parse::Is::False
                | sql_parse::Is::NotFalse => FullType::new(BaseType::Bool, true),
                sql_parse::Is::Unknown | sql_parse::Is::NotUnknown => {
                    typer.issues.push(issue_todo!(expression));
                    FullType::invalid()
                }
            }
        }
        Expression::Invalid(_) => FullType::invalid(),
        Expression::Case { .. } => {
            typer.issues.push(issue_todo!(expression));
            FullType::invalid()
        }
        Expression::Cast {
            expr,
            as_span,
            type_,
            ..
        } => {
            let col = parse_column(type_.clone(), "", as_span.clone(), typer.issues);
            if typer.dialect().is_maria() {
                match type_.type_ {
                    sql_parse::Type::Char(_)
                    | sql_parse::Type::Date
                    | sql_parse::Type::Inet4
                    | sql_parse::Type::Inet6
                    | sql_parse::Type::DateTime(_)
                    | sql_parse::Type::Double(_)
                    | sql_parse::Type::Float8
                    | sql_parse::Type::Float(_)
                    | sql_parse::Type::Integer(_)
                    | sql_parse::Type::Int(_)
                    | sql_parse::Type::Binary(_)
                    | sql_parse::Type::Timestamptz
                    | sql_parse::Type::Time(_) => {}
                    sql_parse::Type::Boolean
                    | sql_parse::Type::TinyInt(_)
                    | sql_parse::Type::SmallInt(_)
                    | sql_parse::Type::BigInt(_)
                    | sql_parse::Type::VarChar(_)
                    | sql_parse::Type::TinyText(_)
                    | sql_parse::Type::MediumText(_)
                    | sql_parse::Type::Text(_)
                    | sql_parse::Type::LongText(_)
                    | sql_parse::Type::Enum(_)
                    | sql_parse::Type::Set(_)
                    | sql_parse::Type::Numeric(_, _, _)
                    | sql_parse::Type::Timestamp(_)
                    | sql_parse::Type::TinyBlob(_)
                    | sql_parse::Type::MediumBlob(_)
                    | sql_parse::Type::Blob(_)
                    | sql_parse::Type::LongBlob(_)
                    | sql_parse::Type::Json
                    | sql_parse::Type::Bit(_, _)
                    | sql_parse::Type::Bytea
                    | sql_parse::Type::Named(_) // TODO lookup name
                    | sql_parse::Type::VarBinary(_) => {
                        typer
                            .issues
                            .push(Issue::err("Type not allow in cast", type_));
                    }
                };
            } else {
                //TODO check me
            }
            let e = type_expression(typer, expr, flags, col.type_.base());
            //TODO check if it can possible be valid cast
            FullType::new(col.type_.t, e.not_null)
        }
        Expression::Count { expr, .. } => {
            match expr.deref() {
                Expression::Identifier(parts) => {
                    resolve_kleene_identifier(typer, parts, &None, |_, _, _, _| {})
                }
                arg => {
                    type_expression(typer, arg, flags.without_values(), BaseType::Any);
                }
            }
            FullType::new(BaseType::Integer, true)
        }
        Expression::GroupConcat { expr, .. } => {
            type_expression(typer, expr, flags.without_values(), BaseType::Any);
            FullType::new(BaseType::String, true)
        }
        Expression::Variable {
            variable,
            variable_span,
            ..
        } => match variable {
            Variable::TimeZone => FullType::new(BaseType::String, true),
            Variable::Other(_) => {
                typer
                    .issues
                    .push(Issue::err("Unknown variable", variable_span));
                FullType::new(BaseType::Any, false)
            }
        },
    }
}
