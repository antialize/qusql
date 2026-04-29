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

use alloc::{boxed::Box, format, string::ToString, sync::Arc, vec};
use qusql_parse::{Expression, Identifier, Spanned, UnaryOperator, Variable, issue_todo};

use crate::{
    Type,
    schema::parse_column,
    type_::{ArgType, BaseType, FullType},
    type_binary_expression::type_binary_expression,
    type_function::{type_aggregate_function, type_function},
    type_select::type_union_select,
    typer::{Restrict, Typer, did_you_mean},
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
    operand: &Expression<'a>,
    flags: ExpressionFlags,
) -> FullType<'a> {
    let op_span = op.span();
    match op {
        UnaryOperator::Binary(_) | UnaryOperator::LogicalNot(_) | UnaryOperator::Minus(_) => {
            let op_type = type_expression(typer, operand, flags.with_true(false), BaseType::Any);
            let t = match &op_type.t {
                Type::F32
                | Type::F64
                | Type::I16
                | Type::I24
                | Type::I32
                | Type::I64
                | Type::I8
                | Type::Invalid
                | Type::Base(BaseType::Integer)
                | Type::Base(BaseType::Float) => op_type.t,
                Type::Args(..)
                | Type::Base(..)
                | Type::Enum(..)
                | Type::JSON
                | Type::Geometry
                | Type::Range(..)
                | Type::Array(..)
                | Type::Set(..) => {
                    typer.err(format!("Expected numeric type got {}", op_type.t), &op_span);
                    Type::Invalid
                }
                Type::U16 => Type::I16,
                Type::U24 => Type::I24,
                Type::U32 => Type::I32,
                Type::U64 => Type::I64,
                Type::U8 => Type::I8,
                Type::Null => Type::Null,
            };
            FullType::new(t, op_type.not_null)
        }
        UnaryOperator::Not(_) => {
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
    context: BaseType,
) -> FullType<'a> {
    match expression {
        Expression::Binary(e) => type_binary_expression(typer, &e.op, &e.lhs, &e.rhs, flags),
        Expression::Unary(e) => type_unary_expression(typer, &e.op, &e.operand, flags),
        Expression::Subquery(e) => {
            let select_type = type_union_select(typer, &e.expression, false);
            if let [v] = select_type.columns.as_slice() {
                let mut r = v.type_.clone();
                r.not_null = false;
                r
            } else {
                typer.err("Subquery should yield one column", &e.expression);
                FullType::invalid()
            }
        }
        Expression::ListHack(v) => {
            typer.err("_LIST_ only allowed in IN ()", v);
            FullType::invalid()
        }
        Expression::Null(_) => FullType::new(Type::Null, false),
        Expression::Bool(_) => FullType::new(BaseType::Bool, true),
        Expression::String(_) => FullType::new(BaseType::String, true),
        Expression::Integer(_) => FullType::new(BaseType::Integer, true),
        Expression::Float(_) => FullType::new(BaseType::Float, true),
        Expression::Function(e) => type_function(
            typer,
            &e.function,
            &e.args,
            &e.function_span,
            flags,
            context,
        ),
        Expression::WindowFunction(e) => {
            if let Some((_, partition_by)) = &e.over.window_spec.partition_by {
                for e in partition_by {
                    type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
                }
            }
            if let Some((_, order_by)) = &e.over.window_spec.order_by {
                for (e, _) in order_by {
                    type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
                }
            }
            type_function(
                typer,
                &e.function,
                &e.args,
                &e.function_span,
                flags,
                context,
            )
        }
        Expression::AggregateFunction(e) => {
            if let Some((_, filter)) = &e.filter {
                type_expression(typer, filter, ExpressionFlags::default(), BaseType::Bool);
            }
            if let Some((_, within_group_order)) = &e.within_group {
                for (e, _) in within_group_order {
                    type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
                }
            }
            if let Some(over) = &e.over {
                if let Some((_, partition_by)) = &over.window_spec.partition_by {
                    for e in partition_by {
                        type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
                    }
                }
                if let Some((_, order_by)) = &over.window_spec.order_by {
                    for (e, _) in order_by {
                        type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
                    }
                }
            }
            type_aggregate_function(
                typer,
                &e.function,
                &e.args,
                &e.function_span,
                &e.distinct_span,
                flags,
            )
        }
        Expression::Identifier(e) => {
            let mut t: Option<FullType> = None;
            let searched_name = match e.parts.as_slice() {
                [part] => {
                    let col = match part {
                        qusql_parse::IdentifierPart::Name(n) => n,
                        qusql_parse::IdentifierPart::Star(v) => {
                            typer.err("Not supported here", v);
                            return FullType::invalid();
                        }
                    };
                    let mut cnt = 0;
                    for r in &mut typer.reference_types {
                        for c in &mut r.columns {
                            if c.0 == *col {
                                cnt += 1;
                                if flags.not_null {
                                    c.1.not_null = true;
                                }
                                t = Some(c.1.clone());
                            }
                        }
                    }
                    if cnt > 1 {
                        let mut issue = typer.issues.err("Ambiguous reference", col);
                        for r in &typer.reference_types {
                            for c in &r.columns {
                                if c.0 == *col {
                                    issue.frag("Defined here", &r.span);
                                }
                            }
                        }
                        return FullType::invalid();
                    }
                    if t.is_none() {
                        for r in &typer.outer_reference_types {
                            for c in &r.columns {
                                if c.0 == *col {
                                    t = Some(c.1.clone());
                                }
                            }
                        }
                    }
                    col.value
                }
                [p1, p2] => {
                    let tbl = match p1 {
                        qusql_parse::IdentifierPart::Name(n) => n,
                        qusql_parse::IdentifierPart::Star(v) => {
                            typer.err("Not supported here", v);
                            return FullType::invalid();
                        }
                    };
                    let col = match p2 {
                        qusql_parse::IdentifierPart::Name(n) => n,
                        qusql_parse::IdentifierPart::Star(v) => {
                            typer.err("Not supported here", v);
                            return FullType::invalid();
                        }
                    };
                    for r in &mut typer.reference_types {
                        if r.name == Some(tbl.clone()) {
                            for c in &mut r.columns {
                                if c.0 == *col {
                                    if flags.not_null {
                                        c.1.not_null = true;
                                    }
                                    t = Some(c.1.clone());
                                }
                            }
                        }
                    }
                    if t.is_none() {
                        for r in &typer.outer_reference_types {
                            if r.name == Some(tbl.clone()) {
                                for c in &r.columns {
                                    if c.0 == *col {
                                        t = Some(c.1.clone());
                                    }
                                }
                            }
                        }
                    }
                    col.value
                }
                _ => {
                    typer.err("Bad identifier length", expression);
                    return FullType::invalid();
                }
            };
            match t {
                None => {
                    let mut issue = typer.issues.err("Unknown identifier", expression);
                    let candidates = typer
                        .reference_types
                        .iter()
                        .flat_map(|r| r.columns.iter().map(|(id, _)| id.value))
                        .chain(
                            typer
                                .outer_reference_types
                                .iter()
                                .flat_map(|r| r.columns.iter().map(|(id, _)| id.value)),
                        );
                    if let Some(s) = did_you_mean(searched_name, candidates) {
                        issue.help(alloc::format!("did you mean `{s}`?"));
                    }
                    FullType::invalid()
                }
                Some(type_) => type_,
            }
        }
        Expression::Arg(e) => FullType::new(
            Type::Args(
                BaseType::Any,
                Arc::new(vec![(e.index, ArgType::Normal, e.span.clone())]),
            ),
            false,
        ),
        Expression::Exists(e) => {
            type_union_select(typer, &e.subquery, false);
            FullType::new(BaseType::Bool, true)
        }
        Expression::In(e) => {
            let f2 = if flags.true_ {
                flags.with_not_null(true).with_true(false)
            } else {
                flags
            };

            let mut lhs_type = type_expression(typer, &e.lhs, f2, BaseType::Any);
            let mut not_null = lhs_type.not_null;
            // Hack to allow null arguments on the right hand side of an in expression
            // where the lhs is not null
            lhs_type.not_null = false;
            for rhs in &e.rhs {
                let rhs_type = match rhs {
                    Expression::Subquery(q) => {
                        let rhs_type = type_union_select(typer, &q.expression, false);
                        if rhs_type.columns.len() != 1 {
                            typer.err(
                                format!(
                                    "Subquery in IN should yield one column but gave {}",
                                    rhs_type.columns.len()
                                ),
                                &q.expression,
                            );
                        }
                        if let Some(c) = rhs_type.columns.first() {
                            c.type_.clone()
                        } else {
                            FullType::invalid()
                        }
                    }
                    Expression::ListHack(e) => FullType::new(
                        Type::Args(
                            BaseType::Any,
                            Arc::new(vec![(e.index, ArgType::ListHack, e.span.clone())]),
                        ),
                        false,
                    ),
                    _ => type_expression(typer, rhs, flags.without_values(), BaseType::Any),
                };
                not_null &= rhs_type.not_null;
                if typer.matched_type(&lhs_type, &rhs_type).is_none() {
                    typer
                        .err("Incompatible types", &e.in_span)
                        .frag(lhs_type.t.to_string(), &e.lhs)
                        .frag(rhs_type.to_string(), rhs);
                }
            }
            FullType::new(BaseType::Bool, not_null)
        }
        Expression::MemberOf(e) => {
            let lhs_type = type_expression(typer, &e.lhs, flags, BaseType::Any);
            let rhs_type = type_expression(typer, &e.rhs, flags, BaseType::String); // JSON array as string
            // MEMBER OF returns boolean
            FullType::new(BaseType::Bool, lhs_type.not_null && rhs_type.not_null)
        }
        Expression::Is(e) => {
            let (flags, base_type) = match e.is {
                qusql_parse::Is::Null => (flags.without_values(), BaseType::Any),
                qusql_parse::Is::NotNull => {
                    if flags.true_ {
                        (flags.with_not_null(true).with_true(false), BaseType::Any)
                    } else {
                        (flags.with_not_null(false), BaseType::Any)
                    }
                }
                qusql_parse::Is::True
                | qusql_parse::Is::NotTrue
                | qusql_parse::Is::False
                | qusql_parse::Is::NotFalse => (flags.without_values(), BaseType::Bool),
                qusql_parse::Is::Unknown | qusql_parse::Is::NotUnknown => {
                    (flags.without_values(), BaseType::Any)
                }
                qusql_parse::Is::DistinctFrom(_) | qusql_parse::Is::NotDistinctFrom(_) => {
                    (flags.without_values(), BaseType::Any)
                }
            };
            let t = type_expression(typer, &e.lhs, flags, base_type);
            match e.is {
                qusql_parse::Is::Null => {
                    if t.not_null {
                        typer.warn("Cannot be null", &e.lhs);
                    }
                    FullType::new(BaseType::Bool, true)
                }
                qusql_parse::Is::NotNull
                | qusql_parse::Is::True
                | qusql_parse::Is::NotTrue
                | qusql_parse::Is::DistinctFrom(_)
                | qusql_parse::Is::NotDistinctFrom(_)
                | qusql_parse::Is::False
                | qusql_parse::Is::NotFalse => FullType::new(BaseType::Bool, true),
                qusql_parse::Is::Unknown | qusql_parse::Is::NotUnknown => {
                    issue_todo!(typer.issues, &e.lhs);
                    FullType::invalid()
                }
            }
        }
        Expression::Invalid(_) => FullType::invalid(),
        Expression::Case(e) => {
            if let Some(e) = &e.value {
                issue_todo!(typer.issues, e);
                FullType::invalid()
            } else {
                let not_null = true;
                let mut t: Option<Type> = None;
                for when in &e.whens {
                    let op_type = type_expression(typer, &when.when, flags, BaseType::Bool);
                    typer.ensure_base(&when.when, &op_type, BaseType::Bool);
                    let t2 = type_expression(typer, &when.then, flags, BaseType::Any);
                    if let Some(t1) = t {
                        t = typer.matched_type(&t1, &t2.t)
                    } else {
                        t = Some(t2.t);
                    }
                }
                if let Some((_, else_)) = &e.else_ {
                    let t2 = type_expression(typer, else_, flags, BaseType::Any);
                    if let Some(t1) = t {
                        t = typer.matched_type(&t1, &t2.t)
                    } else {
                        t = Some(t2.t);
                    }
                }
                if let Some(t) = t {
                    FullType::new(t, not_null)
                } else {
                    FullType::invalid()
                }
            }
        }
        Expression::Cast(e) => {
            let col = parse_column(
                e.type_.clone(),
                Identifier::new("", e.as_span.clone()),
                typer.issues,
                None,
                None,
                &[],
            );
            if typer.dialect().is_maria() {
                match e.type_.type_ {
                            qusql_parse::Type::Char(_)
                            | qusql_parse::Type::Date
                            | qusql_parse::Type::Inet4
                            | qusql_parse::Type::Inet6
                            | qusql_parse::Type::InetAddr
                            | qusql_parse::Type::Cidr
                            | qusql_parse::Type::Macaddr
                            | qusql_parse::Type::Macaddr8
                            | qusql_parse::Type::TsQuery
                            | qusql_parse::Type::TsVector
                            | qusql_parse::Type::Uuid
                            | qusql_parse::Type::Xml
                            | qusql_parse::Type::Range(_)
                            | qusql_parse::Type::MultiRange(_)
                            | qusql_parse::Type::DateTime(_)
                            | qusql_parse::Type::Double(_)
                            | qusql_parse::Type::Float8
                            | qusql_parse::Type::Float(_)
                            | qusql_parse::Type::Integer(_)
                            | qusql_parse::Type::Int(_)
                            | qusql_parse::Type::Binary(_)
                            | qusql_parse::Type::Timestamptz
                            | qusql_parse::Type::Time(_) => {}
                            qusql_parse::Type::Boolean
                            | qusql_parse::Type::TinyInt(_)
                            | qusql_parse::Type::SmallInt(_)
                            | qusql_parse::Type::BigInt(_)
                            | qusql_parse::Type::MediumInt(_)
                            | qusql_parse::Type::VarChar(_)
                            | qusql_parse::Type::TinyText(_)
                            | qusql_parse::Type::MediumText(_)
                            | qusql_parse::Type::Text(_)
                            | qusql_parse::Type::LongText(_)
                            | qusql_parse::Type::Enum(_)
                            | qusql_parse::Type::Set(_)
                            | qusql_parse::Type::Numeric(_)
                            | qusql_parse::Type::Decimal(_)
                            | qusql_parse::Type::Timestamp(_)
                            | qusql_parse::Type::TinyBlob(_)
                            | qusql_parse::Type::MediumBlob(_)
                            | qusql_parse::Type::Blob(_)
                            | qusql_parse::Type::LongBlob(_)
                            | qusql_parse::Type::Json
                            | qusql_parse::Type::Jsonb
                            | qusql_parse::Type::Bit(_, _)
                            | qusql_parse::Type::VarBit(_)
                            | qusql_parse::Type::Bytea
                            | qusql_parse::Type::Named(_) // TODO lookup name
                            | qusql_parse::Type::Array(_, _)
                            | qusql_parse::Type::VarBinary(_)
                            | qusql_parse::Type::BigSerial
                            | qusql_parse::Type::Serial
                            | qusql_parse::Type::SmallSerial
                            | qusql_parse::Type::Money
                            | qusql_parse::Type::Timetz(_)
                            | qusql_parse::Type::Interval(_)
                            | qusql_parse::Type::Point
                            | qusql_parse::Type::Line
                            | qusql_parse::Type::Lseg
                            | qusql_parse::Type::Box
                            | qusql_parse::Type::Path
                            | qusql_parse::Type::Polygon
                            | qusql_parse::Type::Circle
                            | qusql_parse::Type::Table(_, _) => {
                                typer
                                    .err("Type not allow in cast", &e.type_);
                            }
                        };
            } else {
                //TODO check me
            }
            let e = type_expression(typer, &e.expr, flags, col.type_.base());
            //TODO check if it can possible be valid cast
            FullType::new(col.type_.t, e.not_null)
        }
        Expression::GroupConcat(e) => {
            let e = type_expression(typer, &e.expr, flags.without_values(), BaseType::Any);
            FullType::new(BaseType::String, e.not_null)
        }
        Expression::Variable(e) => match &e.variable {
            Variable::TimeZone => FullType::new(BaseType::String, true),
            Variable::Other(_) => {
                typer.err("Unknown variable", e);
                FullType::new(BaseType::Any, false)
            }
        },
        Expression::Interval(e) => {
            let cnt = match e.time_unit.0 {
                qusql_parse::TimeUnit::Microsecond => 1,
                qusql_parse::TimeUnit::Second => 1,
                qusql_parse::TimeUnit::Minute => 1,
                qusql_parse::TimeUnit::Hour => 1,
                qusql_parse::TimeUnit::Day => 1,
                qusql_parse::TimeUnit::Week => 1,
                qusql_parse::TimeUnit::Month => 1,
                qusql_parse::TimeUnit::Quarter => 1,
                qusql_parse::TimeUnit::Year => 1,
                qusql_parse::TimeUnit::SecondMicrosecond => 2,
                qusql_parse::TimeUnit::MinuteMicrosecond => 3,
                qusql_parse::TimeUnit::MinuteSecond => 2,
                qusql_parse::TimeUnit::HourMicrosecond => 4,
                qusql_parse::TimeUnit::HourSecond => 3,
                qusql_parse::TimeUnit::HourMinute => 2,
                qusql_parse::TimeUnit::DayMicrosecond => 5,
                qusql_parse::TimeUnit::DaySecond => 4,
                qusql_parse::TimeUnit::DayMinute => 3,
                qusql_parse::TimeUnit::DayHour => 2,
                qusql_parse::TimeUnit::YearMonth => 2,
                qusql_parse::TimeUnit::Epoch
                | qusql_parse::TimeUnit::Dow
                | qusql_parse::TimeUnit::Doy
                | qusql_parse::TimeUnit::Century
                | qusql_parse::TimeUnit::Decade
                | qusql_parse::TimeUnit::IsoDow
                | qusql_parse::TimeUnit::IsoYear
                | qusql_parse::TimeUnit::Julian
                | qusql_parse::TimeUnit::Millennium
                | qusql_parse::TimeUnit::Timezone
                | qusql_parse::TimeUnit::TimezoneHour
                | qusql_parse::TimeUnit::TimezoneMinute => 1,
            };
            if cnt != e.time_interval.0.len() {
                typer.err(
                    format!(
                        "Expected {} values for {:?} got {}",
                        cnt,
                        e.time_unit.0,
                        e.time_interval.0.len()
                    ),
                    &e.time_interval.1,
                );
            }
            FullType::new(BaseType::TimeInterval, true)
        }
        Expression::Extract(e) => {
            let t = type_expression(typer, &e.date, flags, BaseType::Any);
            FullType::new(BaseType::Integer, t.not_null)
        }
        Expression::TimestampAdd(e) => {
            let t1 = type_expression(typer, &e.interval, flags, BaseType::Integer);
            let t2 = type_expression(typer, &e.datetime, flags, BaseType::Any);
            typer.ensure_base(&e.interval, &t1, BaseType::Integer);
            typer.ensure_datetime(&e.datetime, &t2, Restrict::Require, Restrict::Allow);
            FullType::new(BaseType::DateTime, t1.not_null && t2.not_null)
        }
        Expression::TimestampDiff(e) => {
            let t1 = type_expression(typer, &e.e1, flags, BaseType::Any);
            let t2 = type_expression(typer, &e.e2, flags, BaseType::Any);
            typer.ensure_datetime(&e.e1, &t1, Restrict::Require, Restrict::Allow);
            typer.ensure_datetime(&e.e2, &t2, Restrict::Require, Restrict::Allow);
            FullType::new(BaseType::Integer, t1.not_null && t2.not_null)
        }
        Expression::MatchAgainst(e) => {
            for col in &e.columns {
                type_expression(typer, col, flags.without_values(), BaseType::Any);
            }
            type_expression(typer, &e.expr, flags.without_values(), BaseType::String);
            FullType::new(BaseType::Float, true)
        }
        Expression::Convert(e) => {
            if let Some(type_) = &e.type_ {
                let col = parse_column(
                    type_.clone(),
                    Identifier::new("", e.convert_span.clone()),
                    typer.issues,
                    None,
                    None,
                    &[],
                );
                let inner = type_expression(typer, &e.expr, flags, col.type_.base());
                FullType::new(col.type_.t, inner.not_null)
            } else {
                // CONVERT(expr USING charset) — returns a string
                let inner = type_expression(typer, &e.expr, flags, BaseType::String);
                FullType::new(BaseType::String, inner.not_null)
            }
        }
        Expression::UserVariable(_) => FullType::new(BaseType::Any, false),
        Expression::TypeCast(e) => {
            let col = parse_column(
                e.type_.clone(),
                Identifier::new("", e.doublecolon_span.clone()),
                typer.issues,
                None,
                None,
                &[],
            );
            let inner = type_expression(typer, &e.expr, flags, col.type_.base());
            // Constrain any argument placeholders (e.g. $2::jsonb) by matching
            // inferred inner type against the cast target type.
            typer.matched_type(&col.type_.t, &inner.t);
            FullType::new(col.type_.t, inner.not_null)
        }
        Expression::Array(e) => {
            let mut element_type: Option<Type<'a>> = None;
            let mut not_null = true;
            for elem in &e.elements {
                let et = type_expression(typer, elem, flags.without_values(), BaseType::Any);
                not_null = not_null && et.not_null;
                if let Some(prev) = element_type {
                    element_type = typer.matched_type(&prev, &et.t);
                } else {
                    element_type = Some(et.t);
                }
            }
            let inner = element_type.unwrap_or(Type::Base(BaseType::Any));
            FullType::new(Type::Array(Box::new(inner)), not_null)
        }
        Expression::ArraySubscript(e) => {
            let arr_type = type_expression(typer, &e.expr, flags, BaseType::Any);
            let inner_type = if let Type::Array(inner) = arr_type.t {
                *inner
            } else if arr_type.t.base() == BaseType::Any {
                Type::Base(BaseType::Any)
            } else {
                typer.err(format!("Expected array type got {}", arr_type.t), &e.expr);
                Type::Invalid
            };
            type_expression(typer, &e.lower, flags.without_values(), BaseType::Integer);
            if let Some(upper) = &e.upper {
                type_expression(typer, upper, flags.without_values(), BaseType::Integer);
            }
            FullType::new(inner_type, false)
        }
        Expression::Default(_) => FullType::new(BaseType::Any, false),
        Expression::Between(e) => {
            let lhs_type = type_expression(typer, &e.lhs, flags.without_values(), BaseType::Any);
            let low_type = type_expression(typer, &e.low, flags.without_values(), BaseType::Any);
            let high_type = type_expression(typer, &e.high, flags.without_values(), BaseType::Any);
            if typer.matched_type(&lhs_type, &low_type).is_none() {
                typer
                    .err("Incompatible types in BETWEEN", &e.between_span)
                    .frag(lhs_type.t.to_string(), &e.lhs)
                    .frag(low_type.t.to_string(), &e.low);
            }
            if typer.matched_type(&lhs_type, &high_type).is_none() {
                typer
                    .err("Incompatible types in BETWEEN", &e.between_span)
                    .frag(lhs_type.t.to_string(), &e.lhs)
                    .frag(high_type.t.to_string(), &e.high);
            }
            FullType::new(
                BaseType::Bool,
                lhs_type.not_null && low_type.not_null && high_type.not_null,
            )
        }
        Expression::Quantifier(e) => match &e.operand {
            Expression::Subquery(q) => {
                let select_type = type_union_select(typer, &q.expression, false);
                if let [v] = select_type.columns.as_slice() {
                    let mut r = v.type_.clone();
                    r.not_null = false;
                    r
                } else {
                    typer.err(
                        "Subquery in quantifier should yield one column",
                        &q.expression,
                    );
                    FullType::invalid()
                }
            }
            _ => {
                // Array operand: ANY($1) or ANY($1::type[])
                let arr_type = type_expression(typer, &e.operand, flags, BaseType::Any);
                let inner = if let Type::Array(inner) = arr_type.t {
                    *inner
                } else {
                    Type::Base(BaseType::Any)
                };
                FullType::new(inner, false)
            }
        },
        Expression::FieldAccess(e) => {
            issue_todo!(typer.issues, e);
            FullType::invalid()
        }
        Expression::Trim(e) => {
            if let Some(what) = &e.what {
                type_expression(typer, what, flags.without_values(), BaseType::String);
            }
            let value_type =
                type_expression(typer, &e.value, flags.without_values(), BaseType::String);
            typer.ensure_base(&e.value, &value_type, BaseType::String);
            FullType::new(BaseType::String, value_type.not_null)
        }
        Expression::Char(e) => {
            let mut not_null = true;
            for arg in &e.args {
                let arg_type =
                    type_expression(typer, arg, flags.without_values(), BaseType::Integer);
                typer.ensure_base(arg, &arg_type, BaseType::Integer);
                not_null &= arg_type.not_null;
            }
            FullType::new(BaseType::String, not_null)
        }
        Expression::Row(e) => {
            issue_todo!(typer.issues, e);
            FullType::invalid()
        }
    }
}
