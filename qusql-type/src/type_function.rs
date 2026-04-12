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
use qusql_parse::{Expression, Function, Identifier, Span};

use crate::{
    Type,
    schema::parse_column,
    type_::{BaseType, FullType},
    type_expression::{ExpressionFlags, type_expression},
    typer::{Restrict, Typer},
};

fn arg_cnt<'a>(
    typer: &mut Typer<'a, '_>,
    rng: core::ops::Range<usize>,
    args: &[Expression<'a>],
    span: &Span,
) {
    if args.len() >= rng.start && args.len() <= rng.end {
        return;
    }

    let mut issue = if rng.is_empty() {
        typer.err(
            format!("Expected {} arguments got {}", rng.start, args.len()),
            span,
        )
    } else {
        typer.err(
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
            issue.frag(format!("Argument {}", rng.end + cnt), arg);
        }
    }
}

fn typed_args<'a, 'b, 'c>(
    typer: &mut Typer<'a, 'b>,
    args: &'c [Expression<'a>],
    flags: ExpressionFlags,
) -> Vec<(&'c Expression<'a>, FullType<'a>)> {
    let mut typed: Vec<(&'_ Expression, FullType<'a>)> = Vec::new();
    for arg in args {
        // TODO we need not always disable the not null flag here
        // TODO we should not supply base type any here, this function needs to die
        typed.push((
            arg,
            type_expression(typer, arg, flags.without_values(), BaseType::Any),
        ));
    }
    typed
}

pub(crate) fn type_function<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    func: &Function<'a>,
    args: &[Expression<'a>],
    span: &Span,
    flags: ExpressionFlags,
) -> FullType<'a> {
    let mut tf = |return_type: Type<'a>,
                  required_args: &[BaseType],
                  optional_args: &[BaseType]|
     -> FullType<'a> {
        let mut not_null = true;
        let mut arg_iter = args.iter();
        arg_cnt(
            typer,
            required_args.len()..required_args.len() + optional_args.len(),
            args,
            span,
        );
        for et in required_args {
            if let Some(arg) = arg_iter.next() {
                let t = type_expression(typer, arg, flags.without_values(), *et);
                not_null = not_null && t.not_null;
                typer.ensure_base(arg, &t, *et);
            }
        }
        for et in optional_args {
            if let Some(arg) = arg_iter.next() {
                let t = type_expression(typer, arg, flags.without_values(), *et);
                not_null = not_null && t.not_null;
                typer.ensure_base(arg, &t, *et);
            }
        }
        for arg in arg_iter {
            type_expression(typer, arg, flags.without_values(), BaseType::Any);
        }
        FullType::new(return_type, not_null)
    };

    match func {
        Function::Rand => tf(Type::F64, &[], &[BaseType::Integer]),
        Function::Right | Function::Left => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::Integer],
            &[],
        ),
        Function::SubStr => {
            arg_cnt(typer, 2..3, args, span);

            let mut return_type = if let Some(arg) = args.first() {
                let t = type_expression(typer, arg, flags.without_values(), BaseType::Any);
                if !matches!(t.base(), BaseType::Any | BaseType::String | BaseType::Bytes) {
                    typer.err(format!("Expected type String or Bytes got {t}"), arg);
                }
                t
            } else {
                FullType::invalid()
            };

            if let Some(arg) = args.get(1) {
                let t = type_expression(typer, arg, flags.without_values(), BaseType::Integer);
                return_type.not_null = return_type.not_null && t.not_null;
                typer.ensure_base(arg, &t, BaseType::Integer);
            };

            if let Some(arg) = args.get(2) {
                let t = type_expression(typer, arg, flags.without_values(), BaseType::Integer);
                return_type.not_null = return_type.not_null && t.not_null;
                typer.ensure_base(arg, &t, BaseType::Integer);
            };

            return_type
        }
        Function::FindInSet => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::SubStringIndex => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String, BaseType::Integer],
            &[],
        ),
        Function::ExtractValue => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::Replace => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String, BaseType::String],
            &[],
        ),
        Function::CharacterLength => tf(BaseType::Integer.into(), &[BaseType::String], &[]),
        Function::UnixTimestamp => {
            let mut not_null = true;
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 0..1, args, span);
            if let Some((a, t)) = typed.first() {
                not_null = not_null && t.not_null;
                // TODO the argument can be both a DATE, a DATE_TIME or a TIMESTAMP
                typer.ensure_base(*a, t, BaseType::DateTime);
            }
            FullType::new(Type::I64, not_null)
        }
        Function::IfNull => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let t = if let Some((e, t)) = typed.first() {
                if t.not_null {
                    typer.warn("Cannot be null", *e);
                }
                t.clone()
            } else {
                FullType::invalid()
            };
            if let Some((e, t2)) = typed.get(1) {
                typer.ensure_type(*e, t2, &t);
                t2.clone()
            } else {
                t.clone()
            }
        }
        Function::Lead | Function::Lag => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            if let Some((a, t)) = typed.get(1) {
                typer.ensure_base(*a, t, BaseType::Integer);
            }
            if let Some((_, t)) = typed.first() {
                let mut t = t.clone();
                t.not_null = false;
                t
            } else {
                FullType::invalid()
            }
        }
        Function::JsonExtract => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonValue => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonReplace => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..999, args, span);
            for (i, (a, t)) in typed.iter().enumerate() {
                if i == 0 || i % 2 == 1 {
                    typer.ensure_base(*a, t, BaseType::String);
                }
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonSet => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..999, args, span);
            for (i, (a, t)) in typed.iter().enumerate() {
                if i == 0 || i % 2 == 1 {
                    typer.ensure_base(*a, t, BaseType::String);
                }
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonUnquote => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::JsonQuery => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonRemove => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonContains => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            if let (Some(t0), Some(t1), t2) = (typed.first(), typed.get(1), typed.get(2)) {
                let not_null =
                    t0.1.not_null && t1.1.not_null && t2.map(|t| t.1.not_null).unwrap_or(true);
                FullType::new(Type::Base(BaseType::Bool), not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::JsonContainsPath => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..999, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonOverlaps => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            if let (Some(t0), Some(t1)) = (typed.first(), typed.get(1)) {
                let not_null = t0.1.not_null && t1.1.not_null;
                FullType::new(Type::Base(BaseType::Bool), not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::Min | Function::Max | Function::Sum => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t2)) = typed.first() {
                // TODO check that the type can be mined or maxed
                // Result can be null if there are no rows to aggregate over
                let mut v = t2.clone();
                v.not_null = false;
                v
            } else {
                FullType::invalid()
            }
        }
        Function::Avg => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), span);
                }
                FullType::new(BaseType::Float, false)
            } else {
                FullType::invalid()
            }
        }
        Function::Count => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                match arg {
                    Expression::Identifier(parts)
                        if parts.parts.len() == 1
                            && matches!(parts.parts[0], qusql_parse::IdentifierPart::Star(_)) => {}
                    _ => {
                        type_expression(typer, arg, flags.without_values(), BaseType::Any);
                    }
                }
            }
            FullType::new(BaseType::Integer, true)
        }
        Function::Now => tf(BaseType::DateTime.into(), &[], &[BaseType::Integer]),
        Function::CurDate | Function::UtcDate => tf(BaseType::Date.into(), &[], &[]),
        Function::CurTime | Function::UtcTime => tf(BaseType::Time.into(), &[], &[]),
        Function::UtcTimeStamp => tf(BaseType::DateTime.into(), &[], &[]),
        Function::CurrentTimestamp => tf(BaseType::TimeStamp.into(), &[], &[BaseType::Integer]),
        Function::Concat => {
            let typed = typed_args(typer, args, flags);
            let mut not_null = true;
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::Any);
                not_null = not_null && t.not_null;
            }
            FullType::new(BaseType::String, not_null)
        }
        Function::Least | Function::Greatest => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..9999, args, span);
            if let Some((a, at)) = typed.first() {
                let mut not_null = true;
                let mut t = at.t.clone();
                for (b, bt) in &typed[1..] {
                    not_null = not_null && bt.not_null;
                    if bt.t == t {
                        continue;
                    };
                    if let Some(tt) = typer.matched_type(&bt.t, &t) {
                        t = tt;
                    } else {
                        typer
                            .err("None matching input types", span)
                            .frag(format!("Type {}", at.t), *a)
                            .frag(format!("Type {}", bt.t), *b);
                    }
                }
                FullType::new(t, true);
            }
            FullType::new(BaseType::Any, true)
        }
        Function::If => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::Bool);
            }
            let mut ans = FullType::invalid();
            if let Some((e1, t1)) = typed.get(1) {
                not_null = not_null && t1.not_null;
                if let Some((e2, t2)) = typed.get(2) {
                    not_null = not_null && t2.not_null;
                    if let Some(t) = typer.matched_type(t1, t2) {
                        ans = FullType::new(t, not_null);
                    } else {
                        typer
                            .err("Incompatible types", span)
                            .frag(format!("Of type {}", t1.t), *e1)
                            .frag(format!("Of type {}", t2.t), *e2);
                    }
                }
            }
            ans
        }
        Function::FromUnixTime => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                // TODO float og int
                typer.ensure_base(*e, t, BaseType::Float);
            }
            if let Some((e, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
                FullType::new(BaseType::String, not_null)
            } else {
                FullType::new(BaseType::DateTime, not_null)
            }
        }
        Function::DateFormat => tf(
            BaseType::String.into(),
            &[BaseType::DateTime, BaseType::String],
            &[BaseType::String],
        ),
        Function::Value => {
            let typed = typed_args(typer, args, flags);
            if !flags.in_on_duplicate_key_update {
                typer.err("VALUE is only allowed within ON DUPLICATE KEY UPDATE", span);
            }
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t)) = typed.first() {
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Length => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let mut not_null = true;
            for (_, t) in &typed {
                not_null = not_null && t.not_null;
                if typer
                    .matched_type(t, &FullType::new(BaseType::String, false))
                    .is_none()
                    && typer
                        .matched_type(t, &FullType::new(BaseType::Bytes, false))
                        .is_none()
                {
                    typer.err(format!("Expected type Bytes or String got {t}"), span);
                }
            }
            FullType::new(Type::I64, not_null)
        }
        Function::Strftime => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.last() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::DateTime);
            }
            FullType::new(BaseType::String, not_null)
        }
        Function::StartsWith => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.last() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::Bool, not_null)
        }
        Function::Datetime => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::DateTime, not_null)
        }
        Function::AddMonths => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.last() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                let t = typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                FullType::new(t, not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::AddDate | Function::DateSub => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.last()
                && t.base() != BaseType::Integer
            {
                typer.ensure_base(*e, t, BaseType::TimeInterval);
            }
            if let Some((e, t)) = typed.first() {
                let t = typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                FullType::new(t, false)
            } else {
                FullType::invalid()
            }
        }
        Function::AddTime | Function::SubTime => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.last() {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::Time);
            }
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                let t = typer.ensure_datetime(*e, t, Restrict::Allow, Restrict::Require);
                FullType::new(t, not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::ConvertTz => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(2) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                let t = typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Require);
                FullType::new(t, not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::Date | Function::LastDay => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                t.not_null
            } else {
                true
            };
            FullType::new(BaseType::Date, not_null)
        }
        Function::Time => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Allow, Restrict::Require);
                t.not_null
            } else {
                true
            };
            FullType::new(BaseType::Time, not_null)
        }
        Function::DateDiff => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.last() {
                not_null = not_null && t.not_null;
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
            }
            if let Some((e, t)) = typed.first() {
                not_null = not_null && t.not_null;
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
            }
            FullType::new(BaseType::Integer, not_null)
        }
        Function::DayName | Function::MonthName => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                t.not_null
            } else {
                true
            };
            FullType::new(BaseType::String, not_null)
        }
        Function::DayOfMonth
        | Function::DayOfWeek
        | Function::DayOfYear
        | Function::Month
        | Function::Quarter
        | Function::ToDays
        | Function::ToSeconds
        | Function::Year
        | Function::Weekday => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                t.not_null
            } else {
                true
            };
            FullType::new(BaseType::Integer, not_null)
        }
        Function::Week | Function::YearWeek => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                t.not_null
            } else {
                true
            };
            if let Some((e, t)) = typed.get(2) {
                typer.ensure_base(*e, t, BaseType::Integer);
            };
            FullType::new(BaseType::Integer, not_null)
        }
        Function::Hour | Function::MicroSecond | Function::Minute | Function::Second => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Allow, Restrict::Require);
                t.not_null
            } else {
                true
            };
            FullType::new(BaseType::Integer, not_null)
        }
        Function::FromDays => tf(BaseType::Date.into(), &[BaseType::Integer], &[]),
        Function::SecToTime => tf(BaseType::Time.into(), &[BaseType::Integer], &[]),
        Function::MakeDate => FullType::new(
            tf(
                BaseType::Date.into(),
                &[BaseType::Integer, BaseType::Integer],
                &[],
            )
            .t,
            false,
        ),
        Function::MakeTime => FullType::new(
            tf(
                BaseType::Time.into(),
                &[BaseType::Integer, BaseType::Integer, BaseType::Integer],
                &[],
            )
            .t,
            false,
        ),
        Function::PeriodAdd | Function::PeriodDiff => tf(
            BaseType::Integer.into(),
            &[BaseType::Integer, BaseType::Integer],
            &[],
        ),
        Function::StrToDate => tf(
            BaseType::DateTime.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::SysDate => tf(BaseType::DateTime.into(), &[], &[]),
        Function::TimeFormat => tf(
            BaseType::String.into(),
            &[BaseType::Time, BaseType::String],
            &[],
        ),
        Function::TimeToSec => tf(BaseType::Float.into(), &[BaseType::Time], &[]),
        Function::TimeDiff => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Allow, Restrict::Require);
                t.not_null
            } else {
                true
            };
            let not_null = if let Some((e, t)) = typed.last() {
                typer.ensure_datetime(*e, t, Restrict::Allow, Restrict::Require);
                t.not_null & not_null
            } else {
                not_null
            };
            FullType::new(BaseType::Time, not_null)
        }
        Function::Timestamp => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                t.not_null
            } else {
                true
            };
            let not_null = if let Some((e, t)) = typed.get(2) {
                typer.ensure_datetime(*e, t, Restrict::Disallow, Restrict::Require);
                t.not_null & not_null
            } else {
                not_null
            };
            FullType::new(BaseType::DateTime, not_null)
        }
        Function::Sleep => tf(BaseType::Integer.into(), &[BaseType::Float], &[]),
        // Math / trig functions
        Function::Abs => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), *e);
                }
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Acos
        | Function::Asin
        | Function::Cos
        | Function::Cot
        | Function::Degrees
        | Function::Exp
        | Function::Ln
        | Function::Log2
        | Function::Log10
        | Function::Radians
        | Function::Sin
        | Function::Sqrt
        | Function::Tan => tf(Type::F64, &[BaseType::Float], &[]),
        Function::Atan => tf(Type::F64, &[BaseType::Float], &[BaseType::Float]),
        Function::Atan2 => tf(Type::F64, &[BaseType::Float, BaseType::Float], &[]),
        Function::Ceil | Function::Floor => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), *e);
                }
                FullType::new(Type::I64, t.not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::Log => tf(Type::F64, &[BaseType::Float], &[BaseType::Float]),
        Function::Mod => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (e, t) in &typed {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), *e);
                }
            }
            if let Some((_, t)) = typed.first() {
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Pi => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(Type::F64, true)
        }
        Function::Pow => tf(Type::F64, &[BaseType::Float, BaseType::Float], &[]),
        Function::Round => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            if let Some((e, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), *e);
                }
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Sign => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), *e);
                }
                FullType::new(Type::I8, t.not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::Truncate => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            if let Some((e, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), *e);
                }
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Ascii => tf(BaseType::Integer.into(), &[BaseType::String], &[]),
        Function::Bin => tf(BaseType::String.into(), &[BaseType::Integer], &[]),
        Function::BitLength => tf(BaseType::Integer.into(), &[BaseType::String], &[]),
        Function::Char => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..999, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            FullType::new(BaseType::String, false)
        }
        Function::Chr => tf(BaseType::String.into(), &[BaseType::Integer], &[]),
        Function::ConcatWs => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::Conv => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::Integer, BaseType::Integer],
            &[],
        ),
        Function::Crc32 | Function::Crc32c => tf(Type::U32, &[BaseType::String], &[]),
        Function::CurrentCatalog
        | Function::CurrentRole
        | Function::CurrentUser
        | Function::SessionUser => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::Elt => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::Exists => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::Any);
            }
            FullType::new(BaseType::Bool, true)
        }
        Function::ExportSet => tf(
            BaseType::String.into(),
            &[BaseType::Integer, BaseType::String, BaseType::String],
            &[BaseType::String, BaseType::Integer],
        ),
        Function::Field => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Any);
            }
            FullType::new(BaseType::Integer, true)
        }
        Function::Format => tf(
            BaseType::String.into(),
            &[BaseType::Float, BaseType::Integer],
            &[BaseType::String],
        ),
        Function::FromBase64 => tf(BaseType::Bytes.into(), &[BaseType::String], &[]),
        Function::Hex => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::String | BaseType::Bytes
                ) {
                    typer.err(format!("Expected integer, string or bytes got {t}"), *e);
                }
                FullType::new(BaseType::String, t.not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::Insert => tf(
            BaseType::String.into(),
            &[
                BaseType::String,
                BaseType::Integer,
                BaseType::Integer,
                BaseType::String,
            ],
            &[],
        ),
        Function::InStr => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::JsonArray => {
            let typed = typed_args(typer, args, flags);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Any);
            }
            FullType::new(Type::JSON, true)
        }
        Function::JsonArrayAgg => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::Any);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonArrayAppend | Function::JsonArrayInsert | Function::JsonInsert => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..999, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonArrayIntersect => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonCompact
        | Function::JsonDetailed
        | Function::JsonLoose
        | Function::JsonNormalize
        | Function::JsonPretty => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonDepth | Function::JsonLength => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::Integer, false)
        }
        Function::JsonEquals | Function::JsonValid | Function::JsonSchemaValid => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::Bool, false)
        }
        Function::JsonExists => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::Bool, false)
        }
        Function::JsonKeys => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonMerge | Function::JsonMergePath | Function::JsonMergePerserve => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonObject => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 0..999, args, span);
            for (i, (e, t)) in typed.iter().enumerate() {
                if i % 2 == 0 {
                    typer.ensure_base(*e, t, BaseType::String);
                } else {
                    typer.ensure_base(*e, t, BaseType::Any);
                }
            }
            FullType::new(Type::JSON, true)
        }
        Function::JsonObjectAgg => {
            arg_cnt(typer, 2..2, args, span);
            if let Some(key) = args.first() {
                let key_t = type_expression(typer, key, flags.without_values(), BaseType::Any);
                if !matches!(key_t.base(), BaseType::Any | BaseType::String) {
                    typer.err(format!("Expected string key type got {key_t}"), key);
                }
            }
            if let Some(value) = args.get(1) {
                type_expression(typer, value, flags.without_values(), BaseType::Any);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonObjectFilterKeys => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonObjectToArray => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonQuote => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::JsonSearch => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..999, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonTable => {
            typer.err("JSON_TABLE typing not implemented", span);
            FullType::invalid()
        }
        Function::JsonType => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::LCase | Function::Lower => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::LengthB | Function::OctetLength => {
            tf(BaseType::Integer.into(), &[BaseType::String], &[])
        }
        Function::LoadFile => tf(BaseType::Bytes.into(), &[BaseType::String], &[]),
        Function::Locate | Function::Position => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[BaseType::Integer],
        ),
        Function::LPad | Function::RPad => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::Integer, BaseType::String],
            &[],
        ),
        Function::LTrim | Function::RTrim => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::MakeSet => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..999, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::Mid => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::Integer],
            &[BaseType::Integer],
        ),
        Function::NaturalSortkey => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::Coalesce => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..9999, args, span);
            let mut t: Option<Type<'a>> = None;
            for (e, et) in &typed {
                if let Some(ref prev) = t {
                    if let Some(merged) = typer.matched_type(prev, &et.t) {
                        t = Some(merged);
                    } else {
                        typer
                            .err("Incompatible types in COALESCE", span)
                            .frag(format!("Of type {}", prev), *e);
                    }
                } else {
                    t = Some(et.t.clone());
                }
            }
            FullType::new(t.unwrap_or(Type::Invalid), false)
        }
        Function::NullIf => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((_, t)) = typed.first() {
                let mut v = t.clone();
                v.not_null = false;
                v
            } else {
                FullType::invalid()
            }
        }
        Function::NVL2 => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            if let Some((_, t)) = typed.get(1) {
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Oct => tf(BaseType::String.into(), &[BaseType::Integer], &[]),
        Function::Ord => tf(BaseType::Integer.into(), &[BaseType::String], &[]),
        Function::Quote => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::Repeat => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::Integer],
            &[],
        ),
        Function::Reverse => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::SFormat => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..999, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::SoundEx => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::Space => tf(BaseType::String.into(), &[BaseType::Integer], &[]),
        Function::StrCmp => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::ToBase64 => tf(BaseType::String.into(), &[BaseType::Bytes], &[]),
        Function::ToChar => tf(
            BaseType::String.into(),
            &[BaseType::Any, BaseType::String],
            &[],
        ),
        Function::UCase | Function::Upper => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::UncompressedLength => tf(BaseType::Integer.into(), &[BaseType::Bytes], &[]),
        Function::UnHex => tf(BaseType::Bytes.into(), &[BaseType::String], &[]),
        Function::UpdateXml => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String, BaseType::String],
            &[],
        ),
        Function::WeekOfYear => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = if let Some((e, t)) = typed.first() {
                typer.ensure_datetime(*e, t, Restrict::Require, Restrict::Allow);
                t.not_null
            } else {
                true
            };
            FullType::new(BaseType::Integer, not_null)
        }
        Function::AesDecrypt | Function::AesEncrypt => tf(
            BaseType::Bytes.into(),
            &[BaseType::Bytes, BaseType::Bytes],
            &[BaseType::Bytes],
        ),
        Function::AnyValue => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t)) = typed.first() {
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Benchmark => tf(
            BaseType::Integer.into(),
            &[BaseType::Integer, BaseType::Any],
            &[],
        ),
        Function::BinToUuid => tf(
            BaseType::String.into(),
            &[BaseType::Bytes],
            &[BaseType::Integer],
        ),
        Function::BitCount => tf(Type::I64, &[BaseType::Integer], &[]),
        Function::Charset | Function::Coercibility | Function::Collation => {
            tf(BaseType::String.into(), &[BaseType::Any], &[])
        }
        Function::Compress => tf(BaseType::Bytes.into(), &[BaseType::Bytes], &[]),
        Function::ConnectionId | Function::FoundRows | Function::RowCount => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(Type::I64, true)
        }
        Function::DatabaseFunc | Function::SchemaFunc => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, false)
        }
        Function::FirstValue | Function::LastValue => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t)) = typed.first() {
                let mut v = t.clone();
                v.not_null = false;
                v
            } else {
                FullType::invalid()
            }
        }
        Function::FormatBytes | Function::FormatPicoTime => {
            tf(BaseType::String.into(), &[BaseType::Integer], &[])
        }
        Function::GetFormat => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::GetLock => tf(
            BaseType::Bool.into(),
            &[BaseType::String, BaseType::Integer],
            &[],
        ),
        Function::Grouping => {
            arg_cnt(typer, 1..999, args, span);
            for arg in args {
                type_expression(typer, arg, flags.without_values(), BaseType::Any);
            }
            FullType::new(BaseType::Integer, true)
        }
        Function::IcuVersion => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::Inet6Aton | Function::InetAton => {
            tf(BaseType::Bytes.into(), &[BaseType::String], &[])
        }
        Function::Inet6Ntoa | Function::InetNtoa => {
            tf(BaseType::String.into(), &[BaseType::Bytes], &[])
        }
        Function::IsFreeLock | Function::IsUsedLock => {
            tf(BaseType::Bool.into(), &[BaseType::String], &[])
        }
        Function::IsIPv4 | Function::IsIPv4Compat | Function::IsIPv4Mapped | Function::IsIPv6 => {
            tf(BaseType::Bool.into(), &[BaseType::String], &[])
        }
        Function::IsUuid => tf(BaseType::Bool.into(), &[BaseType::String], &[]),
        Function::LastInsertId => {
            arg_cnt(typer, 0..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::Integer);
            }
            FullType::new(Type::U64, true)
        }
        Function::Md5 | Function::Sha | Function::Sha1 => {
            tf(BaseType::String.into(), &[BaseType::Bytes], &[])
        }
        Function::Sha2 => tf(
            BaseType::String.into(),
            &[BaseType::Bytes, BaseType::Integer],
            &[],
        ),
        Function::NameConst => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((_, t)) = typed.get(1) {
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::NthValue => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            if let Some((_, t)) = typed.first() {
                let mut v = t.clone();
                v.not_null = false;
                v
            } else {
                FullType::invalid()
            }
        }
        Function::Ntile => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::Integer);
            }
            FullType::new(BaseType::Integer, true)
        }
        Function::PsCurrentThreadId => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(Type::U64, true)
        }
        Function::PsThreadId => tf(Type::U64, &[BaseType::Integer], &[]),
        Function::RandomBytes => tf(BaseType::Bytes.into(), &[BaseType::Integer], &[]),
        Function::RegexpInstr => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[
                BaseType::Integer,
                BaseType::Integer,
                BaseType::Integer,
                BaseType::String,
            ],
        ),
        Function::RegexpLike => tf(
            BaseType::Bool.into(),
            &[BaseType::String, BaseType::String],
            &[BaseType::String],
        ),
        Function::RegexpReplace => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String, BaseType::String],
            &[BaseType::Integer, BaseType::Integer, BaseType::String],
        ),
        Function::RegexpSubstr => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String],
            &[BaseType::Integer, BaseType::Integer, BaseType::String],
        ),
        Function::ReleaseAllLocks => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Integer, true)
        }
        Function::ReleaseLock => tf(BaseType::Bool.into(), &[BaseType::String], &[]),
        Function::RolesGraphml => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::RowNumber | Function::CumeDist | Function::PercentRank => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(Type::F64, true)
        }
        Function::DenseRank | Function::Rank => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Integer, true)
        }
        Function::SessionUserFunc | Function::SystemUser | Function::UserFunc => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::StatementDigest | Function::StatementDigestText => {
            tf(BaseType::String.into(), &[BaseType::String], &[])
        }
        Function::Uncompress => tf(BaseType::Bytes.into(), &[BaseType::Bytes], &[]),
        Function::Uuid | Function::UuidShort => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::UuidToBin => tf(
            BaseType::Bytes.into(),
            &[BaseType::String],
            &[BaseType::Integer],
        ),
        Function::ValidatePasswordStrength => {
            tf(BaseType::Integer.into(), &[BaseType::String], &[])
        }
        Function::Version => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::WeightString => tf(BaseType::Bytes.into(), &[BaseType::String], &[]),
        // Aggregate / window functions that may appear in non-aggregate context
        Function::ArrayAgg | Function::JsonAgg | Function::JsonbAgg => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::Any);
            }
            FullType::new(Type::JSON, false)
        }
        Function::BitAnd | Function::BitOr | Function::BitXor => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            FullType::new(Type::U64, false)
        }
        Function::BoolAnd | Function::BoolOr => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Bool);
            }
            FullType::new(BaseType::Bool, false)
        }
        Function::Corr
        | Function::CovarPop
        | Function::CovarSamp
        | Function::RegrAvgx
        | Function::RegrAvgy
        | Function::RegrIntercept
        | Function::RegrR2
        | Function::RegrSlope
        | Function::RegrSxx
        | Function::RegrSxy
        | Function::RegrSyy => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(Type::F64, false)
        }
        Function::RegrCount => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(Type::I64, false)
        }
        Function::JsonbObjectAgg => {
            arg_cnt(typer, 2..2, args, span);
            if let Some(key) = args.first() {
                let key_t = type_expression(typer, key, flags.without_values(), BaseType::Any);
                if !matches!(key_t.base(), BaseType::Any | BaseType::String) {
                    typer.err(format!("Expected string key type got {key_t}"), key);
                }
            }
            if let Some(value) = args.get(1) {
                type_expression(typer, value, flags.without_values(), BaseType::Any);
            }
            FullType::new(Type::JSON, false)
        }
        Function::PercentileCont | Function::PercentileDisc => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(Type::F64, false)
        }
        Function::Mode
        | Function::Std
        | Function::Stddev
        | Function::StddevPop
        | Function::StddevSamp
        | Function::Variance
        | Function::VarPop
        | Function::VarSamp => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first()
                && !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                )
            {
                typer.err(format!("Expected numeric type got {t}"), *e);
            }
            FullType::new(Type::F64, false)
        }
        Function::StringAgg => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::Xmlagg => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        // PostGIS / geometry functions
        // Geometry type is represented as Any since the type system has no geometry type yet
        Function::GeometryType => {
            // GeometryType(geom) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::Box2D => {
            // Box2D(geom) -> geometry bounding box (represented as Geometry)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StAsEwkb => {
            // ST_AsEWKB(geom) -> bytes
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Bytes, not_null)
        }
        Function::StAsGeoJson => {
            // ST_AsGeoJSON(geom) -> text (JSON)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StGeomFromEwkb => {
            // ST_GeomFromEWKB(bytes) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Bytes);
            }
            FullType::new(Type::Geometry, false)
        }
        Function::StGeomFromText => {
            // ST_GeomFromText(text[, srid]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            FullType::new(Type::Geometry, false)
        }
        Function::StGeomFromGeoJson => {
            // ST_GeomFromGeoJSON(json) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::Geometry, false)
        }
        Function::StSetSrid => {
            // ST_SetSRID(geom, srid) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StSimplifyPreserveTopology => {
            // ST_SimplifyPreserveTopology(geom, tolerance) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::Other(parts) => {
            // Type all arguments regardless of whether we know the function
            typed_args(typer, args, flags);
            // Look up by the unqualified name (last part)
            let fn_name = parts.last().map(|id| id.value).unwrap_or_default();
            let lookup_key = Identifier {
                value: fn_name,
                span: parts
                    .last()
                    .map(|id| id.span.clone())
                    .unwrap_or_else(|| span.clone()),
            };
            if let Some(def) = typer.schemas.functions.get(&lookup_key) {
                let col = parse_column(
                    def.return_type.clone(),
                    def.name.clone(),
                    typer.issues,
                    Some(typer.options),
                    Some(&typer.schemas.types),
                );
                col.type_
            } else {
                typer.err(format!("Unknown function '{fn_name}'"), span);
                FullType::invalid()
            }
        }
        _ => {
            typer.err("Typing for function not implemented", span);
            FullType::invalid()
        }
    }
}

pub(crate) fn type_aggregate_function<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    func: &Function<'a>,
    args: &[Expression<'a>],
    span: &Span,
    distinct_span: &Option<Span>,
    flags: ExpressionFlags,
) -> FullType<'a> {
    if distinct_span.is_some() && args.is_empty() {
        typer.err("DISTINCT requires an argument", span);
    }

    match func {
        Function::Count => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                match arg {
                    Expression::Identifier(parts)
                        if parts.parts.len() == 1
                            && matches!(parts.parts[0], qusql_parse::IdentifierPart::Star(_)) => {}
                    _ => {
                        type_expression(typer, arg, flags.without_values(), BaseType::Any);
                    }
                }
            }
            FullType::new(BaseType::Integer, true)
        }
        Function::Avg => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                let t = type_expression(typer, arg, flags.without_values(), BaseType::Any);
                if !matches!(
                    t.base(),
                    BaseType::Any | BaseType::Integer | BaseType::Float
                ) {
                    typer.err(format!("Expected numeric type got {t}"), arg);
                }
            }
            FullType::new(BaseType::Float, false)
        }
        Function::Min | Function::Max | Function::Sum => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t2)) = typed.first() {
                let mut v = t2.clone();
                v.not_null = false;
                v
            } else {
                FullType::invalid()
            }
        }
        Function::JsonArrayAgg => {
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                type_expression(typer, arg, flags.without_values(), BaseType::Any);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonObjectAgg => {
            arg_cnt(typer, 2..2, args, span);
            if let Some(key) = args.first() {
                let key_t = type_expression(typer, key, flags.without_values(), BaseType::Any);
                if !matches!(key_t.base(), BaseType::Any | BaseType::String) {
                    typer.err(format!("Expected string key type got {key_t}"), key);
                }
            }
            if let Some(value) = args.get(1) {
                type_expression(typer, value, flags.without_values(), BaseType::Any);
            }
            FullType::new(Type::JSON, false)
        }
        _ => {
            typer.err("Unsupported aggregate function", span);
            FullType::invalid()
        }
    }
}
