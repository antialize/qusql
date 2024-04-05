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
use sql_parse::{Expression, Function, Issue, Span};

use crate::{
    type_::{BaseType, FullType},
    type_expression::{type_expression, ExpressionFlags},
    typer::Typer,
    Type,
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
        Issue::err(
            format!("Expected {} arguments got {}", rng.start, args.len()),
            span,
        )
    } else {
        Issue::err(
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
            issue = issue.frag(format!("Argument {}", rng.end + cnt), arg);
        }
    }
    typer.issues.push(issue);
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
                    typer.issues.push(Issue::err(
                        format!("Expected type String or Bytes got {}", t),
                        arg,
                    ));
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
                    typer.issues.push(Issue::warn("Cannot be null", *e));
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
        Function::Now => tf(BaseType::DateTime.into(), &[], &[BaseType::Integer]),
        Function::CurDate => tf(BaseType::Date.into(), &[], &[]),
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
                        typer.issues.push(
                            Issue::err("None matching input types", span)
                                .frag(format!("Type {}", at.t), *a)
                                .frag(format!("Type {}", bt.t), *b),
                        );
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
                        typer.issues.push(
                            Issue::err("Incompatible types", span)
                                .frag(format!("Of type {}", t1.t), *e1)
                                .frag(format!("Of type {}", t2.t), *e2),
                        );
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
                typer.issues.push(Issue::err(
                    "VALUE is only allowed within ON DUPLICATE KEY UPDATE",
                    span,
                ));
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
                    typer.issues.push(Issue::err(
                        format!("Expected type Bytes or String got {}", t),
                        span,
                    ));
                }
            }
            FullType::new(Type::I64, not_null)
        }
        _ => {
            typer
                .issues
                .push(Issue::err("Typing for function not implemented", span));
            FullType::invalid()
        }
    }
}
