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

use alloc::{boxed::Box, format, vec::Vec};
use qusql_parse::{Expression, Function, Identifier, Span};

use crate::{
    Type,
    schema::{QualifiedIdentifier, lookup_name, parse_column},
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
    context: BaseType,
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
            FullType::new(Type::Base(BaseType::Any), false)
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
        Function::JsonbSet => {
            // jsonb_set(target jsonb, path text[], new_value jsonb[, create_missing bool]) -> jsonb
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..4, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Any); // text[] path
            }
            if let Some((e, t)) = typed.get(3) {
                typer.ensure_base(*e, t, BaseType::Bool);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::JSON, not_null)
        }
        Function::JsonBuildObject => {
            // json_build_object(key, value, key, value, ...) -> json
            // accepts any number of alternating key/value pairs (even count)
            typed_args(typer, args, flags);
            FullType::new(Type::JSON, true)
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
            if typer.dialect().is_maria() {
                FullType::new(BaseType::Integer, true)
            } else {
                FullType::new(Type::I64, true)
            }
        }
        Function::Now => {
            let ret = if context == BaseType::TimeStamp {
                BaseType::TimeStamp
            } else {
                BaseType::DateTime
            };
            tf(ret.into(), &[], &[BaseType::Integer])
        }
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
            if typer.dialect().is_maria() {
                FullType::new(BaseType::Integer, not_null)
            } else {
                FullType::new(Type::I64, not_null)
            }
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
        // PostgreSQL string functions
        Function::Btrim => tf(
            BaseType::String.into(),
            &[BaseType::String],
            &[BaseType::String],
        ),
        Function::Casefold => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::Initcap => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::Normalize => tf(BaseType::String.into(), &[BaseType::String], &[BaseType::Any]),
        Function::ParseIdent => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let mut not_null = true;
            for (e, t) in &typed {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::Any);
            }
            FullType::new(Type::Array(Box::new(BaseType::String.into())), not_null)
        }
        Function::PgClientEncoding => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::QuoteIdent => tf(BaseType::String.into(), &[BaseType::String], &[]),
        Function::QuoteLiteral | Function::QuoteNullable => {
            tf(BaseType::String.into(), &[BaseType::Any], &[])
        }
        Function::RegexpCount => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[BaseType::Integer, BaseType::String],
        ),
        Function::RegexpMatch | Function::RegexpMatches | Function::RegexpSplitToArray => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            let mut not_null = true;
            for (e, t) in &typed {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::Array(Box::new(BaseType::String.into())), not_null)
        }
        Function::RegexpSplitToTable | Function::StringToTable => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            FullType::new(BaseType::String, false)
        }
        Function::SplitPart => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String, BaseType::Integer],
            &[],
        ),
        Function::Strpos => tf(
            BaseType::Integer.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::StringToArray => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            let mut not_null = true;
            for (e, t) in &typed {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::Array(Box::new(BaseType::String.into())), not_null)
        }
        Function::ToAscii => tf(BaseType::String.into(), &[BaseType::String], &[BaseType::Any]),
        Function::ToBin | Function::ToHex | Function::ToOct => {
            tf(BaseType::String.into(), &[BaseType::Integer], &[])
        }
        Function::Translate => tf(
            BaseType::String.into(),
            &[BaseType::String, BaseType::String, BaseType::String],
            &[],
        ),
        Function::UnicodeAssigned => tf(BaseType::Bool.into(), &[BaseType::String], &[]),
        Function::Unistr => tf(BaseType::String.into(), &[BaseType::String], &[]),
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
        // Single-arg float -> float64 trig/math (radians variants)
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
        | Function::Tan
        // Degree-based equivalents
        | Function::Acosd
        | Function::Asind
        | Function::Cosd
        | Function::Cotd
        | Function::Sind
        | Function::Tand
        // Hyperbolic functions
        | Function::Sinh
        | Function::Cosh
        | Function::Tanh
        | Function::Asinh
        | Function::Acosh
        | Function::Atanh
        // Other single-arg float->float64
        | Function::Cbrt
        | Function::Erf
        | Function::Erfc
        | Function::Gamma
        | Function::Lgamma => tf(Type::F64, &[BaseType::Float], &[]),
        Function::Atan
        | Function::Atand => tf(Type::F64, &[BaseType::Float], &[BaseType::Float]),
        Function::Atan2
        | Function::Atan2d => tf(Type::F64, &[BaseType::Float, BaseType::Float], &[]),
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
        Function::Factorial => {
            // factorial(bigint) -> numeric (can be very large)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Integer);
                FullType::new(BaseType::Float, t.not_null)
            } else {
                FullType::invalid()
            }
        }
        Function::Gcd | Function::Lcm => {
            // gcd/lcm(numeric_type, numeric_type) -> numeric_type (same as first arg)
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
        Function::Log => tf(Type::F64, &[BaseType::Float], &[BaseType::Float]),
        Function::MinScale | Function::Scale => {
            // min_scale(numeric) / scale(numeric) -> integer
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Integer, not_null)
        }
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
        Function::RandomNormal => {
            // random_normal([mean double precision [, stddev double precision]]) -> double precision
            typed_args(typer, args, flags);
            arg_cnt(typer, 0..2, args, span);
            FullType::new(Type::F64, true)
        }
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
        Function::SetSeed => {
            // setseed(double precision) -> void (we return integer as approximation)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(BaseType::Integer, true)
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
        Function::TrimScale => {
            // trim_scale(numeric) -> numeric (same type as input)
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
        Function::WidthBucket => {
            // width_bucket(operand, low, high, count) -> integer
            // width_bucket(operand, thresholds_array) -> integer
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..4, args, span);
            if typed.len() == 4
                && let Some((e, t)) = typed.get(3) {
                    typer.ensure_base(*e, t, BaseType::Integer);
                }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Integer, not_null)
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
        Function::Format => {
            let is_pg = typer.dialect().is_postgresql();
            let typed = typed_args(typer, args, flags);
            if is_pg {
                // PostgreSQL: format(format_string text [, args...]) - variadic, first arg is string
                arg_cnt(typer, 1..999, args, span);
                if let Some((e, t)) = typed.first() {
                    typer.ensure_base(*e, t, BaseType::String);
                }
            } else {
                // MySQL: FORMAT(number, decimals[, locale])
                arg_cnt(typer, 2..2, args, span);
                if let Some((e, t)) = typed.first() {
                    typer.ensure_base(*e, t, BaseType::Float);
                }
                if let Some((e, t)) = typed.get(1) {
                    typer.ensure_base(*e, t, BaseType::Integer);
                }
            }
            FullType::new(BaseType::String, false)
        }
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
        Function::LCase | Function::Lower => {
            // PostgreSQL overloads lower(): string lowercase AND range lower-bound.
            // For a range arg, return the element type; for string/any, return String.
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                let t = type_expression(typer, arg, flags.without_values(), BaseType::Any);
                if let Type::Range(elem) = t.t {
                    FullType::new(elem, false)
                } else {
                    FullType::new(BaseType::String, t.not_null)
                }
            } else {
                FullType::invalid()
            }
        }
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
        Function::ToDate => tf(
            BaseType::Date.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::ToNumber => tf(
            BaseType::Float.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::ToTimestamp => tf(
            BaseType::DateTime.into(),
            &[BaseType::String, BaseType::String],
            &[],
        ),
        Function::UCase | Function::Upper => {
            // PostgreSQL overloads upper(): string uppercase AND range upper-bound.
            arg_cnt(typer, 1..1, args, span);
            if let Some(arg) = args.first() {
                let t = type_expression(typer, arg, flags.without_values(), BaseType::Any);
                match t.base() {
                    BaseType::Any | BaseType::String => FullType::new(BaseType::String, t.not_null),
                    _ => FullType::new(BaseType::Any, t.not_null),
                }
            } else {
                FullType::invalid()
            }
        }
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
        // PostgreSQL bit string functions
        Function::GetBit => tf(
            BaseType::Integer.into(),
            &[BaseType::Any, BaseType::Integer],
            &[],
        ),
        Function::SetBit => tf(
            BaseType::Any.into(),
            &[BaseType::Any, BaseType::Integer, BaseType::Integer],
            &[],
        ),
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
        // PostgreSQL system functions
        Function::InetServerAddr => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, false)
        }
        Function::InetServerPort => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Integer, false)
        }
        Function::PgPostmasterStartTime => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::DateTime, true)
        }
        Function::PostgisFullVersion => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        // PostgreSQL network address functions
        Function::Abbrev | Function::Host => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::Broadcast | Function::HostMask | Function::NetMask | Function::Network => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[])
        }
        Function::Family | Function::MaskLen => {
            tf(BaseType::Integer.into(), &[BaseType::Any], &[])
        }
        Function::InetMerge => tf(BaseType::Any.into(), &[BaseType::Any, BaseType::Any], &[]),
        Function::InetSameFamily => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Any], &[])
        }
        Function::Macaddr8Set7bit => tf(BaseType::Any.into(), &[BaseType::Any], &[]),
        Function::SetMaskLen => {
            tf(BaseType::Any.into(), &[BaseType::Any, BaseType::Integer], &[])
        }
        // PostgreSQL text search functions
        Function::ArrayToTsvector | Function::JsonToTsvector | Function::JsonbToTsvector
        | Function::Setweight | Function::Strip | Function::TsDelete | Function::TsFilter => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::GetCurrentTsConfig => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::Numnode => tf(BaseType::Integer.into(), &[BaseType::Any], &[]),
        Function::PhraseToTsquery
        | Function::PlainToTsquery
        | Function::ToTsquery
        | Function::ToTsvector
        | Function::TsRewrite
        | Function::TsqueryPhrase
        | Function::WebsearchToTsquery => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::Querytree | Function::TsHeadline => {
            tf(BaseType::String.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::TsRank | Function::TsRankCd => {
            tf(BaseType::Float.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::TsvectorToArray => tf(BaseType::Any.into(), &[BaseType::Any], &[]),
        Function::Unnest => tf(BaseType::Any.into(), &[BaseType::Any], &[]),
        // Text search debug functions
        Function::TsDebug | Function::TsLexize | Function::TsParse | Function::TsTokenType
        | Function::TsStat => tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any]),
        // PostgreSQL UUID functions
        Function::GenRandomUuid | Function::Uuidv4 => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Uuid, true)
        }
        Function::Uuidv7 => {
            arg_cnt(typer, 0..1, args, span);
            FullType::new(BaseType::Uuid, true)
        }
        Function::UuidExtractTimestamp => tf(BaseType::DateTime.into(), &[BaseType::Uuid], &[]),
        Function::UuidExtractVersion => tf(BaseType::Integer.into(), &[BaseType::Uuid], &[]),
        // PostgreSQL sequence functions
        Function::Nextval | Function::Currval | Function::Setval => {
            tf(Type::I64, &[BaseType::Any], &[BaseType::Any])
        }
        Function::Lastval => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(Type::I64, false)
        }
        // PostgreSQL array functions
        Function::ArrayAppend
        | Function::ArrayCat
        | Function::ArrayFill
        | Function::ArrayPrepend
        | Function::ArrayRemove
        | Function::ArrayReplace
        | Function::ArrayReverse
        | Function::ArraySample
        | Function::ArrayShuffle
        | Function::ArraySort
        | Function::TrimArray => tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any]),
        Function::ArrayDims => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::ArrayToString => {
            tf(BaseType::String.into(), &[BaseType::Any, BaseType::String], &[BaseType::String])
        }
        Function::ArrayLength
        | Function::ArrayLower
        | Function::ArrayNdims
        | Function::ArrayPosition
        | Function::ArrayUpper
        | Function::Cardinality => {
            tf(BaseType::Integer.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::ArrayPositions => tf(BaseType::Any.into(), &[BaseType::Any, BaseType::Any], &[]),
        // PostgreSQL system information functions (9.27)
        Function::CurrentDatabase => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, false)
        }
        Function::CurrentQuery => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::CurrentSchemas => {
            arg_cnt(typer, 0..1, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::IcuUnicodeVersion | Function::UnicodeVersion => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::InetClientAddr => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Any, true)
        }
        Function::InetClientPort => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Integer, true)
        }
        Function::PgBackendPid | Function::PgTriggerDepth => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Integer, false)
        }
        Function::PgConfLoadTime | Function::PgXactCommitTimestamp => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::DateTime, false)
        }
        Function::PgCurrentLogfile => {
            arg_cnt(typer, 0..1, args, span);
            FullType::new(BaseType::String, true)
        }
        Function::PgJitAvailable | Function::PgIsOtherTempSchema => {
            arg_cnt(typer, 0..1, args, span);
            FullType::new(BaseType::Bool, false)
        }
        Function::PgMyTempSchema | Function::PgCurrentSnapshot | Function::PgCurrentXactId
        | Function::PgAvailableWalSummaries | Function::PgGetWalSummarizerState => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::PgCurrentXactIdIfAssigned => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Any, true)
        }
        Function::PgNotificationQueueUsage => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Float, false)
        }
        Function::PgLastCommittedXact | Function::PgControlCheckpoint | Function::PgControlInit
        | Function::PgControlRecovery | Function::PgControlSystem => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::PgListeningChannels => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::PgBlockingPids | Function::PgSafeSnapshotBlockingPids => {
            tf(BaseType::Any.into(), &[BaseType::Integer], &[])
        }
        Function::MxidAge => tf(BaseType::Integer.into(), &[BaseType::Any], &[]),
        Function::HasAnyColumnPrivilege
        | Function::HasColumnPrivilege
        | Function::HasDatabasePrivilege
        | Function::HasForeignDataWrapperPrivilege
        | Function::HasFunctionPrivilege
        | Function::HasLanguagePrivilege
        | Function::HasLargeobjectPrivilege
        | Function::HasParameterPrivilege
        | Function::HasSchemaPrivilege
        | Function::HasSequencePrivilege
        | Function::HasServerPrivilege
        | Function::HasTablePrivilege
        | Function::HasTablespacePrivilege
        | Function::HasTypePrivilege
        | Function::PgHasRole => tf(BaseType::Bool.into(), &[BaseType::Any], &[BaseType::Any]),
        Function::RowSecurityActive => tf(BaseType::Bool.into(), &[BaseType::Any], &[]),
        Function::Makeaclitem => tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any]),
        Function::PgCollationIsVisible
        | Function::PgConversionIsVisible
        | Function::PgFunctionIsVisible
        | Function::PgOpclassIsVisible
        | Function::PgOperatorIsVisible
        | Function::PgOpfamilyIsVisible
        | Function::PgStatisticsObjIsVisible
        | Function::PgTableIsVisible
        | Function::PgTsConfigIsVisible
        | Function::PgTsDictIsVisible
        | Function::PgTsParserIsVisible
        | Function::PgTsTemplateIsVisible
        | Function::PgTypeIsVisible => tf(BaseType::Bool.into(), &[BaseType::Any], &[]),
        Function::PgInputIsValid => tf(BaseType::Bool.into(), &[BaseType::String, BaseType::String], &[]),
        Function::PgVisibleInSnapshot => tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Any], &[]),
        Function::PgIndexColumnHasProperty => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Integer, BaseType::String], &[])
        }
        Function::PgIndexHasProperty => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::String], &[])
        }
        Function::PgIndexamHasProperty => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::String], &[])
        }
        Function::PgCharToEncoding => tf(BaseType::Integer.into(), &[BaseType::String], &[]),
        Function::ToRegtypemod => tf(BaseType::Integer.into(), &[BaseType::String], &[]),
        Function::PgEncodingToChar => tf(BaseType::String.into(), &[BaseType::Integer], &[]),
        Function::PgXactStatus => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::PgDescribeObject | Function::PgGetUserbyid => {
            tf(BaseType::String.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::ColDescription | Function::ObjDescription | Function::ShobjDescription => {
            tf(BaseType::String.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::FormatType => {
            tf(BaseType::String.into(), &[BaseType::Any, BaseType::Any], &[])
        }
        Function::PgGetSerialSequence => {
            tf(BaseType::String.into(), &[BaseType::String, BaseType::String], &[])
        }
        Function::PgGetConstraintdef
        | Function::PgGetFunctiondef
        | Function::PgGetFunctionArguments
        | Function::PgGetFunctionIdentityArguments
        | Function::PgGetFunctionResult
        | Function::PgGetIndexdef
        | Function::PgGetPartitionConstraintdef
        | Function::PgGetPartkeydef
        | Function::PgGetRuledef
        | Function::PgGetStatisticsobjdef
        | Function::PgGetTriggerdef
        | Function::PgGetViewdef => tf(BaseType::String.into(), &[BaseType::Any], &[BaseType::Any]),
        Function::PgGetExpr => {
            tf(BaseType::String.into(), &[BaseType::Any, BaseType::Any], &[BaseType::Bool])
        }
        Function::PgTablespaceLocation => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::PgTypeof => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::PgSettingsGetFlags => tf(BaseType::Any.into(), &[BaseType::String], &[]),
        Function::PgSnapshotXip
        | Function::PgSnapshotXmax
        | Function::PgSnapshotXmin
        | Function::PgGetAcl
        | Function::PgGetObjectAddress
        | Function::PgInputErrorInfo => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::ToRegclass
        | Function::ToRegcollation
        | Function::ToRegnamespace
        | Function::ToRegoper
        | Function::ToRegoperator
        | Function::ToRegproc
        | Function::ToRegprocedure
        | Function::ToRegrole
        | Function::ToRegtype => tf(BaseType::Any.into(), &[BaseType::String], &[]),
        // PostgreSQL XML functions
        Function::XmlIsWellFormed
        | Function::XmlIsWellFormedContent
        | Function::XmlIsWellFormedDocument => {
            tf(BaseType::Bool.into(), &[BaseType::String], &[])
        }
        Function::XpathExists => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Any], &[BaseType::Any])
        }
        Function::XmlComment | Function::XmlText => {
            tf(BaseType::Any.into(), &[BaseType::String], &[])
        }
        Function::XmlConcat => tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any]),
        Function::Xpath => {
            tf(BaseType::Any.into(), &[BaseType::Any, BaseType::Any], &[BaseType::Any])
        }
        Function::CursorToXml
        | Function::CursorToXmlschema
        | Function::DatabaseToXml
        | Function::DatabaseToXmlAndXmlschema
        | Function::DatabaseToXmlschema
        | Function::QueryToXml
        | Function::QueryToXmlAndXmlschema
        | Function::QueryToXmlschema
        | Function::SchemaToXml
        | Function::SchemaToXmlAndXmlschema
        | Function::SchemaToXmlschema
        | Function::TableToXml
        | Function::TableToXmlAndXmlschema
        | Function::TableToXmlschema => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any])
        }
        // PostgreSQL JSON functions
        Function::ToJson | Function::ToJsonb | Function::ArrayToJson | Function::RowToJson => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::JsonBuildArray
        | Function::JsonbBuildArray
        | Function::JsonbBuildObject
        | Function::JsonbInsert
        | Function::JsonbObject
        | Function::JsonbSetLax
        | Function::JsonbStripNulls
        | Function::JsonStripNulls
        | Function::JsonScalar
        | Function::JsonExtractPath
        | Function::JsonbExtractPath => {
            // variadic: accept any number of arguments
            typed_args(typer, args, flags);
            FullType::new(Type::JSON, true)
        }
        Function::JsonSerialize => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::JsonbPretty => tf(BaseType::String.into(), &[BaseType::Any], &[]),
        Function::JsonTypeof | Function::JsonbTypeof => {
            tf(BaseType::String.into(), &[BaseType::Any], &[])
        }
        Function::JsonExtractPathText | Function::JsonbExtractPathText => {
            tf(BaseType::String.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::JsonArrayLength | Function::JsonbArrayLength => {
            tf(BaseType::Integer.into(), &[BaseType::Any], &[])
        }
        Function::JsonbPopulateRecordValid => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Any], &[])
        }
        Function::JsonbPathExists | Function::JsonbPathExistsTz => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Any], &[BaseType::Any])
        }
        Function::JsonbPathMatch | Function::JsonbPathMatchTz => {
            tf(BaseType::Bool.into(), &[BaseType::Any, BaseType::Any], &[BaseType::Any])
        }
        Function::JsonArrayElements
        | Function::JsonArrayElementsText
        | Function::JsonbArrayElements
        | Function::JsonbArrayElementsText
        | Function::JsonEach
        | Function::JsonEachText
        | Function::JsonbEach
        | Function::JsonbEachText
        | Function::JsonObjectKeys
        | Function::JsonbObjectKeys
        | Function::JsonPopulateRecord
        | Function::JsonbPopulateRecord
        | Function::JsonPopulateRecordset
        | Function::JsonbPopulateRecordset
        | Function::JsonToRecord
        | Function::JsonbToRecord
        | Function::JsonToRecordset
        | Function::JsonbToRecordset => {
            tf(BaseType::Any.into(), &[BaseType::Any], &[BaseType::Any])
        }
        Function::JsonbPathQuery
        | Function::JsonbPathQueryTz
        | Function::JsonbPathQueryArray
        | Function::JsonbPathQueryArrayTz
        | Function::JsonbPathQueryFirst
        | Function::JsonbPathQueryFirstTz => {
            tf(BaseType::Any.into(), &[BaseType::Any, BaseType::Any], &[BaseType::Any])
        }
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
        // PostgreSQL geometric functions (non-PostGIS) - Table 9.37 / 9.38
        Function::Area | Function::Diameter | Function::Height | Function::Radius
        | Function::Width => tf(Type::F64, &[BaseType::Any], &[]),
        Function::BoundBox => tf(BaseType::Any.into(), &[BaseType::Any, BaseType::Any], &[]),
        Function::Center => tf(BaseType::Any.into(), &[BaseType::Any], &[]),
        Function::Diagonal => tf(BaseType::Any.into(), &[BaseType::Any], &[]),
        Function::Isclosed | Function::IsOpen => tf(BaseType::Bool.into(), &[BaseType::Any], &[]),
        Function::Npoints => tf(BaseType::Integer.into(), &[BaseType::Any], &[]),
        Function::Pclose | Function::Popen => tf(BaseType::Any.into(), &[BaseType::Any], &[]),
        Function::Slope => tf(Type::F64, &[BaseType::Any, BaseType::Any], &[]),
        // PostgreSQL enum support functions
        Function::EnumFirst | Function::EnumLast => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t)) = typed.first() {
                t.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::EnumRange => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let inner = if let Some((_, t)) = typed.first() {
                t.t.clone()
            } else {
                BaseType::Any.into()
            };
            FullType::new(Type::Array(Box::new(inner)), false)
        }
        // PostGIS / geometry functions
        // Geometry type is represented as Any since the type system has no geometry type yet
        Function::GeometryType | Function::StGeometryType => {
            // GeometryType(geom) / ST_GeometryType(geom) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::Box2D | Function::Box3D => {
            // Box2D/Box3D(geom) -> geometry bounding box
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Output functions returning bytes ---
        Function::StAsEwkb | Function::StAsBinary => {
            // ST_AsEWKB/ST_AsBinary(geom) -> bytea
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Bytes, not_null)
        }
        // --- Output functions returning text ---
        Function::StAsGeoJson => {
            // ST_AsGeoJSON(geom[, max_decimal_digits[, options]]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StAsEwkt | Function::StAsText => {
            // ST_AsEWKT/ST_AsText(geom) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StAsGml => {
            // ST_AsGML(geom[, version[, precision[, options[, namespace_prefix]]]]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..5, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StAsHexEwkb => {
            // ST_AsHEXEWKB(geom[, NDRorXDR]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StAsKml => {
            // ST_AsKML(geom[, version[, precision]]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StAsSvg => {
            // ST_AsSVG(geom[, rel[, maxdecimaldigits]]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StGeoHash => {
            // ST_GeoHash(geom[, maxchars]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StSummary => {
            // ST_Summary(geom) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        Function::StIsValidReason => {
            // ST_IsValidReason(geom[, flags]) -> text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::String, not_null)
        }
        // --- Float measurement functions ---
        Function::StArea | Function::StLength | Function::StLength2D | Function::StLength3D
        | Function::StPerimeter | Function::StPerimeter2D | Function::StPerimeter3D => {
            // ST_Area/ST_Length*/ST_Perimeter*(geom) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StDistance | Function::StMaxDistance => {
            // ST_Distance/ST_MaxDistance(geomA, geomB) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StDistanceSphere => {
            // ST_Distance_Sphere(geomA, geomB) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StDistanceSpheroidal => {
            // ST_Distance_Spheroid(geomA, geomB, spheroid) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            if let Some((e, t)) = typed.get(2) {
                typer.ensure_base(*e, t, BaseType::String);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StHausdorffDistance => {
            // ST_HausdorffDistance(geomA, geomB[, densifyFrac]) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            if let Some((e, t)) = typed.get(2) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StAzimuth => {
            // ST_Azimuth(geomA, geomB) -> float8 (radians)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StLineLocatePoint => {
            // ST_Line_Locate_Point(geom, point) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        // --- Coordinate extractors (return float) ---
        Function::StX | Function::StY | Function::StZ | Function::StM => {
            // ST_X/Y/Z/M(geom) -> float8 (NULL if coordinate not present)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        Function::StXMax | Function::StXMin | Function::StYMax | Function::StYMin
        | Function::StZMax | Function::StZMin => {
            // ST_XMax/XMin/YMax/YMin/ZMax/ZMin(box) -> float8
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::F64, not_null)
        }
        // --- Integer geometry properties ---
        Function::StSRID | Function::StDimension | Function::StNPoints | Function::StNRings
        | Function::StNumGeometries | Function::StNumInteriorRing
        | Function::StNumInteriorRings | Function::StNumPoints | Function::StMemSize
        | Function::StLineCrossingDirection => {
            // ST_SRID/Dimension/NPoints/NRings/NumGeometries/NumInteriorRing*/NumPoints(geom) -> int
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Integer, not_null)
        }
        Function::StCoordDim | Function::StNDims | Function::StZmflag => {
            // ST_CoordDim/NDims/Zmflag(geom) -> smallint
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Integer, not_null)
        }
        // --- Unary boolean predicates ---
        Function::StHasArc | Function::StIsClosed | Function::StIsEmpty | Function::StIsRing
        | Function::StIsSimple | Function::StIsValid => {
            // ST_HasArc/IsClosed/IsEmpty/IsRing/IsSimple/IsValid(geom) -> bool
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Bool, not_null)
        }
        // --- Binary boolean predicates ---
        Function::StContains | Function::StContainsProperly | Function::StCovers
        | Function::StCoveredBy | Function::StCrosses | Function::StDisjoint
        | Function::StEquals | Function::StIntersects | Function::StOrderingEquals
        | Function::StOverlaps | Function::StTouches | Function::StWithin => {
            // ST_Contains/ContainsProperly/Covers/.../Within(geomA, geomB) -> bool
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Bool, not_null)
        }
        Function::StDWithin | Function::StDFullyWithin => {
            // ST_DWithin/DFullyWithin(geomA, geomB, distance) -> bool
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..4, args, span);
            if let Some((e, t)) = typed.get(2) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Bool, not_null)
        }
        Function::StRelate => {
            // ST_Relate(geomA, geomB[, intersectionMatrixPattern]) -> bool or text
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            if typed.len() == 2 {
                // Returns the DE-9IM matrix as text
                FullType::new(BaseType::String, not_null)
            } else {
                // Returns bool when pattern provided
                FullType::new(BaseType::Bool, not_null)
            }
        }
        Function::StPointInsideCircle => {
            // ST_Point_Inside_Circle(geom, x, y, radius) -> bool
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 4..4, args, span);
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(BaseType::Bool, not_null)
        }
        // --- Geometry constructors from text ---
        Function::StGeomFromText | Function::StGeometryFromText | Function::StWktToSQL => {
            // ST_GeomFromText/GeometryFromText/WKTToSQL(text[, srid]) -> geometry
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
        Function::StGeomCollFromText | Function::StLineFromText | Function::StPolygonFromText
        | Function::StPointFromText => {
            // ST_GeomCollFromText/LineFromText/PolygonFromText/PointFromText(text[, srid]) -> geometry
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
        // --- Geometry constructors from binary ---
        Function::StGeomFromEwkb | Function::StWkbToSQL => {
            // ST_GeomFromEWKB/WKBToSQL(bytes) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Bytes);
            }
            FullType::new(Type::Geometry, false)
        }
        Function::StGeomFromWkb | Function::StLineFromWkb | Function::StLinestringFromWkb
        | Function::StPointFromWkb => {
            // ST_GeomFromWKB/LineFromWKB/LinestringFromWKB/PointFromWKB(bytes[, srid]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::Bytes);
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
        Function::StGeomFromGml | Function::StGmlToSQL | Function::StGeomFromEwkt => {
            // ST_GeomFromGML/GMLToSQL/GeomFromEWKT(text) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::Geometry, false)
        }
        Function::StGeomFromKml => {
            // ST_GeomFromKML(text) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            if let Some((e, t)) = typed.first() {
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(Type::Geometry, false)
        }
        // --- Point constructors ---
        Function::StMakePoint => {
            // ST_MakePoint(x, y[, z[, m]]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..4, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(Type::Geometry, true)
        }
        Function::StMakePointM => {
            // ST_MakePointM(x, y, m) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(Type::Geometry, true)
        }
        Function::StPoint => {
            // ST_Point(x, y) -> geometry (OGC alias for ST_MakePoint)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            for (e, t) in &typed {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            FullType::new(Type::Geometry, true)
        }
        // --- Envelope / extent constructors ---
        Function::StMakeEnvelope => {
            // ST_MakeEnvelope(xmin, ymin, xmax, ymax[, srid]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 4..5, args, span);
            for (e, t) in typed.iter().take(4) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            if let Some((e, t)) = typed.get(4) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            FullType::new(Type::Geometry, true)
        }
        // --- SetSRID / Transform ---
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
        Function::StTransform => {
            // ST_Transform(geom, srid) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Unary geometry transformations (geom -> geom) ---
        Function::StBoundary | Function::StBuildArea | Function::StCentroid
        | Function::StConvexHull | Function::StForce2D | Function::StForce3D
        | Function::StForce3DM | Function::StForce3DZ | Function::StForce4D
        | Function::StForceCollection | Function::StForceRHR | Function::StLineMerge
        | Function::StLineToCurve | Function::StMulti | Function::StPointOnSurface
        | Function::StReverse | Function::StShiftLongitude => {
            // Single-geometry-in, geometry-out
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StEnvelope | Function::StEndPoint | Function::StStartPoint
        | Function::StExteriorRing => {
            // ST_Envelope/EndPoint/StartPoint/ExteriorRing(geom) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Geometry-with-integer-arg accessors ---
        Function::StGeometryN | Function::StInteriorRingN | Function::StPointN => {
            // ST_GeometryN/InteriorRingN/PointN(geom, n) -> geometry (nullable)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            FullType::new(Type::Geometry, false)
        }
        Function::StCollectionExtract => {
            // ST_CollectionExtract(geom, type) -> geometry (type: 1=point, 2=line, 3=polygon)
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Binary geometry operations (geomA, geomB -> geom) ---
        Function::StClosestPoint | Function::StDifference | Function::StIntersection
        | Function::StLongestLine | Function::StShortestLine | Function::StSymDifference
        | Function::StUnion | Function::StMakeLine => {
            // Binary geom -> geom
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StCollect => {
            // ST_Collect(geomA, geomB) or ST_Collect(geom[]) -> geometry
            typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            FullType::new(Type::Geometry, false)
        }
        // --- Buffer ---
        Function::StBuffer => {
            // ST_Buffer(geom, radius[, buffer_style_params]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Simplify ---
        Function::StSimplify => {
            // ST_Simplify(geom, tolerance) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
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
        // --- Segmentize / SnapToGrid ---
        Function::StSegmentize => {
            // ST_Segmentize(geom, max_length) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StSnapToGrid => {
            // ST_SnapToGrid(geom, size...) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..5, args, span);
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Rotate / Scale / Translate ---
        Function::StRotate => {
            // ST_Rotate(geom, radians[, x, y | point]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..4, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StRotateX | Function::StRotateY | Function::StRotateZ => {
            // ST_RotateX/Y/Z(geom, radians) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StScale => {
            // ST_Scale(geom, xfactor, yfactor[, zfactor]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..4, args, span);
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StTranslate => {
            // ST_Translate(geom, x, y[, z]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..4, args, span);
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StTransScale => {
            // ST_TransScale(geom, deltaX, deltaY, XFactor, YFactor) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 5..5, args, span);
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StAffine => {
            // ST_Affine(geom, a, b, c, d, e, f[, g, h, i, xoff, yoff, zoff]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 7..13, args, span);
            for (e, t) in typed.iter().skip(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- LineString constructors ---
        Function::StLineFromMultiPoint | Function::StLineSubstring => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StLineInterpolatePoint => {
            // ST_Line_Interpolate_Point(geom, fraction) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StCurveToLine => {
            // ST_CurveToLine(geom[, segments_per_quarter]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Polygon constructors ---
        Function::StMakePolygon => {
            // ST_MakePolygon(outerring[, interiorring, ...]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..9999, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StPolygon => {
            // ST_Polygon(linestring, srid) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Point/line manipulation ---
        Function::StAddPoint => {
            // ST_AddPoint(geom, point[, position]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            if let Some((e, t)) = typed.get(2) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StRemovePoint => {
            // ST_RemovePoint(geom, offset) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StSetPoint => {
            // ST_SetPoint(geom, index, point) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(2).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StAddMeasure => {
            // ST_AddMeasure(geom, measure_start, measure_end) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            if let Some((e, t)) = typed.get(2) {
                typer.ensure_base(*e, t, BaseType::Float);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Minimum bounding circle ---
        Function::StMinimumBoundingCircle => {
            // ST_MinimumBoundingCircle(geom[, segs_per_quarter]) -> geometry
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            if let Some((e, t)) = typed.get(1) {
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        // --- Polygonize (aggregate, but may appear in non-aggregate context) ---
        Function::StPolygonize => {
            // ST_Polygonize(geom) aggregate -> geometry
            typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            FullType::new(Type::Geometry, false)
        }
        // --- Additional PostGIS functions ---
        Function::StMakeValid => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StIsValidDetail => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::StDump | Function::StDumpPoints | Function::StDumpRings | Function::StDumpSegments => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::StSnap => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false)
                && typed.get(1).map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StNode | Function::StSplit | Function::StSharedPaths | Function::StExpand => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StEstimatedExtent => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            FullType::new(Type::Geometry, false)
        }
        Function::StFlipCoordinates
        | Function::StForceCw
        | Function::StForceCcw
        | Function::StForcePolygonCw
        | Function::StForcePolygonCcw => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StConcaveHull => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StVoronoiPolygons | Function::StVoronoiLines => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StDelaunayTriangles => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StSubdivide => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StGeneratePoints => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StBoundingDiagonal => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..2, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StMaximumInscribedCircle => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            FullType::new(BaseType::Any, false)
        }
        Function::StChaikinSmoothing => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 1..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StFrechetDistance => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            FullType::new(Type::F64, false)
        }
        Function::StProject => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 3..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StLocateAlong | Function::StLocateBetween => {
            let typed = typed_args(typer, args, flags);
            arg_cnt(typer, 2..3, args, span);
            let not_null = typed.first().map(|(_, t)| t.not_null).unwrap_or(false);
            FullType::new(Type::Geometry, not_null)
        }
        Function::StInterpolatePoint => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            FullType::new(Type::F64, false)
        }
        Function::StMakeBox2D | Function::St3DMakeBox => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            FullType::new(Type::Geometry, false)
        }
        Function::St3DDistance | Function::St3DMaxDistance => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            FullType::new(Type::F64, false)
        }
        Function::St3DIntersects => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 2..2, args, span);
            FullType::new(BaseType::Bool, false)
        }
        Function::StExtent | Function::St3DExtent => {
            typed_args(typer, args, flags);
            arg_cnt(typer, 1..1, args, span);
            FullType::new(Type::Geometry, false)
        }
        Function::Other(parts) => {
            // Type all arguments regardless of whether we know the function
            typed_args(typer, args, flags);
            // Look up by function name, respecting dialect schema conventions
            let fn_ident = Identifier {
                value: parts.last().map(|id| id.value).unwrap_or_default(),
                span: parts
                    .last()
                    .map(|id| id.span.clone())
                    .unwrap_or_else(|| span.clone()),
            };
            let fn_name = fn_ident.value;
            let is_pg = typer.dialect().is_postgresql();
            let lookup_key = match parts.as_slice() {
                [_] => QualifiedIdentifier::Unqualified(fn_ident.clone()),
                [schema, _] if is_pg => {
                    QualifiedIdentifier::Qualified(schema.clone(), fn_ident.clone())
                }
                _ => {
                    let msg = if is_pg {
                        "Expected at most schema.function qualified name"
                    } else {
                        "Schema-qualified function names are not supported in MySQL"
                    };
                    typer.issues.err(msg, span);
                    QualifiedIdentifier::Unqualified(fn_ident.clone())
                }
            };
            if let Some(def) = lookup_name(&typer.schemas.functions, &lookup_key, typer.search_path()) {
                let col = parse_column(
                    def.return_type.clone(),
                    def.name.clone(),
                    typer.issues,
                    Some(typer.options),
                    Some(&typer.schemas.types),
                    typer.search_path(),
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
            if typer.dialect().is_maria() {
                FullType::new(BaseType::Integer, true)
            } else {
                FullType::new(Type::I64, true)
            }
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
