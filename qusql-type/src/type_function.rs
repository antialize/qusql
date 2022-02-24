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
use sql_parse::{issue_todo, Expression, Function, Issue, Span};

use crate::{
    type_::{BaseType, FullType},
    type_expression::type_expression,
    typer::Typer,
    Type,
};

fn arg_cnt<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
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

pub(crate) fn type_function<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    func: &Function<'a>,
    args: &[Expression<'a>],
    span: &Span,
) -> FullType<'a> {
    let mut typed = Vec::new();
    for arg in args {
        typed.push((arg, type_expression(typer, arg, false)));
    }
    match func {
        Function::UnixTimestamp => {
            arg_cnt(typer, 0..1, args, span);
            if let Some(arg) = args.get(0) {
                typer.issues.push(issue_todo!(arg));
            }
            FullType::new(Type::U64, true)
        }
        Function::Rand => {
            arg_cnt(typer, 0..1, args, span);
            if let Some(arg) = args.get(0) {
                typer.issues.push(issue_todo!(arg));
            }
            FullType::new(Type::F64, true)
        }
        Function::IfNull => {
            arg_cnt(typer, 2..2, args, span);
            let t = if let Some((e, t)) = typed.get(0) {
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
        Function::JsonExtract => {
            arg_cnt(typer, 2..999, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonValue => {
            arg_cnt(typer, 2..2, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(Type::JSON, false)
        }
        Function::JsonUnquote => {
            arg_cnt(typer, 1..1, args, span);
            for (a, t) in &typed {
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(BaseType::String, false)
        }
        Function::Min | Function::Max | Function::Sum => {
            arg_cnt(typer, 1..1, args, span);
            if let Some((_, t2)) = typed.get(0) {
                //TODO check that the type can be mined or maxed
                t2.clone()
            } else {
                FullType::invalid()
            }
        }
        Function::Right | Function::Left => {
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((a, t)) = typed.get(0) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*a, t, BaseType::String);
            }
            if let Some((a, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*a, t, BaseType::Integer);
            }
            FullType::new(BaseType::String, not_null)
        }
        Function::FindInSet => {
            arg_cnt(typer, 2..2, args, span);
            let mut not_null = true;
            if let Some((a, t)) = typed.get(0) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*a, t, BaseType::String);
            }
            if let Some((a, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*a, t, BaseType::String);
            }
            FullType::new(BaseType::Integer, not_null)
        }
        Function::Now => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::DateTime, true)
        }
        Function::CurDate => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::Date, true)
        }
        Function::CurrentTimestamp => {
            arg_cnt(typer, 0..0, args, span);
            FullType::new(BaseType::TimeStamp, true)
        }
        Function::Concat => {
            let mut not_null = true;
            for (e, t) in typed {
                typer.ensure_base(e, &t, BaseType::String);
                not_null = not_null && t.not_null;
            }
            FullType::new(BaseType::String, not_null)
        }
        Function::Least | Function::Greatest => {
            arg_cnt(typer, 1..9999, args, span);
            if let Some((a, at)) = typed.get(0) {
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
        Function::SubStringIndex => {
            arg_cnt(typer, 3..3, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.get(0) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(2) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::Integer);
            }
            FullType::new(BaseType::String, not_null)
        }
        Function::Replace => {
            arg_cnt(typer, 3..3, args, span);
            let mut not_null = true;
            if let Some((e, t)) = typed.get(0) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(1) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            if let Some((e, t)) = typed.get(2) {
                not_null = not_null && t.not_null;
                typer.ensure_base(*e, t, BaseType::String);
            }
            FullType::new(BaseType::String, not_null)
        }
        _ => {
            typer
                .issues
                .push(Issue::err("Tying for function not implemnted", span));
            FullType::invalid()
        }
    }
}
