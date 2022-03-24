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

use sql_parse::{Identifier, Issue, OptSpanned, Update};

use crate::{
    type_::BaseType,
    type_expression::{type_expression, ExpressionFlags},
    type_reference::type_reference,
    typer::{typer_stack, Typer},
    Type,
};

pub(crate) fn type_update<'a, 'b>(typer: &mut Typer<'a, 'b>, update: &Update<'a>) {
    let mut guard = typer_stack(
        typer,
        |t| core::mem::take(&mut t.reference_types),
        |t, v| t.reference_types = v,
    );
    let typer = &mut guard.typer;

    for f in &update.flags {
        match f {
            sql_parse::UpdateFlag::LowPriority(_) | sql_parse::UpdateFlag::Ignore(_) => (),
        }
    }

    for reference in &update.tables {
        type_reference(typer, reference, false);
    }

    for (key, value) in &update.set {
        let value_type = type_expression(typer, value, ExpressionFlags::default());
        match key.as_slice() {
            [key] => {
                let mut cnt = 0;
                let mut t = None;
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == key.value {
                            cnt += 1;
                            t = Some(c.clone());
                        }
                    }
                }
                if cnt > 1 {
                    let mut issue = Issue::err("Ambiguous reference", &key.opt_span().unwrap());
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            if c.0 == key.value {
                                issue = issue.frag("Defined here", &r.span);
                            }
                        }
                    }
                    typer.issues.push(issue);
                } else if let Some(t) = t {
                    if typer.matched_type(&value_type, &t.1).is_none() {
                        typer.issues.push(Issue::err(
                            alloc::format!("Got type {} expected {}", value_type, t.1),
                            value,
                        ));
                    } else if let Type::Args(_, args) = &value_type.t {
                        for (idx, _) in args {
                            typer.constrain_arg(*idx, &t.1);
                        }
                    }
                } else {
                    typer
                        .issues
                        .push(Issue::err("Unknown identifier", &key.opt_span().unwrap()));
                }
            }
            [Identifier { value: table, .. }, Identifier { value: column, .. }] => {
                let mut t = None;
                for r in &typer.reference_types {
                    if r.name != Some(*table) {
                        continue;
                    }
                    for c in &r.columns {
                        if c.0 == *column {
                            t = Some(c.clone());
                        }
                    }
                }
                if let Some(t) = t {
                    if typer.matched_type(&value_type, &t.1).is_none() {
                        typer.issues.push(Issue::err(
                            alloc::format!("Got type {} expected {}", value_type, t.1),
                            value,
                        ));
                    } else if let Type::Args(_, args) = &value_type.t {
                        for (idx, _) in args {
                            typer.constrain_arg(*idx, &t.1);
                        }
                    }
                } else {
                    typer
                        .issues
                        .push(Issue::err("Unknown identifier", &key.opt_span().unwrap()));
                }
            }
            _ => {
                typer
                    .issues
                    .push(Issue::err("Unknown identifier", &key.opt_span().unwrap()));
            }
        }
    }

    if let Some((where_, _)) = &update.where_ {
        let t = type_expression(typer, where_, ExpressionFlags::default());
        typer.ensure_base(where_, &t, BaseType::Bool);
    }
}
