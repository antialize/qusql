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
use sql_parse::{issue_todo, Insert, Issue, Replace, Spanned};

use crate::{type_expression::type_expression, type_select::type_select, typer::Typer, Type};

enum InsertOrReplace<'a, 'b> {
    Replace(&'b Replace<'a>),
    Insert(&'b Insert<'a>),
}

fn type_insert_or_replace<'a, 'b>(typer: &mut Typer<'a, 'b>, ior: InsertOrReplace<'a, '_>) -> bool {
    match ior {
        InsertOrReplace::Replace(replace) => {
            for flag in &replace.flags {
                match &flag {
                    sql_parse::ReplaceFlag::LowPriority(_) | sql_parse::ReplaceFlag::Delayed(_) => {
                    }
                }
            }
        }
        InsertOrReplace::Insert(insert) => {
            for flag in &insert.flags {
                match &flag {
                    sql_parse::InsertFlag::LowPriority(_)
                    | sql_parse::InsertFlag::HighPriority(_)
                    | sql_parse::InsertFlag::Delayed(_)
                    | sql_parse::InsertFlag::Ignore(_) => (),
                }
            }
        }
    }

    let table = match ior {
        InsertOrReplace::Replace(replace) => &replace.table,
        InsertOrReplace::Insert(insert) => &insert.table,
    };

    let columns = match ior {
        InsertOrReplace::Replace(replace) => &replace.columns,
        InsertOrReplace::Insert(insert) => &insert.columns,
    };

    if let Some(v) = table.get(1..) {
        for t in v {
            typer.issues.push(issue_todo!(t));
        }
    }

    let t = &table[0];
    let (s, auto_increment) = if let Some(schema) = typer.schemas.schemas.get(t.value) {
        if schema.view {
            typer
                .issues
                .push(Issue::err("Inserts into views not yet implemented", t));
        }
        let mut col_types = Vec::new();

        for col in columns {
            if let Some(schema_col) = schema.columns.get(col.value) {
                col_types.push((schema_col.type_.ref_clone(), col.span()));
            } else {
                typer
                    .issues
                    .push(Issue::err("No such column in schema", col));
            }
        }
        (
            Some(col_types),
            schema.columns.iter().any(|(_, c)| c.auto_increment),
        )
    } else {
        typer.issues.push(Issue::err("Unknown table", t));
        (None, false)
    };

    let values = match ior {
        InsertOrReplace::Replace(replace) => &replace.values,
        InsertOrReplace::Insert(insert) => &insert.values,
    };

    if let Some(values) = &values {
        for row in &values.1 {
            for (j, e) in row.iter().enumerate() {
                let t = type_expression(typer, e, false);
                if let Some((et, ets)) = s.as_ref().and_then(|v| v.get(j)) {
                    if typer.matched_type(&t, et).is_none() {
                        typer.issues.push(
                            Issue::err(format!("Got type {}", t.t), e)
                                .frag(format!("Expected {}", et.t), ets),
                        );
                    } else if let Type::Args(_, args) = &t.t {
                        for (idx, _) in args {
                            typer.constrain_arg(*idx, et);
                        }
                    }
                }
            }
        }
    }

    let select = match ior {
        InsertOrReplace::Replace(replace) => &replace.select,
        InsertOrReplace::Insert(insert) => &insert.select,
    };
    if let Some(select) = &select {
        let select = type_select(typer, select, true);
        if let Some(s) = s {
            for i in 0..usize::max(s.len(), select.columns.len()) {
                match (s.get(i), select.columns.get(i)) {
                    (Some((et, ets)), Some(t)) => {
                        if typer.matched_type(&t.type_, et).is_none() {
                            typer.issues.push(
                                Issue::err(format!("Got type {}", t.type_.t), &t.span)
                                    .frag(format!("Expected {}", et.t), ets),
                            );
                        }
                    }
                    (None, Some(t)) => {
                        typer
                            .issues
                            .push(Issue::err("Column in select not in insert", &t.span));
                    }
                    (Some((_, ets)), None) => {
                        typer
                            .issues
                            .push(Issue::err("Missing column in select", ets));
                    }
                    (None, None) => {
                        panic!("ICE")
                    }
                }
            }
        }
    }

    let set = match ior {
        InsertOrReplace::Replace(replace) => &replace.set,
        InsertOrReplace::Insert(_) => &None,
    };
    if let Some(set) = set {
        issue_todo!(set);
    }
    auto_increment
}

pub(crate) fn type_insert<'a, 'b>(typer: &mut Typer<'a, 'b>, insert: &Insert<'a>) -> bool {
    type_insert_or_replace(typer, InsertOrReplace::Insert(insert))
}

pub(crate) fn type_replace<'a, 'b>(typer: &mut Typer<'a, 'b>, replace: &Replace<'a>) {
    type_insert_or_replace(typer, InsertOrReplace::Replace(replace));
}
