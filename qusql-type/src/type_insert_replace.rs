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
use sql_parse::{issue_todo, InsertReplace, InsertReplaceFlag, InsertReplaceType, Issue, Spanned};

use crate::{
    type_expression::type_expression,
    type_select::type_select,
    typer::{typer_stack, ReferenceType, Typer},
    Type,
};

/// Does the insert yield an auto increment id
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoIncrementId {
    Yes,
    No,
    Optional,
}

pub(crate) fn type_insert_replace<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    ior: &InsertReplace<'a>,
) -> AutoIncrementId {
    let table = &ior.table;
    let columns = &ior.columns;

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

    if let Some(values) = &ior.values {
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

    if let Some(select) = &ior.select {
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

    let mut guard = typer_stack(
        typer,
        |t| core::mem::take(&mut t.reference_types),
        |t, v| t.reference_types = v,
    );
    let typer = &mut guard.typer;

    if let Some(s) = typer.schemas.schemas.get(t.value) {
        let mut columns = Vec::new();
        for (n, t) in &s.columns {
            columns.push((*n, t.type_.ref_clone()));
        }
        for v in &typer.reference_types {
            if v.name == Some(t.value) {
                typer.issues.push(
                    Issue::err("Duplicate definitions", t).frag("Already defined here", &v.span),
                );
            }
        }
        typer.reference_types.push(ReferenceType {
            name: Some(t.value),
            span: t.span(),
            columns,
        });
    }

    if let Some((_, set)) = &ior.set {
        for (key, _, value) in set {
            let value_type = type_expression(typer, value, false);
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
                let mut issue = Issue::err("Ambiguous reference", key);
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
                        format!("Got type {} expected {}", value_type, t.1),
                        value,
                    ));
                } else if let Type::Args(_, args) = &value_type.t {
                    for (idx, _) in args {
                        typer.constrain_arg(*idx, &t.1);
                    }
                }
            } else {
                typer.issues.push(Issue::err("Unknown identifier", key));
            }
        }
    }

    if let Some((_, update)) = &ior.on_duplicate_key_update {
        for (key, _, value) in update {
            let value_type = type_expression(typer, value, false);
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
                let mut issue = Issue::err("Ambiguous reference", key);
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
                        format!("Got type {} expected {}", value_type, t.1),
                        value,
                    ));
                } else if let Type::Args(_, args) = &value_type.t {
                    for (idx, _) in args {
                        typer.constrain_arg(*idx, &t.1);
                    }
                }
            } else {
                typer.issues.push(Issue::err("Unknown identifier", key));
            }
        }
    }
    core::mem::drop(guard);

    if auto_increment && matches!(ior.type_, InsertReplaceType::Insert(_)) {
        if ior
            .flags
            .iter()
            .any(|f| matches!(f, InsertReplaceFlag::Ignore(_)))
            || ior.on_duplicate_key_update.is_some()
        {
            AutoIncrementId::Optional
        } else {
            AutoIncrementId::Yes
        }
    } else {
        AutoIncrementId::No
    }
}
