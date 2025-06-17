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
use sql_parse::{
    issue_todo, InsertReplace, InsertReplaceFlag, InsertReplaceSetPair, InsertReplaceType,
    OptSpanned, Spanned,
};

use crate::{
    type_expression::{type_expression, ExpressionFlags},
    type_select::{type_select, type_select_exprs, SelectType},
    typer::{typer_stack, unqualified_name, ReferenceType, Typer},
    BaseType, SelectTypeColumn, Type,
};

/// Does the insert yield an auto increment id
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoIncrementId {
    Yes,
    No,
    Optional,
}

pub(crate) fn type_insert_replace<'a>(
    typer: &mut Typer<'a, '_>,
    ior: &InsertReplace<'a>,
) -> (AutoIncrementId, Option<SelectType<'a>>) {
    let table = unqualified_name(typer.issues, &ior.table);
    let columns = &ior.columns;

    let (s, auto_increment) = if let Some(schema) = typer.schemas.schemas.get(table.value) {
        if schema.view {
            typer.err("Inserts into views not yet implemented", table);
        }
        let mut col_types = Vec::new();

        for col in columns {
            if let Some(schema_col) = schema.get_column(col.value) {
                col_types.push((schema_col.type_.clone(), col.span()));
            } else {
                typer.err("No such column in schema", col);
            }
        }

        if let Some(set) = &ior.set {
            for col in &schema.columns {
                if col.auto_increment
                    || col.default
                    || !col.type_.not_null
                    || col.as_.is_some()
                    || col.generated
                    || set.pairs.iter().any(|v| v.column==col.identifier)
                {
                    continue;
                }
                typer.err(
                    format!(
                        "No value for column {} provided, but it has no default value",
                        &col.identifier
                    ),
                    set
                );
            }
        } else {
            for col in &schema.columns {
                if col.auto_increment
                    || col.default
                    || !col.type_.not_null
                    || col.as_.is_some()
                    || col.generated
                    || columns.contains(&col.identifier)
                {
                    continue;
                }
                typer.err(
                    format!(
                        "No value for column {} provided, but it has no default value",
                        &col.identifier
                    ),
                    &columns.opt_span().unwrap()
                );
            }
        }

        (
            Some(col_types),
            schema.columns.iter().any(|c| c.auto_increment),
        )
    } else {
        typer.err("Unknown table", table);
        (None, false)
    };

    if let Some(values) = &ior.values {
        for row in &values.1 {
            for (j, e) in row.iter().enumerate() {
                if let Some((et, ets)) = s.as_ref().and_then(|v| v.get(j)) {
                    let t = type_expression(typer, e, ExpressionFlags::default(), et.base());
                    if typer.matched_type(&t, et).is_none() {
                        typer
                            .err(format!("Got type {}", t.t), e)
                            .frag(format!("Expected {}", et.t), ets);
                    } else if let Type::Args(_, args) = &t.t {
                        for (idx, arg_type, _) in args.iter() {
                            typer.constrain_arg(*idx, arg_type, et);
                        }
                    }
                } else {
                    type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
                }
            }
            if let Some(s) = &s {
                if s.len() != row.len() {
                    typer
                        .err(
                            format!("Got {} columns", row.len()),
                            &row.opt_span().unwrap(),
                        )
                        .frag(
                            format!("Expected {}", columns.len()),
                            &columns.opt_span().unwrap(),
                        );
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
                            typer
                                .err(format!("Got type {}", t.type_.t), &t.span)
                                .frag(format!("Expected {}", et.t), ets);
                        }
                    }
                    (None, Some(t)) => {
                        typer.err("Column in select not in insert", &t.span);
                    }
                    (Some((_, ets)), None) => {
                        typer.err("Missing column in select", ets);
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

    if let Some(s) = typer.schemas.schemas.get(table.value) {
        let mut columns = Vec::new();
        for c in &s.columns {
            columns.push((c.identifier.clone(), c.type_.clone()));
        }
        for v in &typer.reference_types {
            if v.name == Some(table.clone()) {
                typer
                    .issues
                    .err("Duplicate definitions", table)
                    .frag("Already defined here", &v.span);
            }
        }
        typer.reference_types.push(ReferenceType {
            name: Some(table.clone()),
            span: table.span(),
            columns,
        });
    }

    if let Some(set) = &ior.set {
        for InsertReplaceSetPair { column, value, .. } in &set.pairs {
            let mut cnt = 0;
            let mut t = None;
            for r in &typer.reference_types {
                for c in &r.columns {
                    if c.0 == *column {
                        cnt += 1;
                        t = Some(c.clone());
                    }
                }
            }
            if cnt > 1 {
                type_expression(typer, value, ExpressionFlags::default(), BaseType::Any);
                let mut issue = typer.issues.err("Ambiguous reference", column);
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == *column {
                            issue.frag("Defined here", &r.span);
                        }
                    }
                }
            } else if let Some(t) = t {
                let value_type =
                    type_expression(typer, value, ExpressionFlags::default(), t.1.base());
                if typer.matched_type(&value_type, &t.1).is_none() {
                    typer.err(format!("Got type {} expected {}", value_type, t.1), value);
                } else if let Type::Args(_, args) = &value_type.t {
                    for (idx, arg_type, _) in args.iter() {
                        typer.constrain_arg(*idx, arg_type, &t.1);
                    }
                }
            } else {
                type_expression(typer, value, ExpressionFlags::default(), BaseType::Any);
                typer.err("Unknown identifier", column);
            }
        }
    }

    if let Some(up) = &ior.on_duplicate_key_update {
        for InsertReplaceSetPair { value, column, .. } in &up.pairs {
            let mut cnt = 0;
            let mut t = None;
            for r in &typer.reference_types {
                for c in &r.columns {
                    if c.0 == *column {
                        cnt += 1;
                        t = Some(c.clone());
                    }
                }
            }
            let flags = ExpressionFlags::default().with_in_on_duplicate_key_update(true);
            if cnt > 1 {
                type_expression(typer, value, flags, BaseType::Any);
                let mut issue = typer.issues.err("Ambiguous reference", column);
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == *column {
                            issue.frag("Defined here", &r.span);
                        }
                    }
                }
            } else if let Some(t) = t {
                let value_type = type_expression(typer, value, flags, t.1.base());
                if typer.matched_type(&value_type, &t.1).is_none() {
                    typer.err(format!("Got type {} expected {}", value_type, t.1), value);
                } else if let Type::Args(_, args) = &value_type.t {
                    for (idx, arg_type, _) in args.iter() {
                        typer.constrain_arg(*idx, arg_type, &t.1);
                    }
                }
            } else {
                type_expression(typer, value, flags, BaseType::Any);
                typer.err("Unknown identifier", column);
            }
        }
    }

    if let Some(on_conflict) = &ior.on_conflict {
        match &on_conflict.target {
            sql_parse::OnConflictTarget::Column { name } => {
                let mut t = None;
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == *name {
                            t = Some(c.clone());
                        }
                    }
                }
                if t.is_none() {
                    typer.err("Unknown identifier", name);
                }
                //TODO check if there is a unique constraint on column
            }
            sql_parse::OnConflictTarget::OnConstraint {
                on_constraint_span, ..
            } => {
                issue_todo!(typer.issues, on_constraint_span);
            }
            sql_parse::OnConflictTarget::None => (),
        }

        match &on_conflict.action {
            sql_parse::OnConflictAction::DoNothing(_) => (),
            sql_parse::OnConflictAction::DoUpdateSet { sets, where_, .. } => {
                for (key, value) in sets {
                    let mut cnt = 0;
                    let mut t = None;
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            if c.0 == *key {
                                cnt += 1;
                                t = Some(c.clone());
                            }
                        }
                    }
                    let flags = ExpressionFlags::default().with_in_on_duplicate_key_update(true);
                    if cnt > 1 {
                        type_expression(typer, value, flags, BaseType::Any);
                        let mut issue = typer.issues.err("Ambiguous reference", key);
                        for r in &typer.reference_types {
                            for c in &r.columns {
                                if c.0 == *key {
                                    issue.frag("Defined here", &r.span);
                                }
                            }
                        }
                    } else if let Some(t) = t {
                        let value_type = type_expression(typer, value, flags, t.1.base());
                        if typer.matched_type(&value_type, &t.1).is_none() {
                            typer.err(format!("Got type {} expected {}", value_type, t.1), value);
                        } else if let Type::Args(_, args) = &value_type.t {
                            for (idx, arg_type, _) in args.iter() {
                                typer.constrain_arg(*idx, arg_type, &t.1);
                            }
                        }
                    } else {
                        type_expression(typer, value, flags, BaseType::Any);
                        typer.err("Unknown identifier", key);
                    }
                }
                if let Some((_, where_)) = where_ {
                    type_expression(typer, where_, ExpressionFlags::default(), BaseType::Bool);
                }
            }
        }
    }

    let returning_select = match &ior.returning {
        Some((returning_span, returning_exprs)) => {
            let columns = type_select_exprs(typer, returning_exprs, true)
                .into_iter()
                .map(|(name, type_, span)| SelectTypeColumn { name, type_, span })
                .collect();
            Some(SelectType {
                columns,
                select_span: returning_span.join_span(returning_exprs),
            })
        }
        None => None,
    };

    core::mem::drop(guard);

    let auto_increment_id = if auto_increment && matches!(ior.type_, InsertReplaceType::Insert(_)) {
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
    };

    (auto_increment_id, returning_select)
}
