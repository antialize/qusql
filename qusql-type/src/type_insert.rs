use sql_ast::{Insert, Issue, Replace, Select};

use crate::{type_expression::type_expression, type_select::type_select, typer::Typer};

enum InsertOrReplace<'a, 'b> {
    Replace(&'b Replace<'a>),
    Insert(&'b Insert<'a>),
}

fn type_insert_or_replace<'a>(typer: &mut Typer<'a>, ior: InsertOrReplace<'a, '_>) {
    match ior {
        InsertOrReplace::Replace(replace) => {
            for flag in &replace.flags {
                match &flag {
                    sql_ast::ReplaceFlag::LowPriority(_) | sql_ast::ReplaceFlag::Delayed(_) => (),
                }
            }
        }
        InsertOrReplace::Insert(insert) => {
            for flag in &insert.flags {
                match &flag {
                    sql_ast::InsertFlag::LowPriority(_)
                    | sql_ast::InsertFlag::HighPriority(_)
                    | sql_ast::InsertFlag::Delayed(_)
                    | sql_ast::InsertFlag::Ignore(_) => (),
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
            typer.issues.push(Issue::todo(t));
        }
    }

    let t = &table[0];
    let s = if let Some(schema) = typer.schemas.schemas.get(t.0) {
        if schema.view {
            typer
                .issues
                .push(Issue::err("Inserts into views not yet implemented", t));
        }
        let mut col_types = Vec::new();
        for col in columns {
            if let Some(schema_col) = schema.columns.get(col.0) {
                col_types.push((schema_col.type_.ref_clone(), col.1.clone()));
            } else {
                typer
                    .issues
                    .push(Issue::err("No such column in schema", col));
            }
        }
        Some(col_types)
    } else {
        typer.issues.push(Issue::err("Unknown table", t));
        None
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
                    if typer.common_type(&t, et).is_none() {
                        typer.issues.push(
                            Issue::err(format!("Got type {}", t.t), e)
                                .frag(format!("Expected {}", et.t), ets),
                        );
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
                        if typer.common_type(&t.type_, et).is_none() {
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
}

pub(crate) fn type_insert<'a>(typer: &mut Typer<'a>, insert: &Insert<'a>) {
    type_insert_or_replace(typer, InsertOrReplace::Insert(insert))
}

pub(crate) fn type_replace<'a>(typer: &mut Typer<'a>, replace: &Replace<'a>) {
    type_insert_or_replace(typer, InsertOrReplace::Replace(replace))
}
