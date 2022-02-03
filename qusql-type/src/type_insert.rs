use sql_ast::{Insert, Issue};

use crate::{type_expression::type_expression, typer::Typer};

pub(crate) fn type_insert<'a>(typer: &mut Typer<'a>, insert: &Insert<'a>) {
    for flag in &insert.flags {
        match &flag {
            sql_ast::InsertFlag::LowPriority(_)
            | sql_ast::InsertFlag::HighPriority(_)
            | sql_ast::InsertFlag::Delayed(_)
            | sql_ast::InsertFlag::Ignore(_) => (),
        }
    }

    if let Some(v) = insert.table.get(1..) {
        for t in v {
            typer.issues.push(Issue::todo(t));
        }
    }

    let t = &insert.table[0];
    let s = if let Some(schema) = typer.schemas.schemas.get(t.0) {
        if schema.view {
            typer
                .issues
                .push(Issue::err("Inserts into views not yet implemented", t));
        }
        let mut col_types = Vec::new();
        for col in &insert.columns {
            if let Some(schema_col) = schema.columns.get(col.0) {
                col_types.push(schema_col.type_.ref_clone());
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

    if let Some(values) = &insert.values {
        for row in &values.1 {
            for e in row {
                type_expression(typer, e, false);
                //TODO compare types
            }
        }
    }

    if let Some(select) = &insert.select {
        typer.issues.push(Issue::todo(select));
    }
}
