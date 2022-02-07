use sql_ast::{Issue, TableReference};

use crate::{
    type_expression::type_expression,
    type_select::type_union_select,
    typer::{ReferenceType, Typer},
};

pub(crate) fn type_reference<'a>(
    typer: &mut Typer<'a>,
    reference: &TableReference<'a>,
    force_null: bool,
) {
    match reference {
        sql_ast::TableReference::Table {
            identifier, as_, ..
        } => {
            let identifier = match identifier.as_slice() {
                [v] => v,
                _ => {
                    typer.issues.push(Issue::todo(reference));
                    return;
                }
            };
            if let Some(s) = typer.schemas.schemas.get(&identifier.0) {
                let mut columns = Vec::new();
                for (n, t) in &s.columns {
                    let mut type_ = t.type_.ref_clone();
                    type_.not_null = type_.not_null && !force_null;
                    columns.push((*n, type_));
                }
                let name = as_.as_ref().unwrap_or(identifier).clone();
                for v in &typer.reference_types {
                    if v.name.0 == name.0 {
                        typer.issues.push(
                            Issue::err("Duplicate definitions", &name)
                                .frag("Allready defined here", &v.name),
                        );
                    }
                }
                typer.reference_types.push(ReferenceType { name, columns });
            } else {
                typer
                    .issues
                    .push(Issue::err("Unknown table or view", identifier))
            }
        }
        sql_ast::TableReference::Query { query, as_, .. } => {
            let select = type_union_select(typer, query);

            let name = if let Some(as_) = as_ {
                as_.clone()
            } else {
                ("", 0..0)
            };

            typer.reference_types.push(ReferenceType {
                name,
                columns: select
                    .columns
                    .iter()
                    .filter_map(|v| {
                        if let Some(name) = v.name {
                            Some((name, v.type_.clone()))
                        } else {
                            None
                        }
                    })
                    .collect(),
            });
        }
        sql_ast::TableReference::Join {
            join,
            left,
            right,
            specification,
        } => {
            let (left_force_null, right_force_null) = match join {
                sql_ast::JoinType::Left(_) => (force_null, true),
                _ => {
                    typer.issues.push(Issue::todo(join));
                    (force_null, force_null)
                }
            };
            type_reference(typer, left, left_force_null);
            type_reference(typer, right, right_force_null);
            match &specification {
                Some(sql_ast::JoinSpecification::On(e, r)) => {
                    let t = type_expression(typer, e, false);
                    typer.ensure_bool(e, &t);
                }
                Some(s @ sql_ast::JoinSpecification::Using(_, _)) => {
                    typer.issues.push(Issue::todo(s));
                }
                None => (),
            }
        }
    }
}
