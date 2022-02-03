use sql_ast::{Issue, TableReference};

use crate::{
    type_expression::type_expression,
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
            if identifier.len() != 1 {
                typer.issues.push(Issue::todo(reference));
                return;
            }
            let identifier = &identifier[0];
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
        sql_ast::TableReference::Query {
            query,
            as_span,
            as_,
        } => {
            //     Query {
            //         query: Box<Statement<'a>>,
            //         as_span: Option<Span>,
            //         as_: Option<(&'a str, Span)>,
            //         //TODO collist
            //     },
            typer.issues.push(Issue::todo(reference));
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
                Some(s @ sql_ast::JoinSpecification::On(e, r)) => {
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
