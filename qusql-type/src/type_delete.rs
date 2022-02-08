use sql_ast::{issue_todo, Delete, Issue, OptSpanned, Spanned};

use crate::{
    type_expression::type_expression,
    typer::{ReferenceType, Typer},
};

pub(crate) fn type_delete<'a, 'b>(typer: &mut Typer<'a, 'b>, delete: &Delete<'a>) {
    let old_reference_type = std::mem::take(&mut typer.reference_types);

    for flag in &delete.flags {
        match flag {
            sql_ast::DeleteFlag::LowPriority(_)
            | sql_ast::DeleteFlag::Quick(_)
            | sql_ast::DeleteFlag::Ignore(_) => (),
        }
    }

    let identifier = match delete.table.as_slice() {
        [v] => v,
        _ => {
            typer
                .issues
                .push(issue_todo!(&delete.table.opt_span().unwrap()));
            return;
        }
    };

    if let Some(s) = typer.schemas.schemas.get(&identifier.value) {
        let mut columns = Vec::new();
        for (n, t) in &s.columns {
            columns.push((*n, t.type_.ref_clone()));
        }
        typer.reference_types.push(ReferenceType {
            name: Some(identifier.value),
            span: identifier.span(),
            columns,
        });
    } else {
        typer
            .issues
            .push(Issue::err("Unknown table or view", identifier))
    }

    if let Some((where_, _)) = &delete.where_ {
        let t = type_expression(typer, where_, false);
        typer.ensure_bool(where_, &t);
    }

    typer.reference_types = old_reference_type;
}
