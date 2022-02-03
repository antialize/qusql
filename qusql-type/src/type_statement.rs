use sql_ast::{Issue, Statement};

use crate::{type_delete::type_delete, type_insert::type_insert, typer::Typer};

pub(crate) fn type_statement<'a>(typer: &mut Typer<'a>, statement: &Statement<'a>) {
    match statement {
        Statement::Select(s) => {
            let st = crate::type_select::type_select(typer, s);
            println!("HI {:#?}", st);
        }
        Statement::Delete(d) => {
            type_delete(typer, d);
        }
        Statement::Insert(i) => {
            type_insert(typer, i);
        }
        //Statement::Update(_) => todo!(),
        //Statement::Union(_) => todo!(),
        //Statement::Replace(_) => todo!(),
        s => typer
            .issues
            .push(Issue::err("Cannot type statement of this type", s)),
    }
}
