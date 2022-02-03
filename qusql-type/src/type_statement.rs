use sql_ast::{Issue, Statement};

use crate::{
    type_delete::type_delete, type_insert::type_insert, type_update::type_update, typer::Typer,
};

pub(crate) fn type_statement<'a>(typer: &mut Typer<'a>, statement: &Statement<'a>) {
    match statement {
        Statement::Select(s) => {
            let st = crate::type_select::type_select(typer, s, true);
            //println!("HI {:#?}", st);
            //println!("HAT {:#?}", typer.arg_types);
        }
        Statement::Delete(d) => {
            type_delete(typer, d);
        }
        Statement::Insert(i) => {
            type_insert(typer, i);
        }
        Statement::Update(u) => {
            type_update(typer, u);
            //println!("HAT {:#?}", typer.arg_types);
        }
        //Statement::Union(_) => todo!(),
        //Statement::Replace(_) => todo!(),
        s => typer
            .issues
            .push(Issue::err("Cannot type statement of this type", s)),
    }
}
