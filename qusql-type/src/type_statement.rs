use sql_ast::{Issue, Statement};

use crate::{
    type_delete::type_delete,
    type_insert::{type_insert, type_replace},
    type_select::{type_union, SelectType},
    type_update::type_update,
    typer::Typer,
};

pub(crate) enum InnerStatementType<'a> {
    Select(SelectType<'a>),
    Delete,
    Insert { auto_increment: bool },
    Update,
    Replace,
    Invalid,
}

pub(crate) fn type_statement<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    statement: &Statement<'a>,
) -> InnerStatementType<'a> {
    match statement {
        Statement::Select(s) => {
            InnerStatementType::Select(crate::type_select::type_select(typer, s, true))
        }
        Statement::Delete(d) => {
            type_delete(typer, d);
            InnerStatementType::Delete
        }
        Statement::Insert(i) => {
            type_insert(typer, i);
            InnerStatementType::Insert {
                auto_increment: false,
            }
        }
        Statement::Update(u) => {
            type_update(typer, u);
            InnerStatementType::Update
        }
        Statement::Union(u) => InnerStatementType::Select(type_union(typer, u)),
        Statement::Replace(r) => {
            type_replace(typer, r);
            InnerStatementType::Replace
        }
        s => {
            typer
                .issues
                .push(Issue::err("Cannot type statement of this type", s));
            InnerStatementType::Invalid
        }
    }
}
