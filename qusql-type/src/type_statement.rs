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
        Statement::Insert(i) => InnerStatementType::Insert {
            auto_increment: type_insert(typer, i),
        },
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
