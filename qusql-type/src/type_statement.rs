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

use sql_parse::{InsertReplaceType, Issue, Statement};

use crate::{
    type_delete::type_delete,
    type_insert_replace::{type_insert_replace, AutoIncrementId},
    type_select::{type_union, SelectType},
    type_update::type_update,
    typer::Typer,
};

pub(crate) enum InnerStatementType<'a> {
    Select(SelectType<'a>),
    Delete,
    Insert {
        auto_increment_id: AutoIncrementId,
        returning: Option<SelectType<'a>>,
    },
    Update,
    Replace {
        returning: Option<SelectType<'a>>,
    },
    Invalid,
}

pub(crate) fn type_statement<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    statement: &Statement<'a>,
) -> InnerStatementType<'a> {
    match statement {
        Statement::Select(s) => InnerStatementType::Select(crate::type_select::type_select(
            typer,
            s,
            typer.options.warn_duplicate_column_in_select,
        )),
        Statement::Delete(d) => {
            type_delete(typer, d);
            InnerStatementType::Delete
        }
        Statement::InsertReplace(ior) => {
            let (auto_increment_id, returning) = type_insert_replace(typer, ior);
            match &ior.type_ {
                InsertReplaceType::Insert(_) => InnerStatementType::Insert {
                    auto_increment_id,
                    returning,
                },
                InsertReplaceType::Replace(_) => InnerStatementType::Replace { returning },
            }
        }
        Statement::Update(u) => {
            type_update(typer, u);
            InnerStatementType::Update
        }
        Statement::Union(u) => InnerStatementType::Select(type_union(typer, u)),
        s => {
            typer
                .issues
                .push(Issue::err("Cannot type statement of this type", s));
            InnerStatementType::Invalid
        }
    }
}
