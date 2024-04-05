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

use sql_parse::{InsertReplaceType, Issue, Statement, WithBlock};

use alloc::vec::Vec;

use crate::{
    schema::{Column, Schema},
    type_delete::type_delete,
    type_insert_replace::{type_insert_replace, AutoIncrementId},
    type_select::{type_union, SelectType},
    type_update::type_update,
    typer::Typer,
};

pub(crate) enum InnerStatementType<'a> {
    Select(SelectType<'a>),
    Delete {
        returning: Option<SelectType<'a>>,
    },
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

fn type_with_query<'a>(
    typer: &mut Typer<'a, '_>,
    with_blocks: &[WithBlock<'a>],
    inner: &Statement<'a>,
) -> InnerStatementType<'a> {
    if let Some((block, rem_blocks)) = with_blocks.split_first() {
        let r = type_statement(typer, &block.statement);
        let s = match r {
            InnerStatementType::Select(v) => Some(v),
            InnerStatementType::Delete { returning } => returning,
            InnerStatementType::Insert { returning, .. } => returning,
            _ => None,
        };
        if let Some(s) = s {
            let mut columns = Vec::new();
            for c in s.columns {
                if let Some(name) = c.name {
                    columns.push(Column {
                        identifier: name,
                        identifier_span: c.span,
                        type_: c.type_,
                        auto_increment: false,
                    });
                }
            }
            let schema: Schema<'a> = Schema {
                identifier_span: block.identifier.span.clone(),
                columns,
                view: true,
            };

            let mut schemas = typer.with_schemas.clone();
            schemas.insert(block.identifier.as_str(), &schema);
            let mut typer = typer.with_schemas(schemas);
            type_with_query(&mut typer, rem_blocks, inner)
        } else {
            type_with_query(typer, rem_blocks, inner)
        }
    } else {
        type_statement(typer, inner)
    }
}

pub(crate) fn type_statement<'a>(
    typer: &mut Typer<'a, '_>,
    statement: &Statement<'a>,
) -> InnerStatementType<'a> {
    match statement {
        Statement::Select(s) => InnerStatementType::Select(crate::type_select::type_select(
            typer,
            s,
            typer.options.warn_duplicate_column_in_select,
        )),
        Statement::Delete(d) => {
            let returning = type_delete(typer, d);
            InnerStatementType::Delete { returning }
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
        Statement::WithQuery(w) => type_with_query(typer, &w.with_blocks, &w.statement),
        s => {
            typer
                .issues
                .push(Issue::err("Cannot type statement of this type", s));
            InnerStatementType::Invalid
        }
    }
}
