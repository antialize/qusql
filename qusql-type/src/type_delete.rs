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

use alloc::vec::Vec;
use sql_parse::{Delete, Issue, OptSpanned, Spanned};

use crate::{
    type_expression::{type_expression, ExpressionFlags},
    type_reference::type_reference,
    typer::{typer_stack, ReferenceType, Typer, unqualified_name},
};

pub(crate) fn type_delete<'a, 'b>(typer: &mut Typer<'a, 'b>, delete: &Delete<'a>) {
    let mut guard = typer_stack(
        typer,
        |t| core::mem::take(&mut t.reference_types),
        |t, v| t.reference_types = v,
    );
    let typer = &mut guard.typer;

    for flag in &delete.flags {
        match flag {
            sql_parse::DeleteFlag::LowPriority(_)
            | sql_parse::DeleteFlag::Quick(_)
            | sql_parse::DeleteFlag::Ignore(_) => (),
        }
    }

    if !delete.using.is_empty() && typer.dialect().is_maria() {
        // For maria tables must be mentioned in the using clause
        // while for postgresql they should be disjoint
        for reference in &delete.using {
            type_reference(typer, reference, false);
        }
        for table in &delete.tables {
            let identifier = unqualified_name(&mut typer.issues, table);
            if !typer.schemas.schemas.contains_key(&identifier.value) {
                typer
                    .issues
                    .push(Issue::err("Unknown table or view", identifier))
            }
        }
    } else {
        if delete.tables.len() > 1 {
            typer.issues.push(Issue::err(
                "Expected only one table here",
                &delete.tables.opt_span().unwrap(),
            ));
        }
        let identifier = unqualified_name(&mut typer.issues, &delete.tables[0]);
        if let Some(s) = typer.schemas.schemas.get(&identifier.value) {
            let mut columns = Vec::new();
            for col in &s.columns {
                columns.push((col.identifier, col.type_.ref_clone()));
            }
            typer.reference_types.push(ReferenceType {
                name: Some(identifier.value),
                span: identifier.span(),
                columns,
            });
        } else {
            typer
                .issues
                .push(Issue::err("Unknown table or view", identifier));
        }
        for reference in &delete.using {
            type_reference(typer, reference, false);
        }
    }
    if let Some((where_, _)) = &delete.where_ {
        let t = type_expression(
            typer,
            where_,
            ExpressionFlags::default(),
            crate::BaseType::Bool,
        );
        typer.ensure_base(where_, &t, crate::type_::BaseType::Bool);
    }
}
