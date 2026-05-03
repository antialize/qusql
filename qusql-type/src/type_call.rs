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

use qusql_parse::{Call, FunctionParamDirection};

use crate::{
    schema::{lookup_name, parse_column},
    type_::BaseType,
    type_expression::{ExpressionFlags, type_expression},
    typer::Typer,
};
use alloc::vec::Vec;

pub(crate) fn type_call<'a>(typer: &mut Typer<'a, '_>, call: &Call<'a>) {
    let identifier = &call.name.identifier;
    let key = typer.qname_to_key(&call.name);
    let search_path = typer.search_path();

    // Look up the procedure in the schema.
    let proc = lookup_name(&typer.schemas.procedures, &key, search_path);

    if proc.is_none() {
        typer.err("Unknown procedure", identifier);
        // Still type all arguments to catch errors in them.
        for arg in &call.args {
            type_expression(typer, arg, ExpressionFlags::default(), BaseType::Any);
        }
        return;
    }
    let proc = proc.unwrap();

    // Collect the parameters that accept call arguments (IN, INOUT, or no direction).
    let in_params: Vec<_> = proc
        .params
        .iter()
        .filter(|p| !matches!(p.direction, Some(FunctionParamDirection::Out(_))))
        .collect();

    let expected = in_params.len();
    let got = call.args.len();
    if got != expected {
        typer.err(
            alloc::format!(
                "Procedure expects {} argument{}, got {}",
                expected,
                if expected == 1 { "" } else { "s" },
                got
            ),
            call,
        );
    }

    for (arg, param) in call.args.iter().zip(in_params.iter()) {
        let param_type = parse_column(
            param.type_.clone(),
            identifier.clone(),
            typer.issues,
            Some(typer.options),
            Some(&typer.schemas.types),
            typer.search_path(),
        );
        let arg_type = type_expression(
            typer,
            arg,
            ExpressionFlags::default(),
            param_type.type_.base(),
        );
        if typer
            .matched_type(&arg_type.t, &param_type.type_.t)
            .is_none()
        {
            typer.err(
                alloc::format!("Got type {} expected {}", arg_type, param_type.type_),
                arg,
            );
        }
    }
    // Type any extra args (when count is wrong) to catch sub-errors.
    for arg in call.args.iter().skip(in_params.len()) {
        type_expression(typer, arg, ExpressionFlags::default(), BaseType::Any);
    }
}
