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

use qusql_parse::Set;

use crate::{
    BaseType,
    type_expression::{ExpressionFlags, type_expression},
    typer::Typer,
};

pub(crate) fn type_set<'a>(typer: &mut Typer<'a, '_>, set: &Set<'a>) {
    for (_, expr) in &set.values {
        type_expression(typer, expr, ExpressionFlags::default(), BaseType::Any);
    }
}
