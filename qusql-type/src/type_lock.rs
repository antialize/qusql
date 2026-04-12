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

use qusql_parse::{Lock, Unlock};

use crate::typer::{Typer, unqualified_name};

pub(crate) fn type_lock<'a>(typer: &mut Typer<'a, '_>, lock: &Lock<'a>) {
    for member in &lock.members {
        let identifier = unqualified_name(typer.issues, &member.table_name);
        if typer.get_schema(identifier.value).is_none() {
            typer.err("Unknown table", identifier);
        }
    }
}

pub(crate) fn type_unlock(_typer: &mut Typer<'_, '_>, _unlock: &Unlock) {
    // Nothing to validate for UNLOCK TABLES
}
