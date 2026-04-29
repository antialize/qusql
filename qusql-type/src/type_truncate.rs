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

use qusql_parse::TruncateTable;

use crate::typer::Typer;

pub(crate) fn type_truncate<'a>(typer: &mut Typer<'a, '_>, truncate: &TruncateTable<'a>) {
    for spec in &truncate.tables {
        let key = typer.qname_to_key(&spec.table_name);
        match typer.get_schema_by_key(&key) {
            None => {
                typer.err("Unknown table", &spec.table_name.identifier);
            }
            Some(schema) if schema.view => {
                typer.err("Cannot truncate a view", &spec.table_name.identifier);
            }
            _ => (),
        }
    }
}
