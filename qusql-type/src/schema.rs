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

use crate::{type_::FullType, RefOrVal, Type};
use sql_ast::{parse_statements, DataType, Issue, ParseOptions, Span, Spanned};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Column<'a> {
    pub identifier_span: Span,
    pub type_: FullType<'a>,
    pub auto_increment: bool,
}

#[derive(Debug)]
pub struct Schema<'a> {
    pub identifier_span: Span,
    pub columns: HashMap<&'a str, Column<'a>>,
    pub view: bool,
}

#[derive(Debug)]
pub struct Procedure {}

#[derive(Debug)]
pub struct Functions {}

#[derive(Debug)]

pub struct Schemas<'a> {
    pub schemas: HashMap<&'a str, Schema<'a>>,
    pub procedures: HashMap<&'a str, Procedure>,
    pub functions: HashMap<&'a str, Functions>,
}

fn parse_column<'a>(
    data_type: DataType<'a>,
    identifier_span: Span,
    _issues: &mut Vec<Issue>,
) -> Column<'a> {
    let mut not_null = false;
    let mut unsigned = false;
    let mut auto_increment = false;
    for p in data_type.properties {
        match p {
            sql_ast::DataTypeProperty::Signed(_) => unsigned = false,
            sql_ast::DataTypeProperty::Unsigned(_) => unsigned = true,
            sql_ast::DataTypeProperty::Null(_) => not_null = false,
            sql_ast::DataTypeProperty::NotNull(_) => not_null = true,
            sql_ast::DataTypeProperty::AutoIncrement(_) => auto_increment = true,
            _ => {} // TODO default,
        }
    }
    let type_ = match data_type.type_ {
        sql_ast::Type::TinyInt(v) => {
            if !unsigned && matches!(v, Some((1, _))) {
                Type::Bool
            } else if unsigned {
                Type::U8
            } else {
                Type::I8
            }
        }
        sql_ast::Type::SmallInt(_) => {
            if unsigned {
                Type::U16
            } else {
                Type::I16
            }
        }
        sql_ast::Type::Int(_) => {
            if unsigned {
                Type::U32
            } else {
                Type::I32
            }
        }
        sql_ast::Type::BigInt(_) => {
            if unsigned {
                Type::U64
            } else {
                Type::I64
            }
        }
        sql_ast::Type::VarChar(_) => Type::Text,
        sql_ast::Type::TinyText(_) => Type::Text,
        sql_ast::Type::MediumText(_) => Type::Text,
        sql_ast::Type::Text(_) => Type::Text,
        sql_ast::Type::LongText(_) => Type::Text,
        sql_ast::Type::Enum(e) => {
            Type::Enum(RefOrVal::Val(e.into_iter().map(|s| s.value).collect()))
        }
        sql_ast::Type::Set(s) => Type::Set(RefOrVal::Val(s.into_iter().map(|s| s.value).collect())),
        sql_ast::Type::Float(_) => Type::F32,
        sql_ast::Type::Double(_) => Type::F64,
        sql_ast::Type::DateTime(_) => Type::DateTime,
        sql_ast::Type::Timestamp(_) => Type::Timestamp,
        sql_ast::Type::Time(_) => Type::Time,
        sql_ast::Type::TinyBlob(_) => Type::Bytes,
        sql_ast::Type::MediumBlob(_) => Type::Bytes,
        sql_ast::Type::Date => Type::Date,
        sql_ast::Type::Blob(_) => Type::Bytes,
        sql_ast::Type::LongBlob(_) => Type::Bytes,
        sql_ast::Type::VarBinary(_) => Type::Bytes,
    };
    Column {
        identifier_span,
        type_: FullType { t: type_, not_null },
        auto_increment,
    }
}

pub fn parse_schemas<'a>(
    src: &'a str,
    issues: &mut Vec<Issue>,
    options: &ParseOptions,
) -> Schemas<'a> {
    let statements = parse_statements(src, issues, options);

    let mut schemas = Schemas {
        schemas: Default::default(),
        procedures: Default::default(),
        functions: Default::default(),
    };

    for statement in statements {
        match statement {
            sql_ast::Statement::CreateTable(t) => {
                let mut replace = false;

                let mut schema = Schema {
                    view: false,
                    identifier_span: t.identifier.span.clone(),
                    columns: HashMap::new(),
                };

                for o in t.create_options {
                    match o {
                        sql_ast::CreateOption::OrReplace(_) => {
                            replace = true;
                        }
                        sql_ast::CreateOption::Temporary(s) => {
                            issues.push(Issue::err("Not supported", &s))
                        }
                        sql_ast::CreateOption::Algorithm(_, _) => {}
                        sql_ast::CreateOption::Definer { .. } => {}
                        sql_ast::CreateOption::SqlSecurityDefiner(_, _) => {}
                        sql_ast::CreateOption::SqlSecurityUser(_, _) => {}
                    }
                }
                // TODO: do we care about table options
                for d in t.create_definitions {
                    match d {
                        sql_ast::CreateDefinition::ColumnDefinition {
                            identifier,
                            data_type,
                        } => {
                            let column = parse_column(data_type, identifier.span.clone(), issues);
                            match schema.columns.entry(identifier.value) {
                                std::collections::hash_map::Entry::Occupied(e) => {
                                    issues.push(
                                        Issue::err("Column allready defined", &identifier)
                                            .frag("Defined here", &e.get().identifier_span),
                                    );
                                }
                                std::collections::hash_map::Entry::Vacant(e) => {
                                    e.insert(column);
                                }
                            }
                        }
                    }
                }
                match schemas.schemas.entry(t.identifier.value) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if t.if_not_exists.is_none() {
                            issues.push(
                                Issue::err("Table already defined", &t.identifier)
                                    .frag("Defined here", &e.get().identifier_span),
                            );
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(schema);
                    }
                }
            }
            sql_ast::Statement::CreateView(v) => {
                let mut replace = false;
                let schema = Schema {
                    view: true,
                    identifier_span: v.name.span.clone(),
                    columns: HashMap::new(),
                };
                for o in v.create_options {
                    match o {
                        sql_ast::CreateOption::OrReplace(_) => {
                            replace = true;
                        }
                        sql_ast::CreateOption::Temporary(s) => {
                            issues.push(Issue::err("Not supported", &s))
                        }
                        sql_ast::CreateOption::Algorithm(_, _) => {}
                        sql_ast::CreateOption::Definer { .. } => {}
                        sql_ast::CreateOption::SqlSecurityDefiner(_, _) => {}
                        sql_ast::CreateOption::SqlSecurityUser(_, _) => {}
                    }
                }
                // TODO typecheck view query to find schema
                match schemas.schemas.entry(v.name.value) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if v.if_not_exists.is_none() {
                            issues.push(
                                Issue::err("View already defined", &v.name)
                                    .frag("Defined here", &e.get().identifier_span),
                            );
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(schema);
                    }
                }
            }
            sql_ast::Statement::CreateTrigger(_) => {}
            // sql_ast::Statement::CreateFunction(_) => todo!(),
            // sql_ast::Statement::Select(_) => todo!(),
            // sql_ast::Statement::Delete(_) => todo!(),
            // sql_ast::Statement::Insert(_) => todo!(),
            // sql_ast::Statement::Update(_) => todo!(),
            sql_ast::Statement::DropTable(t) => {
                for i in t.tables {
                    match schemas.schemas.entry(i.value) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            if e.get().view {
                                issues.push(
                                    Issue::err("Name defines a view not a table", &i)
                                        .frag("View defined here", &e.get().identifier_span),
                                )
                            } else {
                                e.remove();
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(_) => {
                            if t.if_exists.is_none() {
                                issues.push(Issue::err(
                                    "A table with this name does not exist to drop",
                                    &i,
                                ));
                            }
                        }
                    }
                }
            }
            sql_ast::Statement::DropFunction(f) => {
                match schemas.functions.entry(f.function.value) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {
                        if f.if_exists.is_none() {
                            issues.push(Issue::err(
                                "A function with this name does not exist to drop",
                                &f.function,
                            ));
                        }
                    }
                }
            }
            sql_ast::Statement::DropProcedure(p) => {
                match schemas.procedures.entry(p.procedure.value) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {
                        if p.if_exists.is_none() {
                            issues.push(Issue::err(
                                "A procedure with this name does not exist to drop",
                                &p.procedure,
                            ));
                        }
                    }
                }
            }
            //sql_ast::Statement::DropEvent(_) => todo!(),
            sql_ast::Statement::DropDatabase(_) => {}
            sql_ast::Statement::DropServer(_) => {}
            sql_ast::Statement::DropTrigger(_) => {}
            sql_ast::Statement::DropView(v) => {
                for i in v.views {
                    match schemas.schemas.entry(i.value) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            if !e.get().view {
                                issues.push(
                                    Issue::err("Name defines a table not a view", &i)
                                        .frag("Table defined here", &e.get().identifier_span),
                                );
                            } else {
                                e.remove();
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(_) => {
                            if v.if_exists.is_none() {
                                issues.push(Issue::err(
                                    "A view with this name does not exist to drop",
                                    &i,
                                ));
                            }
                        }
                    }
                }
            }
            sql_ast::Statement::Set(_) => {}
            sql_ast::Statement::AlterTable(a) => {
                let e = match schemas.schemas.entry(a.table.value) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        let e = e.into_mut();
                        if e.view {
                            issues.push(Issue::err("Cannot alter view", &a.table));
                            continue;
                        }
                        e
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {
                        if a.if_exists.is_none() {
                            issues.push(Issue::err("Table not found", &a.table));
                        }
                        continue;
                    }
                };
                for s in a.alter_specifications {
                    match s {
                        sql_ast::AlterSpecification::AddIndex { .. } => {}
                        sql_ast::AlterSpecification::AddForeginKey { .. } => {}
                        sql_ast::AlterSpecification::Modify {
                            if_exists,
                            col,
                            definition,
                            ..
                        } => {
                            let c = match e.columns.get_mut(col.value) {
                                Some(v) => v,
                                None => {
                                    if if_exists.is_none() {
                                        issues.push(
                                            Issue::err("No such column in table", &col)
                                                .frag("Table defined here", &e.identifier_span),
                                        );
                                    }
                                    continue;
                                }
                            };
                            *c = parse_column(definition, col.span(), issues);
                        }
                    }
                }
            }
            // sql_ast::Statement::Block(_) => todo!(),
            // sql_ast::Statement::If(_) => todo!(),
            // sql_ast::Statement::Invalid => todo!(),
            // sql_ast::Statement::Union(_) => todo!(),
            // sql_ast::Statement::Replace(_) => todo!(),
            // sql_ast::Statement::Case(_) => todo!(),
            s => issues.push(Issue::err("Unsupported statement in schema definition", &s)),
        }
    }
    schemas
}
