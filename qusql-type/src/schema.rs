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

//! Facility for parsing SQL schemas into a terse format that can be used
//! for typing statements.
//!
//! ```
//! use sql_type::{schema::parse_schemas, TypeOptions, SQLDialect};
//! let schemas = "
//!     -- Table structure for table `events`
//!     DROP TABLE IF EXISTS `events`;
//!     CREATE TABLE `events` (
//!       `id` bigint(20) NOT NULL,
//!       `user` int(11) NOT NULL,
//!       `event_key` int(11) NOT NULL,
//!       `time` datetime NOT NULL
//!     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
//!
//!     -- Table structure for table `events_keys`
//!     DROP TABLE IF EXISTS `event_keys`;
//!     CREATE TABLE `event_keys` (
//!       `id` int(11) NOT NULL,
//!       `name` text NOT NULL
//!     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
//!
//!     -- Stand-in structure for view `events_view`
//!     -- (See below for the actual view)
//!     DROP VIEW IF EXISTS `events_view`;
//!     CREATE TABLE `events_view` (
//!         `id` int(11),
//!         `user` int(11) NOT NULL,
//!         `event_key` text NOT NULL,
//!         `time` datetime NOT NULL
//!     );
//!
//!     -- Indexes for table `events`
//!     ALTER TABLE `events`
//!       ADD PRIMARY KEY (`id`),
//!       ADD KEY `time` (`time`),
//!       ADD KEY `event_key` (`event_key`);
//!
//!     -- Indexes for table `event_keys`
//!     ALTER TABLE `event_keys`
//!       ADD PRIMARY KEY (`id`);
//!
//!     -- Constraints for table `events`
//!     ALTER TABLE `events`
//!       ADD CONSTRAINT `event_key` FOREIGN KEY (`event_key`) REFERENCES `event_keys` (`id`);
//!
//!     -- Structure for view `events_view`
//!     DROP TABLE IF EXISTS `events_view`;
//!     DROP VIEW IF EXISTS `events_view`;
//!     CREATE ALGORITHM=UNDEFINED DEFINER=`phpmyadmin`@`localhost`
//!         SQL SECURITY DEFINER VIEW `events_view` AS
//!         SELECT
//!             `events`.`id` AS `id`,
//!             `events`.`user` AS `user`,
//!             `event_keys`.`name` AS `event_key`,
//!             `events`.`time` AS `time`
//!         FROM `events`, `event_keys`
//!         WHERE `events`.`event_key` = `event_keys`.`id`;
//!     ";
//!
//! let mut issues = Vec::new();
//! let schemas = parse_schemas(schemas,
//!     &mut issues,
//!     &TypeOptions::new().dialect(SQLDialect::MariaDB));
//!
//! assert!(issues.is_empty());
//!
//! for (name, schema) in schemas.schemas {
//!     println!("{name}: {schema:?}")
//! }
//! ```

use crate::{
    type_::{BaseType, FullType},
    RefOrVal, Type, TypeOptions,
};
use alloc::{collections::BTreeMap, vec::Vec};
use sql_parse::{parse_statements, DataType, Issue, Span, Spanned};

/// A column in a schema
#[derive(Debug)]
pub struct Column<'a> {
    pub identifier: &'a str,
    /// Span of identifier
    pub identifier_span: Span,
    /// Type of the column
    pub type_: FullType<'a>,
    /// True if the column is auto_increment
    pub auto_increment: bool,
}

/// Schema representing a table or view
#[derive(Debug)]
pub struct Schema<'a> {
    /// Span of identifier
    pub identifier_span: Span,
    /// List of columns
    pub columns: Vec<Column<'a>>,
    /// True if this is a view instead of a table
    pub view: bool,
}

impl<'a> Schema<'a> {
    pub fn get_column(&self, identifier: &str) -> Option<&Column<'a>> {
        for column in &self.columns {
            if column.identifier == identifier {
                return Some(column);
            }
        }
        None
    }
    pub fn get_column_mut(&mut self, identifier: &str) -> Option<&mut Column<'a>> {
        for column in &mut self.columns {
            if column.identifier == identifier {
                return Some(column);
            }
        }
        None
    }
}

/// A procedure
#[derive(Debug)]
pub struct Procedure {}

/// A function
#[derive(Debug)]
pub struct Functions {}

/// A description of tables, view, procedures and function in a schemas definition file
#[derive(Debug)]

pub struct Schemas<'a> {
    /// Map from name to Tables or views
    pub schemas: BTreeMap<&'a str, Schema<'a>>,
    /// Map from name to procedure
    pub procedures: BTreeMap<&'a str, Procedure>,
    /// Map from name to function
    pub functions: BTreeMap<&'a str, Functions>,
}

pub(crate) fn parse_column<'a>(
    data_type: DataType<'a>,
    identifier: &'a str,
    identifier_span: Span,
    _issues: &mut Vec<Issue>,
) -> Column<'a> {
    let mut not_null = false;
    let mut unsigned = false;
    let mut auto_increment = false;
    for p in data_type.properties {
        match p {
            sql_parse::DataTypeProperty::Signed(_) => unsigned = false,
            sql_parse::DataTypeProperty::Unsigned(_) => unsigned = true,
            sql_parse::DataTypeProperty::Null(_) => not_null = false,
            sql_parse::DataTypeProperty::NotNull(_) => not_null = true,

            sql_parse::DataTypeProperty::AutoIncrement(_) => auto_increment = true,
            _ => {} // TODO default,
        }
    }
    let type_ = match data_type.type_ {
        sql_parse::Type::TinyInt(v) => {
            if !unsigned && matches!(v, Some((1, _))) {
                BaseType::Bool.into()
            } else if unsigned {
                Type::U8
            } else {
                Type::I8
            }
        }
        sql_parse::Type::SmallInt(_) => {
            if unsigned {
                Type::U16
            } else {
                Type::I16
            }
        }
        sql_parse::Type::Int(_) => {
            if unsigned {
                Type::U32
            } else {
                Type::I32
            }
        }
        sql_parse::Type::BigInt(_) => {
            if unsigned {
                Type::U64
            } else {
                Type::I64
            }
        }
        sql_parse::Type::VarChar(_) => BaseType::String.into(),
        sql_parse::Type::TinyText(_) => BaseType::String.into(),
        sql_parse::Type::MediumText(_) => BaseType::String.into(),
        sql_parse::Type::Text(_) => BaseType::String.into(),
        sql_parse::Type::LongText(_) => BaseType::String.into(),
        sql_parse::Type::Enum(e) => {
            Type::Enum(RefOrVal::Val(e.into_iter().map(|s| s.value).collect()))
        }
        sql_parse::Type::Set(s) => {
            Type::Set(RefOrVal::Val(s.into_iter().map(|s| s.value).collect()))
        }
        sql_parse::Type::Float(_) => Type::F32,
        sql_parse::Type::Double(_) => Type::F64,
        sql_parse::Type::DateTime(_) => BaseType::DateTime.into(),
        sql_parse::Type::Timestamp(_) => BaseType::TimeStamp.into(),
        sql_parse::Type::Time(_) => BaseType::Time.into(),
        sql_parse::Type::TinyBlob(_) => BaseType::Bytes.into(),
        sql_parse::Type::MediumBlob(_) => BaseType::Bytes.into(),
        sql_parse::Type::Date => BaseType::Date.into(),
        sql_parse::Type::Blob(_) => BaseType::Bytes.into(),
        sql_parse::Type::LongBlob(_) => BaseType::Bytes.into(),
        sql_parse::Type::VarBinary(_) => BaseType::Bytes.into(),
    };
    Column {
        identifier,
        identifier_span,
        type_: FullType { t: type_, not_null },
        auto_increment,
    }
}

/// Parse a schema definition and return a terse description
///
/// Errors and warnings are added to issues. The schema is successfully
/// parsed if no errors are added to issues.
///
/// The schema definition in srs should be a sequence of the following
/// statements:
/// - Drop table
/// - Drop function
/// - Drop view
/// - Drop procedure
/// - Create table
/// - Create function
/// - Create view
/// - Create procedure
/// - Alter table
pub fn parse_schemas<'a>(
    src: &'a str,
    issues: &mut Vec<Issue>,
    options: &TypeOptions,
) -> Schemas<'a> {
    let statements = parse_statements(src, issues, &options.parse_options);

    let mut schemas = Schemas {
        schemas: Default::default(),
        procedures: Default::default(),
        functions: Default::default(),
    };

    for statement in statements {
        match statement {
            sql_parse::Statement::CreateTable(t) => {
                let mut replace = false;

                let mut schema = Schema {
                    view: false,
                    identifier_span: t.identifier.span.clone(),
                    columns: Default::default(),
                };

                for o in t.create_options {
                    match o {
                        sql_parse::CreateOption::OrReplace(_) => {
                            replace = true;
                        }
                        sql_parse::CreateOption::Temporary(s) => {
                            issues.push(Issue::err("Not supported", &s))
                        }
                        sql_parse::CreateOption::Algorithm(_, _) => {}
                        sql_parse::CreateOption::Definer { .. } => {}
                        sql_parse::CreateOption::SqlSecurityDefiner(_, _) => {}
                        sql_parse::CreateOption::SqlSecurityUser(_, _) => {}
                    }
                }
                // TODO: do we care about table options
                for d in t.create_definitions {
                    match d {
                        sql_parse::CreateDefinition::ColumnDefinition {
                            identifier,
                            data_type,
                        } => {
                            let column = parse_column(
                                data_type,
                                identifier.value,
                                identifier.span.clone(),
                                issues,
                            );
                            if let Some(oc) = schema.get_column(column.identifier) {
                                issues.push(
                                    Issue::err("Column already defined", &identifier)
                                        .frag("Defined here", &oc.identifier_span),
                                );
                            } else {
                                schema.columns.push(column);
                            }
                        }
                    }
                }
                match schemas.schemas.entry(t.identifier.value) {
                    alloc::collections::btree_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if t.if_not_exists.is_none() {
                            issues.push(
                                Issue::err("Table already defined", &t.identifier)
                                    .frag("Defined here", &e.get().identifier_span),
                            );
                        }
                    }
                    alloc::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(schema);
                    }
                }
            }
            sql_parse::Statement::CreateView(v) => {
                let mut replace = false;
                let schema = Schema {
                    view: true,
                    identifier_span: v.name.span.clone(),
                    columns: Default::default(),
                };
                for o in v.create_options {
                    match o {
                        sql_parse::CreateOption::OrReplace(_) => {
                            replace = true;
                        }
                        sql_parse::CreateOption::Temporary(s) => {
                            issues.push(Issue::err("Not supported", &s))
                        }
                        sql_parse::CreateOption::Algorithm(_, _) => {}
                        sql_parse::CreateOption::Definer { .. } => {}
                        sql_parse::CreateOption::SqlSecurityDefiner(_, _) => {}
                        sql_parse::CreateOption::SqlSecurityUser(_, _) => {}
                    }
                }
                // TODO typecheck view query to find schema
                match schemas.schemas.entry(v.name.value) {
                    alloc::collections::btree_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if v.if_not_exists.is_none() {
                            issues.push(
                                Issue::err("View already defined", &v.name)
                                    .frag("Defined here", &e.get().identifier_span),
                            );
                        }
                    }
                    alloc::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(schema);
                    }
                }
            }
            sql_parse::Statement::CreateTrigger(_) => {}
            // sql_parse::Statement::CreateFunction(_) => todo!(),
            // sql_parse::Statement::Select(_) => todo!(),
            // sql_parse::Statement::Delete(_) => todo!(),
            // sql_parse::Statement::Insert(_) => todo!(),
            // sql_parse::Statement::Update(_) => todo!(),
            sql_parse::Statement::DropTable(t) => {
                for i in t.tables {
                    match schemas.schemas.entry(i.value) {
                        alloc::collections::btree_map::Entry::Occupied(e) => {
                            if e.get().view {
                                issues.push(
                                    Issue::err("Name defines a view not a table", &i)
                                        .frag("View defined here", &e.get().identifier_span),
                                )
                            } else {
                                e.remove();
                            }
                        }
                        alloc::collections::btree_map::Entry::Vacant(_) => {
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
            sql_parse::Statement::DropFunction(f) => {
                match schemas.functions.entry(f.function.value) {
                    alloc::collections::btree_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    alloc::collections::btree_map::Entry::Vacant(_) => {
                        if f.if_exists.is_none() {
                            issues.push(Issue::err(
                                "A function with this name does not exist to drop",
                                &f.function,
                            ));
                        }
                    }
                }
            }
            sql_parse::Statement::DropProcedure(p) => {
                match schemas.procedures.entry(p.procedure.value) {
                    alloc::collections::btree_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    alloc::collections::btree_map::Entry::Vacant(_) => {
                        if p.if_exists.is_none() {
                            issues.push(Issue::err(
                                "A procedure with this name does not exist to drop",
                                &p.procedure,
                            ));
                        }
                    }
                }
            }
            //sql_parse::Statement::DropEvent(_) => todo!(),
            sql_parse::Statement::DropDatabase(_) => {}
            sql_parse::Statement::DropServer(_) => {}
            sql_parse::Statement::DropTrigger(_) => {}
            sql_parse::Statement::DropView(v) => {
                for i in v.views {
                    match schemas.schemas.entry(i.value) {
                        alloc::collections::btree_map::Entry::Occupied(e) => {
                            if !e.get().view {
                                issues.push(
                                    Issue::err("Name defines a table not a view", &i)
                                        .frag("Table defined here", &e.get().identifier_span),
                                );
                            } else {
                                e.remove();
                            }
                        }
                        alloc::collections::btree_map::Entry::Vacant(_) => {
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
            sql_parse::Statement::Set(_) => {}
            sql_parse::Statement::AlterTable(a) => {
                let e = match schemas.schemas.entry(a.table.value) {
                    alloc::collections::btree_map::Entry::Occupied(e) => {
                        let e = e.into_mut();
                        if e.view {
                            issues.push(Issue::err("Cannot alter view", &a.table));
                            continue;
                        }
                        e
                    }
                    alloc::collections::btree_map::Entry::Vacant(_) => {
                        if a.if_exists.is_none() {
                            issues.push(Issue::err("Table not found", &a.table));
                        }
                        continue;
                    }
                };
                for s in a.alter_specifications {
                    match s {
                        sql_parse::AlterSpecification::AddIndex { .. } => {}
                        sql_parse::AlterSpecification::AddForeignKey { .. } => {}
                        sql_parse::AlterSpecification::Modify {
                            if_exists,
                            col,
                            definition,
                            ..
                        } => {
                            let c = match e.get_column_mut(col.value) {
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
                            *c = parse_column(definition, c.identifier, col.span(), issues);
                        }
                    }
                }
            }
            // sql_parse::Statement::Block(_) => todo!(),
            // sql_parse::Statement::If(_) => todo!(),
            // sql_parse::Statement::Invalid => todo!(),
            // sql_parse::Statement::Union(_) => todo!(),
            // sql_parse::Statement::Replace(_) => todo!(),
            // sql_parse::Statement::Case(_) => todo!(),
            s => issues.push(Issue::err("Unsupported statement in schema definition", &s)),
        }
    }
    schemas
}
