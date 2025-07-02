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
//! use sql_type::{schema::parse_schemas, TypeOptions, SQLDialect, Issues};
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
//! let mut issues = Issues::new(schemas);
//! let schemas = parse_schemas(schemas,
//!     &mut issues,
//!     &TypeOptions::new().dialect(SQLDialect::MariaDB));
//!
//! assert!(issues.is_ok());
//!
//! for (name, schema) in schemas.schemas {
//!     println!("{name}: {schema:?}")
//! }
//! ```

use crate::{
    type_::{BaseType, FullType},
    type_statement,
    typer::unqualified_name,
    Type, TypeOptions,
};
use alloc::{collections::BTreeMap, sync::Arc, vec::Vec};
use sql_parse::{parse_statements, DataType, Expression, Identifier, Issues, Span, Spanned};

/// A column in a schema
#[derive(Debug)]
pub struct Column<'a> {
    pub identifier: Identifier<'a>,
    /// Type of the column
    pub type_: FullType<'a>,
    /// True if the column is auto_increment
    pub auto_increment: bool,
    pub default: bool,
    pub as_: Option<alloc::boxed::Box<Expression<'a>>>,
    pub generated: bool,
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
        self.columns
            .iter()
            .find(|&column| column.identifier.value == identifier)
    }
    pub fn get_column_mut(&mut self, identifier: &str) -> Option<&mut Column<'a>> {
        self.columns
            .iter_mut()
            .find(|column| column.identifier.value == identifier)
    }
}

/// A procedure
#[derive(Debug)]
pub struct Procedure {}

/// A function
#[derive(Debug)]
pub struct Functions {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexKey<'a> {
    pub table: Option<Identifier<'a>>,
    pub index: Identifier<'a>,
}

/// A description of tables, view, procedures and function in a schemas definition file
#[derive(Debug, Default)]
pub struct Schemas<'a> {
    /// Map from name to Tables or views
    pub schemas: BTreeMap<Identifier<'a>, Schema<'a>>,
    /// Map from name to procedure
    pub procedures: BTreeMap<Identifier<'a>, Procedure>,
    /// Map from name to function
    pub functions: BTreeMap<Identifier<'a>, Functions>,
    /// Map from (table, index) to location
    pub indices: BTreeMap<IndexKey<'a>, Span>,
}

pub(crate) fn parse_column<'a>(
    data_type: DataType<'a>,
    identifier: Identifier<'a>,
    _issues: &mut Issues<'a>,
    options: Option<&TypeOptions>,
) -> Column<'a> {
    let mut not_null = false;
    let mut unsigned = false;
    let mut auto_increment = false;
    let mut default = false;
    let mut _as = None;
    let mut generated = false;
    let mut primary_key = false;
    let is_sqlite = options
        .map(|v| v.parse_options.get_dialect().is_sqlite())
        .unwrap_or_default();
    for p in data_type.properties {
        match p {
            sql_parse::DataTypeProperty::Signed(_) => unsigned = false,
            sql_parse::DataTypeProperty::Unsigned(_) => unsigned = true,
            sql_parse::DataTypeProperty::Null(_) => not_null = false,
            sql_parse::DataTypeProperty::NotNull(_) => not_null = true,
            sql_parse::DataTypeProperty::AutoIncrement(_) => auto_increment = true,
            sql_parse::DataTypeProperty::As((_, e)) => _as = Some(e),
            sql_parse::DataTypeProperty::Default(_) => default = true,
            sql_parse::DataTypeProperty::GeneratedAlways(_) => generated = true,
            sql_parse::DataTypeProperty::PrimaryKey(_) => primary_key = true,
            _ => {}
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
        sql_parse::Type::Char(_) => BaseType::String.into(),
        sql_parse::Type::VarChar(_) => BaseType::String.into(),
        sql_parse::Type::TinyText(_) => BaseType::String.into(),
        sql_parse::Type::MediumText(_) => BaseType::String.into(),
        sql_parse::Type::Text(_) => BaseType::String.into(),
        sql_parse::Type::LongText(_) => BaseType::String.into(),
        sql_parse::Type::Enum(e) => Type::Enum(Arc::new(e.into_iter().map(|s| s.value).collect())),
        sql_parse::Type::Set(s) => Type::Set(Arc::new(s.into_iter().map(|s| s.value).collect())),
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
        sql_parse::Type::Binary(_) => BaseType::Bytes.into(),
        sql_parse::Type::Boolean => BaseType::Bool.into(),
        sql_parse::Type::Integer(_) => {
            if is_sqlite && primary_key {
                auto_increment = true;
            }
            BaseType::Integer.into()
        }
        sql_parse::Type::Float8 => BaseType::Float.into(),
        sql_parse::Type::Numeric(_, _, _) => todo!("Numeric"),
        sql_parse::Type::Timestamptz => BaseType::TimeStamp.into(),
        sql_parse::Type::Json => BaseType::String.into(),
        sql_parse::Type::Bit(_, _) => BaseType::Bytes.into(),
        sql_parse::Type::Bytea => BaseType::Bytes.into(),
        sql_parse::Type::Named(_) => BaseType::String.into(), // TODO lookup name??
        sql_parse::Type::Inet4 => BaseType::String.into(),
        sql_parse::Type::Inet6 => BaseType::String.into(),
    };

    Column {
        identifier,
        type_: FullType {
            t: type_,
            not_null,
            list_hack: false,
        },
        auto_increment,
        as_: _as,
        default,
        generated,
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
    issues: &mut Issues<'a>,
    options: &TypeOptions,
) -> Schemas<'a> {
    let statements = parse_statements(src, issues, &options.parse_options);

    let mut schemas = Schemas {
        schemas: Default::default(),
        procedures: Default::default(),
        functions: Default::default(),
        indices: Default::default(),
    };

    for statement in statements {
        match statement {
            sql_parse::Statement::CreateTable(t) => {
                let mut replace = false;

                let id = unqualified_name(issues, &t.identifier);

                let mut schema = Schema {
                    view: false,
                    identifier_span: id.span.clone(),
                    columns: Default::default(),
                };

                for o in t.create_options {
                    match o {
                        sql_parse::CreateOption::OrReplace(_) => {
                            replace = true;
                        }
                        sql_parse::CreateOption::Temporary(s) => {
                            issues.err("Not supported", &s);
                        }
                        sql_parse::CreateOption::Unique(s) => {
                            issues.err("Not supported", &s);
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
                            let column =
                                parse_column(data_type, identifier.clone(), issues, Some(options));
                            if let Some(oc) = schema.get_column(column.identifier.value) {
                                issues
                                    .err("Column already defined", &identifier)
                                    .frag("Defined here", &oc.identifier);
                            } else {
                                schema.columns.push(column);
                            }
                        }
                        sql_parse::CreateDefinition::ConstraintDefinition { .. } => {}
                    }
                }
                match schemas.schemas.entry(id.clone()) {
                    alloc::collections::btree_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if t.if_not_exists.is_none() {
                            issues
                                .err("Table already defined", &t.identifier)
                                .frag("Defined here", &e.get().identifier_span);
                        }
                    }
                    alloc::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(schema);
                    }
                }
            }
            sql_parse::Statement::CreateView(v) => {
                let mut replace = false;
                let mut schema = Schema {
                    view: true,
                    identifier_span: v.name.span(),
                    columns: Default::default(),
                };
                for o in v.create_options {
                    match o {
                        sql_parse::CreateOption::OrReplace(_) => {
                            replace = true;
                        }
                        sql_parse::CreateOption::Temporary(s) => {
                            issues.err("Not supported", &s);
                        }
                        sql_parse::CreateOption::Unique(s) => {
                            issues.err("Not supported", &s);
                        }
                        sql_parse::CreateOption::Algorithm(_, _) => {}
                        sql_parse::CreateOption::Definer { .. } => {}
                        sql_parse::CreateOption::SqlSecurityDefiner(_, _) => {}
                        sql_parse::CreateOption::SqlSecurityUser(_, _) => {}
                    }
                }

                {
                    let mut typer: crate::typer::Typer<'a, '_> = crate::typer::Typer {
                        schemas: &schemas,
                        issues,
                        reference_types: Vec::new(),
                        arg_types: Default::default(),
                        options,
                        with_schemas: Default::default(),
                    };

                    let t = type_statement::type_statement(&mut typer, &v.select);
                    let s = if let type_statement::InnerStatementType::Select(s) = t {
                        s
                    } else {
                        issues.err("Not supported", &v.select.span());
                        continue;
                    };

                    for column in s.columns {
                        //let column: crate::SelectTypeColumn<'a> = column;
                        let name = column.name.unwrap();

                        schema.columns.push(Column {
                            identifier: name,
                            type_: column.type_,
                            auto_increment: false,
                            default: false,
                            as_: None,
                            generated: false,
                        });
                    }
                }

                match schemas
                    .schemas
                    .entry(unqualified_name(issues, &v.name).clone())
                {
                    alloc::collections::btree_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if v.if_not_exists.is_none() {
                            issues
                                .err("View already defined", &v.name)
                                .frag("Defined here", &e.get().identifier_span);
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
                    match schemas.schemas.entry(unqualified_name(issues, &i).clone()) {
                        alloc::collections::btree_map::Entry::Occupied(e) => {
                            if e.get().view {
                                issues
                                    .err("Name defines a view not a table", &i)
                                    .frag("View defined here", &e.get().identifier_span);
                            } else {
                                e.remove();
                            }
                        }
                        alloc::collections::btree_map::Entry::Vacant(_) => {
                            if t.if_exists.is_none() {
                                issues.err("A table with this name does not exist to drop", &i);
                            }
                        }
                    }
                }
            }
            sql_parse::Statement::DropFunction(f) => {
                match schemas
                    .functions
                    .entry(unqualified_name(issues, &f.function).clone())
                {
                    alloc::collections::btree_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    alloc::collections::btree_map::Entry::Vacant(_) => {
                        if f.if_exists.is_none() {
                            issues.err(
                                "A function with this name does not exist to drop",
                                &f.function,
                            );
                        }
                    }
                }
            }
            sql_parse::Statement::DropProcedure(p) => {
                match schemas
                    .procedures
                    .entry(unqualified_name(issues, &p.procedure).clone())
                {
                    alloc::collections::btree_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    alloc::collections::btree_map::Entry::Vacant(_) => {
                        if p.if_exists.is_none() {
                            issues.err(
                                "A procedure with this name does not exist to drop",
                                &p.procedure,
                            );
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
                    match schemas.schemas.entry(unqualified_name(issues, &i).clone()) {
                        alloc::collections::btree_map::Entry::Occupied(e) => {
                            if !e.get().view {
                                issues
                                    .err("Name defines a table not a view", &i)
                                    .frag("Table defined here", &e.get().identifier_span);
                            } else {
                                e.remove();
                            }
                        }
                        alloc::collections::btree_map::Entry::Vacant(_) => {
                            if v.if_exists.is_none() {
                                issues.err("A view with this name does not exist to drop", &i);
                            }
                        }
                    }
                }
            }
            sql_parse::Statement::Set(_) => {}
            sql_parse::Statement::AlterTable(a) => {
                let e = match schemas
                    .schemas
                    .entry(unqualified_name(issues, &a.table).clone())
                {
                    alloc::collections::btree_map::Entry::Occupied(e) => {
                        let e = e.into_mut();
                        if e.view {
                            issues.err("Cannot alter view", &a.table);
                            continue;
                        }
                        e
                    }
                    alloc::collections::btree_map::Entry::Vacant(_) => {
                        if a.if_exists.is_none() {
                            issues.err("Table not found", &a.table);
                        }
                        continue;
                    }
                };
                for s in a.alter_specifications {
                    match s {
                        sql_parse::AlterSpecification::AddIndex {
                            if_not_exists,
                            name,
                            cols,
                            ..
                        } => {
                            for col in &cols {
                                if e.get_column(&col.name).is_none() {
                                    issues
                                        .err("No such column in table", col)
                                        .frag("Table defined here", &a.table);
                                }
                            }

                            if let Some(name) = &name {
                                let ident = if options.parse_options.get_dialect().is_postgresql() {
                                    IndexKey {
                                        table: None,
                                        index: name.clone(),
                                    }
                                } else {
                                    IndexKey {
                                        table: Some(unqualified_name(issues, &a.table).clone()),
                                        index: name.clone(),
                                    }
                                };

                                if let Some(old) = schemas.indices.insert(ident, name.span()) {
                                    if if_not_exists.is_none() {
                                        issues
                                            .err(
                                                "Multiple indeces with the same identifier",
                                                &name.span(),
                                            )
                                            .frag("Already defined here", &old);
                                    }
                                }
                            }
                        }
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
                                        issues
                                            .err("No such column in table", &col)
                                            .frag("Table defined here", &e.identifier_span);
                                    }
                                    continue;
                                }
                            };
                            *c = parse_column(
                                definition,
                                c.identifier.clone(),
                                issues,
                                Some(options),
                            );
                        }
                        sql_parse::AlterSpecification::AddColumn {
                            identifier,
                            data_type,
                            ..
                        } => {
                            e.columns.push(parse_column(
                                data_type,
                                identifier,
                                issues,
                                Some(options),
                            ));
                        }
                        sql_parse::AlterSpecification::OwnerTo { .. } => {}
                        sql_parse::AlterSpecification::DropColumn { column, .. } => {
                            let cnt = e.columns.len();
                            e.columns.retain(|c| c.identifier != column);
                            if cnt == e.columns.len() {
                                issues
                                    .err("No such column in table", &column)
                                    .frag("Table defined here", &e.identifier_span);
                            }
                        }
                        sql_parse::AlterSpecification::AlterColumn {
                            column,
                            alter_column_action,
                            ..
                        } => {
                            let c = match e.get_column_mut(column.value) {
                                Some(v) => v,
                                None => {
                                    issues
                                        .err("No such column in table", &column)
                                        .frag("Table defined here", &e.identifier_span);
                                    continue;
                                }
                            };
                            match alter_column_action {
                                sql_parse::AlterColumnAction::SetDefault { .. } => (),
                                sql_parse::AlterColumnAction::DropDefault { .. } => (),
                                sql_parse::AlterColumnAction::Type { type_, .. } => {
                                    *c = parse_column(type_, column, issues, Some(options))
                                }
                                sql_parse::AlterColumnAction::SetNotNull { .. } => {
                                    c.type_.not_null = true
                                }
                                sql_parse::AlterColumnAction::DropNotNull { .. } => {
                                    c.type_.not_null = false
                                }
                            }
                        }
                    }
                }
            }
            sql_parse::Statement::Do(_) => {
                //todo!()
            }
            // sql_parse::Statement::Block(_) => todo!(),
            // sql_parse::Statement::If(_) => todo!(),
            // sql_parse::Statement::Invalid => todo!(),
            // sql_parse::Statement::Union(_) => todo!(),
            // sql_parse::Statement::Replace(_) => todo!(),
            // sql_parse::Statement::Case(_) => todo!(),
            sql_parse::Statement::CreateIndex(ci) => {
                let t = unqualified_name(issues, &ci.table_name);

                if let Some(table) = schemas.schemas.get(t) {
                    for col in &ci.column_names {
                        if table.get_column(col).is_none() {
                            issues
                                .err("No such column in table", col)
                                .frag("Table defined here", &table.identifier_span);
                        }
                    }
                    // TODO type where_
                } else {
                    issues.err("No such table", &ci.table_name);
                }

                let ident = if options.parse_options.get_dialect().is_postgresql() {
                    IndexKey {
                        table: None,
                        index: ci.index_name.clone(),
                    }
                } else {
                    IndexKey {
                        table: Some(t.clone()),
                        index: ci.index_name.clone(),
                    }
                };

                if let Some(old) = schemas.indices.insert(ident, ci.span()) {
                    if ci.if_not_exists.is_none() {
                        issues
                            .err("Multiple indeces with the same identifier", &ci)
                            .frag("Already defined here", &old);
                    }
                }
            }
            sql_parse::Statement::DropIndex(ci) => {
                let key = IndexKey {
                    table: ci.on.as_ref().map(|(_, t)| t.identifier.clone()),
                    index: ci.index_name.clone(),
                };
                if schemas.indices.remove(&key).is_none() && ci.if_exists.is_none() {
                    issues.err("No such index", &ci);
                }
            }
            sql_parse::Statement::Commit(_) => (),
            sql_parse::Statement::Begin(_) => (),
            sql_parse::Statement::CreateFunction(_) => (),
            s => {
                issues.err(
                    alloc::format!("Unsupported statement {s:?} in schema definition"),
                    &s,
                );
            }
        }
    }

    let dummy_schemas = Schemas::default();

    let mut typer = crate::typer::Typer {
        schemas: &dummy_schemas,
        issues,
        reference_types: Vec::new(),
        arg_types: Default::default(),
        options,
        with_schemas: Default::default(),
    };

    // Compute nullity of generated columns
    for (name, schema) in &mut schemas.schemas {
        if schema.columns.iter().all(|v| v.as_.is_none()) {
            continue;
        }
        typer.reference_types.clear();
        let mut columns = Vec::new();
        for c in &schema.columns {
            columns.push((c.identifier.clone(), c.type_.clone()));
        }
        typer.reference_types.push(crate::typer::ReferenceType {
            name: Some(name.clone()),
            span: schema.identifier_span.clone(),
            columns,
        });
        for c in &mut schema.columns {
            if let Some(as_) = &c.as_ {
                let full_type = crate::type_expression::type_expression(
                    &mut typer,
                    as_,
                    crate::type_expression::ExpressionFlags::default(),
                    BaseType::Any,
                );
                c.type_.not_null = full_type.not_null;
            }
        }
    }
    schemas
}
