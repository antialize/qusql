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
//! use qusql_type::{schema::parse_schemas, TypeOptions, SQLDialect, Issues};
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
    Type, TypeOptions,
    type_::{BaseType, FullType},
    type_statement,
    typer::unqualified_name,
};
use alloc::{borrow::Cow, collections::BTreeMap, sync::Arc, vec::Vec};
use qusql_parse::{
    AddColumn, AddIndex, AlterColumn, DataType, DropColumn, Expression, Identifier, Issues,
    ModifyColumn, Span, Spanned, Statement, parse_statements,
};

/// A column in a schema
#[derive(Debug)]
pub struct Column<'a> {
    pub identifier: Identifier<'a>,
    /// Type of the column
    pub type_: FullType<'a>,
    /// True if the column is auto_increment
    pub auto_increment: bool,
    pub default: bool,
    pub as_: Option<Expression<'a>>,
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

/// A stored procedure definition
#[derive(Debug)]
pub struct ProcedureDef<'a> {
    pub name: Identifier<'a>,
    pub params: Vec<qusql_parse::FunctionParam<'a>>,
    pub span: Span,
}

/// Parsed body of a stored function, with an offset for mapping spans
/// back to the outer source file.
#[derive(Debug)]
pub struct FunctionDefBody<'a> {
    /// Parsed statements from the function body
    pub statements: Vec<Statement<'a>>,
    /// The body source string (borrowed from the outer source)
    pub src: &'a str,
    /// Byte offset of `src` within the outer source file.
    /// Add this to any span from `statements` to get the outer-file span.
    pub span_offset: usize,
}

/// A stored function definition
#[derive(Debug)]
pub struct FunctionDef<'a> {
    pub name: Identifier<'a>,
    pub params: Vec<qusql_parse::FunctionParam<'a>>,
    pub return_type: qusql_parse::DataType<'a>,
    pub span: Span,
    /// Parsed body, present when the function was defined with a
    /// dollar-quoted (non-escaped) AS body string.
    pub body: Option<FunctionDefBody<'a>>,
}

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
    pub procedures: BTreeMap<Identifier<'a>, ProcedureDef<'a>>,
    /// Map from name to function
    pub functions: BTreeMap<Identifier<'a>, FunctionDef<'a>>,
    /// Map from (table, index) to location
    pub indices: BTreeMap<IndexKey<'a>, Span>,
}

/// Try to parse a borrowed string as SQL statements.
/// Returns the parsed body if the string is a non-escaped borrow from `src`,
/// or None if the string is escaped (Cow::Owned).
fn try_parse_body<'a>(
    src: &'a str,
    body_str: &qusql_parse::SString<'a>,
    issues: &mut Issues<'a>,
    options: &qusql_parse::ParseOptions,
) -> Option<FunctionDefBody<'a>> {
    let Cow::Borrowed(borrowed) = &body_str.value else {
        return None;
    };
    let span_offset = borrowed.as_ptr() as usize - src.as_ptr() as usize;
    let body_options = options.clone().function_body(true).span_offset(span_offset);
    let statements = parse_statements(borrowed, issues, &body_options);
    Some(FunctionDefBody {
        statements,
        src: borrowed,
        span_offset: 0,
    })
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
            qusql_parse::DataTypeProperty::Signed(_) => unsigned = false,
            qusql_parse::DataTypeProperty::Unsigned(_) => unsigned = true,
            qusql_parse::DataTypeProperty::Null(_) => not_null = false,
            qusql_parse::DataTypeProperty::NotNull(_) => not_null = true,
            qusql_parse::DataTypeProperty::AutoIncrement(_) => auto_increment = true,
            qusql_parse::DataTypeProperty::As((_, e)) => _as = Some(e),
            qusql_parse::DataTypeProperty::Default(_) => default = true,
            qusql_parse::DataTypeProperty::GeneratedAlways(_) => generated = true,
            qusql_parse::DataTypeProperty::PrimaryKey(_) => primary_key = true,
            _ => {}
        }
    }
    let type_ = match data_type.type_ {
        qusql_parse::Type::TinyInt(v) => {
            if !unsigned && matches!(v, Some((1, _))) {
                BaseType::Bool.into()
            } else if unsigned {
                Type::U8
            } else {
                Type::I8
            }
        }
        qusql_parse::Type::SmallInt(_) => {
            if unsigned {
                Type::U16
            } else {
                Type::I16
            }
        }
        qusql_parse::Type::MediumInt(_) => {
            if unsigned {
                Type::U24
            } else {
                Type::I24
            }
        }
        qusql_parse::Type::Int(_) => {
            if unsigned {
                Type::U32
            } else {
                Type::I32
            }
        }
        qusql_parse::Type::BigInt(_) => {
            if unsigned {
                Type::U64
            } else {
                Type::I64
            }
        }
        qusql_parse::Type::Char(_) => BaseType::String.into(),
        qusql_parse::Type::VarChar(_) => BaseType::String.into(),
        qusql_parse::Type::TinyText(_) => BaseType::String.into(),
        qusql_parse::Type::MediumText(_) => BaseType::String.into(),
        qusql_parse::Type::Text(_) => BaseType::String.into(),
        qusql_parse::Type::LongText(_) => BaseType::String.into(),
        qusql_parse::Type::Enum(e) => {
            Type::Enum(Arc::new(e.into_iter().map(|s| s.value).collect()))
        }
        qusql_parse::Type::Set(s) => Type::Set(Arc::new(s.into_iter().map(|s| s.value).collect())),
        qusql_parse::Type::Float(_) => Type::F32,
        qusql_parse::Type::Double(_) => Type::F64,
        qusql_parse::Type::DateTime(_) => BaseType::DateTime.into(),
        qusql_parse::Type::Timestamp(_) => BaseType::TimeStamp.into(),
        qusql_parse::Type::Time(_) => BaseType::Time.into(),
        qusql_parse::Type::TinyBlob(_) => BaseType::Bytes.into(),
        qusql_parse::Type::MediumBlob(_) => BaseType::Bytes.into(),
        qusql_parse::Type::Date => BaseType::Date.into(),
        qusql_parse::Type::Blob(_) => BaseType::Bytes.into(),
        qusql_parse::Type::LongBlob(_) => BaseType::Bytes.into(),
        qusql_parse::Type::VarBinary(_) => BaseType::Bytes.into(),
        qusql_parse::Type::Binary(_) => BaseType::Bytes.into(),
        qusql_parse::Type::Boolean => BaseType::Bool.into(),
        qusql_parse::Type::Integer(_) => {
            if is_sqlite && primary_key {
                auto_increment = true;
            }
            BaseType::Integer.into()
        }
        qusql_parse::Type::Float8 => BaseType::Float.into(),
        qusql_parse::Type::Numeric(_) => todo!("Numeric"),
        qusql_parse::Type::Decimal(_) => todo!("Decimal"),
        qusql_parse::Type::Timestamptz => BaseType::TimeStamp.into(),
        qusql_parse::Type::Json => BaseType::String.into(),
        qusql_parse::Type::Jsonb => BaseType::String.into(),
        qusql_parse::Type::Bit(_, _) => BaseType::Bytes.into(),
        qusql_parse::Type::VarBit(_) => BaseType::Bytes.into(),
        qusql_parse::Type::Bytea => BaseType::Bytes.into(),
        qusql_parse::Type::Named(_) => BaseType::String.into(), // TODO lookup name??
        qusql_parse::Type::Inet4 => BaseType::String.into(),
        qusql_parse::Type::Inet6 => BaseType::String.into(),
        qusql_parse::Type::InetAddr => BaseType::String.into(),
        qusql_parse::Type::Cidr => BaseType::String.into(),
        qusql_parse::Type::Macaddr => BaseType::String.into(),
        qusql_parse::Type::Macaddr8 => BaseType::String.into(),
        qusql_parse::Type::Array(_, _) => todo!("Array type not yet implemented"),
        qusql_parse::Type::Table(_, _) => todo!("Table type not yet implemented"),
        qusql_parse::Type::Serial
        | qusql_parse::Type::SmallSerial
        | qusql_parse::Type::BigSerial => BaseType::Integer.into(),
        qusql_parse::Type::Money => BaseType::Float.into(),
        qusql_parse::Type::Timetz(_) => BaseType::Time.into(),
        qusql_parse::Type::Interval(_) => BaseType::TimeInterval.into(),
        qusql_parse::Type::TsQuery => BaseType::String.into(),
        qusql_parse::Type::TsVector => BaseType::String.into(),
        qusql_parse::Type::Uuid => BaseType::String.into(),
        qusql_parse::Type::Xml => BaseType::String.into(),
        qusql_parse::Type::Range(_) => BaseType::Bytes.into(),
        qusql_parse::Type::MultiRange(_) => BaseType::Bytes.into(),
        qusql_parse::Type::Point
        | qusql_parse::Type::Line
        | qusql_parse::Type::Lseg
        | qusql_parse::Type::Box
        | qusql_parse::Type::Path
        | qusql_parse::Type::Polygon
        | qusql_parse::Type::Circle => BaseType::Bytes.into(),
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

/// Processing context for schema evaluation: holds mutable schema state, issue
/// sink, the source text for span-offset calculations, and parse/type options.
///
/// A new `SchemaCtx` with a different `src` / `issues` is created each time we
/// descend into an embedded dollar-quoted SQL string so that inner spans are
/// resolved against the correct slice.
struct SchemaCtx<'a, 'b> {
    schemas: &'b mut Schemas<'a>,
    issues: &'b mut Issues<'a>,
    /// The source text slice that all spans inside `issues` refer to.
    src: &'a str,
    options: &'b TypeOptions,
}

impl<'a, 'b> SchemaCtx<'a, 'b> {
    fn new(
        schemas: &'b mut Schemas<'a>,
        issues: &'b mut Issues<'a>,
        src: &'a str,
        options: &'b TypeOptions,
    ) -> Self {
        Self {
            schemas,
            issues,
            src,
            options,
        }
    }

    // ------------------------------------------------------------------ //
    //  Top-level entry points                                              //
    // ------------------------------------------------------------------ //

    fn process_statements(&mut self, statements: Vec<qusql_parse::Statement<'a>>) {
        for statement in statements {
            self.process_statement(statement);
        }
    }

    fn process_statement(&mut self, statement: qusql_parse::Statement<'a>) {
        match statement {
            qusql_parse::Statement::CreateTable(t) => self.process_create_table(*t),
            qusql_parse::Statement::CreateView(v) => self.process_create_view(*v),
            qusql_parse::Statement::CreateFunction(f) => self.process_create_function(*f),
            qusql_parse::Statement::CreateProcedure(p) => self.process_create_procedure(*p),
            qusql_parse::Statement::CreateIndex(ci) => self.process_create_index(*ci),
            qusql_parse::Statement::CreateTrigger(_) => {}
            qusql_parse::Statement::CreateTypeEnum(s) => {
                self.issues.err("not implemented", &s);
            }
            qusql_parse::Statement::AlterTable(a) => self.process_alter_table(*a),
            qusql_parse::Statement::DropTable(t) => self.process_drop_table(*t),
            qusql_parse::Statement::DropView(v) => self.process_drop_view(*v),
            qusql_parse::Statement::DropFunction(f) => self.process_drop_function(*f),
            qusql_parse::Statement::DropProcedure(p) => self.process_drop_procedure(*p),
            qusql_parse::Statement::DropIndex(ci) => self.process_drop_index(*ci),
            qusql_parse::Statement::DropDatabase(s) => {
                self.issues.err("not implemented", &s);
            }
            qusql_parse::Statement::DropServer(s) => {
                self.issues.err("not implemented", &s);
            }
            qusql_parse::Statement::DropTrigger(_) => {}
            qusql_parse::Statement::DropType(s) => {
                self.issues.err("not implemented", &s);
            }
            // Control-flow: recurse into all reachable branches.
            qusql_parse::Statement::Do(d) => self.process_do(*d),
            qusql_parse::Statement::Block(b) => self.process_statements(b.statements),
            qusql_parse::Statement::If(i) => self.process_if(*i),
            // SELECT: scan args for embedded dollar-quoted SQL.
            qusql_parse::Statement::Select(s) => self.process_select(*s),
            // DML: might call functions that affect schema-level metadata in
            // theory, but we cannot evaluate them statically — skip.
            qusql_parse::Statement::InsertReplace(_) => {}
            qusql_parse::Statement::Update(_) => {}
            qusql_parse::Statement::Delete(_) => {}
            // Transaction control: we assume all transactions commit.
            qusql_parse::Statement::Commit(_) => {}
            qusql_parse::Statement::Begin(_) => {}
            qusql_parse::Statement::Set(_) => {}
            // Statements with no schema effect.
            qusql_parse::Statement::Call(_) => {}
            qusql_parse::Statement::Grant(_) => {}
            qusql_parse::Statement::CommentOn(_) => {}
            qusql_parse::Statement::ExecuteFunction(_) => {}
            qusql_parse::Statement::DeclareVariable(_) => {}
            qusql_parse::Statement::DeclareCursorMariaDb(_) => {}
            qusql_parse::Statement::DeclareHandler(_) => {}
            qusql_parse::Statement::OpenCursor(_) => {}
            qusql_parse::Statement::CloseCursor(_) => {}
            qusql_parse::Statement::FetchCursor(_) => {}
            qusql_parse::Statement::Leave(_) => {}
            qusql_parse::Statement::Iterate(_) => {}
            qusql_parse::Statement::Loop(_) => {}
            qusql_parse::Statement::While(_) => {}
            qusql_parse::Statement::Repeat(_) => {}
            qusql_parse::Statement::Perform(_) => {}
            qusql_parse::Statement::Raise(_) => {}
            qusql_parse::Statement::Assign(_) => {}
            qusql_parse::Statement::PlpgsqlExecute(_) => {}
            s => {
                self.issues.err(
                    alloc::format!("Unsupported statement {s:?} in schema definition"),
                    &s,
                );
            }
        }
    }

    // ------------------------------------------------------------------ //
    //  CREATE statements                                                   //
    // ------------------------------------------------------------------ //

    // ------------------------------------------------------------------ //
    //  CREATE statements                                                   //
    // ------------------------------------------------------------------ //

    fn process_create_table(&mut self, t: qusql_parse::CreateTable<'a>) {
        let mut replace = false;
        let id = unqualified_name(self.issues, &t.identifier);
        let mut schema = Schema {
            view: false,
            identifier_span: id.span.clone(),
            columns: Default::default(),
        };
        for o in t.create_options {
            match o {
                qusql_parse::CreateOption::OrReplace(_) => replace = true,
                qusql_parse::CreateOption::Temporary { temporary_span, .. } => {
                    self.issues.err("Not supported", &temporary_span);
                }
                qusql_parse::CreateOption::Materialized(s) => {
                    self.issues.err("Not supported", &s);
                }
                qusql_parse::CreateOption::Concurrently(s) => {
                    self.issues.err("Not supported", &s);
                }
                qusql_parse::CreateOption::Unique(s) => {
                    self.issues.err("Not supported", &s);
                }
                _ => {}
            }
        }
        for d in t.create_definitions {
            match d {
                qusql_parse::CreateDefinition::ColumnDefinition {
                    identifier,
                    data_type,
                } => {
                    let column = parse_column(
                        data_type,
                        identifier.clone(),
                        self.issues,
                        Some(self.options),
                    );
                    if let Some(oc) = schema.get_column(column.identifier.value) {
                        self.issues
                            .err("Column already defined", &identifier)
                            .frag("Defined here", &oc.identifier);
                    } else {
                        schema.columns.push(column);
                    }
                }
                qusql_parse::CreateDefinition::IndexDefinition { .. } => {}
                qusql_parse::CreateDefinition::ForeignKeyDefinition { .. } => {}
                qusql_parse::CreateDefinition::CheckConstraintDefinition { .. } => {}
                qusql_parse::CreateDefinition::LikeTable { .. } => {}
            }
        }
        match self.schemas.schemas.entry(id.clone()) {
            alloc::collections::btree_map::Entry::Occupied(mut e) => {
                if replace {
                    e.insert(schema);
                } else if t.if_not_exists.is_none() {
                    self.issues
                        .err("Table already defined", &t.identifier)
                        .frag("Defined here", &e.get().identifier_span);
                }
            }
            alloc::collections::btree_map::Entry::Vacant(e) => {
                e.insert(schema);
            }
        }
    }

    fn process_create_view(&mut self, v: qusql_parse::CreateView<'a>) {
        let mut replace = false;
        let mut schema = Schema {
            view: true,
            identifier_span: v.name.span(),
            columns: Default::default(),
        };
        for o in v.create_options {
            match o {
                qusql_parse::CreateOption::OrReplace(_) => replace = true,
                qusql_parse::CreateOption::Temporary { temporary_span, .. } => {
                    self.issues.err("Not supported", &temporary_span);
                }
                qusql_parse::CreateOption::Materialized(s) => {
                    self.issues.err("Not supported", &s);
                }
                qusql_parse::CreateOption::Concurrently(s) => {
                    self.issues.err("Not supported", &s);
                }
                qusql_parse::CreateOption::Unique(s) => {
                    self.issues.err("Not supported", &s);
                }
                _ => {}
            }
        }
        {
            let mut typer: crate::typer::Typer<'a, '_> = crate::typer::Typer {
                schemas: self.schemas,
                issues: self.issues,
                reference_types: Vec::new(),
                arg_types: Default::default(),
                options: self.options,
                with_schemas: Default::default(),
            };
            let t = type_statement::type_statement(&mut typer, &v.select);
            let s = if let type_statement::InnerStatementType::Select(s) = t {
                s
            } else {
                self.issues.err("Not supported", &v.select.span());
                return;
            };
            for column in s.columns {
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
        match self
            .schemas
            .schemas
            .entry(unqualified_name(self.issues, &v.name).clone())
        {
            alloc::collections::btree_map::Entry::Occupied(mut e) => {
                if replace {
                    e.insert(schema);
                } else if v.if_not_exists.is_none() {
                    self.issues
                        .err("View already defined", &v.name)
                        .frag("Defined here", &e.get().identifier_span);
                }
            }
            alloc::collections::btree_map::Entry::Vacant(e) => {
                e.insert(schema);
            }
        }
    }

    fn process_create_function(&mut self, f: qusql_parse::CreateFunction<'a>) {
        let mut replace = false;
        for o in &f.create_options {
            if matches!(o, qusql_parse::CreateOption::OrReplace(_)) {
                replace = true;
            }
        }
        let body = f
            .body
            .as_ref()
            .and_then(|b| b.strings.first())
            .and_then(|s| try_parse_body(self.src, s, self.issues, &self.options.parse_options));
        let name = f.name.clone();
        let def = FunctionDef {
            name: f.name.clone(),
            params: f.params,
            return_type: f.return_type,
            span: f.create_span.join_span(&f.function_span),
            body,
        };
        match self.schemas.functions.entry(name) {
            alloc::collections::btree_map::Entry::Occupied(mut e) => {
                if replace {
                    e.insert(def);
                } else if f.if_not_exists.is_none() {
                    self.issues
                        .err("Function already defined", &f.name)
                        .frag("Defined here", &e.get().span);
                }
            }
            alloc::collections::btree_map::Entry::Vacant(e) => {
                e.insert(def);
            }
        }
    }

    fn process_create_procedure(&mut self, p: qusql_parse::CreateProcedure<'a>) {
        let mut replace = false;
        for o in &p.create_options {
            if matches!(o, qusql_parse::CreateOption::OrReplace(_)) {
                replace = true;
            }
        }
        let name = p.name.clone();
        let def = ProcedureDef {
            name: p.name.clone(),
            params: p.params,
            span: p.create_span.join_span(&p.procedure_span),
        };
        match self.schemas.procedures.entry(name) {
            alloc::collections::btree_map::Entry::Occupied(mut e) => {
                if replace {
                    e.insert(def);
                } else if p.if_not_exists.is_none() {
                    self.issues
                        .err("Procedure already defined", &p.name)
                        .frag("Defined here", &e.get().span);
                }
            }
            alloc::collections::btree_map::Entry::Vacant(e) => {
                e.insert(def);
            }
        }
    }

    fn process_create_index(&mut self, ci: qusql_parse::CreateIndex<'a>) {
        let t = unqualified_name(self.issues, &ci.table_name);
        if let Some(table) = self.schemas.schemas.get(t) {
            for col in &ci.column_names {
                if let qusql_parse::IndexColExpr::Column(name) = &col.expr
                    && table.get_column(name.value).is_none()
                {
                    self.issues
                        .err("No such column in table", col)
                        .frag("Table defined here", &table.identifier_span);
                }
            }
        } else {
            self.issues.err("No such table", &ci.table_name);
        }
        let index_name = match &ci.index_name {
            Some(name) => name.clone(),
            None => return,
        };
        let ident = if self.options.parse_options.get_dialect().is_postgresql() {
            IndexKey {
                table: None,
                index: index_name.clone(),
            }
        } else {
            IndexKey {
                table: Some(t.clone()),
                index: index_name.clone(),
            }
        };
        if let Some(old) = self.schemas.indices.insert(ident, ci.span())
            && ci.if_not_exists.is_none()
        {
            self.issues
                .err("Multiple indeces with the same identifier", &ci)
                .frag("Already defined here", &old);
        }
    }

    // ------------------------------------------------------------------ //
    //  ALTER statements                                                    //
    // ------------------------------------------------------------------ //

    fn process_alter_table(&mut self, a: qusql_parse::AlterTable<'a>) {
        let e = match self
            .schemas
            .schemas
            .entry(unqualified_name(self.issues, &a.table).clone())
        {
            alloc::collections::btree_map::Entry::Occupied(e) => {
                let e = e.into_mut();
                if e.view {
                    self.issues.err("Cannot alter view", &a.table);
                    return;
                }
                e
            }
            alloc::collections::btree_map::Entry::Vacant(_) => {
                if a.if_exists.is_none() {
                    self.issues.err("Table not found", &a.table);
                }
                return;
            }
        };
        for s in a.alter_specifications {
            process_alter_specification(
                s,
                e,
                &a.table,
                self.issues,
                &mut self.schemas.indices,
                self.options,
            );
        }
    }
}

#[allow(clippy::too_many_lines)]
fn process_alter_specification<'a>(
    s: qusql_parse::AlterSpecification<'a>,
    e: &mut Schema<'a>,
    table_ref: &qusql_parse::QualifiedName<'a>,
    issues: &mut Issues<'a>,
    indices: &mut alloc::collections::BTreeMap<IndexKey<'a>, Span>,
    options: &TypeOptions,
) {
    match s {
        qusql_parse::AlterSpecification::AddIndex(AddIndex {
            if_not_exists,
            name,
            cols,
            ..
        }) => {
            for col in &cols {
                if let qusql_parse::IndexColExpr::Column(cname) = &col.expr
                    && e.get_column(cname.value).is_none()
                {
                    issues
                        .err("No such column in table", col)
                        .frag("Table defined here", table_ref);
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
                        table: Some(unqualified_name(issues, table_ref).clone()),
                        index: name.clone(),
                    }
                };
                if let Some(old) = indices.insert(ident, name.span())
                    && if_not_exists.is_none()
                {
                    issues
                        .err("Multiple indeces with the same identifier", &name.span())
                        .frag("Already defined here", &old);
                }
            }
        }
        qusql_parse::AlterSpecification::AddForeignKey { .. } => {}
        qusql_parse::AlterSpecification::Modify(ModifyColumn {
            if_exists,
            col,
            definition,
            ..
        }) => match e.get_column_mut(col.value) {
            Some(c) => {
                let new_col = parse_column(definition, c.identifier.clone(), issues, Some(options));
                *c = new_col;
            }
            None if if_exists.is_none() => {
                issues
                    .err("No such column in table", &col)
                    .frag("Table defined here", &e.identifier_span);
            }
            None => {}
        },
        qusql_parse::AlterSpecification::AddColumn(AddColumn {
            identifier,
            data_type,
            ..
        }) => {
            e.columns
                .push(parse_column(data_type, identifier, issues, Some(options)));
        }
        qusql_parse::AlterSpecification::OwnerTo { .. } => {}
        qusql_parse::AlterSpecification::DropColumn(DropColumn {
            column, if_exists, ..
        }) => {
            let cnt = e.columns.len();
            e.columns.retain(|c| c.identifier != column);
            if cnt == e.columns.len() && if_exists.is_none() {
                issues
                    .err("No such column in table", &column)
                    .frag("Table defined here", &e.identifier_span);
            }
        }
        qusql_parse::AlterSpecification::AlterColumn(AlterColumn {
            column,
            alter_column_action,
            ..
        }) => match e.get_column_mut(column.value) {
            Some(c) => match alter_column_action {
                qusql_parse::AlterColumnAction::SetDefault { .. } => {}
                qusql_parse::AlterColumnAction::DropDefault { .. } => {}
                qusql_parse::AlterColumnAction::Type { type_, .. } => {
                    *c = parse_column(type_, column, issues, Some(options));
                }
                qusql_parse::AlterColumnAction::SetNotNull { .. } => c.type_.not_null = true,
                qusql_parse::AlterColumnAction::DropNotNull { .. } => c.type_.not_null = false,
                a @ qusql_parse::AlterColumnAction::AddGenerated { .. } => {
                    issues.err("not implemented", &a);
                }
            },
            None => {
                issues
                    .err("No such column in table", &column)
                    .frag("Table defined here", &e.identifier_span);
            }
        },
        qusql_parse::AlterSpecification::DropIndex(_) => {
            issues.err("not implemented: DROP INDEX", table_ref);
        }
        qusql_parse::AlterSpecification::RenameColumn { .. } => {
            issues.err("not implemented: RENAME COLUMN", table_ref);
        }
        qusql_parse::AlterSpecification::RenameIndex { .. } => {
            issues.err("not implemented: RENAME INDEX", table_ref);
        }
        qusql_parse::AlterSpecification::RenameConstraint { .. } => {
            issues.err("not implemented: RENAME CONSTRAINT", table_ref);
        }
        qusql_parse::AlterSpecification::RenameTo { .. } => {
            issues.err("not implemented: RENAME TO", table_ref);
        }
        qusql_parse::AlterSpecification::Change { .. } => {
            issues.err("not implemented: CHANGE COLUMN", table_ref);
        }
        qusql_parse::AlterSpecification::Lock { .. }
        | qusql_parse::AlterSpecification::DropForeignKey { .. }
        | qusql_parse::AlterSpecification::DropPrimaryKey { .. }
        | qusql_parse::AlterSpecification::Algorithm { .. }
        | qusql_parse::AlterSpecification::AutoIncrement { .. }
        | qusql_parse::AlterSpecification::ReplicaIdentity(_)
        | qusql_parse::AlterSpecification::ValidateConstraint(_)
        | qusql_parse::AlterSpecification::AddTableConstraint(_)
        | qusql_parse::AlterSpecification::DisableTrigger(_)
        | qusql_parse::AlterSpecification::EnableTrigger(_)
        | qusql_parse::AlterSpecification::DisableRule(_)
        | qusql_parse::AlterSpecification::EnableRule(_)
        | qusql_parse::AlterSpecification::DisableRowLevelSecurity(_)
        | qusql_parse::AlterSpecification::EnableRowLevelSecurity(_)
        | qusql_parse::AlterSpecification::ForceRowLevelSecurity(_)
        | qusql_parse::AlterSpecification::NoForceRowLevelSecurity(_) => {}
    }
}

impl<'a, 'b> SchemaCtx<'a, 'b> {
    // ------------------------------------------------------------------ //
    //  DROP statements                                                     //
    // ------------------------------------------------------------------ //

    fn process_drop_table(&mut self, t: qusql_parse::DropTable<'a>) {
        for i in t.tables {
            match self
                .schemas
                .schemas
                .entry(unqualified_name(self.issues, &i).clone())
            {
                alloc::collections::btree_map::Entry::Occupied(e) => {
                    if e.get().view {
                        self.issues
                            .err("Name defines a view not a table", &i)
                            .frag("View defined here", &e.get().identifier_span);
                    } else {
                        e.remove();
                    }
                }
                alloc::collections::btree_map::Entry::Vacant(_) => {
                    if t.if_exists.is_none() {
                        self.issues
                            .err("A table with this name does not exist to drop", &i);
                    }
                }
            }
        }
    }

    fn process_drop_view(&mut self, v: qusql_parse::DropView<'a>) {
        for i in v.views {
            match self
                .schemas
                .schemas
                .entry(unqualified_name(self.issues, &i).clone())
            {
                alloc::collections::btree_map::Entry::Occupied(e) => {
                    if !e.get().view {
                        self.issues
                            .err("Name defines a table not a view", &i)
                            .frag("Table defined here", &e.get().identifier_span);
                    } else {
                        e.remove();
                    }
                }
                alloc::collections::btree_map::Entry::Vacant(_) => {
                    if v.if_exists.is_none() {
                        self.issues
                            .err("A view with this name does not exist to drop", &i);
                    }
                }
            }
        }
    }

    fn process_drop_function(&mut self, f: qusql_parse::DropFunction<'a>) {
        for (func_name, _args) in &f.functions {
            match self
                .schemas
                .functions
                .entry(unqualified_name(self.issues, func_name).clone())
            {
                alloc::collections::btree_map::Entry::Occupied(e) => {
                    e.remove();
                }
                alloc::collections::btree_map::Entry::Vacant(_) => {
                    if f.if_exists.is_none() {
                        self.issues.err(
                            "A function with this name does not exist to drop",
                            func_name,
                        );
                    }
                }
            }
        }
    }

    fn process_drop_procedure(&mut self, p: qusql_parse::DropProcedure<'a>) {
        let name = unqualified_name(self.issues, &p.procedure);
        match self.schemas.procedures.entry(name.clone()) {
            alloc::collections::btree_map::Entry::Occupied(e) => {
                e.remove();
            }
            alloc::collections::btree_map::Entry::Vacant(_) => {
                if p.if_exists.is_none() {
                    self.issues.err(
                        "A procedure with this name does not exist to drop",
                        &p.procedure,
                    );
                }
            }
        }
    }

    fn process_drop_index(&mut self, ci: qusql_parse::DropIndex<'a>) {
        let key = IndexKey {
            table: ci.on.as_ref().map(|(_, t)| t.identifier.clone()),
            index: ci.index_name.clone(),
        };
        if self.schemas.indices.remove(&key).is_none() && ci.if_exists.is_none() {
            self.issues.err("No such index", &ci);
        }
    }

    // ------------------------------------------------------------------ //
    //  Control-flow / embedded SQL                                         //
    // ------------------------------------------------------------------ //

    /// DO $$ ... $$: re-parse the dollar-quoted body and recurse.
    fn process_do(&mut self, d: qusql_parse::Do<'a>) {
        match d.body {
            qusql_parse::DoBody::Statements(stmts) => self.process_statements(stmts),
            qusql_parse::DoBody::String(s, _) => {
                let span_offset = s.as_ptr() as usize - self.src.as_ptr() as usize;
                let body_opts = self
                    .options
                    .parse_options
                    .clone()
                    .function_body(true)
                    .span_offset(span_offset);
                let stmts = parse_statements(s, self.issues, &body_opts);
                self.process_statements(stmts);
            }
        }
    }

    /// IF ... THEN / ELSEIF / ELSE: recurse into all branches.
    fn process_if(&mut self, i: qusql_parse::If<'a>) {
        for cond in i.conditions {
            self.process_statements(cond.then);
        }
        if let Some((_, stmts)) = i.else_ {
            self.process_statements(stmts);
        }
    }

    /// SELECT: scan expression arguments for dollar-quoted embedded SQL strings.
    fn process_select(&mut self, s: qusql_parse::Select<'a>) {
        for se in s.select_exprs {
            self.scan_expr_for_sql(se.expr);
        }
    }

    /// Recursively scan an expression for dollar-quoted string literals and
    /// process any found SQL strings as nested schema statements.
    fn scan_expr_for_sql(&mut self, expr: qusql_parse::Expression<'a>) {
        match expr {
            qusql_parse::Expression::String(s) => {
                if let Cow::Borrowed(borrowed) = &s.value {
                    let span_offset = borrowed.as_ptr() as usize - self.src.as_ptr() as usize;
                    let opts = self.options.parse_options.clone().span_offset(span_offset);
                    let stmts = parse_statements(borrowed, self.issues, &opts);
                    self.process_statements(stmts);
                }
            }
            qusql_parse::Expression::Function(f) => {
                for arg in f.args {
                    self.scan_expr_for_sql(arg);
                }
            }
            _ => {}
        }
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

    SchemaCtx::new(&mut schemas, issues, src, options).process_statements(statements);

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
