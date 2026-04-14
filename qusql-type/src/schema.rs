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

//! Parse and evaluate SQL schema definitions into a typed representation
//! used for statement type-checking.
//!
//! Supports DDL statements (CREATE/ALTER/DROP for tables, views, functions,
//! procedures, indices, and types) across MySQL/MariaDB, PostgreSQL/PostGIS,
//! and SQLite dialects. Includes a limited schema-level evaluator that can
//! interpret PL/pgSQL function bodies, DO blocks, IF/ELSE control flow,
//! INSERT/DELETE/TRUNCATE for in-memory row tracking, and expressions
//! (EXISTS, COALESCE, aggregates). PostgreSQL/PostGIS built-in schemas
//! (e.g. `spatial_ref_sys`, `geometry_columns`) are injected automatically.
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
    AddColumn, AddIndex, AlterColumn, DataType, DropColumn, Expression, FunctionParam, Identifier,
    IdentifierPart, Issues, ModifyColumn, Span, Spanned, Statement, parse_statements,
};

/// A column in a schema
#[derive(Debug, Clone)]
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
    pub params: Vec<FunctionParam<'a>>,
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
}

/// A stored function definition
#[derive(Debug)]
pub struct FunctionDef<'a> {
    pub name: Identifier<'a>,
    pub params: Vec<FunctionParam<'a>>,
    pub return_type: DataType<'a>,
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

/// A user-defined type registered via `CREATE TYPE`
#[derive(Debug)]
pub enum TypeDef<'a> {
    /// A PostgreSQL enum type
    Enum {
        values: Arc<Vec<Cow<'a, str>>>,
        span: Span,
    },
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
    /// Map from type name to type definition (e.g. enums created with `CREATE TYPE ... AS ENUM`)
    pub types: BTreeMap<Identifier<'a>, TypeDef<'a>>,
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
    })
}

pub(crate) fn parse_column<'a>(
    data_type: DataType<'a>,
    identifier: Identifier<'a>,
    _issues: &mut Issues<'a>,
    options: Option<&TypeOptions>,
    types: Option<&BTreeMap<Identifier<'a>, TypeDef<'a>>>,
) -> Column<'a> {
    let mut not_null = false;
    let mut unsigned = false;
    let mut auto_increment = false;
    let mut default = false;
    let mut as_ = None;
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
            qusql_parse::DataTypeProperty::As((_, e)) => as_ = Some(e),
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
        qusql_parse::Type::Named(qname) => {
            // Look up user-defined types (e.g. enums created with CREATE TYPE ... AS ENUM).
            // Only unqualified names are looked up; schema-qualified names (e.g. public.mytype)
            // are not stored with a prefix in the types map.
            if let Some(types) = types
                && qname.prefix.is_empty()
                && let Some(TypeDef::Enum { values, .. }) = types.get(qname.identifier.value)
            {
                Type::Enum(values.clone())
            } else {
                BaseType::String.into()
            }
        }
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
        as_,
        default,
        generated,
    }
}

/// A runtime SQL value produced during schema evaluation.
#[derive(Clone, Debug, PartialEq)]
enum SqlValue<'a> {
    Null,
    Bool(bool),
    Integer(i64),
    /// A text slice directly from the SQL source - span arithmetic still works.
    SourceText(&'a str),
    /// A computed / owned text value.
    OwnedText(alloc::string::String),
}

impl<'a> SqlValue<'a> {
    fn as_source_text(&self) -> Option<&'a str> {
        if let SqlValue::SourceText(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn is_truthy(&self) -> bool {
        match self {
            SqlValue::Bool(b) => *b,
            SqlValue::Integer(i) => *i != 0,
            SqlValue::Null => false,
            SqlValue::SourceText(_) | SqlValue::OwnedText(_) => true,
        }
    }

    fn sql_eq(&self, other: &SqlValue<'a>) -> Option<bool> {
        match (self, other) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => None,
            (SqlValue::Bool(a), SqlValue::Bool(b)) => Some(a == b),
            (SqlValue::Integer(a), SqlValue::Integer(b)) => Some(a == b),
            (SqlValue::SourceText(a), SqlValue::SourceText(b)) => Some(a == b),
            (SqlValue::SourceText(a), SqlValue::OwnedText(b)) => Some(*a == b.as_str()),
            (SqlValue::OwnedText(a), SqlValue::SourceText(b)) => Some(a.as_str() == *b),
            (SqlValue::OwnedText(a), SqlValue::OwnedText(b)) => Some(a == b),
            _ => None,
        }
    }

    fn sql_lte(&self, other: &SqlValue<'a>) -> Option<bool> {
        match (self, other) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => None,
            (SqlValue::Integer(a), SqlValue::Integer(b)) => Some(a <= b),
            _ => None,
        }
    }
}

/// A single row of evaluated column values.
type Row<'a> = BTreeMap<&'a str, SqlValue<'a>>;

/// Processing context for schema evaluation: holds mutable schema state, issue
/// sink, the source text for span-offset calculations, and parse/type options.
struct SchemaCtx<'a, 'b> {
    schemas: &'b mut Schemas<'a>,
    issues: &'b mut Issues<'a>,
    /// The source text slice that all spans inside `issues` refer to.
    src: &'a str,
    options: &'b TypeOptions,
    /// Active function argument bindings: parameter name -> SQL value.
    /// Set when evaluating a known function body.
    bindings: BTreeMap<&'a str, SqlValue<'a>>,
    /// In-memory row store for tables populated during schema evaluation.
    rows: BTreeMap<&'a str, Vec<Row<'a>>>,
    /// Table rows made available to aggregate functions during eval_condition.
    /// Temporarily swapped via core::mem::take so eval functions can take &mut self.
    current_table_rows: Vec<Row<'a>>,
    /// The row currently being evaluated (e.g. during WHERE clause filtering).
    /// Set by eval_select_matching_rows around each row's eval call.
    current_row: Option<Row<'a>>,
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
            bindings: Default::default(),
            rows: Default::default(),
            current_table_rows: Default::default(),
            current_row: Default::default(),
        }
    }

    /// Process a list of top-level schema statements.  Each statement is
    /// independent: errors in one do not stop processing of the next.
    fn process_top_level_statements(&mut self, statements: Vec<qusql_parse::Statement<'a>>) {
        for statement in statements {
            let _ = self.process_statement(statement);
        }
    }

    /// Process a list of statements in a block or function body, stopping at
    /// the first `Err` (error or `RETURN`).
    fn process_statements(
        &mut self,
        statements: Vec<qusql_parse::Statement<'a>>,
    ) -> Result<(), ()> {
        for statement in statements {
            self.process_statement(statement)?;
        }
        Ok(())
    }

    fn process_statement(&mut self, statement: qusql_parse::Statement<'a>) -> Result<(), ()> {
        match statement {
            qusql_parse::Statement::CreateTable(t) => {
                self.process_create_table(*t);
                Ok(())
            }
            qusql_parse::Statement::CreateView(v) => {
                self.process_create_view(*v);
                Ok(())
            }
            qusql_parse::Statement::CreateFunction(f) => {
                self.process_create_function(*f);
                Ok(())
            }
            qusql_parse::Statement::CreateProcedure(p) => {
                self.process_create_procedure(*p);
                Ok(())
            }
            qusql_parse::Statement::CreateIndex(ci) => {
                self.process_create_index(*ci);
                Ok(())
            }
            qusql_parse::Statement::CreateTrigger(_) => Ok(()),
            qusql_parse::Statement::CreateTypeEnum(s) => {
                self.process_create_type_enum(*s);
                Ok(())
            }
            qusql_parse::Statement::AlterTable(a) => {
                self.process_alter_table(*a);
                Ok(())
            }
            qusql_parse::Statement::DropTable(t) => {
                self.process_drop_table(*t);
                Ok(())
            }
            qusql_parse::Statement::DropView(v) => {
                self.process_drop_view(*v);
                Ok(())
            }
            qusql_parse::Statement::DropFunction(f) => {
                self.process_drop_function(*f);
                Ok(())
            }
            qusql_parse::Statement::DropProcedure(p) => {
                self.process_drop_procedure(*p);
                Ok(())
            }
            qusql_parse::Statement::DropIndex(ci) => {
                self.process_drop_index(*ci);
                Ok(())
            }
            qusql_parse::Statement::DropDatabase(s) => {
                self.issues.err("not implemented", &s);
                Err(())
            }
            qusql_parse::Statement::DropServer(s) => {
                self.issues.err("not implemented", &s);
                Err(())
            }
            qusql_parse::Statement::DropTrigger(_) => Ok(()),
            qusql_parse::Statement::DropType(s) => {
                self.process_drop_type(*s);
                Ok(())
            }
            // Control-flow: recurse into all reachable branches.
            qusql_parse::Statement::Do(d) => self.process_do(*d),
            qusql_parse::Statement::Block(b) => self.process_statements(b.statements),
            qusql_parse::Statement::If(i) => self.process_if(*i),
            // SELECT: may call a known function whose body we can evaluate.
            qusql_parse::Statement::Select(s) => self.process_select(*s),
            // DML: track row insertions so conditions like EXISTS(...) can be evaluated.
            qusql_parse::Statement::InsertReplace(i) => {
                self.process_insert(*i);
                Ok(())
            }
            // Transaction control: we assume all transactions commit.
            qusql_parse::Statement::Commit(_) => Ok(()),
            qusql_parse::Statement::Begin(_) => Ok(()),
            // Statements with no schema effect.
            qusql_parse::Statement::Grant(_) => Ok(()),
            qusql_parse::Statement::CommentOn(_) => Ok(()),
            qusql_parse::Statement::Analyze(_) => Ok(()),
            // Variable / cursor plumbing — no schema effect.
            qusql_parse::Statement::Set(_) => Ok(()),
            qusql_parse::Statement::Assign(_) => Ok(()),
            qusql_parse::Statement::Perform(_) => Ok(()),
            qusql_parse::Statement::DeclareVariable(_) => Ok(()),
            qusql_parse::Statement::DeclareHandler(_) => Ok(()),
            qusql_parse::Statement::ExecuteFunction(_) => Ok(()),
            // RAISE EXCEPTION aborts execution; anything else is a log/notice with no schema effect.
            qusql_parse::Statement::Raise(r) => {
                if matches!(r.level, Some(qusql_parse::RaiseLevel::Exception(_))) {
                    Err(())
                } else {
                    Ok(())
                }
            }
            qusql_parse::Statement::Return(_) => Err(()),
            qusql_parse::Statement::PlpgsqlExecute(e) => self.process_plpgsql_execute(*e),
            s => {
                self.issues.err(
                    alloc::format!("Unsupported statement {s:?} in schema definition"),
                    &s,
                );
                Err(())
            }
        }
    }

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
                        Some(&self.schemas.types),
                    );
                    if let Some(oc) = schema.get_column(column.identifier.value) {
                        self.issues
                            .err("Column already defined", &identifier)
                            .frag("Defined here", &oc.identifier);
                    } else {
                        schema.columns.push(column);
                    }
                }
                qusql_parse::CreateDefinition::IndexDefinition {
                    index_type,
                    index_name,
                    cols,
                    ..
                } => {
                    // Validate that every column referenced by the index exists.
                    for col in &cols {
                        if let qusql_parse::IndexColExpr::Column(cname) = &col.expr
                            && schema.get_column(cname.value).is_none()
                        {
                            self.issues
                                .err("No such column in table", col)
                                .frag("Table defined here", &schema.identifier_span);
                        }
                    }
                    // PRIMARY KEY implies NOT NULL on each listed column.
                    if matches!(index_type, qusql_parse::IndexType::Primary(_)) {
                        for col in &cols {
                            if let qusql_parse::IndexColExpr::Column(cname) = &col.expr
                                && let Some(c) = schema.get_column_mut(cname.value)
                            {
                                c.type_.not_null = true;
                            }
                        }
                    }
                    // Register named indices.
                    if let Some(name) = index_name {
                        let ident = if self.options.parse_options.get_dialect().is_postgresql() {
                            IndexKey {
                                table: None,
                                index: name.clone(),
                            }
                        } else {
                            IndexKey {
                                table: Some(id.clone()),
                                index: name.clone(),
                            }
                        };
                        let span = name.span();
                        if let Some(old) = self.schemas.indices.insert(ident, span) {
                            self.issues
                                .err("Multiple indices with the same identifier", &name)
                                .frag("Already defined here", &old);
                        }
                    }
                }
                qusql_parse::CreateDefinition::ForeignKeyDefinition { .. } => {}
                qusql_parse::CreateDefinition::CheckConstraintDefinition { .. } => {}
                qusql_parse::CreateDefinition::LikeTable { source_table, .. } => {
                    let source_id = unqualified_name(self.issues, &source_table);
                    if let Some(src) = self.schemas.schemas.get(source_id) {
                        let cols: Vec<Column<'a>> = src.columns.to_vec();
                        for col in cols {
                            if schema.get_column(col.identifier.value).is_none() {
                                schema.columns.push(col);
                            }
                        }
                    } else {
                        self.issues.err("Table not found", &source_table);
                    }
                }
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
                .err("Multiple indices with the same identifier", &ci)
                .frag("Already defined here", &old);
        }
    }

    fn process_create_type_enum(&mut self, s: qusql_parse::CreateTypeEnum<'a>) {
        let name = unqualified_name(self.issues, &s.name);
        let mut replace = false;
        for o in &s.create_options {
            if matches!(o, qusql_parse::CreateOption::OrReplace(_)) {
                replace = true;
            }
        }
        let values = Arc::new(s.values.into_iter().map(|v| v.value).collect());
        let typedef = TypeDef::Enum {
            values,
            span: s.as_enum_span,
        };
        match self.schemas.types.entry(name.clone()) {
            alloc::collections::btree_map::Entry::Occupied(mut e) => {
                if replace {
                    e.insert(typedef);
                }
                // Otherwise silently skip - SQL uses EXCEPTION WHEN duplicate_object to handle re-runs
            }
            alloc::collections::btree_map::Entry::Vacant(e) => {
                e.insert(typedef);
            }
        }
    }

    fn process_drop_type(&mut self, s: qusql_parse::DropType<'a>) {
        let if_exists = s.if_exists;
        for name in s.names {
            let id = unqualified_name(self.issues, &name);
            if self.schemas.types.remove(id).is_none() && if_exists.is_none() {
                self.issues.err("Type not found", &name);
            }
        }
    }

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
                &self.schemas.types,
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
    types: &BTreeMap<Identifier<'a>, TypeDef<'a>>,
) {
    match s {
        qusql_parse::AlterSpecification::AddIndex(AddIndex {
            if_not_exists,
            name,
            cols,
            index_type,
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
            // PRIMARY KEY implies NOT NULL on each listed column.
            if matches!(index_type, qusql_parse::IndexType::Primary(_)) {
                for col in &cols {
                    if let qusql_parse::IndexColExpr::Column(cname) = &col.expr
                        && let Some(c) = e.get_column_mut(cname.value)
                    {
                        c.type_.not_null = true;
                    }
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
                        .err("Multiple indices with the same identifier", &name.span())
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
                let new_col = parse_column(
                    definition,
                    c.identifier.clone(),
                    issues,
                    Some(options),
                    Some(types),
                );
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
            e.columns.push(parse_column(
                data_type,
                identifier,
                issues,
                Some(options),
                Some(types),
            ));
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
                qusql_parse::AlterColumnAction::SetDefault { .. } => c.default = true,
                qusql_parse::AlterColumnAction::DropDefault { .. } => c.default = false,
                qusql_parse::AlterColumnAction::Type { type_, .. } => {
                    *c = parse_column(type_, column, issues, Some(options), Some(types));
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
        qusql_parse::AlterSpecification::DropIndex(drop_idx) => {
            let is_postgresql = options.parse_options.get_dialect().is_postgresql();
            let key = if is_postgresql {
                IndexKey {
                    table: None,
                    index: drop_idx.name.clone(),
                }
            } else {
                IndexKey {
                    table: Some(unqualified_name(issues, table_ref).clone()),
                    index: drop_idx.name.clone(),
                }
            };
            if indices.remove(&key).is_none() {
                issues.err("No such index to drop", &drop_idx.name);
            }
        }
        qusql_parse::AlterSpecification::RenameColumn(qusql_parse::RenameColumn {
            old_col_name,
            new_col_name,
            ..
        }) => match e.get_column_mut(old_col_name.value) {
            Some(c) => c.identifier = new_col_name,
            None => {
                issues
                    .err("No such column in table", &old_col_name)
                    .frag("Table defined here", &e.identifier_span);
            }
        },
        qusql_parse::AlterSpecification::RenameIndex(qusql_parse::RenameIndex {
            old_index_name,
            new_index_name,
            ..
        }) => {
            let is_postgresql = options.parse_options.get_dialect().is_postgresql();
            let table_id = unqualified_name(issues, table_ref).clone();
            let old_key = if is_postgresql {
                IndexKey {
                    table: None,
                    index: old_index_name.clone(),
                }
            } else {
                IndexKey {
                    table: Some(table_id.clone()),
                    index: old_index_name.clone(),
                }
            };
            match indices.remove(&old_key) {
                Some(span) => {
                    let new_key = if is_postgresql {
                        IndexKey {
                            table: None,
                            index: new_index_name,
                        }
                    } else {
                        IndexKey {
                            table: Some(table_id),
                            index: new_index_name,
                        }
                    };
                    indices.insert(new_key, span);
                }
                None => {
                    issues.err("No such index to rename", &old_index_name);
                }
            }
        }
        // We do not track constraints, so RENAME CONSTRAINT is a no-op.
        qusql_parse::AlterSpecification::RenameConstraint(_) => {}
        // RENAME TO inside ALTER TABLE requires renaming the table's map key, which is
        // not accessible here.  The standalone RENAME TABLE statement (handled by
        // process_rename_table) should be preferred; this variant is left as a no-op.
        qusql_parse::AlterSpecification::RenameTo(_) => {}
        qusql_parse::AlterSpecification::Change(qusql_parse::Change {
            column,
            new_column,
            definition,
            ..
        }) => match e.get_column_mut(column.value) {
            Some(c) => {
                *c = parse_column(definition, new_column, issues, Some(options), Some(types));
            }
            None => {
                issues
                    .err("No such column in table", &column)
                    .frag("Table defined here", &e.identifier_span);
            }
        },
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
    /// DO $$ ... $$: re-parse the dollar-quoted body and recurse.
    fn process_do(&mut self, d: qusql_parse::Do<'a>) -> Result<(), ()> {
        match d.body {
            qusql_parse::DoBody::Statements(stmts) => self.process_statements(stmts)?,
            qusql_parse::DoBody::String(s, _) => {
                let span_offset = s.as_ptr() as usize - self.src.as_ptr() as usize;
                let body_opts = self
                    .options
                    .parse_options
                    .clone()
                    .function_body(true)
                    .span_offset(span_offset);
                let stmts = parse_statements(s, self.issues, &body_opts);
                self.process_statements(stmts)?;
            }
        }
        Ok(())
    }

    /// IF ... THEN / ELSEIF / ELSE: recurse into all branches.
    fn process_if(&mut self, i: qusql_parse::If<'a>) -> Result<(), ()> {
        for cond in i.conditions {
            match self.eval_condition(&cond.search_condition) {
                Ok(true) => return self.process_statements(cond.then),
                Ok(false) => continue,
                Err(()) => {
                    // Cannot evaluate condition — pessimistically execute this branch
                    // and stop (we cannot know which subsequent branch would be taken).
                    return self.process_statements(cond.then);
                }
            }
        }
        if let Some((_, stmts)) = i.else_ {
            return self.process_statements(stmts);
        }
        Ok(())
    }

    /// SELECT used at the top level of a schema file must be a bare list of
    /// function calls with no FROM / WHERE / LIMIT / etc.  Each expression is
    /// dispatched to `process_expression`.
    fn process_select(&mut self, s: qusql_parse::Select<'a>) -> Result<(), ()> {
        // Reject anything that looks like a real query.
        if let Some(span) = &s.from_span {
            self.issues
                .err("SELECT with FROM is not supported at schema level", span);
            return Err(());
        }
        if let Some((_, span)) = &s.where_ {
            self.issues
                .err("SELECT with WHERE is not supported at schema level", span);
            return Err(());
        }
        if let Some((span, _)) = &s.group_by {
            self.issues.err(
                "SELECT with GROUP BY is not supported at schema level",
                span,
            );
            return Err(());
        }
        if let Some((_, span)) = &s.having {
            self.issues
                .err("SELECT with HAVING is not supported at schema level", span);
            return Err(());
        }
        if let Some((span, _, _)) = &s.limit {
            self.issues
                .err("SELECT with LIMIT is not supported at schema level", span);
            return Err(());
        }
        if let Some((span, _)) = &s.order_by {
            self.issues.err(
                "SELECT with ORDER BY is not supported at schema level",
                span,
            );
            return Err(());
        }
        for se in s.select_exprs {
            self.process_expression(se.expr)?;
        }
        Ok(())
    }

    fn process_expression(&mut self, expr: Expression<'a>) -> Result<(), ()> {
        match expr {
            Expression::Function(f) => self.process_function_call(*f),
            other => {
                self.issues.err(
                    "Only function calls are supported as statements at schema level",
                    &other,
                );
                Err(())
            }
        }
    }

    fn process_function_call(
        &mut self,
        call: qusql_parse::FunctionCallExpression<'a>,
    ) -> Result<(), ()> {
        let func_name = match &call.function {
            qusql_parse::Function::Other(parts) if parts.len() == 1 => parts[0].value,
            _ => {
                self.issues.err(
                    "Only plain unqualified function calls are supported at schema level",
                    &call,
                );
                return Err(());
            }
        };

        let func_info = self
            .schemas
            .functions
            .values()
            .find(|f| f.name.value == func_name)
            .and_then(|f| {
                f.body
                    .as_ref()
                    .map(|b| (f.params.clone(), b.statements.clone()))
            });

        let Some((params, statements)) = func_info else {
            self.issues.err(
                alloc::format!("Unknown function or function has no evaluable body: {func_name}"),
                &call,
            );
            return Err(());
        };

        let mut bindings = BTreeMap::new();
        for (param, arg) in params.iter().zip(call.args.iter()) {
            let Some(name) = &param.name else { continue };
            let value = match arg {
                Expression::String(s) => match &s.value {
                    Cow::Borrowed(b) => SqlValue::SourceText(b),
                    Cow::Owned(o) => SqlValue::OwnedText(o.clone()),
                },
                Expression::Integer(i) => SqlValue::Integer(i.value as i64),
                Expression::Bool(b) => SqlValue::Bool(b.value),
                Expression::Null(_) => SqlValue::Null,
                _ => continue,
            };
            bindings.insert(name.value, value);
        }

        let old_bindings = core::mem::replace(&mut self.bindings, bindings);
        let _ = self.process_statements(statements);
        self.bindings = old_bindings;
        Ok(())
    }

    fn process_insert(&mut self, i: qusql_parse::InsertReplace<'a>) {
        let Some((_, value_rows)) = i.values else {
            return;
        };
        // Only unqualified table names are tracked.
        let table_name = match i.table.prefix.as_slice() {
            [] => i.table.identifier.value,
            _ => return,
        };
        let col_names: Vec<&'a str> = i.columns.iter().map(|c| c.value).collect();
        for row_exprs in value_rows {
            let mut row: Row<'a> = BTreeMap::new();
            for (col, expr) in col_names.iter().zip(row_exprs.iter()) {
                if let Ok(val) = self.eval_expr(expr) {
                    row.insert(col, val);
                }
            }
            self.rows.entry(table_name).or_default().push(row);
        }
    }

    /// Evaluate an expression to a `SqlValue`.
    /// Reads the current row from `self.current_row` (set by eval_select_matching_rows).
    /// Aggregate functions read rows from `self.current_table_rows` (set by eval_condition).
    /// Returns `Err(())` for expression types not yet handled by the evaluator.
    fn eval_expr(&mut self, expr: &Expression<'a>) -> Result<SqlValue<'a>, ()> {
        match expr {
            Expression::Null(_) => Ok(SqlValue::Null),
            Expression::Bool(b) => Ok(SqlValue::Bool(b.value)),
            Expression::Integer(i) => Ok(SqlValue::Integer(i.value as i64)),
            Expression::String(s) => Ok(match &s.value {
                Cow::Borrowed(b) => SqlValue::SourceText(b),
                Cow::Owned(o) => SqlValue::OwnedText(o.clone()),
            }),
            Expression::Identifier(id) => {
                if let [IdentifierPart::Name(name)] = id.parts.as_slice() {
                    self.bindings
                        .get(name.value)
                        .cloned()
                        .or_else(|| {
                            self.current_row
                                .as_ref()
                                .and_then(|r| r.get(name.value).cloned())
                        })
                        .ok_or(())
                } else {
                    Err(())
                }
            }
            Expression::Exists(e) => Ok(SqlValue::Bool(self.eval_exists(&e.subquery)?)),
            Expression::Function(f) => self.eval_function_expr(f),
            Expression::AggregateFunction(f) => self.eval_aggregate(f),
            Expression::Binary(b) => self.eval_binary_expr(b),
            _ => {
                self.issues
                    .err("Unimplemented expression in schema evaluator", expr);
                Err(())
            }
        }
    }

    fn eval_exists(&mut self, stmt: &qusql_parse::Statement<'a>) -> Result<bool, ()> {
        let qusql_parse::Statement::Select(s) = stmt else {
            return Err(());
        };
        Ok(!self.eval_select_matching_rows(s)?.is_empty())
    }

    /// Return the rows from the single table in a SELECT that satisfy the WHERE clause.
    fn eval_select_matching_rows(
        &mut self,
        s: &qusql_parse::Select<'a>,
    ) -> Result<Vec<Row<'a>>, ()> {
        let table_name = self.select_single_table_name(s);
        // Clone up-front to avoid a borrow conflict with &mut self eval calls below.
        let source_rows: Vec<Row<'a>> = table_name
            .and_then(|n| self.rows.get(n).cloned())
            .unwrap_or_default();

        let where_expr: Option<Expression<'a>> = s.where_.as_ref().map(|(e, _)| e.clone());
        let mut result = Vec::new();
        for row in source_rows {
            let saved_row = self.current_row.replace(row.clone());
            let eval_result = match &where_expr {
                Some(expr) => self.eval_expr(expr).map(|v| v.is_truthy()),
                None => Ok(true),
            };
            self.current_row = saved_row;
            if eval_result? {
                result.push(row);
            }
        }
        Ok(result)
    }

    /// Extract the unqualified name of the single table in the FROM clause, if any.
    fn select_single_table_name<'s>(&self, s: &'s qusql_parse::Select<'a>) -> Option<&'a str> {
        use qusql_parse::TableReference;
        let refs = s.table_references.as_deref()?;
        if let [TableReference::Table { identifier, .. }] = refs
            && identifier.prefix.is_empty()
        {
            return Some(identifier.identifier.value);
        }
        None
    }

    fn eval_function_expr(
        &mut self,
        f: &qusql_parse::FunctionCallExpression<'a>,
    ) -> Result<SqlValue<'a>, ()> {
        use qusql_parse::Function;
        match &f.function {
            Function::Coalesce => {
                // Clone args to avoid borrow conflict between &f and &mut self.
                let args: Vec<_> = f.args.clone();
                for arg in &args {
                    let v = self.eval_expr(arg)?;
                    if v != SqlValue::Null {
                        return Ok(v);
                    }
                }
                Ok(SqlValue::Null)
            }
            Function::Exists => {
                let Some(Expression::Subquery(sq)) = f.args.first() else {
                    self.issues.err("EXISTS without subquery argument", f);
                    return Err(());
                };
                let qusql_parse::Statement::Select(s) = &sq.expression else {
                    self.issues.err("EXISTS argument is not a SELECT", f);
                    return Err(());
                };
                let s = s.clone();
                Ok(SqlValue::Bool(
                    !self.eval_select_matching_rows(&s)?.is_empty(),
                ))
            }
            _ => {
                self.issues
                    .err("Unimplemented function in schema evaluator", f);
                Err(())
            }
        }
    }

    fn eval_aggregate(
        &mut self,
        f: &qusql_parse::AggregateFunctionCallExpression<'a>,
    ) -> Result<SqlValue<'a>, ()> {
        use qusql_parse::Function;
        match &f.function {
            Function::Max => {
                let col_expr = f.args.first().ok_or(())?.clone();
                // Take rows out so we can call &mut self methods during iteration.
                let rows = core::mem::take(&mut self.current_table_rows);
                let mut max: Option<SqlValue<'a>> = None;
                for r in &rows {
                    // Set current_row so column references in the expression resolve correctly.
                    let saved = self.current_row.replace(r.clone());
                    // Skip rows where evaluation fails (NULL semantics for aggregates).
                    let v = self.eval_expr(&col_expr);
                    self.current_row = saved;
                    if let Ok(v) = v
                        && v != SqlValue::Null
                    {
                        max = Some(match max {
                            None => v,
                            Some(SqlValue::Integer(m)) => {
                                if let SqlValue::Integer(n) = &v {
                                    SqlValue::Integer(m.max(*n))
                                } else {
                                    v
                                }
                            }
                            Some(existing) => existing,
                        });
                    }
                }
                self.current_table_rows = rows;
                Ok(max.unwrap_or(SqlValue::Null))
            }
            _ => {
                self.issues
                    .err("Unimplemented aggregate function in schema evaluator", f);
                Err(())
            }
        }
    }

    fn eval_binary_expr(
        &mut self,
        b: &qusql_parse::BinaryExpression<'a>,
    ) -> Result<SqlValue<'a>, ()> {
        use qusql_parse::BinaryOperator;
        let lhs = self.eval_expr(&b.lhs)?;
        let rhs = self.eval_expr(&b.rhs)?;
        // NULL comparisons propagate NULL (not an error).
        Ok(match &b.op {
            BinaryOperator::Eq(_) => lhs.sql_eq(&rhs).map_or(SqlValue::Null, SqlValue::Bool),
            BinaryOperator::Neq(_) => lhs
                .sql_eq(&rhs)
                .map_or(SqlValue::Null, |v| SqlValue::Bool(!v)),
            BinaryOperator::LtEq(_) => lhs.sql_lte(&rhs).map_or(SqlValue::Null, SqlValue::Bool),
            BinaryOperator::Lt(_) => rhs
                .sql_lte(&lhs)
                .map_or(SqlValue::Null, |v| SqlValue::Bool(!v)),
            BinaryOperator::GtEq(_) => rhs.sql_lte(&lhs).map_or(SqlValue::Null, SqlValue::Bool),
            BinaryOperator::Gt(_) => lhs
                .sql_lte(&rhs)
                .map_or(SqlValue::Null, |v| SqlValue::Bool(!v)),
            BinaryOperator::And(_) => SqlValue::Bool(lhs.is_truthy() && rhs.is_truthy()),
            BinaryOperator::Or(_) => SqlValue::Bool(lhs.is_truthy() || rhs.is_truthy()),
            _ => {
                self.issues
                    .err("Unimplemented binary operator in schema evaluator", b);
                return Err(());
            }
        })
    }

    /// Evaluate the search condition of an IF branch as a boolean.
    /// Sets `self.current_table_rows` from the FROM clause (if any) so that
    /// aggregate expressions in the condition can read the table rows.
    /// Returns `Err(())` if the condition uses constructs the evaluator does not handle.
    fn eval_condition(&mut self, s: &qusql_parse::Select<'a>) -> Result<bool, ()> {
        let expr = s.select_exprs.first().map(|se| se.expr.clone()).ok_or(())?;

        // Load the FROM table's rows into current_table_rows for aggregate evaluation.
        let table_rows: Vec<Row<'a>> = self
            .select_single_table_name(s)
            .and_then(|n| self.rows.get(n).cloned())
            .unwrap_or_default();
        let saved = core::mem::replace(&mut self.current_table_rows, table_rows);
        let result = self.eval_expr(&expr);
        self.current_table_rows = saved;

        Ok(result?.is_truthy())
    }

    fn process_plpgsql_execute(&mut self, e: qusql_parse::PlpgsqlExecute<'a>) -> Result<(), ()> {
        let sql = self.resolve_expr_to_bound_string(&e.command);
        let Some(sql) = sql else {
            self.issues.err(
                "EXECUTE argument could not be resolved to a known SQL string",
                &e,
            );
            return Err(());
        };
        let span_offset = sql.as_ptr() as usize - self.src.as_ptr() as usize;
        let opts = self.options.parse_options.clone().span_offset(span_offset);
        let stmts = parse_statements(sql, self.issues, &opts);
        let _ = self.process_statements(stmts);
        Ok(())
    }

    /// Try to resolve an expression to a `&'a str` from the current bindings.
    /// Only succeeds for bare identifier expressions that name a bound parameter
    /// whose value is a borrow from the original source text.
    fn resolve_expr_to_bound_string(&self, expr: &Expression<'a>) -> Option<&'a str> {
        let Expression::Identifier(ident) = expr else {
            return None;
        };
        let [IdentifierPart::Name(name)] = ident.parts.as_slice() else {
            return None;
        };
        self.bindings
            .get(name.value)
            .and_then(|v| v.as_source_text())
    }
}

/// Parse a schema definition and return a terse description
///
/// Errors and warnings are added to issues. The schema is successfully
/// parsed if no errors are added to issues.
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
        types: Default::default(),
    };

    SchemaCtx::new(&mut schemas, issues, src, options).process_top_level_statements(statements);

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
