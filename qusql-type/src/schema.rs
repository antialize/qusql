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
//! for (key, schema) in schemas.schemas {
//!     println!("{}: {schema:?}", key.table_name())
//! }
//! ```

use crate::{
    Type, TypeOptions,
    type_::{BaseType, FullType},
    type_statement,
    typer::{resolve_table_name, unqualified_name},
};
use alloc::{
    borrow::Cow,
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    sync::Arc,
    vec::Vec,
};
use qusql_parse::{
    AddColumn, AddIndex, AlterColumn, DataType, DataTypeProperty, DropColumn, Expression,
    FunctionParam, Identifier, IdentifierPart, Issues, ModifyColumn, OptSpanned, QualifiedName,
    Span, Spanned, Statement, parse_statements,
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
#[derive(Debug, Clone)]
pub struct ProcedureDef<'a> {
    pub name: Identifier<'a>,
    pub params: Vec<FunctionParam<'a>>,
    pub span: Span,
    /// Statements extracted from the procedure body (if the body was a BEGIN...END block)
    pub body: Option<Vec<Statement<'a>>>,
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
    /// Schema name (PostgreSQL only); `None` for MySQL/MariaDB/SQLite.
    pub schema: Option<Identifier<'a>>,
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

/// A schema-qualified or unqualified table/view identifier used as a `Schemas.schemas` map key.
///
/// In PostgreSQL mode every name is stored as `Qualified`; unqualified names use the
/// implicit `"public"` schema.  In MySQL / MariaDB / SQLite mode every name is `Unqualified`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualifiedIdentifier<'a> {
    /// Unqualified table name (MySQL / MariaDB / SQLite)
    Unqualified(Identifier<'a>),
    /// Schema-qualified table name, e.g. `"public"."tablename"` (PostgreSQL)
    Qualified(Identifier<'a>, Identifier<'a>),
}

impl<'a> QualifiedIdentifier<'a> {
    /// The table (rightmost) identifier.
    pub fn table_name(&self) -> &Identifier<'a> {
        match self {
            QualifiedIdentifier::Unqualified(id) | QualifiedIdentifier::Qualified(_, id) => id,
        }
    }
    /// The schema (leftmost) identifier, if any.
    pub fn schema_name(&self) -> Option<&Identifier<'a>> {
        match self {
            QualifiedIdentifier::Unqualified(_) => None,
            QualifiedIdentifier::Qualified(s, _) => Some(s),
        }
    }
}

/// Like [`lookup_name`] but returns both the canonical map key and the value.
/// Useful when the resolved schema is needed (e.g. to build an `IndexKey`).
pub fn lookup_name_key<'a, 'b, T>(
    map: &'b BTreeMap<QualifiedIdentifier<'a>, T>,
    name: &QualifiedIdentifier<'a>,
    search_path: &[&'a str],
) -> Option<(&'b QualifiedIdentifier<'a>, &'b T)> {
    if let Some(kv) = map.get_key_value(name) {
        return Some(kv);
    }
    if let QualifiedIdentifier::Unqualified(table) = name {
        for schema in search_path {
            let qualified =
                QualifiedIdentifier::Qualified(Identifier::new(schema, 0..0), table.clone());
            if let Some(kv) = map.get_key_value(&qualified) {
                return Some(kv);
            }
        }
    }
    None
}

/// Look up a name in a schema-qualified map, with optional search-path fallback.
///
/// For an exact (qualified) hit the answer is immediate.  When `name` is
/// `Unqualified` and the key in `map` is `Qualified`, each entry in
/// `search_path` is tried in order so that e.g. a bare table reference is
/// resolved against the PostgreSQL `search_path`.  Pass an empty slice for
/// MySQL / MariaDB / SQLite.
pub fn lookup_name<'a, 'b, T>(
    map: &'b BTreeMap<QualifiedIdentifier<'a>, T>,
    name: &QualifiedIdentifier<'a>,
    search_path: &[&'a str],
) -> Option<&'b T> {
    lookup_name_key(map, name, search_path).map(|(_, v)| v)
}

/// Mutable variant of [`lookup_name`]: returns a mutable reference to the map value.
pub fn lookup_name_mut<'a, 'b, T>(
    map: &'b mut BTreeMap<QualifiedIdentifier<'a>, T>,
    name: &QualifiedIdentifier<'a>,
    search_path: &[&'a str],
) -> Option<&'b mut T> {
    if map.contains_key(name) {
        return map.get_mut(name);
    }
    if let QualifiedIdentifier::Unqualified(table) = name {
        for schema in search_path {
            let qualified =
                QualifiedIdentifier::Qualified(Identifier::new(schema, 0..0), table.clone());
            if map.contains_key(&qualified) {
                return map.get_mut(&qualified);
            }
        }
    }
    None
}

/// A description of tables, views, procedures, and functions in a schema definition file.
#[derive(Debug, Default)]
pub struct Schemas<'a> {
    /// Map from qualified table/view name to its schema definition.
    ///
    /// PostgreSQL: all keys are `QualifiedIdentifier::Qualified`; unqualified names are
    /// stored under `"public"`.  MySQL/MariaDB/SQLite: all keys are `QualifiedIdentifier::Unqualified`.
    pub schemas: BTreeMap<QualifiedIdentifier<'a>, Schema<'a>>,
    /// Set of schema names registered via `CREATE SCHEMA` (PostgreSQL only).
    pub schema_names: BTreeSet<Identifier<'a>>,
    /// Map from qualified name to stored procedure.
    pub procedures: BTreeMap<QualifiedIdentifier<'a>, ProcedureDef<'a>>,
    /// Map from qualified name to stored function.
    pub functions: BTreeMap<QualifiedIdentifier<'a>, FunctionDef<'a>>,
    /// Map from (table, index) to location.
    pub indices: BTreeMap<IndexKey<'a>, Span>,
    /// Map from qualified type name to type definition (e.g. enums created with `CREATE TYPE … AS ENUM`).
    pub types: BTreeMap<QualifiedIdentifier<'a>, TypeDef<'a>>,
    /// Map of sequence names registered via `CREATE SEQUENCE` (PostgreSQL only).
    /// The value is `()` — only existence is tracked.
    pub sequences: BTreeMap<QualifiedIdentifier<'a>, ()>,
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

fn type_kind_from_parse<'a>(
    type_: qusql_parse::Type<'a>,
    unsigned: bool,
    is_sqlite: bool,
    is_postgresql: bool,
    types: Option<&BTreeMap<QualifiedIdentifier<'a>, TypeDef<'a>>>,
    issues: &mut Issues<'a>,
) -> Type<'a> {
    match type_ {
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
            if is_sqlite {
                BaseType::Integer.into()
            } else {
                Type::I32
            }
        }
        qusql_parse::Type::Float8 => BaseType::Float.into(),
        qusql_parse::Type::Numeric(ref v) => {
            let span = v.as_ref().map(|(_, _, s)| s.clone()).unwrap_or(0..0);
            issues.err("NUMERIC type is not yet supported", &span);
            BaseType::Float.into()
        }
        qusql_parse::Type::Decimal(ref v) => {
            let span = v.as_ref().map(|(_, _, s)| s.clone()).unwrap_or(0..0);
            issues.err("DECIMAL type is not yet supported", &span);
            BaseType::Float.into()
        }
        qusql_parse::Type::Timestamptz => BaseType::TimeStamp.into(),
        qusql_parse::Type::Json => BaseType::String.into(),
        qusql_parse::Type::Jsonb => BaseType::String.into(),
        qusql_parse::Type::Bit(_, _) => BaseType::Bytes.into(),
        qusql_parse::Type::VarBit(_) => BaseType::Bytes.into(),
        qusql_parse::Type::Bytea => BaseType::Bytes.into(),
        qusql_parse::Type::Named(qname) => {
            // Look up user-defined types (e.g. enums created with CREATE TYPE ... AS ENUM).
            if let Some(types) = types {
                let key = match qname.prefix.as_slice() {
                    [] => QualifiedIdentifier::Unqualified(qname.identifier.clone()),
                    [(schema, _)] if is_postgresql => {
                        QualifiedIdentifier::Qualified(schema.clone(), qname.identifier.clone())
                    }
                    _ => return BaseType::String.into(),
                };
                let search_path: &[&str] = if is_postgresql { &["public"] } else { &[] };
                match lookup_name(types, &key, search_path) {
                    Some(TypeDef::Enum { values, .. }) => Type::Enum(values.clone()),
                    _ => BaseType::String.into(),
                }
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
        qusql_parse::Type::Array(inner, _) => Type::Array(Box::new(type_kind_from_parse(
            *inner,
            unsigned,
            is_sqlite,
            is_postgresql,
            types,
            issues,
        ))),
        qusql_parse::Type::Table(ref span, _) => {
            issues.err("TABLE type is not yet supported", span);
            BaseType::String.into()
        }
        qusql_parse::Type::Serial => Type::I32,
        qusql_parse::Type::SmallSerial => Type::I16,
        qusql_parse::Type::BigSerial => Type::I64,
        qusql_parse::Type::Money => BaseType::Float.into(),
        qusql_parse::Type::Timetz(_) => BaseType::Time.into(),
        qusql_parse::Type::Interval(_) => BaseType::TimeInterval.into(),
        qusql_parse::Type::TsQuery => BaseType::String.into(),
        qusql_parse::Type::TsVector => BaseType::String.into(),
        qusql_parse::Type::Uuid => BaseType::Uuid.into(),
        qusql_parse::Type::Xml => BaseType::String.into(),
        qusql_parse::Type::Range(sub) | qusql_parse::Type::MultiRange(sub) => {
            use qusql_parse::RangeSubtype;
            let elem = match sub {
                RangeSubtype::Int4 => BaseType::Integer,
                RangeSubtype::Int8 => BaseType::Integer,
                RangeSubtype::Num => BaseType::Float,
                RangeSubtype::Ts => BaseType::DateTime,
                RangeSubtype::Tstz => BaseType::TimeStamp,
                RangeSubtype::Date => BaseType::Date,
            };
            Type::Range(elem)
        }
        qusql_parse::Type::Point
        | qusql_parse::Type::Line
        | qusql_parse::Type::Lseg
        | qusql_parse::Type::Box
        | qusql_parse::Type::Path
        | qusql_parse::Type::Polygon
        | qusql_parse::Type::Circle => Type::Geometry,
    }
}

pub(crate) fn parse_column<'a>(
    data_type: DataType<'a>,
    identifier: Identifier<'a>,
    _issues: &mut Issues<'a>,
    options: Option<&TypeOptions>,
    types: Option<&BTreeMap<QualifiedIdentifier<'a>, TypeDef<'a>>>,
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
    let is_postgresql = options
        .map(|v| v.parse_options.get_dialect().is_postgresql())
        .unwrap_or_default();
    for p in data_type.properties {
        match p {
            DataTypeProperty::Signed(_) => unsigned = false,
            DataTypeProperty::Unsigned(_) => unsigned = true,
            DataTypeProperty::Null(_) => not_null = false,
            DataTypeProperty::NotNull(_) => not_null = true,
            DataTypeProperty::AutoIncrement(_) => auto_increment = true,
            DataTypeProperty::As((_, e)) => as_ = Some(e),
            DataTypeProperty::Default(_) => default = true,
            DataTypeProperty::GeneratedAlways(_) => generated = true,
            DataTypeProperty::GeneratedAlwaysAsExpr { .. } => generated = true,
            DataTypeProperty::PrimaryKey(_) => primary_key = true,
            _ => {}
        }
    }
    // SQLite INTEGER PRIMARY KEY is an alias for rowid (auto-increment)
    if is_sqlite && primary_key && matches!(data_type.type_, qusql_parse::Type::Integer(_)) {
        auto_increment = true;
    }
    // PRIMARY KEY implies NOT NULL
    if primary_key {
        not_null = true;
    }
    let type_ = type_kind_from_parse(
        data_type.type_,
        unsigned,
        is_sqlite,
        is_postgresql,
        types,
        _issues,
    );
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
type Row<'a> = Rc<Vec<(&'a str, SqlValue<'a>)>>;

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
    /// The return value of the most recently executed RETURN statement.
    /// Set by the Return arm in process_statement; consumed by eval_function_expr.
    return_value: Option<SqlValue<'a>>,
    /// Current PostgreSQL search path for unqualified name resolution.
    /// Updated by `SET [LOCAL] search_path TO ...`.
    /// For MySQL/MariaDB/SQLite this is always empty.
    search_path: Vec<Identifier<'a>>,
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
            return_value: None,
            search_path: if options.parse_options.get_dialect().is_postgresql() {
                alloc::vec![Identifier::new("public", 0..0)]
            } else {
                Vec::new()
            },
        }
    }

    /// Returns the current search path as a slice of `&str` for use with `lookup_name`.
    fn search_path_strs(&self) -> Vec<&'a str> {
        self.search_path.iter().map(|id| id.value).collect()
    }

    /// Build a `QualifiedIdentifier` key using the current dialect convention.
    /// PostgreSQL: unqualified names get the `"public"` schema prefix.
    /// MySQL/MariaDB/SQLite: all names are `Unqualified`.
    fn make_table_key(
        &self,
        schema: Option<Identifier<'a>>,
        table: Identifier<'a>,
    ) -> QualifiedIdentifier<'a> {
        if self.options.parse_options.get_dialect().is_postgresql() {
            QualifiedIdentifier::Qualified(
                schema.unwrap_or_else(|| Identifier::new("public", 0..0)),
                table,
            )
        } else {
            QualifiedIdentifier::Unqualified(table)
        }
    }

    /// Parse a `QualifiedName` into a `QualifiedIdentifier` key, respecting dialect rules.
    /// Returns `None` and emits an error if the qualification is invalid for the dialect.
    fn parse_qname(&mut self, qname: &QualifiedName<'a>) -> Option<QualifiedIdentifier<'a>> {
        let is_pg = self.options.parse_options.get_dialect().is_postgresql();
        match qname.prefix.as_slice() {
            [] => Some(self.make_table_key(None, qname.identifier.clone())),
            [(schema, _)] if is_pg => {
                Some(self.make_table_key(Some(schema.clone()), qname.identifier.clone()))
            }
            _ => {
                let msg = if is_pg {
                    "Expected at most schema.table qualified name"
                } else {
                    "Schema-qualified names are not supported in MySQL"
                };
                self.issues.err(msg, &qname.prefix.opt_span().unwrap());
                None
            }
        }
    }

    /// Like [`parse_qname`] but keeps unqualified names as `Unqualified` rather than
    /// pre-qualifying them with `"public"`.  Use this for lookups where `lookup_name`
    /// + `search_path_strs()` should resolve the schema, rather than hardcoding one.
    fn parse_qname_for_lookup(
        &mut self,
        qname: &QualifiedName<'a>,
    ) -> Option<QualifiedIdentifier<'a>> {
        let is_pg = self.options.parse_options.get_dialect().is_postgresql();
        match qname.prefix.as_slice() {
            [] => Some(QualifiedIdentifier::Unqualified(qname.identifier.clone())),
            [(schema, _)] if is_pg => Some(QualifiedIdentifier::Qualified(
                schema.clone(),
                qname.identifier.clone(),
            )),
            _ => {
                let msg = if is_pg {
                    "Expected at most schema.table qualified name"
                } else {
                    "Schema-qualified names are not supported in MySQL"
                };
                self.issues.err(msg, &qname.prefix.opt_span().unwrap());
                None
            }
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
            qusql_parse::Statement::CreateSchema(s) => {
                self.process_create_schema(*s);
                Ok(())
            }
            qusql_parse::Statement::DropDatabase(s) => {
                // In PostgreSQL, DROP SCHEMA and DROP DATABASE both parse to DropDatabase.
                // We implement DROP SCHEMA; DROP DATABASE itself is left as an error.
                if self.options.parse_options.get_dialect().is_postgresql() {
                    self.process_drop_schema(*s);
                } else {
                    self.issues.err("not implemented", &s);
                }
                Ok(())
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
            qusql_parse::Statement::InsertReplace(i) => self.process_insert(*i),
            // Transaction control: we assume all transactions commit.
            qusql_parse::Statement::Commit(_) => Ok(()),
            qusql_parse::Statement::Begin(_) => Ok(()),
            // Statements with no schema effect.
            qusql_parse::Statement::Grant(_) => Ok(()),
            qusql_parse::Statement::CommentOn(_) => Ok(()),
            qusql_parse::Statement::Analyze(_) => Ok(()),
            qusql_parse::Statement::CreateSequence(s) => {
                self.process_create_sequence(*s);
                Ok(())
            }
            qusql_parse::Statement::DropSequence(s) => {
                self.process_drop_sequence(*s);
                Ok(())
            }
            // Variable / cursor plumbing — update search_path if relevant, otherwise ignore.
            qusql_parse::Statement::Set(s) => {
                self.process_set(*s);
                Ok(())
            }
            // Assign and Perform may call known functions with schema effects.
            qusql_parse::Statement::Assign(a) => {
                for se in a.value.select_exprs {
                    self.process_expression(se.expr)?;
                }
                Ok(())
            }
            qusql_parse::Statement::Perform(p) => self.process_expression(p.expr),
            qusql_parse::Statement::DeclareVariable(d) => {
                // Evaluate the DEFAULT expression for its side effects (may call user-defined functions).
                if let Some((_, select)) = d.default {
                    self.eval_condition(&select)?;
                }
                Ok(())
            }
            // DeclareHandler bodies only run on error; we model the happy path, so skip them.
            qusql_parse::Statement::DeclareHandler(_) => Ok(()),
            qusql_parse::Statement::ExecuteFunction(s) => {
                self.issues.err("not implemented", &s);
                Err(())
            }
            // RAISE EXCEPTION aborts execution; anything else is a log/notice with no schema effect.
            qusql_parse::Statement::Raise(r) => {
                if matches!(r.level, Some(qusql_parse::RaiseLevel::Exception(_))) {
                    Err(())
                } else {
                    Ok(())
                }
            }
            qusql_parse::Statement::Return(r) => {
                self.return_value = self.eval_expr(&r.expr).ok();
                Err(())
            }
            qusql_parse::Statement::PlpgsqlExecute(e) => self.process_plpgsql_execute(*e),
            qusql_parse::Statement::Update(u) => self.process_update(*u),
            qusql_parse::Statement::Delete(d) => self.process_delete(*d),
            qusql_parse::Statement::AlterType(a) => {
                self.process_alter_type(*a);
                Ok(())
            }
            qusql_parse::Statement::TruncateTable(t) => {
                self.process_truncate_table(*t);
                Ok(())
            }
            qusql_parse::Statement::RenameTable(r) => {
                self.process_rename_table(*r);
                Ok(())
            }
            qusql_parse::Statement::Call(c) => self.process_call(*c),
            s => {
                self.issues.err(
                    alloc::format!("Unsupported statement {s:?} in schema definition"),
                    &s,
                );
                Err(())
            }
        }
    }

    fn process_create_sequence(&mut self, s: qusql_parse::CreateSequence<'a>) {
        let Some(key) = self.parse_qname(&s.name) else {
            return;
        };
        if s.if_not_exists.is_some() {
            let sp = self.search_path_strs();
            if lookup_name(&self.schemas.sequences, &key, &sp).is_some() {
                return;
            }
        }
        self.schemas.sequences.insert(key, ());
    }

    fn process_drop_sequence(&mut self, s: qusql_parse::DropSequence<'a>) {
        for name in s.sequences {
            let Some(key) = self.parse_qname(&name) else {
                continue;
            };
            if s.if_exists.is_none() {
                let sp = self.search_path_strs();
                if lookup_name(&self.schemas.sequences, &key, &sp).is_none() {
                    self.issues.err(
                        alloc::format!("Unknown sequence `{}`", name.identifier.value),
                        &name.identifier,
                    );
                }
            }
            self.schemas.sequences.remove(&key);
        }
    }

    fn process_create_table(&mut self, t: qusql_parse::CreateTable<'a>) {
        let mut replace = false;
        let Some(key) = self.parse_qname(&t.identifier) else {
            return;
        };
        let id = key.table_name().clone();
        let identifier_span = id.span.clone();
        let mut schema = Schema {
            view: false,
            identifier_span,
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
                                schema: key.schema_name().cloned(),
                                table: None,
                                index: name.clone(),
                            }
                        } else {
                            IndexKey {
                                schema: None,
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
                    let source_key = match self.parse_qname_for_lookup(&source_table) {
                        Some(k) => k,
                        None => continue,
                    };
                    let sp = self.search_path_strs();
                    if let Some(src) = lookup_name(&self.schemas.schemas, &source_key, &sp) {
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
        match self.schemas.schemas.entry(key.clone()) {
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
                outer_reference_types: Vec::new(),
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
                let Some(name) = column.name else {
                    self.issues.err(
                        "View column has no name; add an alias with AS",
                        &v.select.span(),
                    );
                    continue;
                };
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
        let view_key = match self.parse_qname(&v.name) {
            Some(k) => k,
            None => return,
        };
        match self.schemas.schemas.entry(view_key) {
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
        let Some(key) = self.parse_qname(&f.name) else {
            return;
        };
        let def = FunctionDef {
            name: f.name.identifier.clone(),
            params: f.params,
            return_type: f.return_type,
            span: f.create_span.join_span(&f.function_span),
            body,
        };
        match self.schemas.functions.entry(key) {
            alloc::collections::btree_map::Entry::Occupied(mut e) => {
                if replace {
                    e.insert(def);
                } else if f.if_not_exists.is_none() {
                    self.issues
                        .err("Function already defined", &f.name.identifier)
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
        let key = self.make_table_key(None, p.name.clone());
        let body = p.body.map(|stmt| match stmt {
            qusql_parse::Statement::Block(b) => b.statements,
            other => alloc::vec![other],
        });
        let def = ProcedureDef {
            name: p.name.clone(),
            params: p.params,
            span: p.create_span.join_span(&p.procedure_span),
            body,
        };
        match self.schemas.procedures.entry(key) {
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

    fn process_call(&mut self, c: qusql_parse::Call<'a>) -> Result<(), ()> {
        let Some(key) = self.parse_qname_for_lookup(&c.name) else {
            return Ok(());
        };
        let search_path: Vec<&str> = self.search_path_strs();
        // Look up the procedure and clone its body statements.
        let body =
            lookup_name(&self.schemas.procedures, &key, &search_path).and_then(|p| p.body.clone());
        let Some(statements) = body else {
            // Unknown or body-less procedure — no schema effect.
            return Ok(());
        };
        // Ignore Ok/Err from the body: a Err() just means "stopped early" (RAISE EXCEPTION, RETURN, etc.)
        let _ = self.process_statements(statements);
        Ok(())
    }

    fn process_set(&mut self, s: qusql_parse::Set<'a>) {
        if !self.options.parse_options.get_dialect().is_postgresql() {
            return;
        }
        for (var, exprs) in s.values {
            let qusql_parse::SetVariable::Named(qname) = var else {
                continue;
            };
            // Match `search_path` (possibly qualified as `pg_catalog.search_path` etc.)
            if !qname.identifier.value.eq_ignore_ascii_case("search_path") {
                continue;
            }
            // Each expression is either a bare identifier (schema name) or a string literal.
            let mut new_path: Vec<Identifier<'a>> = Vec::new();
            for expr in exprs {
                match expr {
                    Expression::Identifier(id) => {
                        if let Some(part) = id.parts.last()
                            && let qusql_parse::IdentifierPart::Name(name) = part
                        {
                            new_path.push(name.clone());
                        }
                    }
                    Expression::String(s) => {
                        // quoted schema name — wrap it as a synthetic Identifier
                        if let alloc::borrow::Cow::Borrowed(b) = &s.value {
                            new_path.push(Identifier::new(b, 0..0));
                        } else {
                            // escaped/owned string — skip (uncommon in search_path)
                        }
                    }
                    _ => {}
                }
            }
            self.search_path = new_path;
        }
    }

    fn process_create_index(&mut self, ci: qusql_parse::CreateIndex<'a>) {
        let Some(lookup_key) = self.parse_qname_for_lookup(&ci.table_name) else {
            return;
        };
        let t = lookup_key.table_name().clone();
        let sp = self.search_path_strs();
        // Look up the table to validate columns and get the resolved schema for the IndexKey.
        let resolved_schema = match lookup_name_key(&self.schemas.schemas, &lookup_key, &sp) {
            Some((resolved_key, table)) => {
                let schema = resolved_key.schema_name().cloned();
                for col in &ci.column_names {
                    if let qusql_parse::IndexColExpr::Column(name) = &col.expr
                        && table.get_column(name.value).is_none()
                    {
                        self.issues
                            .err("No such column in table", col)
                            .frag("Table defined here", &table.identifier_span);
                    }
                }
                schema
            }
            None => {
                self.issues.err("No such table", &ci.table_name);
                None
            }
        };
        let index_name = match &ci.index_name {
            Some(name) => name.clone(),
            None => return,
        };
        let ident = if self.options.parse_options.get_dialect().is_postgresql() {
            IndexKey {
                schema: resolved_schema,
                table: None,
                index: index_name.clone(),
            }
        } else {
            IndexKey {
                schema: None,
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
        let Some(key) = self.parse_qname(&s.name) else {
            return;
        };
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
        match self.schemas.types.entry(key) {
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
            let Some(key) = self.parse_qname(&name) else {
                continue;
            };
            if self.schemas.types.remove(&key).is_none() && if_exists.is_none() {
                self.issues.err("Type not found", &name);
            }
        }
    }

    fn process_alter_table(&mut self, a: qusql_parse::AlterTable<'a>) {
        let key = match self.parse_qname(&a.table) {
            Some(k) => k,
            None => return,
        };
        let e = match self.schemas.schemas.entry(key) {
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
    types: &BTreeMap<QualifiedIdentifier<'a>, TypeDef<'a>>,
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
                    let (schema, _) = resolve_table_name(issues, options, table_ref);
                    IndexKey {
                        schema: Some(
                            schema
                                .cloned()
                                .unwrap_or_else(|| Identifier::new("public", 0..0)),
                        ),
                        table: None,
                        index: name.clone(),
                    }
                } else {
                    IndexKey {
                        schema: None,
                        table: Some(resolve_table_name(issues, options, table_ref).1.clone()),
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
            if_not_exists_span,
            ..
        }) => {
            if e.get_column(identifier.value).is_some() {
                if if_not_exists_span.is_none() {
                    issues
                        .err("Column already exists in table", &identifier)
                        .frag("Table defined here", &e.identifier_span);
                }
            } else {
                e.columns.push(parse_column(
                    data_type,
                    identifier,
                    issues,
                    Some(options),
                    Some(types),
                ));
            }
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
                let (schema, _) = resolve_table_name(issues, options, table_ref);
                IndexKey {
                    schema: Some(
                        schema
                            .cloned()
                            .unwrap_or_else(|| Identifier::new("public", 0..0)),
                    ),
                    table: None,
                    index: drop_idx.name.clone(),
                }
            } else {
                IndexKey {
                    schema: None,
                    table: Some(resolve_table_name(issues, options, table_ref).1.clone()),
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
            let (schema_ref, table_id_ref) = resolve_table_name(issues, options, table_ref);
            let schema_id = schema_ref
                .cloned()
                .unwrap_or_else(|| Identifier::new("public", 0..0));
            let table_id = table_id_ref.clone();
            let old_key = if is_postgresql {
                IndexKey {
                    schema: Some(schema_id.clone()),
                    table: None,
                    index: old_index_name.clone(),
                }
            } else {
                IndexKey {
                    schema: None,
                    table: Some(table_id.clone()),
                    index: old_index_name.clone(),
                }
            };
            match indices.remove(&old_key) {
                Some(span) => {
                    let new_key = if is_postgresql {
                        IndexKey {
                            schema: Some(schema_id),
                            table: None,
                            index: new_index_name,
                        }
                    } else {
                        IndexKey {
                            schema: None,
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
            let key = match self.parse_qname(&i) {
                Some(k) => k,
                None => continue,
            };
            match self.schemas.schemas.entry(key) {
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
            let key = match self.parse_qname(&i) {
                Some(k) => k,
                None => continue,
            };
            match self.schemas.schemas.entry(key) {
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
            let Some(key) = self.parse_qname(func_name) else {
                continue;
            };
            if self.schemas.functions.remove(&key).is_none() && f.if_exists.is_none() {
                self.issues.err(
                    "A function with this name does not exist to drop",
                    func_name,
                );
            }
        }
    }

    fn process_drop_procedure(&mut self, p: qusql_parse::DropProcedure<'a>) {
        let Some(key) = self.parse_qname(&p.procedure) else {
            return;
        };
        if self.schemas.procedures.remove(&key).is_none() && p.if_exists.is_none() {
            self.issues.err(
                "A procedure with this name does not exist to drop",
                &p.procedure,
            );
        }
    }

    fn process_drop_index(&mut self, ci: qusql_parse::DropIndex<'a>) {
        let is_pg = self.options.parse_options.get_dialect().is_postgresql();
        let key = if is_pg {
            let schema = ci
                .on
                .as_ref()
                .and_then(|(_, t)| t.prefix.first().map(|(s, _)| s.clone()))
                .unwrap_or_else(|| Identifier::new("public", 0..0));
            IndexKey {
                schema: Some(schema),
                table: None,
                index: ci.index_name.clone(),
            }
        } else {
            IndexKey {
                schema: None,
                table: ci.on.as_ref().map(|(_, t)| t.identifier.clone()),
                index: ci.index_name.clone(),
            }
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
            if self.eval_condition(&cond.search_condition)? {
                return self.process_statements(cond.then);
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
        self.eval_expr(&expr).map(|_| ())
    }

    fn process_update(&mut self, u: qusql_parse::Update<'a>) -> Result<(), ()> {
        // We cannot evaluate SET expressions. Error out if any target table has
        // tracked rows whose state would be changed; if there are no tracked
        // rows the update has no effect on our model.
        let span = u.update_span;
        for tref in u.tables {
            if let qusql_parse::TableReference::Table { identifier, .. } = tref
                && identifier.prefix.is_empty()
                && self
                    .rows
                    .get(identifier.identifier.value)
                    .is_some_and(|r| !r.is_empty())
            {
                self.issues.err(
                    "UPDATE on a table with tracked rows is not supported in schema evaluator",
                    &span,
                );
                return Err(());
            }
        }
        Ok(())
    }

    fn process_delete(&mut self, d: qusql_parse::Delete<'a>) -> Result<(), ()> {
        let qusql_parse::Delete {
            tables,
            using,
            where_,
            order_by,
            limit,
            ..
        } = d;

        // USING / ORDER BY / LIMIT are not supported: error if any target table
        // has tracked rows that would be affected.
        let has_unsupported = !using.is_empty() || order_by.is_some() || limit.is_some();
        if has_unsupported {
            for table in &tables {
                if table.prefix.is_empty()
                    && self
                        .rows
                        .get(table.identifier.value)
                        .is_some_and(|r| !r.is_empty())
                {
                    self.issues.err(
                        "DELETE with USING/ORDER BY/LIMIT on a table with tracked rows \
                         is not supported in schema evaluator",
                        table,
                    );
                    return Err(());
                }
            }
        }

        if let Some((where_expr, _)) = where_ {
            // Evaluate the WHERE for each tracked row; keep rows that do NOT match.
            for table in &tables {
                if table.prefix.is_empty() {
                    let name = table.identifier.value;
                    let Some(source_rows) = self.rows.get(name) else {
                        continue;
                    };
                    let source_rows = source_rows.clone();
                    let mut new_rows = Vec::new();
                    for row in source_rows {
                        let saved = self.current_row.replace(row.clone());
                        let matches = self.eval_expr(&where_expr).map(|v| v.is_truthy());
                        self.current_row = saved;
                        match matches {
                            Ok(true) => {} // row is deleted
                            Ok(false) => new_rows.push(row),
                            Err(()) => return Err(()),
                        }
                    }
                    self.rows.insert(name, new_rows);
                }
            }
        } else {
            // No WHERE - all rows in every target table are deleted.
            for table in tables {
                if table.prefix.is_empty() {
                    self.rows.remove(table.identifier.value);
                }
            }
        }
        Ok(())
    }

    fn process_alter_type(&mut self, a: qusql_parse::AlterType<'a>) {
        let Some(key) = self.parse_qname(&a.name) else {
            return;
        };
        match a.action {
            qusql_parse::AlterTypeAction::AddValue {
                if_not_exists_span,
                new_enum_value,
                ..
            } => {
                let Some(TypeDef::Enum { values, .. }) = self.schemas.types.get_mut(&key) else {
                    self.issues.err("Type not found", &a.name);
                    return;
                };
                let new_val: Cow<'a, str> = new_enum_value.value;
                if values.contains(&new_val) {
                    if if_not_exists_span.is_none() {
                        self.issues.err("Enum value already exists", &a.name);
                    }
                    // IF NOT EXISTS: silently skip
                } else {
                    Arc::make_mut(values).push(new_val);
                }
            }
            qusql_parse::AlterTypeAction::RenameTo { new_name, .. } => {
                let new_key = self.make_table_key(key.schema_name().cloned(), new_name);
                if let Some(typedef) = self.schemas.types.remove(&key) {
                    self.schemas.types.insert(new_key, typedef);
                } else {
                    self.issues.err("Type not found", &a.name);
                }
            }
            qusql_parse::AlterTypeAction::RenameValue {
                existing_enum_value,
                new_enum_value,
                ..
            } => {
                let Some(TypeDef::Enum { values, .. }) = self.schemas.types.get_mut(&key) else {
                    self.issues.err("Type not found", &a.name);
                    return;
                };
                let old_val: Cow<'a, str> = existing_enum_value.value;
                let new_val: Cow<'a, str> = new_enum_value.value;
                if let Some(entry) = Arc::make_mut(values).iter_mut().find(|v| **v == old_val) {
                    *entry = new_val;
                } else {
                    self.issues.err("Enum value not found", &a.name);
                }
            }
            // Other ALTER TYPE actions (OWNER TO, SET SCHEMA, etc.) have no effect on
            // the parts of the schema we track.
            _ => {}
        }
    }

    fn process_truncate_table(&mut self, t: qusql_parse::TruncateTable<'a>) {
        for spec in t.tables {
            // Truncate only clears tracked row state; just need the bare table name.
            let name = unqualified_name(self.issues, &spec.table_name);
            self.rows.remove(name.value);
        }
    }

    fn process_create_schema(&mut self, s: qusql_parse::CreateSchema<'a>) {
        if !self.options.parse_options.get_dialect().is_postgresql() {
            self.issues.err(
                "CREATE SCHEMA is not supported in MySQL; use CREATE DATABASE",
                &s,
            );
            return;
        }
        let Some(name) = s.name else {
            // AUTHORIZATION-only CREATE SCHEMA — nothing to register
            return;
        };
        match self.schemas.schema_names.get(&name) {
            Some(_) if s.if_not_exists.is_none() => {
                self.issues.err("Schema already exists", &name);
            }
            _ => {
                self.schemas.schema_names.insert(name);
            }
        }
    }

    fn process_drop_schema(&mut self, s: qusql_parse::DropDatabase<'a>) {
        if !self.schemas.schema_names.remove(&s.database) && s.if_exists.is_none() {
            self.issues.err("Schema does not exist", &s.database);
        }
        // Remove all tables/views registered under this schema.
        self.schemas
            .schemas
            .retain(|k, _| k.schema_name().map(|n| n == &s.database) != Some(true));
        // Remove all functions in this schema.
        self.schemas
            .functions
            .retain(|k, _| k.schema_name().map(|n| n == &s.database) != Some(true));
        // Remove all procedures in this schema.
        self.schemas
            .procedures
            .retain(|k, _| k.schema_name().map(|n| n == &s.database) != Some(true));
        // Remove all types in this schema.
        self.schemas
            .types
            .retain(|k, _| k.schema_name().map(|n| n == &s.database) != Some(true));
        // Remove all indices associated with this schema.
        self.schemas
            .indices
            .retain(|k, _| k.schema.as_ref() != Some(&s.database));
    }

    fn process_rename_table(&mut self, r: qusql_parse::RenameTable<'a>) {
        for pair in r.table_to_tables {
            let old_key = match self.parse_qname(&pair.table) {
                Some(k) => k,
                None => continue,
            };
            let new_key = match self.parse_qname(&pair.new_table) {
                Some(k) => k,
                None => continue,
            };
            let old_name = old_key.table_name().value;
            let new_name = new_key.table_name().value;
            // Rename in schemas map.
            if let Some(schema) = self.schemas.schemas.remove(&old_key) {
                self.schemas.schemas.insert(new_key, schema);
            } else {
                self.issues.err("Table not found", &pair.table);
            }
            // Rename tracked rows if present.
            if let Some(rows) = self.rows.remove(old_name) {
                self.rows.insert(new_name, rows);
            }
        }
    }

    fn process_insert(&mut self, i: qusql_parse::InsertReplace<'a>) -> Result<(), ()> {
        // Only unqualified table names are tracked.
        let table_name = match i.table.prefix.as_slice() {
            [] => i.table.identifier.value,
            _ => return Ok(()),
        };
        let col_names: Vec<&'a str> = i.columns.iter().map(|c| c.value).collect();

        if let Some(set) = i.set {
            // INSERT ... SET col = expr, ...: evaluate as a single-row VALUES insert.
            let mut row: Vec<(&'a str, SqlValue<'a>)> = Vec::new();
            for pair in set.pairs {
                if let Ok(val) = self.eval_expr(&pair.value) {
                    row.push((pair.column.value, val));
                }
            }
            self.rows.entry(table_name).or_default().push(Rc::new(row));
            return Ok(());
        }

        if let Some((_, value_rows)) = i.values {
            // INSERT ... VALUES (...)
            for row_exprs in value_rows {
                let mut row: Vec<(&'a str, SqlValue<'a>)> = Vec::new();
                for (col, expr) in col_names.iter().zip(row_exprs.iter()) {
                    if let Ok(val) = self.eval_expr(expr) {
                        row.push((col, val));
                    }
                }
                self.rows.entry(table_name).or_default().push(Rc::new(row));
            }
            return Ok(());
        }

        let Some(select_stmt) = i.select else {
            return Ok(());
        };
        // Clone select expressions before eval borrows `self`.
        // Only available for a plain SELECT; compound queries (UNION etc.) produce no
        // named expressions to project, so we track rows without column values.
        let select_exprs: Vec<_> = if let qusql_parse::Statement::Select(s) = &select_stmt {
            s.select_exprs.iter().map(|se| se.expr.clone()).collect()
        } else {
            Vec::new()
        };
        let source_rows = self.eval_statement_rows(&select_stmt)?;
        for source_row in source_rows {
            let saved_row = self.current_row.replace(source_row);
            let mut row: Vec<(&'a str, SqlValue<'a>)> = Vec::new();
            for (col, expr) in col_names.iter().zip(select_exprs.iter()) {
                if let Ok(val) = self.eval_expr(expr) {
                    row.push((col, val));
                }
            }
            self.current_row = saved_row;
            self.rows.entry(table_name).or_default().push(Rc::new(row));
        }
        Ok(())
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
                            self.current_row.as_ref().and_then(|r| {
                                r.iter()
                                    .find(|(k, _)| *k == name.value)
                                    .map(|(_, v)| v.clone())
                            })
                        })
                        .ok_or(())
                } else {
                    Err(())
                }
            }
            Expression::Exists(e) => Ok(SqlValue::Bool(self.eval_exists(&e.subquery)?)),
            Expression::Unary(u) => match &u.op {
                qusql_parse::UnaryOperator::Not(_) | qusql_parse::UnaryOperator::LogicalNot(_) => {
                    Ok(SqlValue::Bool(!self.eval_expr(&u.operand)?.is_truthy()))
                }
                qusql_parse::UnaryOperator::Minus(_) => match self.eval_expr(&u.operand)? {
                    SqlValue::Integer(i) => Ok(SqlValue::Integer(-i)),
                    _ => Err(()),
                },
                _ => Err(()),
            },
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
        Ok(!self.eval_statement_rows(stmt)?.is_empty())
    }

    /// Resolve the rows for a SELECT's FROM clause.
    /// Returns empty if there is no FROM clause.
    /// Errors (with a message) for:
    ///   - joins or non-table FROM references (e.g. subqueries in FROM)
    ///   - qualified table names (e.g. `information_schema.columns`)
    ///   - unqualified table names not known to the schema evaluator
    fn resolve_from_rows(&mut self, s: &qusql_parse::Select<'a>) -> Result<Vec<Row<'a>>, ()> {
        use qusql_parse::TableReference;
        let Some(refs) = s.table_references.as_deref() else {
            return Ok(Vec::new());
        };
        let [TableReference::Table { identifier, .. }] = refs else {
            self.issues.err(
                "FROM clause with joins or subqueries is not supported in schema evaluator",
                &refs[0],
            );
            return Err(());
        };
        if !identifier.prefix.is_empty() {
            // Synthesize information_schema.columns from the current schema state.
            if identifier.prefix.len() == 1
                && identifier.prefix[0]
                    .0
                    .value
                    .eq_ignore_ascii_case("information_schema")
                && identifier.identifier.value.eq_ignore_ascii_case("columns")
            {
                let rows = self
                    .schemas
                    .schemas
                    .iter()
                    .flat_map(|(key, schema)| {
                        schema.columns.iter().map(move |col| {
                            Rc::new(alloc::vec![
                                ("table_name", SqlValue::SourceText(key.table_name().value)),
                                ("column_name", SqlValue::SourceText(col.identifier.value)),
                            ])
                        })
                    })
                    .collect();
                return Ok(rows);
            }
            // Handle a schema-qualified table reference, e.g. `migrations.schema_revisions`.
            if identifier.prefix.len() == 1 {
                let schema_name = identifier.prefix[0].0.value;
                let table_name = identifier.identifier.value;
                let qualified_key = QualifiedIdentifier::Qualified(
                    Identifier::new(schema_name, 0..0),
                    Identifier::new(table_name, 0..0),
                );
                let known = lookup_name(&self.schemas.schemas, &qualified_key, &[]).is_some();
                if !known {
                    self.issues.err(
                        alloc::format!(
                            "Unknown table `{schema_name}.{table_name}` referenced in schema evaluator"
                        ),
                        identifier,
                    );
                    return Err(());
                }
                return Ok(self.rows.get(table_name).cloned().unwrap_or_default());
            }
            self.issues.err(
                "Qualified table name in FROM clause is not supported in schema evaluator",
                identifier,
            );
            return Err(());
        }
        let name = identifier.identifier.value;
        let table_key = QualifiedIdentifier::Unqualified(Identifier::new(name, 0..0));
        let sp = self.search_path_strs();
        let known = self.rows.contains_key(name)
            || lookup_name(&self.schemas.schemas, &table_key, &sp).is_some();
        if !known {
            self.issues.err(
                alloc::format!("Unknown table `{name}` referenced in schema evaluator"),
                &identifier.identifier,
            );
            return Err(());
        }
        Ok(self.rows.get(name).cloned().unwrap_or_default())
    }

    /// Evaluate any SELECT-like statement (plain SELECT or UNION/INTERSECT/EXCEPT compound
    /// query) to a list of result rows.
    fn eval_statement_rows(
        &mut self,
        stmt: &qusql_parse::Statement<'a>,
    ) -> Result<Vec<Row<'a>>, ()> {
        match stmt {
            qusql_parse::Statement::Select(s) => self.eval_select_matching_rows(s),
            qusql_parse::Statement::CompoundQuery(cq) => self.eval_compound_query_rows(cq),
            _ => {
                self.issues
                    .err("Unsupported statement kind in INSERT ... SELECT", stmt);
                Err(())
            }
        }
    }

    /// Evaluate a UNION / INTERSECT / EXCEPT compound query to rows.
    /// Only UNION (ALL or deduplicated) is supported; INTERSECT and EXCEPT error out.
    fn eval_compound_query_rows(
        &mut self,
        cq: &qusql_parse::CompoundQuery<'a>,
    ) -> Result<Vec<Row<'a>>, ()> {
        use qusql_parse::CompoundOperator;
        let mut result = self.eval_statement_rows(&cq.left)?;
        for branch in &cq.with {
            match branch.operator {
                CompoundOperator::Union => {
                    let branch_rows = self.eval_statement_rows(&branch.statement)?;
                    result.extend(branch_rows);
                }
                CompoundOperator::Intersect | CompoundOperator::Except => {
                    self.issues.err(
                        "INTERSECT / EXCEPT is not supported in schema evaluator",
                        &branch.operator_span,
                    );
                    return Err(());
                }
            }
        }
        Ok(result)
    }

    /// Return the rows from the single table in a SELECT that satisfy the WHERE clause.
    fn eval_select_matching_rows(
        &mut self,
        s: &qusql_parse::Select<'a>,
    ) -> Result<Vec<Row<'a>>, ()> {
        let source_rows = self.resolve_from_rows(s)?;
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

    fn eval_function_expr(
        &mut self,
        f: &qusql_parse::FunctionCallExpression<'a>,
    ) -> Result<SqlValue<'a>, ()> {
        use qusql_parse::Function;
        match &f.function {
            Function::Other(parts) if !parts.is_empty() => {
                let fn_ident = parts.last().unwrap().clone();
                let schema_ident = (parts.len() >= 2).then(|| parts[parts.len() - 2].clone());
                let key = match schema_ident {
                    Some(s) => QualifiedIdentifier::Qualified(s, fn_ident.clone()),
                    None => QualifiedIdentifier::Unqualified(fn_ident.clone()),
                };
                let is_pg = self.options.parse_options.get_dialect().is_postgresql();
                let search_path: Vec<&str> = self.search_path_strs();
                let func_info =
                    lookup_name(&self.schemas.functions, &key, &search_path).and_then(|func| {
                        func.body
                            .as_ref()
                            .map(|b| (func.params.clone(), b.statements.clone()))
                    });
                let Some((params, statements)) = func_info else {
                    self.issues.err(
                        alloc::format!(
                            "Unknown function or function has no evaluable body: {}",
                            parts
                                .iter()
                                .map(|p| p.value)
                                .collect::<alloc::vec::Vec<_>>()
                                .join(".")
                        ),
                        f,
                    );
                    return Err(());
                };
                let mut bindings = BTreeMap::new();
                for (param, arg) in params.iter().zip(f.args.iter()) {
                    let Some(name) = &param.name else { continue };
                    if let Ok(value) = self.eval_expr(arg) {
                        bindings.insert(name.value, value);
                    }
                }
                let old_bindings = core::mem::replace(&mut self.bindings, bindings);
                let old_return = self.return_value.take();
                let _ = self.process_statements(statements);
                let ret = self.return_value.take().unwrap_or(SqlValue::Null);
                self.return_value = old_return;
                self.bindings = old_bindings;
                Ok(ret)
            }
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
            Function::Nextval => {
                let arg = f.args.first().ok_or(())?;
                let seq_str = match self.eval_expr(arg)? {
                    SqlValue::SourceText(s) => alloc::borrow::Cow::Borrowed(s),
                    SqlValue::OwnedText(s) => alloc::borrow::Cow::Owned(s),
                    _ => {
                        self.issues.err("nextval: argument must be a string", f);
                        return Err(());
                    }
                };
                // Parse "schema.sequence" or "sequence" from the string argument and
                // check that the sequence was registered via CREATE SEQUENCE.
                let seq_name = seq_str.as_ref();
                let key = if let Some((schema, seq)) = seq_name.split_once('.') {
                    QualifiedIdentifier::Qualified(
                        Identifier::new(schema, 0..0),
                        Identifier::new(seq, 0..0),
                    )
                } else {
                    QualifiedIdentifier::Unqualified(Identifier::new(seq_name, 0..0))
                };
                let sp = self.search_path_strs();
                let found = lookup_name(&self.schemas.sequences, &key, &sp).is_some();
                if !found {
                    self.issues
                        .err(alloc::format!("nextval: unknown sequence `{seq_name}`"), f);
                    return Err(());
                }
                Ok(SqlValue::Integer(1))
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
            Function::Count => {
                let rows = core::mem::take(&mut self.current_table_rows);
                let is_star = matches!(
                    f.args.first(),
                    Some(Expression::Identifier(ie))
                        if matches!(ie.parts.as_slice(), [IdentifierPart::Star(_)])
                );
                let count = if f.args.is_empty() || is_star {
                    // COUNT(*) or COUNT() - count all rows
                    rows.len() as i64
                } else {
                    let col_expr = f.args.first().unwrap().clone();
                    let mut n = 0i64;
                    for r in &rows {
                        let saved = self.current_row.replace(r.clone());
                        let v = self.eval_expr(&col_expr);
                        self.current_row = saved;
                        if matches!(v, Ok(v) if v != SqlValue::Null) {
                            n += 1;
                        }
                    }
                    n
                };
                self.current_table_rows = rows;
                Ok(SqlValue::Integer(count))
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
        // Short-circuit AND/OR before evaluating rhs.
        match &b.op {
            BinaryOperator::And(_) => {
                if !lhs.is_truthy() {
                    return Ok(SqlValue::Bool(false));
                }
                return Ok(SqlValue::Bool(self.eval_expr(&b.rhs)?.is_truthy()));
            }
            BinaryOperator::Or(_) => {
                if lhs.is_truthy() {
                    return Ok(SqlValue::Bool(true));
                }
                return Ok(SqlValue::Bool(self.eval_expr(&b.rhs)?.is_truthy()));
            }
            _ => {}
        }
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
        let table_rows = self.resolve_from_rows(s)?;
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
/// Built-in table/view definitions that are automatically available in
/// PostgreSQL and PostGIS databases.  These are injected into every
/// `Schemas` produced by [`parse_schemas`] when the dialect is
/// [`SQLDialect::PostgreSQL`] or [`SQLDialect::PostGIS`].
const POSTGRESQL_BUILTIN_SQL: &str = "
CREATE TABLE spatial_ref_sys (
    srid INTEGER NOT NULL,
    auth_name VARCHAR(256),
    auth_srid INTEGER,
    srtext VARCHAR(2048),
    proj4text VARCHAR(2048)
);
CREATE VIEW geometry_columns AS (
    SELECT '' AS f_table_catalog, '' AS f_table_schema, '' AS f_table_name,
           '' AS f_geometry_column, 0 AS coord_dimension, 0 AS srid, '' AS type
);
CREATE VIEW geography_columns AS (
    SELECT '' AS f_table_catalog, '' AS f_table_schema, '' AS f_table_name,
           '' AS f_geography_column, 0 AS coord_dimension, 0 AS srid, '' AS type
);
";

pub fn parse_schemas<'a>(
    src: &'a str,
    issues: &mut Issues<'a>,
    options: &TypeOptions,
) -> Schemas<'a> {
    let statements = parse_statements(src, issues, &options.parse_options);

    let mut schemas = Schemas {
        schemas: Default::default(),
        schema_names: Default::default(),
        procedures: Default::default(),
        functions: Default::default(),
        indices: Default::default(),
        types: Default::default(),
        sequences: Default::default(),
    };

    SchemaCtx::new(&mut schemas, issues, src, options).process_top_level_statements(statements);

    let dummy_schemas = Schemas::default();

    let mut typer = crate::typer::Typer {
        schemas: &dummy_schemas,
        issues,
        reference_types: Vec::new(),
        outer_reference_types: Vec::new(),
        arg_types: Default::default(),
        options,
        with_schemas: Default::default(),
    };

    // Compute nullity of generated columns
    for (key, schema) in &mut schemas.schemas {
        if schema.columns.iter().all(|v| v.as_.is_none()) {
            continue;
        }
        typer.reference_types.clear();
        let mut columns = Vec::new();
        for c in &schema.columns {
            columns.push((c.identifier.clone(), c.type_.clone()));
        }
        typer.reference_types.push(crate::typer::ReferenceType {
            name: Some(key.table_name().clone()),
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

    // Inject dialect-specific built-in schemas so that system tables like
    // `spatial_ref_sys` are always resolvable without requiring the user to
    // declare them in their schema file.
    let dialect = options.parse_options.get_dialect();
    if dialect.is_postgresql() {
        // Coerce 'static to &'a str — sound because 'static: 'a.
        let builtin_src: &'a str = POSTGRESQL_BUILTIN_SQL;
        let builtin_options = TypeOptions::new().dialect(dialect);
        let builtin_stmts = parse_statements(
            builtin_src,
            &mut Issues::new(builtin_src),
            &builtin_options.parse_options,
        );
        let mut builtin_schemas: Schemas<'a> = Schemas::default();
        SchemaCtx::new(
            &mut builtin_schemas,
            &mut Issues::new(builtin_src),
            builtin_src,
            &builtin_options,
        )
        .process_top_level_statements(builtin_stmts);
        // User-defined tables take priority; only add entries not already present.
        for (k, v) in builtin_schemas.schemas {
            schemas.schemas.entry(k).or_insert(v);
        }
    }

    schemas
}
