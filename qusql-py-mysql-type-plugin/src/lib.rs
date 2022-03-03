use std::collections::HashMap;

use ariadne::{Label, Report, ReportKind, Source};
use ouroboros::self_referencing;
use pyo3::prelude::*;
use sql_type::{Issue, SQLArguments, SQLDialect, TypeOptions};

#[pyclass]
#[self_referencing]
struct Schemas {
    src: std::string::String,
    #[borrows(src)]
    #[covariant]
    schemas: sql_type::schema::Schemas<'this>,
}

fn issue_to_report(issue: Issue) -> Report<std::ops::Range<usize>> {
    let mut builder = Report::build(
        match issue.level {
            sql_type::Level::Warning => ReportKind::Warning,
            sql_type::Level::Error => ReportKind::Error,
        },
        (),
        issue.span.start,
    )
    .with_config(ariadne::Config::default().with_color(false))
    .with_label(
        Label::new(issue.span)
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message),
    );
    for frag in issue.fragments {
        builder = builder.with_label(Label::new(frag.1).with_message(frag.0));
    }
    builder.finish()
}

struct NamedSource<'a>(&'a str, Source);

impl<'a> ariadne::Cache<()> for &NamedSource<'a> {
    fn fetch(&mut self, _: &()) -> Result<&Source, Box<dyn std::fmt::Debug + '_>> {
        Ok(&self.1)
    }

    fn display<'b>(&self, _: &'b ()) -> Option<Box<dyn std::fmt::Display + 'b>> {
        Some(Box::new(self.0.to_string()))
    }
}

fn issues_to_string(name: &str, source: &str, issues: Vec<Issue>) -> (bool, std::string::String) {
    let source = NamedSource(name, Source::from(source));
    let mut err = false;
    let mut out = Vec::new();
    for issue in issues {
        if issue.level == sql_type::Level::Error {
            err = true;
        }
        let r = issue_to_report(issue);
        r.write(&source, &mut out).unwrap();
    }
    (err, std::string::String::from_utf8(out).unwrap())
}

#[pyfunction]
fn parse_schemas(name: &str, src: std::string::String) -> (Schemas, bool, std::string::String) {
    let mut issues = Vec::new();

    let schemas = SchemasBuilder {
        src,
        schemas_builder: |src: &std::string::String| {
            sql_type::schema::parse_schemas(
                src,
                &mut issues,
                &TypeOptions::new().dialect(SQLDialect::MariaDB),
            )
        },
    }
    .build();

    let (err, messages) = issues_to_string(name, schemas.borrow_src(), issues);
    (schemas, err, messages)
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum ArgumentKey {
    Identifier(std::string::String),
    Index(usize),
}

impl IntoPy<PyObject> for ArgumentKey {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            ArgumentKey::Identifier(i) => i.to_object(py),
            ArgumentKey::Index(i) => i.to_object(py),
        }
    }
}

#[pyclass]
struct Any {}

#[pyclass]
struct Integer {}

#[pyclass]
struct Float {}

#[pyclass]
struct Bool {}

#[pyclass]
struct Bytes {}

#[pyclass]
struct String {}

#[pyclass]
struct Enum {
    #[pyo3(get)]
    values: Vec<std::string::String>,
}

#[derive(Clone)]
enum Type {
    Any,
    Integer,
    Float,
    Bool,
    Bytes,
    String,
    Enum(Vec<std::string::String>),
}

impl IntoPy<PyObject> for Type {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            Type::Any => Py::new(py, Any {}).unwrap().to_object(py),
            Type::Integer => Py::new(py, Integer {}).unwrap().to_object(py),
            Type::Float => Py::new(py, Float {}).unwrap().to_object(py),
            Type::Bool => Py::new(py, Bool {}).unwrap().to_object(py),
            Type::Bytes => Py::new(py, Bytes {}).unwrap().to_object(py),
            Type::String => Py::new(py, String {}).unwrap().to_object(py),
            Type::Enum(values) => Py::new(py, Enum { values }).unwrap().to_object(py),
        }
    }
}

#[pyclass]
struct Select {
    #[pyo3(get)]
    columns: Vec<(Option<std::string::String>, Type, bool)>,

    #[pyo3(get)]
    arguments: HashMap<ArgumentKey, (Type, bool)>,
}

#[pyclass]
struct Delete {
    #[pyo3(get)]
    arguments: HashMap<ArgumentKey, (Type, bool)>,
}

#[pyclass]
struct Insert {
    #[pyo3(get)]
    yield_autoincrement: &'static str,

    #[pyo3(get)]
    arguments: HashMap<ArgumentKey, (Type, bool)>,
}

#[pyclass]
struct Update {
    #[pyo3(get)]
    arguments: HashMap<ArgumentKey, (Type, bool)>,
}

#[pyclass]
struct Replace {
    #[pyo3(get)]
    arguments: HashMap<ArgumentKey, (Type, bool)>,
}

#[pyclass]
struct Invalid {}

fn map_type(t: sql_type::Type<'_>) -> Type {
    match t {
        sql_type::Type::Args(_, _) => Type::Any,
        sql_type::Type::Base(v) => {
            match v {
                sql_type::BaseType::Any => Type::Any,
                sql_type::BaseType::Bool => Type::Bool,
                sql_type::BaseType::Bytes => Type::Bytes,
                sql_type::BaseType::Date => Type::Any, //TODO
                sql_type::BaseType::DateTime => Type::Any, //TODO
                sql_type::BaseType::Float => Type::Float,
                sql_type::BaseType::Integer => Type::Integer,
                sql_type::BaseType::String => Type::String,
                sql_type::BaseType::Time => Type::Any, //TODO
                sql_type::BaseType::TimeStamp => Type::Any, //TODO
            }
        }
        sql_type::Type::Enum(v) => Type::Enum(v.iter().map(|v| v.to_string()).collect()),
        sql_type::Type::F32 => Type::Float,
        sql_type::Type::F64 => Type::Float,
        sql_type::Type::I16 => Type::Integer,
        sql_type::Type::I32 => Type::Integer,
        sql_type::Type::I64 => Type::Integer,
        sql_type::Type::I8 => Type::Integer,
        sql_type::Type::Invalid => Type::Any,
        sql_type::Type::JSON => Type::Any,
        sql_type::Type::Set(_) => Type::String,
        sql_type::Type::U16 => Type::Integer,
        sql_type::Type::U32 => Type::Integer,
        sql_type::Type::U64 => Type::Integer,
        sql_type::Type::U8 => Type::Integer,
        sql_type::Type::Null => Type::Any,
    }
}

fn map_arguments(
    arguments: Vec<(sql_type::ArgumentKey<'_>, sql_type::FullType<'_>)>,
) -> HashMap<ArgumentKey, (Type, bool)> {
    arguments
        .into_iter()
        .map(|(k, v)| {
            let k = match k {
                sql_type::ArgumentKey::Index(i) => ArgumentKey::Index(i),
                sql_type::ArgumentKey::Identifier(i) => ArgumentKey::Identifier(i.to_string()),
            };
            (k, (map_type(v.t), v.not_null))
        })
        .collect()
}

#[pyfunction]
fn type_statement(
    py: Python,
    schemas: &Schemas,
    statement: &str,
) -> PyResult<(PyObject, bool, std::string::String)> {
    let mut issues = Vec::new();

    let stmt = sql_type::type_statement(
        schemas.borrow_schemas(),
        statement,
        &mut issues,
        &TypeOptions::new()
            .dialect(SQLDialect::MariaDB)
            .arguments(SQLArguments::Percent),
    );

    let res = match stmt {
        sql_type::StatementType::Select { columns, arguments } => {
            let columns = columns
                .into_iter()
                .map(|v| {
                    (
                        v.name.map(|v| v.to_string()),
                        map_type(v.type_.t),
                        v.type_.not_null,
                    )
                })
                .collect();
            Py::new(
                py,
                Select {
                    arguments: map_arguments(arguments),
                    columns,
                },
            )?
            .to_object(py)
        }
        sql_type::StatementType::Delete { arguments } => Py::new(
            py,
            Delete {
                arguments: map_arguments(arguments),
            },
        )?
        .to_object(py),
        sql_type::StatementType::Insert {
            yield_autoincrement,
            arguments,
        } => {
            let yield_autoincrement = match yield_autoincrement {
                sql_type::AutoIncrementId::Yes => "yes",
                sql_type::AutoIncrementId::No => "no",
                sql_type::AutoIncrementId::Optional => "maybe",
            };
            Py::new(
                py,
                Insert {
                    yield_autoincrement,
                    arguments: map_arguments(arguments),
                },
            )?
            .to_object(py)
        }
        sql_type::StatementType::Update { arguments } => Py::new(
            py,
            Update {
                arguments: map_arguments(arguments),
            },
        )?
        .to_object(py),
        sql_type::StatementType::Replace { arguments } => Py::new(
            py,
            Replace {
                arguments: map_arguments(arguments),
            },
        )?
        .to_object(py),
        sql_type::StatementType::Invalid => Py::new(py, Invalid {})?.to_object(py),
    };

    let (err, messages) = issues_to_string("", statement, issues);
    Ok((res, err, messages))
}

#[pymodule]
fn mysql_type_plugin(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_schemas, m)?)?;
    m.add_function(wrap_pyfunction!(type_statement, m)?)?;
    m.add_class::<Select>()?;
    m.add_class::<Delete>()?;
    m.add_class::<Insert>()?;
    m.add_class::<Update>()?;
    m.add_class::<Replace>()?;
    m.add_class::<Invalid>()?;
    m.add_class::<Integer>()?;
    m.add_class::<Bool>()?;
    m.add_class::<Any>()?;
    m.add_class::<Float>()?;
    m.add_class::<Bytes>()?;
    m.add_class::<String>()?;
    m.add_class::<Enum>()?;
    m.add_class::<Schemas>()?;
    Ok(())
}
