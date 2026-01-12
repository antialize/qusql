use std::collections::HashMap;

use ariadne::{Label, Report, ReportKind, Source};
use pyo3::{prelude::*, IntoPyObjectExt};
use qusql_type::{Issue, Issues, SQLArguments, SQLDialect, TypeOptions};
use yoke::{Yoke, Yokeable};

#[derive(Yokeable)]
struct SchemasAndIssues<'a> {
    schema: qusql_type::schema::Schemas<'a>,
    issues: Issues<'a>,
}

#[pyclass]
struct Schemas(Yoke<SchemasAndIssues<'static>, std::string::String>);

fn issue_to_report(issue: &Issue) -> Report<'static, std::ops::Range<usize>> {
    let mut builder = Report::build(
        match issue.level {
            qusql_type::Level::Warning => ReportKind::Warning,
            qusql_type::Level::Error => ReportKind::Error,
        },
        issue.span.clone(),
    )
    .with_config(ariadne::Config::default().with_color(false))
    .with_label(
        Label::new(issue.span.clone())
            .with_order(-1)
            .with_priority(-1)
            .with_message(issue.message.to_string()),
    );
    for frag in &issue.fragments {
        builder = builder
            .with_label(Label::new(frag.span.clone()).with_message(frag.message.to_string()));
    }
    builder.finish()
}

struct NamedSource<'a>(&'a str, Source<&'a str>);

impl<'a> ariadne::Cache<()> for &NamedSource<'a> {
    type Storage = &'a str;

    fn fetch(&mut self, _id: &()) -> Result<&Source<Self::Storage>, impl std::fmt::Debug> {
        Ok::<_, std::convert::Infallible>(&self.1)
    }

    fn display<'b>(&self, _id: &'b ()) -> Option<impl std::fmt::Display + 'b> {
        Some(self.0.to_string())
    }
}

fn issues_to_string(name: &str, source: &str, issues: &[Issue]) -> (bool, std::string::String) {
    let source = NamedSource(name, Source::from(source));
    let mut err = false;
    let mut out = Vec::new();
    for issue in issues {
        if issue.level == qusql_type::Level::Error {
            err = true;
        }
        let r = issue_to_report(issue);
        r.write(&source, &mut out).unwrap();
    }
    (err, std::string::String::from_utf8(out).unwrap())
}

#[pyfunction]
fn parse_schemas(name: &str, src: std::string::String) -> (Schemas, bool, std::string::String) {
    let schemas =
        Yoke::<SchemasAndIssues<'static>, std::string::String>::attach_to_cart(src, |src: &str| {
            let mut issues = Issues::new(src);
            let schema = qusql_type::schema::parse_schemas(
                src,
                &mut issues,
                &TypeOptions::new().dialect(SQLDialect::MariaDB),
            );
            SchemasAndIssues { schema, issues }
        });

    let (err, messages) =
        issues_to_string(name, schemas.backing_cart(), &schemas.get().issues.issues);
    (Schemas(schemas), err, messages)
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum ArgumentKey {
    Identifier(std::string::String),
    Index(usize),
}

impl<'py> IntoPyObject<'py> for ArgumentKey {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ArgumentKey::Identifier(i) => Ok(i.into_pyobject(py)?.into_any()),
            ArgumentKey::Index(i) => Ok(i.into_pyobject(py)?.into_any()),
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

#[pyclass]
struct List {
    #[pyo3(get)]
    r#type: Py<PyAny>,
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
    List(Box<Type>),
}

impl<'py> IntoPyObject<'py> for Type {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let v = match self {
            Type::Any => Py::new(py, Any {}).unwrap().into_pyobject(py)?.into_any(),
            Type::Integer => Py::new(py, Integer {})
                .unwrap()
                .into_pyobject(py)?
                .into_any(),
            Type::Float => Py::new(py, Float {}).unwrap().into_pyobject(py)?.into_any(),
            Type::Bool => Py::new(py, Bool {}).unwrap().into_pyobject(py)?.into_any(),
            Type::Bytes => Py::new(py, Bytes {}).unwrap().into_pyobject(py)?.into_any(),
            Type::String => Py::new(py, String {})
                .unwrap()
                .into_pyobject(py)?
                .into_any(),
            Type::Enum(values) => Py::new(py, Enum { values })
                .unwrap()
                .into_pyobject(py)?
                .into_any(),
            Type::List(r#type) => Py::new(
                py,
                List {
                    r#type: r#type.into_py_any(py)?,
                },
            )
            .unwrap()
            .into_pyobject(py)?
            .into_any(),
        };
        Ok(v)
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

fn map_type(t: &qusql_type::FullType<'_>) -> Type {
    let b = match &t.t {
        qusql_type::Type::Args(_, _) => Type::Any,
        qusql_type::Type::Base(v) => match v {
            qusql_type::BaseType::Any => Type::Any,
            qusql_type::BaseType::Bool => Type::Bool,
            qusql_type::BaseType::Bytes => Type::Bytes,
            qusql_type::BaseType::Date => Type::Any,
            qusql_type::BaseType::DateTime => Type::Any,
            qusql_type::BaseType::Float => Type::Float,
            qusql_type::BaseType::Integer => Type::Integer,
            qusql_type::BaseType::String => Type::String,
            qusql_type::BaseType::Time => Type::Any,
            qusql_type::BaseType::TimeStamp => Type::Any,
            qusql_type::BaseType::TimeInterval => Type::Any,
        },
        qusql_type::Type::Enum(v) => Type::Enum(v.iter().map(|v| v.to_string()).collect()),
        qusql_type::Type::F32 => Type::Float,
        qusql_type::Type::F64 => Type::Float,
        qusql_type::Type::I16 => Type::Integer,
        qusql_type::Type::I32 => Type::Integer,
        qusql_type::Type::I64 => Type::Integer,
        qusql_type::Type::I8 => Type::Integer,
        qusql_type::Type::Invalid => Type::Any,
        qusql_type::Type::JSON => Type::Any,
        qusql_type::Type::Set(_) => Type::String,
        qusql_type::Type::U16 => Type::Integer,
        qusql_type::Type::U32 => Type::Integer,
        qusql_type::Type::U64 => Type::Integer,
        qusql_type::Type::U8 => Type::Integer,
        qusql_type::Type::Null => Type::Any,
    };
    if t.list_hack {
        Type::List(Box::new(b))
    } else {
        b
    }
}

fn map_arguments(
    arguments: Vec<(qusql_type::ArgumentKey<'_>, qusql_type::FullType<'_>)>,
) -> HashMap<ArgumentKey, (Type, bool)> {
    arguments
        .into_iter()
        .map(|(k, v)| {
            let k = match k {
                qusql_type::ArgumentKey::Index(i) => ArgumentKey::Index(i),
                qusql_type::ArgumentKey::Identifier(i) => ArgumentKey::Identifier(i.to_string()),
            };
            (k, (map_type(&v), v.not_null))
        })
        .collect()
}

#[pyfunction]
fn type_statement(
    py: Python,
    schemas: &Schemas,
    statement: &str,
    dict_result: bool,
) -> PyResult<(Py<PyAny>, bool, std::string::String)> {
    let mut issues = Issues::new(statement);

    let mut options = TypeOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::Percent)
        .list_hack(true);

    if dict_result {
        options = options
            .warn_duplicate_column_in_select(true)
            .warn_unnamed_column_in_select(true);
    }

    let stmt =
        qusql_type::type_statement(&schemas.0.get().schema, statement, &mut issues, &options);

    let res = match stmt {
        qusql_type::StatementType::Select { columns, arguments } => {
            let columns = columns
                .into_iter()
                .map(|v| {
                    (
                        v.name.map(|v| v.to_string()),
                        map_type(&v.type_),
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
            .into_py_any(py)?
        }
        qusql_type::StatementType::Delete {
            arguments,
            returning,
        } => {
            if returning.is_some() {
                // TODO: Implement RETURNING support
                issues.err(
                    "support for RETURNING is not implemented yet",
                    &(0..statement.len()),
                );
            }
            Py::new(
                py,
                Delete {
                    arguments: map_arguments(arguments),
                },
            )?
            .into_py_any(py)?
        }
        qusql_type::StatementType::Insert {
            yield_autoincrement,
            arguments,
            returning,
        } => {
            if returning.is_some() {
                // TODO: Implement RETURNING support
                issues.err(
                    "support for RETURNING is not implemented yet",
                    &(0..statement.len()),
                );
            }
            let yield_autoincrement = match yield_autoincrement {
                qusql_type::AutoIncrementId::Yes => "yes",
                qusql_type::AutoIncrementId::No => "no",
                qusql_type::AutoIncrementId::Optional => "maybe",
            };
            Py::new(
                py,
                Insert {
                    yield_autoincrement,
                    arguments: map_arguments(arguments),
                },
            )?
            .into_py_any(py)?
        }
        qusql_type::StatementType::Update {
            arguments,
            returning,
        } => {
            if returning.is_some() {
                // TODO: Implement RETURNING support
                issues.err(
                    "support for RETURNING is not implemented yet",
                    &(0..statement.len()),
                );
            }
            Py::new(
                py,
                Update {
                    arguments: map_arguments(arguments),
                },
            )?
            .into_py_any(py)?
        }
        qusql_type::StatementType::Replace {
            arguments,
            returning,
        } => {
            if returning.is_some() {
                // TODO: Implement RETURNING support
                issues.err(
                    "support for RETURNING is not implemented yet",
                    &(0..statement.len()),
                );
            }
            Py::new(
                py,
                Replace {
                    arguments: map_arguments(arguments),
                },
            )?
            .into_py_any(py)?
        }
        qusql_type::StatementType::Invalid => Py::new(py, Invalid {})?.into_py_any(py)?,
    };

    let (err, messages) = issues_to_string("", statement, issues.get());
    Ok((res, err, messages))
}

#[pymodule]
fn mysql_type_plugin(m: &Bound<'_, PyModule>) -> PyResult<()> {
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
    m.add_class::<List>()?;
    m.add_class::<Schemas>()?;
    Ok(())
}
