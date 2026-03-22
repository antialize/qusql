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

use alloc::{
    borrow::Cow,
    fmt::{Display, Write},
    sync::Arc,
    vec::Vec,
};
use qusql_parse::Span;

/// Canonical base type / type category of a value.
///
/// Also exported as [`TypeCategory`], which is the preferred name in new code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseType {
    /// There are no constraint of the value
    Any,
    /// The value must be boolean
    Bool,
    /// The value must be a binary blob
    Bytes,
    Date,
    DateTime,
    /// The value must be some kind of float
    Float,
    /// The value must be some kind of integer
    Integer,
    String,
    Time,
    TimeStamp,
    TimeInterval,
    /// Exact numeric (DECIMAL / NUMERIC)
    Decimal,
    /// UUID
    Uuid,
    /// Network address (inet, cidr, macaddr)
    Network,
    /// Geometric types (point, line, polygon, …)
    Geometric,
    /// Range types (int4range, tsrange, …)
    Range,
    /// JSON / JSONB
    Json,
    /// Array types
    Array,
}

/// Type category — same as [`BaseType`] under a more descriptive name.
/// Use this in new code; `BaseType` is kept for backwards compatibility.
pub type TypeCategory = BaseType;

impl Display for BaseType {
    fn fmt(&self, f: &mut alloc::fmt::Formatter<'_>) -> alloc::fmt::Result {
        match self {
            BaseType::Any => f.write_str("any"),
            BaseType::Bool => f.write_str("bool"),
            BaseType::Bytes => f.write_str("bytes"),
            BaseType::Date => f.write_str("date"),
            BaseType::DateTime => f.write_str("datetime"),
            BaseType::Float => f.write_str("float"),
            BaseType::Integer => f.write_str("integer"),
            BaseType::String => f.write_str("string"),
            BaseType::Time => f.write_str("time"),
            BaseType::TimeStamp => f.write_str("timestamp"),
            BaseType::TimeInterval => f.write_str("timeinterval"),
            BaseType::Decimal => f.write_str("decimal"),
            BaseType::Uuid => f.write_str("uuid"),
            BaseType::Network => f.write_str("network"),
            BaseType::Geometric => f.write_str("geometric"),
            BaseType::Range => f.write_str("range"),
            BaseType::Json => f.write_str("json"),
            BaseType::Array => f.write_str("array"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgType {
    Normal,
    ListHack,
}

/// Represent the type of a value
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type<'a> {
    // This type is used internally and should not escape to the user
    #[doc(hidden)]
    Args(BaseType, Arc<Vec<(usize, ArgType, Span)>>),
    Base(BaseType),
    Enum(Arc<Vec<Cow<'a, str>>>),
    F32,
    F64,
    I16,
    I24,
    I32,
    I64,
    I8,
    Invalid,
    JSON,
    Set(Arc<Vec<Cow<'a, str>>>),
    U16,
    U24,
    U32,
    U64,
    U8,
    // This type is used internally and should not escape to the user
    #[doc(hidden)]
    Null,

    // ── Exact numeric ──
    /// DECIMAL / NUMERIC with unspecified precision/scale
    Decimal,

    // ── PostgreSQL-specific concrete types ──
    /// UUID
    Uuid,
    /// IPv4/IPv6 address
    Inet,
    /// Network address with mask
    Cidr,
    /// MAC address (6 bytes)
    Macaddr,
    /// JSONB (binary JSON; semantically distinct from JSON in PostgreSQL)
    Jsonb,
    /// Array of a given element type (e.g. `integer[]`)
    Array(Arc<Type<'a>>),
    /// Range of a given element type (e.g. `int4range`)
    Range(Arc<Type<'a>>),
    /// Multi-range of a given element type
    MultiRange(Arc<Type<'a>>),

    // ── PostgreSQL geometric types ──
    Point,
    Line,
    Lseg,
    /// PostgreSQL BOX geometric type (named GeoBox to avoid conflict with Rust's Box)
    GeoBox,
    Path,
    Polygon,
    Circle,
}

impl<'a> Display for Type<'a> {
    fn fmt(&self, f: &mut alloc::fmt::Formatter<'_>) -> alloc::fmt::Result {
        match self {
            Type::Args(t, a) => {
                write!(f, "args({t}")?;
                for (a, _, _) in a.iter() {
                    write!(f, ", {a}")?;
                }
                f.write_char(')')
            }
            Type::Base(t) => t.fmt(f),
            Type::F32 => f.write_str("f32"),
            Type::F64 => f.write_str("f64"),
            Type::I16 => f.write_str("i16"),
            Type::I24 => f.write_str("i24"),
            Type::I32 => f.write_str("i32"),
            Type::I64 => f.write_str("i64"),
            Type::I8 => f.write_str("i8"),
            Type::Invalid => f.write_str("invalid"),
            Type::JSON => f.write_str("json"),
            Type::U16 => f.write_str("u16"),
            Type::U24 => f.write_str("u24"),
            Type::U32 => f.write_str("u32"),
            Type::U64 => f.write_str("u64"),
            Type::U8 => f.write_str("u8"),
            Type::Null => f.write_str("null"),
            Type::Decimal => f.write_str("decimal"),
            Type::Uuid => f.write_str("uuid"),
            Type::Inet => f.write_str("inet"),
            Type::Cidr => f.write_str("cidr"),
            Type::Macaddr => f.write_str("macaddr"),
            Type::Jsonb => f.write_str("jsonb"),
            Type::Array(inner) => write!(f, "{inner}[]"),
            Type::Range(inner) => write!(f, "{}range", inner),
            Type::MultiRange(inner) => write!(f, "{}multirange", inner),
            Type::Point => f.write_str("point"),
            Type::Line => f.write_str("line"),
            Type::Lseg => f.write_str("lseg"),
            Type::GeoBox => f.write_str("box"),
            Type::Path => f.write_str("path"),
            Type::Polygon => f.write_str("polygon"),
            Type::Circle => f.write_str("circle"),
            Type::Enum(v) => {
                f.write_str("enum(")?;
                for (i, v) in v.iter().enumerate() {
                    if i != 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "'{v}'")?
                }
                f.write_char(')')
            }
            Type::Set(v) => {
                f.write_str("set(")?;
                for (i, v) in v.iter().enumerate() {
                    if i != 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "'{v}'")?
                }
                f.write_char(')')
            }
        }
    }
}

impl<'a> Type<'a> {
    /// Compute the canonical base type / category
    pub fn base(&self) -> BaseType {
        match self {
            Type::Args(t, _) => *t,
            Type::Base(t) => *t,
            Type::Enum(_) => BaseType::String,
            Type::F32 => BaseType::Float,
            Type::F64 => BaseType::Float,
            Type::I16 => BaseType::Integer,
            Type::I24 => BaseType::Integer,
            Type::I32 => BaseType::Integer,
            Type::I64 => BaseType::Integer,
            Type::I8 => BaseType::Integer,
            Type::Invalid => BaseType::Any,
            Type::JSON => BaseType::Json,
            Type::Null => BaseType::Any,
            Type::Set(_) => BaseType::String,
            Type::U16 => BaseType::Integer,
            Type::U24 => BaseType::Integer,
            Type::U32 => BaseType::Integer,
            Type::U64 => BaseType::Integer,
            Type::U8 => BaseType::Integer,
            Type::Decimal => BaseType::Decimal,
            Type::Uuid => BaseType::Uuid,
            Type::Inet | Type::Cidr | Type::Macaddr => BaseType::Network,
            Type::Jsonb => BaseType::Json,
            Type::Array(_) => BaseType::Array,
            Type::Range(_) | Type::MultiRange(_) => BaseType::Range,
            Type::Point
            | Type::Line
            | Type::Lseg
            | Type::GeoBox
            | Type::Path
            | Type::Polygon
            | Type::Circle => BaseType::Geometric,
        }
    }
}

impl<'a> From<BaseType> for Type<'a> {
    fn from(t: BaseType) -> Self {
        Type::Base(t)
    }
}

/// Represent a type with not_null information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullType<'a> {
    pub t: Type<'a>,
    pub not_null: bool,
    pub list_hack: bool,
}

impl<'a> FullType<'a> {
    pub(crate) fn new(t: impl Into<Type<'a>>, not_null: bool) -> Self {
        Self {
            t: t.into(),
            not_null,
            list_hack: false,
        }
    }

    /// Construct a new invalid type
    pub fn invalid() -> Self {
        Self {
            t: Type::Invalid,
            not_null: false,
            list_hack: false,
        }
    }
}

impl<'a> core::ops::Deref for FullType<'a> {
    type Target = Type<'a>;

    fn deref(&self) -> &Self::Target {
        &self.t
    }
}

impl<'a> Display for FullType<'a> {
    fn fmt(&self, f: &mut alloc::fmt::Formatter<'_>) -> alloc::fmt::Result {
        self.t.fmt(f)?;
        if self.list_hack {
            f.write_str(" list_hack")?;
        }
        if self.not_null {
            f.write_str(" not null")?;
        }
        Ok(())
    }
}
