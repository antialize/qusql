use crate::RefOrVal;
use sql_ast::Span;
use std::{
    borrow::Cow,
    collections::HashSet,
    fmt::{Display, Write},
    ops::Deref,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type<'a> {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Text,
    F32,
    F64,
    Integer,
    Float,
    Bytes,
    Time,
    DateTime,
    Timestamp,
    Date,
    Null,
    Invalid,
    Bool,
    Arg(usize, Span),
    Enum(RefOrVal<'a, HashSet<Cow<'a, str>>>),
    Set(RefOrVal<'a, HashSet<Cow<'a, str>>>),
}

impl<'a> Display for Type<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::U8 => f.write_str("u8"),
            Type::I8 => f.write_str("i8"),
            Type::U16 => f.write_str("u16"),
            Type::I16 => f.write_str("i16"),
            Type::U32 => f.write_str("u32"),
            Type::I32 => f.write_str("i32"),
            Type::U64 => f.write_str("u64"),
            Type::I64 => f.write_str("i64"),
            Type::Text => f.write_str("text"),
            Type::F32 => f.write_str("f32"),
            Type::F64 => f.write_str("f64"),
            Type::Integer => f.write_str("integer"),
            Type::Float => f.write_str("float"),
            Type::Bytes => f.write_str("bytes"),
            Type::Time => f.write_str("time"),
            Type::DateTime => f.write_str("datetime"),
            Type::Timestamp => f.write_str("timestamp"),
            Type::Date => f.write_str("date"),
            Type::Null => f.write_str("null"),
            Type::Invalid => f.write_str("invalid"),
            Type::Bool => f.write_str("bool"),
            Type::Arg(c, _) => write!(f, "arg({})", c),
            Type::Enum(v) => {
                f.write_str("enum(")?;
                for (i, v) in v.iter().enumerate() {
                    if i != 0 {
                        f.write_str(", ");
                    }
                    write!(f, "'{}'", v)?
                }
                f.write_char(')')
            }
            Type::Set(v) => {
                f.write_str("set(")?;
                for (i, v) in v.iter().enumerate() {
                    if i != 0 {
                        f.write_str(", ");
                    }
                    write!(f, "'{}'", v)?
                }
                f.write_char(')')
            }
        }
    }
}

impl<'a> Type<'a> {
    pub fn ref_clone(self: &'a Self) -> Self {
        match self {
            Type::Enum(e) => Type::Enum(e.ref_clone()),
            Type::Set(e) => Type::Set(e.ref_clone()),
            t => t.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullType<'a> {
    pub t: Type<'a>,
    pub not_null: bool,
}

impl<'a> FullType<'a> {
    pub fn ref_clone(self: &'a Self) -> Self {
        FullType {
            t: self.t.ref_clone(),
            not_null: self.not_null,
        }
    }
    pub fn new(t: Type<'a>, not_null: bool) -> Self {
        Self { t, not_null }
    }
    pub fn invalid() -> Self {
        Self {
            t: Type::Invalid,
            not_null: false,
        }
    }
}

impl<'a> std::ops::Deref for FullType<'a> {
    type Target = Type<'a>;

    fn deref(&self) -> &Self::Target {
        &self.t
    }
}
