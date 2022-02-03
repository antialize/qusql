use std::{collections::HashSet, borrow::Cow};
use sql_ast::Span;
use crate::RefOrVal;


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
            not_null: self.not_null
        }
    }
    pub fn new(t: Type<'a>, not_null: bool) -> Self {
        Self {
            t,
            not_null,
        }
    }
    pub fn invalid() -> Self {
        Self {
            t: Type::Invalid,
            not_null: false
        }
    }
}
