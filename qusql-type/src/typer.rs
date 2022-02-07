use sql_ast::{Issue, Span, Spanned};

use crate::{schema::Schemas, type_::FullType, Type};

#[derive(Clone, Debug)]
pub(crate) struct ReferenceType<'a> {
    pub(crate) name: (&'a str, Span),
    pub(crate) columns: Vec<(&'a str, FullType<'a>)>,
}

pub(crate) struct Typer<'a> {
    pub(crate) issues: &'a mut Vec<Issue>,
    pub(crate) schemas: &'a Schemas<'a>,
    pub(crate) reference_types: Vec<ReferenceType<'a>>,
    pub(crate) arg_types: Vec<FullType<'a>>,
}

impl<'a> Typer<'a> {
    pub(crate) fn constrain_arg(&mut self, idx: usize, t: &FullType<'a>) {
        while self.arg_types.len() <= idx {
            self.arg_types.push(FullType::invalid());
        }
        self.arg_types[idx] = t.clone();
    }

    pub(crate) fn common_type(
        &mut self,
        t1: &FullType<'a>,
        t2: &FullType<'a>,
    ) -> Option<FullType<'a>> {
        if let Type::Arg(idx, _) = t1.t {
            self.constrain_arg(idx, t2);
            return Some(t2.clone());
        }
        if let Type::Arg(idx, _) = t2.t {
            self.constrain_arg(idx, t1);
            return Some(t1.clone());
        }
        if let Type::Invalid = t1.t {
            return Some(t1.clone());
        }
        if let Type::Invalid = t2.t {
            return Some(t2.clone());
        }
        if let Type::Null = t1.t {
            return Some(t2.clone());
        }
        if let Type::Null = t2.t {
            return Some(t1.clone());
        }

        let not_null = t1.not_null && t2.not_null;
        let t = match t1.t {
            Type::U8 => match t2.t {
                Type::U8 => Type::U8,
                Type::I8 => Type::I16,
                Type::U16 => Type::U16,
                Type::I16 => Type::I16,
                Type::U32 => Type::U32,
                Type::I32 => Type::I32,
                Type::U64 => Type::U64,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::I8 => match t2.t {
                Type::U8 => Type::I16,
                Type::I8 => Type::I16,
                Type::U16 => Type::I32,
                Type::I16 => Type::I16,
                Type::U32 => Type::I64,
                Type::I32 => Type::I32,
                Type::U64 => Type::Integer,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::U16 => match t2.t {
                Type::U8 => Type::U16,
                Type::I8 => Type::I32,
                Type::U16 => Type::U16,
                Type::I16 => Type::I32,
                Type::U32 => Type::U32,
                Type::I32 => Type::I32,
                Type::U64 => Type::U64,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::I16 => match t2.t {
                Type::U8 => Type::I16,
                Type::I8 => Type::I32,
                Type::U16 => Type::I32,
                Type::I16 => Type::I32,
                Type::U32 => Type::I64,
                Type::I32 => Type::I32,
                Type::U64 => Type::Integer,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::U32 => match t2.t {
                Type::U8 => Type::U32,
                Type::I8 => Type::I64,
                Type::U16 => Type::U32,
                Type::I16 => Type::I64,
                Type::U32 => Type::U32,
                Type::I32 => Type::I64,
                Type::U64 => Type::U64,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::I32 => match t2.t {
                Type::U8 => Type::I32,
                Type::I8 => Type::I32,
                Type::U16 => Type::I32,
                Type::I16 => Type::I32,
                Type::U32 => Type::I64,
                Type::I32 => Type::I32,
                Type::U64 => Type::Integer,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::U64 => match t2.t {
                Type::U8 => Type::U64,
                Type::I8 => Type::Integer,
                Type::U16 => Type::U64,
                Type::I16 => Type::Integer,
                Type::U32 => Type::U64,
                Type::I32 => Type::Integer,
                Type::U64 => Type::U64,
                Type::I64 => Type::Integer,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::I64 => match t2.t {
                Type::U8 => Type::I64,
                Type::I8 => Type::I64,
                Type::U16 => Type::I64,
                Type::I16 => Type::I64,
                Type::U32 => Type::I64,
                Type::I32 => Type::I64,
                Type::U64 => Type::Integer,
                Type::I64 => Type::I64,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::Text => match t2.t {
                Type::Text | Type::Enum(_) | Type::Set(_) => Type::Text,
                Type::U8
                | Type::I8
                | Type::U16
                | Type::I16
                | Type::U32
                | Type::I32
                | Type::U64
                | Type::I64
                | Type::F32
                | Type::F64
                | Type::Integer
                | Type::Float
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::F32 => match t2.t {
                Type::U8 => Type::Float,
                Type::I8 => Type::Float,
                Type::U16 => Type::Float,
                Type::I16 => Type::Float,
                Type::U32 => Type::Float,
                Type::I32 => Type::Float,
                Type::U64 => Type::Float,
                Type::I64 => Type::Float,
                Type::F32 => Type::F32,
                Type::F64 => Type::F64,
                Type::Integer => Type::Float,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::F64 => match t2.t {
                Type::U8 => Type::Float,
                Type::I8 => Type::Float,
                Type::U16 => Type::Float,
                Type::I16 => Type::Float,
                Type::U32 => Type::Float,
                Type::I32 => Type::Float,
                Type::U64 => Type::Float,
                Type::I64 => Type::Float,
                Type::F32 => Type::F64,
                Type::F64 => Type::F64,
                Type::Integer => Type::Float,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::Integer => match t2.t {
                Type::U8 => Type::Integer,
                Type::I8 => Type::Integer,
                Type::U16 => Type::Integer,
                Type::I16 => Type::Integer,
                Type::U32 => Type::Integer,
                Type::I32 => Type::Integer,
                Type::U64 => Type::Integer,
                Type::I64 => Type::Integer,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Integer,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::Float => match t2.t {
                Type::U8 => Type::Float,
                Type::I8 => Type::Float,
                Type::U16 => Type::Float,
                Type::I16 => Type::Float,
                Type::U32 => Type::Float,
                Type::I32 => Type::Float,
                Type::U64 => Type::Float,
                Type::I64 => Type::Float,
                Type::F32 => Type::Float,
                Type::F64 => Type::Float,
                Type::Integer => Type::Float,
                Type::Float => Type::Float,
                Type::Text
                | Type::Bytes
                | Type::Time
                | Type::DateTime
                | Type::Timestamp
                | Type::Date
                | Type::Enum(_)
                | Type::Set(_)
                | Type::Bool => return None,
                Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            },
            Type::Bytes => {
                if t2.t == Type::Bytes {
                    Type::Bytes
                } else {
                    return None;
                }
            }
            Type::Time => {
                if t2.t == Type::Time {
                    Type::Time
                } else {
                    return None;
                }
            }
            Type::DateTime => {
                if t2.t == Type::DateTime {
                    Type::DateTime
                } else {
                    return None;
                }
            }
            Type::Timestamp => {
                if t2.t == Type::Timestamp {
                    Type::Timestamp
                } else {
                    return None;
                }
            }
            Type::Date => {
                if t2.t == Type::Date {
                    Type::Date
                } else {
                    return None;
                }
            }
            Type::Null | Type::Invalid | Type::Arg(_, _) => panic!(),
            Type::Bool => {
                if t2.t == Type::Bool {
                    Type::Bool
                } else {
                    return None;
                }
            }
            Type::Enum(_) => {
                match t2.t {
                    Type::Text => Type::Text,
                    Type::Enum(_) => Type::Text, //TODO
                    Type::Set(_) => Type::Text,  //TODO
                    _ => return None,
                }
            }
            Type::Set(_) => {
                match t2.t {
                    Type::Text => Type::Text,
                    Type::Set(_) => Type::Text,  //TODO
                    Type::Enum(_) => Type::Text, //TODO
                    _ => return None,
                }
            }
        };
        Some(FullType::new(t, not_null))
    }

    pub(crate) fn ensure_type(
        &mut self,
        span: &impl Spanned,
        given: &FullType<'a>,
        expected: &FullType<'a>,
    ) {
        if self.common_type(given, expected).is_none() {
            self.issues.push(Issue::err(
                format!("Expected type {} got {}", expected.t, given.t),
                span,
            ));
        }
    }

    pub(crate) fn ensure_bool(&mut self, span: &impl Spanned, given: &FullType<'a>) {
        self.ensure_type(span, given, &FullType::new(Type::Bool, false));
    }

    pub(crate) fn ensure_text(&mut self, span: &impl Spanned, given: &FullType<'a>) {
        self.ensure_type(span, given, &FullType::new(Type::Text, false))
    }
}
