use sql_ast::{Issue, Span, Spanned};

use crate::{schema::Schemas, type_::FullType, Type};

pub(crate) struct ReferenceType<'a> {
    pub(crate) name: (&'a str, Span),
    pub(crate) columns: Vec<(&'a str, FullType<'a>)>,
}

pub(crate) struct Typer<'a> {
    pub(crate) issues: &'a mut Vec<Issue>,
    pub(crate) schemas: &'a Schemas<'a>,
    pub(crate) reference_types: Vec<ReferenceType<'a>>,
}

impl<'a> Typer<'a> {
    pub(crate) fn constrain_arg(&mut self, idx: usize, type_: &Type<'a>, not_null: bool) {
        //TODO
    }

    pub(crate) fn ensure_bool(&mut self, span: &impl Spanned, given: &Type<'a>) {
        match given {
            Type::Bool | Type::Invalid => (),
            Type::Arg(idx, span) => {
                self.constrain_arg(*idx, &Type::Bool, false);
            }
            _ => {
                self.issues.push(Issue::err(
                    format!("Expected type bool got {:?}", given),
                    span,
                ));
            }
        }
    }

    pub(crate) fn ensure_text(&mut self, span: &impl Spanned, given: &Type<'a>) {
        match given {
            Type::Text | Type::Enum(_) | Type::Invalid => (),
            Type::Arg(idx, span) => {
                self.constrain_arg(*idx, &Type::Text, false);
            }
            _ => {
                self.issues.push(Issue::err(
                    format!("Expected text bool got {:?}", given),
                    span,
                ));
            }
        }
    }
}
