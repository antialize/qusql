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

use alloc::borrow::Cow;

use crate::{
    schema::{Schema, Schemas},
    type_::{ArgType, BaseType, FullType},
    ArgumentKey, Type, TypeOptions,
};
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::{collections::BTreeMap, format};
use sql_parse::{
    Identifier, IssueHandle, Issues, OptSpanned, QualifiedName, SQLDialect, Span, Spanned,
};

#[derive(Clone, Debug)]
pub(crate) struct ReferenceType<'a> {
    pub(crate) name: Option<Identifier<'a>>,
    pub(crate) span: Span,
    pub(crate) columns: Vec<(Identifier<'a>, FullType<'a>)>,
}

pub(crate) struct Typer<'a, 'b> {
    pub(crate) issues: &'b mut Issues<'a>,
    pub(crate) schemas: &'b Schemas<'a>,
    pub(crate) with_schemas: BTreeMap<&'a str, &'b Schema<'a>>,
    pub(crate) reference_types: Vec<ReferenceType<'a>>,
    pub(crate) arg_types: Vec<(ArgumentKey<'a>, FullType<'a>)>,
    pub(crate) options: &'b TypeOptions,
}

impl<'a, 'b> Typer<'a, 'b> {
    pub(crate) fn with_schemas<'c>(
        &'c mut self,
        schemas: BTreeMap<&'a str, &'c Schema<'a>>,
    ) -> Typer<'a, 'c>
    where
        'b: 'c,
    {
        Typer::<'a, 'c> {
            issues: self.issues,
            schemas: self.schemas,
            with_schemas: schemas,
            reference_types: self.reference_types.clone(),
            arg_types: self.arg_types.clone(),
            options: self.options,
        }
    }

    pub(crate) fn dialect(&self) -> SQLDialect {
        self.options.parse_options.get_dialect()
    }

    pub(crate) fn constrain_arg(&mut self, idx: usize, arg_type: &ArgType, t: &FullType<'a>) {
        // TODO Use arg_type
        let ot = match self
            .arg_types
            .iter_mut()
            .find(|(k, _)| k == &ArgumentKey::Index(idx))
        {
            Some((_, v)) => v,
            None => {
                self.arg_types
                    .push((ArgumentKey::Index(idx), FullType::new(BaseType::Any, false)));
                &mut self.arg_types.last_mut().unwrap().1
            }
        };
        if t.base() != BaseType::Any || ot.base() == BaseType::Any {
            *ot = t.clone();
        }
        if matches!(arg_type, ArgType::ListHack) {
            ot.list_hack = true;
        }
    }

    pub(crate) fn matched_type(&mut self, t1: &Type<'a>, t2: &Type<'a>) -> Option<Type<'a>> {
        if t1 == &Type::Invalid && t2 == &Type::Invalid {
            return Some(t1.clone());
        }
        if t1 == &Type::Null {
            return Some(t2.clone());
        }
        if t2 == &Type::Null {
            return Some(t1.clone());
        }

        let mut t1b = t1.base();
        let mut t2b = t2.base();
        if t1b == BaseType::Any {
            t1b = t2b;
        }
        if t2b == BaseType::Any {
            t2b = t1b;
        }
        if t1b != t2b {
            return None;
        }

        for t in &[t1, t2] {
            if let Type::Args(_, a) = t {
                for (idx, arg_type, _) in a.iter() {
                    self.constrain_arg(*idx, arg_type, &FullType::new(t1b, false));
                }
            }
        }
        if t1b == BaseType::Any {
            let mut args = Vec::new();
            for t in &[t1, t2] {
                if let Type::Args(_, a) = t {
                    args.extend_from_slice(a);
                }
            }
            if !args.is_empty() {
                return Some(Type::Args(t1b, Arc::new(args)));
            }
        }
        Some(t1b.into())
    }

    pub(crate) fn ensure_type(
        &mut self,
        span: &impl Spanned,
        given: &FullType<'a>,
        expected: &FullType<'a>,
    ) {
        if self.matched_type(given, expected).is_none() {
            self.issues.err(
                format!("Expected type {} got {}", expected.t, given.t),
                span,
            );
        }
    }

    pub(crate) fn ensure_base(
        &mut self,
        span: &impl Spanned,
        given: &FullType<'a>,
        expected: BaseType,
    ) {
        self.ensure_type(span, given, &FullType::new(expected, false));
    }

    pub(crate) fn get_schema(&self, name: &str) -> Option<&'b Schema<'a>> {
        if let Some(schema) = self.with_schemas.get(name) {
            Some(schema)
        } else {
            self.schemas.schemas.get(name)
        }
    }

    pub(crate) fn err(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        span: &impl Spanned,
    ) -> IssueHandle<'a, '_> {
        self.issues.err(message, span)
    }

    pub(crate) fn warn(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        span: &impl Spanned,
    ) -> IssueHandle<'a, '_> {
        self.issues.warn(message, span)
    }
}

pub(crate) struct TyperStack<'a, 'b, 'c, V, D: FnOnce(&mut Typer<'a, 'b>, V)> {
    pub(crate) typer: &'c mut Typer<'a, 'b>,
    value_drop: Option<(V, D)>,
}

impl<'a, 'b, 'c, V, D: FnOnce(&mut Typer<'a, 'b>, V)> Drop for TyperStack<'a, 'b, 'c, V, D> {
    fn drop(&mut self) {
        if let Some((v, d)) = self.value_drop.take() {
            (d)(self.typer, v)
        }
    }
}

pub(crate) fn typer_stack<
    'a,
    'b,
    'c,
    V,
    C: FnOnce(&mut Typer<'a, 'b>) -> V,
    D: FnOnce(&mut Typer<'a, 'b>, V),
>(
    typer: &'c mut Typer<'a, 'b>,
    c: C,
    d: D,
) -> TyperStack<'a, 'b, 'c, V, D> {
    let v = c(typer);
    TyperStack {
        typer,
        value_drop: Some((v, d)),
    }
}

pub(crate) fn unqualified_name<'b, 'c>(
    issues: &mut Issues<'_>,
    name: &'c QualifiedName<'b>,
) -> &'c Identifier<'b> {
    if !name.prefix.is_empty() {
        issues.err(
            "Expected unqualified name",
            &name.prefix.opt_span().unwrap(),
        );
    }
    &name.identifier
}
