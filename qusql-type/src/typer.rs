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
use alloc::boxed::Box;

use crate::{
    ArgumentKey, Type, TypeOptions,
    schema::{QualifiedIdentifier, Schema, Schemas, lookup_name},
    type_::{ArgType, BaseType, FullType},
};
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::{collections::BTreeMap, format};
use qusql_parse::{
    Identifier, IssueHandle, Issues, OptSpanned, QualifiedName, SQLDialect, Span, Spanned,
};

pub(crate) enum Restrict {
    Disallow,
    Allow,
    Require,
}

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
    pub(crate) outer_reference_types: Vec<ReferenceType<'a>>,
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
            outer_reference_types: self.outer_reference_types.clone(),
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

        // Arrays match recursively; an array never matches a non-array concrete type
        match (t1, t2) {
            (Type::Array(i1), Type::Array(i2)) => {
                return self
                    .matched_type(i1, i2)
                    .map(|inner| Type::Array(Box::new(inner)));
            }
            (Type::Array(_), other) if other.base() != BaseType::Any => return None,
            (other, Type::Array(_)) if other.base() != BaseType::Any => return None,
            _ => {}
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
            // UUID is compatible with String (PostgreSQL implicit cast from text literals)
            if matches!(
                (t1b, t2b),
                (BaseType::Uuid, BaseType::String) | (BaseType::String, BaseType::Uuid)
            ) {
                return Some(BaseType::Uuid.into());
            }
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
        // Prefer a specific concrete type (e.g. I32) over a generic base type
        // (e.g. Base(Integer)) when both share the same base.
        let is_concrete = |t: &Type<'_>| {
            !matches!(
                t,
                Type::Base(_) | Type::Args(_, _) | Type::Null | Type::Invalid
            )
        };
        match (is_concrete(t1), is_concrete(t2)) {
            (true, _) => Some(t1.clone()),
            (_, true) => Some(t2.clone()),
            _ => Some(t1b.into()),
        }
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

    pub(crate) fn ensure_datetime(
        &mut self,
        span: &impl Spanned,
        given: &FullType<'a>,
        date: Restrict,
        time: Restrict,
    ) -> Type<'a> {
        let (d, t) = match given.base() {
            BaseType::Any | BaseType::String => {
                let t = match (date, time) {
                    (Restrict::Disallow, Restrict::Require) => BaseType::Time,
                    (Restrict::Require, Restrict::Disallow) => BaseType::Date,
                    (Restrict::Require, Restrict::Require) => BaseType::DateTime,
                    _ => given.base(),
                };
                return Type::Base(t);
            }
            BaseType::Date => (true, false),
            BaseType::DateTime => (true, true),
            BaseType::Time => (false, true),
            BaseType::TimeStamp => (true, true),
            BaseType::Bool
            | BaseType::Bytes
            | BaseType::Float
            | BaseType::Integer
            | BaseType::TimeInterval
            | BaseType::Uuid => {
                self.issues
                    .err(format!("Expected time like type got {}", given.t), span);
                return Type::Invalid;
            }
        };
        match (date, d) {
            (Restrict::Disallow, true) => {
                self.issues
                    .err(format!("Date type now allowed got {}", given.t), span);
            }
            (Restrict::Require, false) => {
                self.issues
                    .err(format!("Date type required got {}", given.t), span);
            }
            _ => (),
        }
        match (time, t) {
            (Restrict::Disallow, true) => {
                self.issues
                    .err(format!("Time type now allowed got {}", given.t), span);
            }
            (Restrict::Require, false) => {
                self.issues
                    .err(format!("Time type required got {}", given.t), span);
            }
            _ => (),
        }
        given.t.clone()
    }

    pub(crate) fn ensure_base(
        &mut self,
        span: &impl Spanned,
        given: &FullType<'a>,
        expected: BaseType,
    ) {
        self.ensure_type(span, given, &FullType::new(expected, false));
    }

    /// Convert a parsed `QualifiedName` to a `QualifiedIdentifier` for use in map lookups.
    ///
    /// Unlike `table_key`, unqualified names are kept as `Unqualified` so the search path
    /// in `lookup_name` / `get_schema_by_key` resolves them — this avoids hardcoding "public".
    /// Emits an error for MySQL-qualified names or names with more than one prefix level.
    pub(crate) fn qname_to_key(&mut self, name: &QualifiedName<'a>) -> QualifiedIdentifier<'a> {
        let is_pg = self.dialect().is_postgresql();
        match name.prefix.as_slice() {
            [] => QualifiedIdentifier::Unqualified(name.identifier.clone()),
            [(schema, _)] if is_pg => {
                QualifiedIdentifier::Qualified(schema.clone(), name.identifier.clone())
            }
            _ => {
                let msg = if is_pg {
                    "Expected at most schema.table qualified name"
                } else {
                    "Schema-qualified names are not supported in MySQL"
                };
                self.issues.err(msg, &name.prefix.opt_span().unwrap());
                QualifiedIdentifier::Unqualified(name.identifier.clone())
            }
        }
    }

    /// The current search path: `["public"]` for PostgreSQL, empty for MySQL / MariaDB / SQLite.
    ///
    /// Used with `lookup_name` / `get_schema_by_key` when looking up `Unqualified` names.
    pub(crate) fn search_path(&self) -> &'static [&'static str] {
        if self.dialect().is_postgresql() {
            &["public"]
        } else {
            &[]
        }
    }

    /// Look up a table/view by its already-converted `QualifiedIdentifier` key.
    ///
    /// Checks CTEs (for `Unqualified` keys) before falling back to the schemas map
    /// with the current search path applied.
    pub(crate) fn get_schema_by_key(
        &self,
        key: &QualifiedIdentifier<'a>,
    ) -> Option<&'b Schema<'a>> {
        if let QualifiedIdentifier::Unqualified(name) = key {
            if let Some(s) = self.with_schemas.get(name.value) {
                return Some(s);
            }
        }
        lookup_name(&self.schemas.schemas, key, self.search_path())
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

/// Return the most similar candidate to `needle` from the given iterator,
/// or `None` if no candidate is within the edit-distance threshold.
///
/// The threshold is `max(1, needle.len() / 3)`, which admits one typo in a
/// short name and proportionally more in longer names.
pub(crate) fn did_you_mean<'a>(
    needle: &str,
    candidates: impl Iterator<Item = &'a str>,
) -> Option<&'a str> {
    let threshold = (needle.len() / 3).max(1);
    let n = needle.len();
    let needle_bytes = needle.as_bytes();
    let mut best: Option<(&'a str, usize)> = None;

    for candidate in candidates {
        let m = candidate.len();
        // Fast reject: if lengths differ by more than the threshold, skip.
        if n.abs_diff(m) > threshold {
            continue;
        }
        // Levenshtein distance with a single working row (O(m) space).
        let mut row: Vec<usize> = (0..=m).collect();
        for (i, &nc) in needle_bytes.iter().enumerate() {
            let mut prev = row[0];
            row[0] = i + 1;
            let cand_bytes = candidate.as_bytes();
            for j in 0..m {
                let temp = row[j + 1];
                row[j + 1] = if nc == cand_bytes[j] {
                    prev
                } else {
                    1 + prev.min(row[j]).min(temp)
                };
                prev = temp;
            }
        }
        let dist = row[m];
        if dist <= threshold {
            match best {
                None => best = Some((candidate, dist)),
                Some((_, bd)) if dist < bd => best = Some((candidate, dist)),
                _ => {}
            }
        }
    }
    best.map(|(s, _)| s)
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

/// Resolve a potentially schema-qualified table name from a `QualifiedName`.
///
/// Returns `(schema, table)` where `schema` is `None` for unqualified names.
/// Issues an error and returns `(None, table)` if:
/// - The name has more than one prefix level.
/// - The name has a schema prefix in a non-PostgreSQL dialect.
pub(crate) fn resolve_table_name<'b, 'c>(
    issues: &mut Issues<'_>,
    options: &TypeOptions,
    name: &'c QualifiedName<'b>,
) -> (Option<&'c Identifier<'b>>, &'c Identifier<'b>) {
    let is_pg = options.parse_options.get_dialect().is_postgresql();
    match name.prefix.as_slice() {
        [] => (None, &name.identifier),
        [(schema, _)] if is_pg => (Some(schema), &name.identifier),
        _ => {
            let msg = if is_pg {
                "Expected at most schema.table qualified name"
            } else {
                "Schema-qualified names are not supported in MySQL"
            };
            issues.err(msg, &name.prefix.opt_span().unwrap());
            (None, &name.identifier)
        }
    }
}
