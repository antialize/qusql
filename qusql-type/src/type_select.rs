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

use alloc::{format, vec::Vec};
use sql_parse::{
    issue_ice, issue_todo, Expression, Identifier, IdentifierPart, Issue, OptSpanned, Select, Span,
    Spanned, Statement, Union,
};

use crate::{
    type_::{BaseType, FullType},
    type_expression::{type_expression, ExpressionFlags},
    type_reference::type_reference,
    typer::{typer_stack, ReferenceType, Typer},
    Type,
};

/// A column in select
#[derive(Debug, Clone)]
pub struct SelectTypeColumn<'a> {
    /// The name of the column if one is specified or can be computed
    pub name: Option<&'a str>,
    /// The type of the data
    pub type_: FullType<'a>,
    /// A span of the expression yielding the column
    pub span: Span,
}

impl<'a> Spanned for SelectTypeColumn<'a> {
    fn span(&self) -> Span {
        self.span.span()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SelectType<'a> {
    pub columns: Vec<SelectTypeColumn<'a>>,
    pub select_span: Span,
}

impl<'a> Spanned for SelectType<'a> {
    fn span(&self) -> Span {
        self.columns
            .opt_span()
            .unwrap_or_else(|| self.select_span.clone())
    }
}

pub(crate) fn resolve_kleene_identifier<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    parts: &[IdentifierPart<'a>],
    as_: &Option<Identifier<'a>>,
    mut cb: impl FnMut(Option<&'a str>, FullType<'a>, Span, bool),
) {
    match parts {
        [sql_parse::IdentifierPart::Name(col)] => {
            let mut cnt = 0;
            let mut t = None;
            for r in &typer.reference_types {
                for c in &r.columns {
                    if c.0 == col.value {
                        cnt += 1;
                        t = Some(c);
                    }
                }
            }
            let name = as_.as_ref().unwrap_or(col);
            if cnt > 1 {
                let mut issue = Issue::err("Ambigious reference", col);
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == col.value {
                            issue = issue.frag("Defined here", &r.span);
                        }
                    }
                }
                typer.issues.push(issue);
                cb(
                    Some(name.value),
                    FullType::invalid(),
                    name.span(),
                    as_.is_some(),
                );
            } else if let Some(t) = t {
                cb(Some(name.value), t.1.clone(), name.span(), as_.is_some());
            } else {
                typer.issues.push(Issue::err("Unknown identifier", col));
                cb(
                    Some(name.value),
                    FullType::invalid(),
                    name.span(),
                    as_.is_some(),
                );
            }
        }
        [sql_parse::IdentifierPart::Star(v)] => {
            if let Some(as_) = as_ {
                typer.issues.push(Issue::err("As not supported for *", as_));
            }
            for r in &typer.reference_types {
                for c in &r.columns {
                    cb(Some(c.0), c.1.clone(), v.clone(), false);
                }
            }
        }
        [sql_parse::IdentifierPart::Name(tbl), sql_parse::IdentifierPart::Name(col)] => {
            let mut t = None;
            for r in &typer.reference_types {
                if r.name == Some(tbl.value) {
                    for c in &r.columns {
                        if c.0 == col.value {
                            t = Some(c);
                        }
                    }
                }
            }
            let name = as_.as_ref().unwrap_or(col);
            if let Some(t) = t {
                cb(Some(name.value), t.1.clone(), name.span(), as_.is_some());
            } else {
                typer.issues.push(Issue::err("Unknown identifier", col));
                cb(
                    Some(name.value),
                    FullType::invalid(),
                    name.span(),
                    as_.is_some(),
                );
            }
        }
        [sql_parse::IdentifierPart::Name(tbl), sql_parse::IdentifierPart::Star(v)] => {
            if let Some(as_) = as_ {
                typer.issues.push(Issue::err("As not supported for *", as_));
            }
            let mut t = None;
            for r in &typer.reference_types {
                if r.name == Some(tbl.value) {
                    t = Some(r);
                }
            }
            if let Some(t) = t {
                for c in &t.columns {
                    cb(Some(c.0), c.1.clone(), v.clone(), false);
                }
            } else {
                typer.issues.push(Issue::err("Unknown table", tbl));
            }
        }
        [sql_parse::IdentifierPart::Star(v), _] => {
            typer.issues.push(Issue::err("Not supported here", v));
        }
        _ => typer.issues.push(Issue::err(
            "Invalid identifier",
            &parts.opt_span().expect("parts span"),
        )),
    }
}

pub(crate) fn type_select<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    select: &Select<'a>,
    warn_duplicate: bool,
) -> SelectType<'a> {
    let mut guard = typer_stack(
        typer,
        |t| t.reference_types.clone(),
        |t, v| t.reference_types = v,
    );
    let typer = &mut guard.typer;

    for flag in &select.flags {
        match &flag {
            sql_parse::SelectFlag::All(_) => typer.issues.push(issue_todo!(flag)),
            sql_parse::SelectFlag::Distinct(_) | sql_parse::SelectFlag::DistinctRow(_) => (),
            sql_parse::SelectFlag::StraightJoin(_) => typer.issues.push(issue_todo!(flag)),
            sql_parse::SelectFlag::HighPriority(_)
            | sql_parse::SelectFlag::SqlSmallResult(_)
            | sql_parse::SelectFlag::SqlBigResult(_)
            | sql_parse::SelectFlag::SqlBufferResult(_)
            | sql_parse::SelectFlag::SqlNoCache(_)
            | sql_parse::SelectFlag::SqlCalcFoundRows(_) => (),
        }
    }

    if let Some(references) = &select.table_references {
        for reference in references {
            type_reference(typer, reference, false);
        }
    }

    if let Some((where_, _)) = &select.where_ {
        let t = type_expression(
            typer,
            where_,
            ExpressionFlags::default()
                .with_not_null(true)
                .with_true(true),
            BaseType::Bool,
        );
        typer.ensure_base(where_, &t, BaseType::Bool);
    }

    let mut result: Vec<(Option<&'a str>, FullType<'a>, Span)> = Vec::new();
    let mut select_refence = ReferenceType {
        name: None,
        span: select.select_exprs.opt_span().expect("select_exprs span"),
        columns: Vec::new(),
    };

    let mut add_result_issues = Vec::new();

    for e in &select.select_exprs {
        let mut add_result = |name: Option<&'a str>, type_: FullType<'a>, span: Span, as_: bool| {
            if let Some(name) = name {
                if as_ {
                    select_refence.columns.push((name, type_.clone()));
                }
                for (on, _, os) in &result {
                    if Some(name) == *on && warn_duplicate {
                        add_result_issues.push(
                            Issue::warn(
                                format!("Multiple columns with the name '{}'", name),
                                &span,
                            )
                            .frag("Also defined here", os),
                        );
                    }
                }
            }
            result.push((name, type_, span));
        };
        if let Expression::Identifier(parts) = &e.expr {
            resolve_kleene_identifier(typer, parts, &e.as_, add_result);
        } else {
            let type_ = type_expression(typer, &e.expr, ExpressionFlags::default(), BaseType::Any);
            if let Some(as_) = &e.as_ {
                add_result(Some(as_.value), type_, as_.span(), true);
            } else {
                if typer.options.warn_unnamed_column_in_select {
                    typer
                        .issues
                        .push(Issue::warn("Unnamed column in select", e));
                }
                add_result(None, type_, 0..0, false);
            };
        }
    }
    typer.issues.extend(add_result_issues.into_iter());
    typer.reference_types.push(select_refence);

    if let Some((_, group_by)) = &select.group_by {
        for e in group_by {
            type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
        }
    }

    if let Some((_, order_by)) = &select.order_by {
        for (e, _) in order_by {
            type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
        }
    }

    if let Some((_, offset, count)) = &select.limit {
        if let Some(offset) = offset {
            let t = type_expression(typer, offset, ExpressionFlags::default(), BaseType::Integer);
            if typer
                .matched_type(&t, &FullType::new(Type::U64, true))
                .is_none()
            {
                typer.issues.push(Issue::err(
                    format!("Expected integer type got {}", t.t),
                    offset,
                ));
            }
        }
        let t = type_expression(typer, count, ExpressionFlags::default(), BaseType::Integer);
        if typer
            .matched_type(&t, &FullType::new(Type::U64, true))
            .is_none()
        {
            typer.issues.push(Issue::err(
                format!("Expected integer type got {}", t.t),
                count,
            ));
        }
    }

    SelectType {
        columns: result
            .into_iter()
            .map(|(name, type_, span)| SelectTypeColumn { name, type_, span })
            .collect(),
        select_span: select.span(),
    }
}

pub(crate) fn type_union<'a, 'b>(typer: &mut Typer<'a, 'b>, union: &Union<'a>) -> SelectType<'a> {
    let mut t = type_union_select(typer, &union.left, true);
    let mut left = union.left.span();
    for w in &union.with {
        let t2 = type_union_select(typer, &w.union_statement, true);

        for i in 0..usize::max(t.columns.len(), t2.columns.len()) {
            if let Some(l) = t.columns.get_mut(i) {
                if let Some(r) = t2.columns.get(i) {
                    if l.name != r.name {
                        if let Some(ln) = l.name {
                            if let Some(rn) = r.name {
                                typer.issues.push(
                                    Issue::err("Incompatible names in union", &w.union_span)
                                        .frag(format!("Column {} is named {}", i, ln), &left)
                                        .frag(
                                            format!("Column {} is named {}", i, rn),
                                            &w.union_statement,
                                        ),
                                );
                            } else {
                                typer.issues.push(
                                    Issue::err("Incompatible names in union", &w.union_span)
                                        .frag(format!("Column {} is named {}", i, ln), &left)
                                        .frag(
                                            format!("Column {} has no name", i),
                                            &w.union_statement,
                                        ),
                                );
                            }
                        } else {
                            typer.issues.push(
                                Issue::err("Incompatible names in union", &w.union_span)
                                    .frag(format!("Column {} has no name", i), &left)
                                    .frag(
                                        format!("Column {} is named {}", i, r.name.expect("name")),
                                        &w.union_statement,
                                    ),
                            );
                        }
                    }
                    if l.type_.t == r.type_.t {
                        l.type_ =
                            FullType::new(l.type_.t.clone(), l.type_.not_null && r.type_.not_null);
                    } else if let Some(t) = typer.matched_type(&l.type_, &r.type_) {
                        l.type_ = FullType::new(t, l.type_.not_null && r.type_.not_null);
                    } else {
                        typer.issues.push(
                            Issue::err("Incompatible types in union", &w.union_span)
                                .frag(format!("Column {} is of type {}", i, l.type_.t), &left)
                                .frag(
                                    format!("Column {} is of type {}", i, r.type_.t),
                                    &w.union_statement,
                                ),
                        );
                    }
                } else if let Some(n) = l.name {
                    typer.issues.push(
                        Issue::err("Incompatible types in union", &w.union_span)
                            .frag(format!("Column {} ({}) only on this side", i, n), &left),
                    );
                } else {
                    typer.issues.push(
                        Issue::err("Incompatible types in union", &w.union_span)
                            .frag(format!("Column {} only on this side", i), &left),
                    );
                }
            } else if let Some(n) = t2.columns[i].name {
                typer.issues.push(
                    Issue::err("Incompatible types in union", &w.union_span).frag(
                        format!("Column {} ({}) only on this side", i, n),
                        &w.union_statement,
                    ),
                );
            } else {
                typer.issues.push(
                    Issue::err("Incompatible types in union", &w.union_span).frag(
                        format!("Column {} only on this side", i),
                        &w.union_statement,
                    ),
                );
            }
        }
        left = left.join_span(&w.union_statement);
    }

    typer.reference_types.push(ReferenceType {
        name: None,
        span: t.span(),
        columns: t
            .columns
            .iter()
            .filter_map(|v| v.name.map(|name| (name, v.type_.clone())))
            .collect(),
    });

    if let Some((_, order_by)) = &union.order_by {
        for (e, _) in order_by {
            type_expression(typer, e, ExpressionFlags::default(), BaseType::Any);
        }
    }

    if let Some((_, offset, count)) = &union.limit {
        if let Some(offset) = offset {
            let t = type_expression(typer, offset, ExpressionFlags::default(), BaseType::Integer);
            if typer
                .matched_type(&t, &FullType::new(Type::U64, true))
                .is_none()
            {
                typer.issues.push(Issue::err(
                    format!("Expected integer type got {}", t.t),
                    offset,
                ));
            }
        }
        let t = type_expression(typer, count, ExpressionFlags::default(), BaseType::Integer);
        if typer
            .matched_type(&t, &FullType::new(Type::U64, true))
            .is_none()
        {
            typer.issues.push(Issue::err(
                format!("Expected integer type got {}", t.t),
                count,
            ));
        }
    }

    typer.reference_types.pop();

    t
}

pub(crate) fn type_union_select<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    statement: &Statement<'a>,
    warn_duplicate: bool,
) -> SelectType<'a> {
    match statement {
        Statement::Select(s) => type_select(typer, s, warn_duplicate),
        Statement::Union(u) => type_union(typer, u),
        s => {
            typer.issues.push(issue_ice!(s));
            SelectType {
                columns: Vec::new(),
                select_span: s.span(),
            }
        }
    }
}
