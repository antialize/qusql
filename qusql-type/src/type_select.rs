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
    Expression, Identifier, IdentifierPart, Issues, OptSpanned, Select, SelectExpr, Span, Spanned,
    Statement, Union, issue_ice, issue_todo,
};

use crate::{
    Type,
    type_::{BaseType, FullType},
    type_expression::{ExpressionFlags, type_expression},
    type_reference::type_reference,
    typer::{ReferenceType, Typer, typer_stack},
};

/// A column in select
#[derive(Debug, Clone)]
pub struct SelectTypeColumn<'a> {
    /// The name of the column if one is specified or can be computed
    pub name: Option<Identifier<'a>>,
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
    mut cb: impl FnMut(&mut Issues<'a>, Option<Identifier<'a>>, FullType<'a>, Span, bool),
) {
    match parts {
        [sql_parse::IdentifierPart::Name(col)] => {
            let mut cnt = 0;
            let mut t = None;
            for r in &typer.reference_types {
                for c in &r.columns {
                    if c.0 == *col {
                        cnt += 1;
                        t = Some(c);
                    }
                }
            }
            let name = as_.as_ref().unwrap_or(col);
            if cnt > 1 {
                let mut issue = typer.issues.err("Ambigious reference", col);
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == *col {
                            issue.frag("Defined here", &r.span);
                        }
                    }
                }
                cb(
                    typer.issues,
                    Some(name.clone()),
                    FullType::invalid(),
                    name.span(),
                    as_.is_some(),
                );
            } else if let Some(t) = t {
                cb(
                    typer.issues,
                    Some(name.clone()),
                    t.1.clone(),
                    name.span(),
                    as_.is_some(),
                );
            } else {
                typer.err("Unknown identifier", col);
                cb(
                    typer.issues,
                    Some(name.clone()),
                    FullType::invalid(),
                    name.span(),
                    as_.is_some(),
                );
            }
        }
        [sql_parse::IdentifierPart::Star(v)] => {
            if let Some(as_) = as_ {
                typer.err("As not supported for *", as_);
            }
            for r in &typer.reference_types {
                for c in &r.columns {
                    cb(
                        typer.issues,
                        Some(c.0.clone()),
                        c.1.clone(),
                        v.clone(),
                        false,
                    );
                }
            }
        }
        [
            sql_parse::IdentifierPart::Name(tbl),
            sql_parse::IdentifierPart::Name(col),
        ] => {
            let mut t = None;
            for r in &typer.reference_types {
                if r.name == Some(tbl.clone()) {
                    for c in &r.columns {
                        if c.0 == *col {
                            t = Some(c);
                        }
                    }
                }
            }
            let name = as_.as_ref().unwrap_or(col);
            if let Some(t) = t {
                cb(
                    typer.issues,
                    Some(name.clone()),
                    t.1.clone(),
                    name.span(),
                    as_.is_some(),
                );
            } else {
                typer.err("Unknown identifier", col);
                cb(
                    typer.issues,
                    Some(name.clone()),
                    FullType::invalid(),
                    name.span(),
                    as_.is_some(),
                );
            }
        }
        [
            sql_parse::IdentifierPart::Name(tbl),
            sql_parse::IdentifierPart::Star(v),
        ] => {
            if let Some(as_) = as_ {
                typer.err("As not supported for *", as_);
            }
            let mut t = None;
            for r in &typer.reference_types {
                if r.name == Some(tbl.clone()) {
                    t = Some(r);
                }
            }
            if let Some(t) = t {
                for c in &t.columns {
                    cb(
                        typer.issues,
                        Some(c.0.clone()),
                        c.1.clone(),
                        v.clone(),
                        false,
                    );
                }
            } else {
                typer.err("Unknown table", tbl);
            }
        }
        [sql_parse::IdentifierPart::Star(v), _] => {
            typer.err("Not supported here", v);
        }
        _ => {
            typer.err("Invalid identifier", &parts.opt_span().expect("parts span"));
        }
    }
}

pub(crate) fn type_select<'a>(
    typer: &mut Typer<'a, '_>,
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
            sql_parse::SelectFlag::All(_) => issue_todo!(typer.issues, flag),
            sql_parse::SelectFlag::Distinct(_) | sql_parse::SelectFlag::DistinctRow(_) => (),
            sql_parse::SelectFlag::StraightJoin(_) => issue_todo!(typer.issues, flag),
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

    let result = type_select_exprs(typer, &select.select_exprs, warn_duplicate);

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

    if let Some((having, _)) = &select.having {
        let t = type_expression(
            typer,
            having,
            ExpressionFlags::default()
                .with_not_null(true)
                .with_true(true),
            BaseType::Bool,
        );
        typer.ensure_base(having, &t, BaseType::Bool);
    }

    if let Some((_, offset, count)) = &select.limit {
        if let Some(offset) = offset {
            let t = type_expression(typer, offset, ExpressionFlags::default(), BaseType::Integer);
            if typer
                .matched_type(&t, &FullType::new(Type::U64, true))
                .is_none()
            {
                typer.err(format!("Expected integer type got {}", t.t), offset);
            }
        }
        let t = type_expression(typer, count, ExpressionFlags::default(), BaseType::Integer);
        if typer
            .matched_type(&t, &FullType::new(Type::U64, true))
            .is_none()
        {
            typer.err(format!("Expected integer type got {}", t.t), count);
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

pub(crate) fn type_select_exprs<'a, 'b>(
    typer: &mut Typer<'a, 'b>,
    select_exprs: &[SelectExpr<'a>],
    warn_duplicate: bool,
) -> Vec<(Option<Identifier<'a>>, FullType<'a>, Span)> {
    let mut result = Vec::new();
    let mut select_reference = ReferenceType {
        name: None,
        span: select_exprs.opt_span().expect("select_exprs span"),
        columns: Vec::new(),
    };

    for e in select_exprs {
        let mut add_result = |issues: &mut Issues<'a>,
                              name: Option<Identifier<'a>>,
                              type_: FullType<'a>,
                              span: Span,
                              as_: bool| {
            if let Some(name) = name.clone() {
                if as_ {
                    select_reference.columns.push((name.clone(), type_.clone()));
                }
                for (on, _, os) in &result {
                    if Some(name.clone()) == *on && warn_duplicate {
                        issues
                            .warn("Also defined here", &span)
                            .frag(format!("Multiple columns with the name '{name}'"), os);
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
                add_result(typer.issues, Some(as_.clone()), type_, as_.span(), true);
            } else {
                if typer.options.warn_unnamed_column_in_select {
                    typer.issues.warn("Unnamed column in select", e);
                }
                add_result(typer.issues, None, type_, 0..0, false);
            };
        }
    }

    typer.reference_types.push(select_reference);

    result
}

pub(crate) fn type_union<'a>(typer: &mut Typer<'a, '_>, union: &Union<'a>) -> SelectType<'a> {
    let mut t = type_union_select(typer, &union.left, true);
    let mut left = union.left.span();
    for w in &union.with {
        let t2 = type_union_select(typer, &w.union_statement, true);

        for i in 0..usize::max(t.columns.len(), t2.columns.len()) {
            if let Some(l) = t.columns.get_mut(i) {
                if let Some(r) = t2.columns.get(i) {
                    if l.name != r.name {
                        if let Some(ln) = &l.name {
                            if let Some(rn) = &r.name {
                                typer
                                    .err("Incompatible names in union", &w.union_span)
                                    .frag(format!("Column {i} is named {ln}"), &left)
                                    .frag(format!("Column {i} is named {rn}"), &w.union_statement);
                            } else {
                                typer
                                    .err("Incompatible names in union", &w.union_span)
                                    .frag(format!("Column {i} is named {ln}"), &left)
                                    .frag(format!("Column {i} has no name"), &w.union_statement);
                            }
                        } else {
                            typer
                                .err("Incompatible names in union", &w.union_span)
                                .frag(format!("Column {i} has no name"), &left)
                                .frag(
                                    format!(
                                        "Column {} is named {}",
                                        i,
                                        r.name.as_ref().expect("name")
                                    ),
                                    &w.union_statement,
                                );
                        }
                    }
                    if l.type_.t == r.type_.t {
                        l.type_ =
                            FullType::new(l.type_.t.clone(), l.type_.not_null && r.type_.not_null);
                    } else if let Some(t) = typer.matched_type(&l.type_, &r.type_) {
                        l.type_ = FullType::new(t, l.type_.not_null && r.type_.not_null);
                    } else {
                        typer
                            .err("Incompatible types in union", &w.union_span)
                            .frag(format!("Column {} is of type {}", i, l.type_.t), &left)
                            .frag(
                                format!("Column {} is of type {}", i, r.type_.t),
                                &w.union_statement,
                            );
                    }
                } else if let Some(n) = &l.name {
                    typer
                        .err("Incompatible types in union", &w.union_span)
                        .frag(format!("Column {i} ({n}) only on this side"), &left);
                } else {
                    typer
                        .err("Incompatible types in union", &w.union_span)
                        .frag(format!("Column {i} only on this side"), &left);
                }
            } else if let Some(n) = &t2.columns[i].name {
                typer
                    .err("Incompatible types in union", &w.union_span)
                    .frag(
                        format!("Column {i} ({n}) only on this side"),
                        &w.union_statement,
                    );
            } else {
                typer
                    .err("Incompatible types in union", &w.union_span)
                    .frag(format!("Column {i} only on this side"), &w.union_statement);
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
            .filter_map(|v| v.name.as_ref().map(|name| (name.clone(), v.type_.clone())))
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
                typer.err(format!("Expected integer type got {}", t.t), offset);
            }
        }
        let t = type_expression(typer, count, ExpressionFlags::default(), BaseType::Integer);
        if typer
            .matched_type(&t, &FullType::new(Type::U64, true))
            .is_none()
        {
            typer.err(format!("Expected integer type got {}", t.t), count);
        }
    }

    typer.reference_types.pop();

    t
}

pub(crate) fn type_union_select<'a>(
    typer: &mut Typer<'a, '_>,
    statement: &Statement<'a>,
    warn_duplicate: bool,
) -> SelectType<'a> {
    match statement {
        Statement::Select(s) => type_select(typer, s, warn_duplicate),
        Statement::Union(u) => type_union(typer, u),
        s => {
            issue_ice!(typer.issues, s);
            SelectType {
                columns: Vec::new(),
                select_span: s.span(),
            }
        }
    }
}
