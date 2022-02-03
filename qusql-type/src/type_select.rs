use sql_ast::{Expression, IdentifierPart, Issue, OptSpanned, Select, Span};

use crate::{
    type_::FullType,
    type_expression::type_expression,
    type_reference::type_reference,
    typer::{ReferenceType, Typer},
};

#[derive(Debug, Clone)]
pub struct SelectTypeColumn<'a> {
    pub name: Option<&'a str>,
    pub type_: FullType<'a>,
}

#[derive(Debug, Clone)]
pub struct SelectType<'a> {
    pub columns: Vec<SelectTypeColumn<'a>>,
}

pub(crate) fn resolve_kleene_identifier<'a>(
    typer: &mut Typer<'a>,
    parts: &[IdentifierPart<'a>],
    as_: &Option<(&'a str, Span)>,
    mut cb: impl FnMut(Option<&'a str>, FullType<'a>, Span, bool) -> (),
) {
    match parts.len() {
        1 => {
            match &parts[0] {
                sql_ast::IdentifierPart::Name(col) => {
                    let mut cnt = 0;
                    let mut t = None;
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            if c.0 == col.0 {
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
                                if c.0 == col.0 {
                                    issue = issue.frag("Defined here", &r.name);
                                }
                            }
                        }
                        typer.issues.push(issue);
                        cb(
                            Some(name.0),
                            FullType::invalid(),
                            name.1.clone(),
                            as_.is_some(),
                        );
                    } else if let Some(t) = t {
                        cb(Some(name.0), t.1.clone(), name.1.clone(), as_.is_some());
                    } else {
                        typer.issues.push(Issue::err("Unknown identifier", col));
                        cb(
                            Some(name.0),
                            FullType::invalid(),
                            name.1.clone(),
                            as_.is_some(),
                        );
                    }
                }
                sql_ast::IdentifierPart::Star(v) => {
                    if let Some(as_) = as_ {
                        typer.issues.push(Issue::err("As not supported for *", as_));
                    }
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            cb(Some(c.0), c.1.clone(), v.clone(), false);
                        }
                    }
                }
            };
        }
        2 => {
            let tbl = match &parts[0] {
                sql_ast::IdentifierPart::Name(n) => n,
                sql_ast::IdentifierPart::Star(v) => {
                    typer.issues.push(Issue::err("Not supported here", v));
                    return;
                }
            };
            match &parts[1] {
                sql_ast::IdentifierPart::Name(col) => {
                    let mut t = None;
                    for r in &typer.reference_types {
                        if r.name.0 == tbl.0 {
                            for c in &r.columns {
                                if c.0 == col.0 {
                                    t = Some(c);
                                }
                            }
                        }
                    }
                    let name = as_.as_ref().unwrap_or(col);
                    if let Some(t) = t {
                        cb(Some(name.0), t.1.clone(), name.1.clone(), as_.is_some());
                    } else {
                        typer.issues.push(Issue::err("Unknown identifier", col));
                        cb(
                            Some(name.0),
                            FullType::invalid(),
                            name.1.clone(),
                            as_.is_some(),
                        );
                    }
                }
                sql_ast::IdentifierPart::Star(v) => {
                    if let Some(as_) = as_ {
                        typer.issues.push(Issue::err("As not supported for *", as_));
                    }
                    let mut t = None;
                    for r in &typer.reference_types {
                        if r.name.0 == tbl.0 {
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
            }
        }
        _ => typer
            .issues
            .push(Issue::err("Invalid identifier", &parts.opt_span().unwrap())),
    }
}

pub(crate) fn type_select<'a>(
    typer: &mut Typer<'a>,
    select: &Select<'a>,
    warn_duplicate: bool,
) -> SelectType<'a> {
    let old_reference_type = typer.reference_types.clone();

    for flag in &select.flags {
        match &flag {
            sql_ast::SelectFlag::All(_) => typer.issues.push(Issue::todo(flag)),
            sql_ast::SelectFlag::Distinct(_) => typer.issues.push(Issue::todo(flag)),
            sql_ast::SelectFlag::DistinctRow(_) => typer.issues.push(Issue::todo(flag)),
            sql_ast::SelectFlag::StraightJoin(_) => typer.issues.push(Issue::todo(flag)),
            sql_ast::SelectFlag::HighPriority(_)
            | sql_ast::SelectFlag::SqlSmallResult(_)
            | sql_ast::SelectFlag::SqlBigResult(_)
            | sql_ast::SelectFlag::SqlBufferResult(_)
            | sql_ast::SelectFlag::SqlNoCache(_)
            | sql_ast::SelectFlag::SqlCalcFoundRows(_) => (),
        }
    }

    if let Some(references) = &select.table_references {
        for reference in references {
            type_reference(typer, reference, false);
        }
    }

    if let Some((where_, _)) = &select.where_ {
        let t = type_expression(typer, where_, true);
        typer.ensure_bool(where_, &t);
    }

    let mut result: Vec<(Option<&'a str>, FullType<'a>, Span)> = Vec::new();
    let mut select_refence = ReferenceType {
        name: ("", 0..0),
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
            let type_ = type_expression(typer, &e.expr, false);
            if let Some((as_, as_span)) = &e.as_ {
                add_result(Some(*as_), type_, as_span.clone(), true);
            } else {
                typer
                    .issues
                    .push(Issue::warn("Unnamed column in select", e));
                add_result(None, type_, 0..0, false);
            };
        }
    }
    typer.issues.extend(add_result_issues.into_iter());
    typer.reference_types.push(select_refence);

    if let Some((_, group_by)) = &select.group_by {
        for e in group_by {
            type_expression(typer, e, false);
        }
    }

    if let Some((_, order_by)) = &select.order_by {
        for (e, _) in order_by {
            type_expression(typer, e, false);
        }
    }

    if let Some((limit_spam, _, _)) = &select.limit {
        typer.issues.push(Issue::todo(limit_spam));
    }

    typer.reference_types = old_reference_type;

    SelectType {
        columns: result
            .into_iter()
            .map(|(name, type_, _)| SelectTypeColumn { name, type_ })
            .collect(),
    }
}
