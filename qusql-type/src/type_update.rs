use sql_ast::{Issue, OptSpanned, Update};

use crate::{
    type_::FullType,
    type_expression::{self, type_expression},
    type_reference::type_reference,
    typer::Typer,
};

pub(crate) fn type_update<'a>(typer: &mut Typer<'a>, update: &Update<'a>) {
    let old_reference_type = std::mem::take(&mut typer.reference_types);
    for f in &update.flags {
        match f {
            sql_ast::UpdateFlag::LowPriority(_) | sql_ast::UpdateFlag::Ignore(_) => (),
        }
    }

    for reference in &update.tables {
        type_reference(typer, reference, false);
    }

    for (key, value) in &update.set {
        let value_type = type_expression(typer, value, false);
        match key.len() {
            1 => {
                let mut cnt = 0;
                let mut t = None;
                for r in &typer.reference_types {
                    for c in &r.columns {
                        if c.0 == key[0].0 {
                            cnt += 1;
                            t = Some(c.clone());
                        }
                    }
                }
                if cnt > 1 {
                    let mut issue = Issue::err("Ambigious reference", &key.opt_span().unwrap());
                    for r in &typer.reference_types {
                        for c in &r.columns {
                            if c.0 == key[0].0 {
                                issue = issue.frag("Defined here", &r.name);
                            }
                        }
                    }
                    typer.issues.push(issue);
                } else if let Some(t) = t {
                    typer.ensure_type(value, &value_type, &t.1);
                } else {
                    typer
                        .issues
                        .push(Issue::err("Unknown identifier", &key.opt_span().unwrap()));
                }
            }
            2 => {
                let mut t = None;
                for r in &typer.reference_types {
                    if r.name.0 != key[0].0 {
                        continue;
                    }
                    for c in &r.columns {
                        if c.0 == key[1].0 {
                            t = Some(c.clone());
                        }
                    }
                }
                if let Some(t) = t {
                    typer.ensure_type(value, &value_type, &t.1);
                } else {
                    typer
                        .issues
                        .push(Issue::err("Unknown identifier", &key.opt_span().unwrap()));
                }
            }
            _ => {
                typer
                    .issues
                    .push(Issue::err("Unknown identifier", &key.opt_span().unwrap()));
            }
        }
    }

    if let Some((where_, _)) = &update.where_ {
        let t = type_expression(typer, where_, true);
        typer.ensure_bool(where_, &t);
    }

    typer.reference_types = old_reference_type;
}
