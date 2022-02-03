use std::{collections::{HashMap, HashSet}, borrow::Cow};

use schema::Schemas;
use sql_ast::{parse_statements, Spanned, OptSpanned, Span, CreateDefinition, DataType, Statement, Select, TableReference, Expression, BinaryOperator, Delete, Function, Insert};
pub use sql_ast::{Issue, Level};


mod type_;
mod ref_or_val;
pub mod schema;
use type_::FullType;
pub use type_::Type;
pub use ref_or_val::RefOrVal;



struct ReferenceType<'a> {
    name: (&'a str, Span),
    columns: Vec<(&'a str, FullType<'a>)>,
}

struct Typer<'a> {
    issues: &'a mut Vec<Issue>,
    schemas: &'a Schemas<'a>,
    reference_types: Vec<ReferenceType<'a>>,
}

#[derive(Debug, Clone)]
pub struct SelectTypeColumn<'a> {
    pub name: Option<&'a str>,
    pub type_: FullType<'a>,
}

#[derive(Debug, Clone)]
pub struct SelectType<'a> {
    columns: Vec<SelectTypeColumn<'a>>
}

impl<'a> Typer<'a> {
    fn type_reference(&mut self, reference: &TableReference<'a>, force_null: bool) {
        match reference {
            sql_ast::TableReference::Table { identifier, as_, .. } => {
                if identifier.len() != 1 {
                    self.issues.push(Issue::todo(reference));
                    return;
                }
                let identifier = &identifier[0];
                if let Some(s) = self.schemas.schemas.get(&identifier.0) {
                    let mut columns = Vec::new();
                    for (n, t) in &s.columns {
                        let mut type_ = t.type_.ref_clone();
                        type_.not_null = type_.not_null && !force_null;
                        columns.push((
                            *n,
                            type_
                        ));
                    }
                    let name= as_.as_ref().unwrap_or(identifier).clone();
                    for v in &self.reference_types {
                        if v.name.0 == name.0 {
                            self.issues.push(Issue::err("Duplicate definitions", &name).frag("Allready defined here", &v.name));
                        }
                    }
                    self.reference_types.push(ReferenceType{
                        name,
                        columns
                    });
                } else {
                    self.issues.push(Issue::err("Unknown table or view", identifier))
                }
            },
            sql_ast::TableReference::Query { query, as_span, as_ } => {
                //     Query {
//         query: Box<Statement<'a>>,
//         as_span: Option<Span>,
//         as_: Option<(&'a str, Span)>,
//         //TODO collist
//     },
                self.issues.push(Issue::todo(reference));
            },
            sql_ast::TableReference::Join { join, left, right, specification } => {
                let (left_force_null, right_force_null) = match join {
                    sql_ast::JoinType::Left(_) => (force_null, true),
                    _ => {
                        self.issues.push(Issue::todo(join));
                        (force_null, force_null)
                    }
                };
                self.type_reference(left, left_force_null);
                self.type_reference(right, right_force_null);
                match &specification {
                    Some(s @ sql_ast::JoinSpecification::On(e, r)) => {
                        let t = self.type_expression(e, false);
                        self.ensure_bool(e, &t.t);
                    }
                    Some(s @ sql_ast::JoinSpecification::Using(_, _)) => {
                        self.issues.push(Issue::todo(s));
                    }
                    None => (),
                }
            },
        }
    }

    fn constrain_arg(&mut self, idx: usize, type_: &Type<'a>, not_null: bool) {
        //TODO
    }

    fn ensure_bool(&mut self, span: &impl Spanned, given: &Type<'a>) {
        match given {
            Type::Bool | Type::Invalid => (),
            Type::Arg(idx, span) => {
                self.constrain_arg(*idx, &Type::Bool, false);
            } _ => {
                self.issues.push(Issue::err(format!("Expected type bool got {:?}", given), span));
            }
        }
    }

    fn ensure_text(&mut self, span: &impl Spanned, given: &Type<'a>) {
        match given {
            Type::Text | Type::Enum(_) | Type::Invalid => (),
            Type::Arg(idx, span) => {
                self.constrain_arg(*idx, &Type::Text, false);
            }
            _ => {
                self.issues.push(Issue::err(format!("Expected text bool got {:?}", given), span));
            }
        }
    }

    fn type_binary_expression(&mut self, op: &BinaryOperator, op_span: &Span, lhs: &Expression<'a>, rhs: &Expression<'a>, outer_where: bool) -> FullType<'a> {
        let outer_where = matches!(op, BinaryOperator::And) && outer_where;
        let lhs_type = self.type_expression(lhs, outer_where);
        let rhs_type = self.type_expression(rhs, outer_where);
        match op {
            BinaryOperator::Or | BinaryOperator::Xor | BinaryOperator::And => {
                self.ensure_bool(lhs, &lhs_type.t);
                self.ensure_bool(rhs, &rhs_type.t);
                FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
            },
            BinaryOperator::Eq | BinaryOperator::Neq | BinaryOperator::GtEq | BinaryOperator::Gt | BinaryOperator::LtEq | BinaryOperator::Lt => {
                if lhs_type.t == Type::Null {
                    self.issues.push(Issue::warn("Comparison with null", lhs));
                }
                if rhs_type.t == Type::Null{
                    self.issues.push(Issue::warn("Comparison with null", rhs));
                }

                let ok = match &lhs_type.t {
                    Type::U8 |
                    Type::I8 |
                    Type::U16 |
                    Type::I16 |
                    Type::U32 |
                    Type::I32 |
                    Type::U64 |
                    Type::I64 |
                    Type::F32 |
                    Type::F64 |
                    Type::Integer |
                    Type::Float => {
                        match &rhs_type.t {
                            Type::U8 |
                            Type::I8 |
                            Type::U16 |
                            Type::I16 |
                            Type::U32 |
                            Type::I32 |
                            Type::U64 |
                            Type::I64 |
                            Type::F32 |
                            Type::F64 |
                            Type::Integer |
                            Type::Float |
                            Type::Null |
                            Type::Invalid => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.t, lhs_type.not_null);
                                true
                            },
                            _ => false,
                        }
                    }
                    Type::Text => {
                        match &rhs_type.t {
                            Type::Text | Type::Null | Type::Invalid | Type::Enum(_) => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.t, lhs_type.not_null);
                                true
                            }
                            _ => false,
                        }
                    }
                    Type::Bytes => {
                        match &rhs_type.t {
                            Type::Bytes | Type::Null | Type::Invalid => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.t, lhs_type.not_null);
                                true
                            }
                            _ => false,
                        }
                    }
                    Type::Time => {self.issues.push(Issue::todo(lhs)); false}
                    Type::DateTime => {self.issues.push(Issue::todo(lhs)); false}
                    Type::Timestamp => {self.issues.push(Issue::todo(lhs)); false}
                    Type::Date => {self.issues.push(Issue::todo(lhs)); false}
                    Type::Null => true,
                    Type::Invalid => true,
                    Type::Bool => {
                        match &rhs_type.t {
                            Type::Bool | Type::Null | Type::Invalid => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.t, lhs_type.not_null);
                                true
                            }
                            _ => false,
                        }
                    }
                    Type::Arg(_, _) => {
                        //TODO constraint arg
                        true
                    }
                    Type::Enum(e) => {
                        match &rhs_type.t {
                            Type::Text | Type::Null | Type::Invalid => true,
                            Type::Enum(e2) => !e.is_disjoint(e2),
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.t, lhs_type.not_null);
                                true
                            }
                            _ => false,
                        }
                    }
                    Type::Set(_) => {self.issues.push(Issue::todo(lhs)); false}
                };
                if !ok {
                    self.issues.push(
                        Issue::err("Type error in comparison", op_span)
                            .frag(format!("Of type {:?}", lhs_type.t), lhs)
                            .frag(format!("Of type {:?}", rhs_type.t), rhs)
                    );
                }
                FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
            },
            BinaryOperator::NullSafeEq => {
                self.issues.push(Issue::todo(op_span));
                FullType::invalid()
            },
            BinaryOperator::ShiftLeft | BinaryOperator::ShiftRight => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Add => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Subtract => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Divide => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Div => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Mod => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Mult => {
                self.issues.push(Issue::todo(op_span));
                 FullType::invalid()
            },
            BinaryOperator::Like | BinaryOperator::NotLike => {
                self.ensure_text(lhs, &lhs_type.t);
                self.ensure_text(rhs, &rhs_type.t);
                FullType::new(Type::Bool, lhs_type.not_null && rhs_type.not_null)
            },
        }
    }

    fn type_function(&mut self, func: &Function<'a>, args: &Vec<Expression<'a>>, span: &Span) -> FullType<'a> {
        let mut arg_types = Vec::new();
        for arg in args {
            arg_types.push(self.type_expression(arg, false));
        }
        match func {
            Function::UnixTimestamp => {
                if let Some(args) = args.get(1..) {
                    for arg in args {
                        self.issues.push(Issue::err("Unexpected argument", arg));
                    }
                }
                if let Some(arg) = args.get(0) {
                    self.issues.push(Issue::todo(arg));
                }
                FullType::new(Type::U64 , true)
            },
            Function::Rand => {
                if let Some(args) = args.get(1..) {
                    for arg in args {
                        self.issues.push(Issue::err("Unexpected argument", arg));
                    }
                }
                if let Some(arg) = args.get(0) {
                    self.issues.push(Issue::todo(arg));
                }
                FullType::new(Type::F64 , true)
            }
            _ => {
                self.issues.push(Issue::todo(span));
                FullType::invalid()
            }
        }
    }

    fn type_expression(&mut self, expression: &Expression<'a>, outer_where: bool) -> FullType<'a> {
        match expression {
            Expression::Binary { op, op_span, lhs, rhs } => {
                self.type_binary_expression(op, op_span, lhs, rhs, outer_where)
            },
            Expression::Unary { op, op_span, operand } => {
                self.issues.push(Issue::todo(expression));
                FullType::invalid()
            },
            Expression::Subquery(_) => {
                self.issues.push(Issue::todo(expression));
                FullType::invalid()
            },
            Expression::Null(_) => FullType::new(Type::Null, false),
            Expression::Bool(_, _) => FullType::new(Type::Bool, true),
            Expression::String(_) => FullType::new(Type::Text, true),
            Expression::Integer(_) => {
                FullType::new(Type::Integer, true)
            },
            Expression::Float(_) => {
                self.issues.push(Issue::todo(expression));
                FullType::new(Type::Float, true)
            },
            Expression::Function(func, args, span) => {
                self.type_function(func, args, span)
            }
            Expression::Identifier(i) => {
                let mut t = None;
                match i.len() {
                    1 => {
                        let col = match &i[0] {
                            sql_ast::IdentifierPart::Name(n) => n,
                            sql_ast::IdentifierPart::Star(v) => {
                                self.issues.push(Issue::err("Not supported here", v));
                                return FullType::invalid();
                            }
                        };
                        let mut cnt = 0;
                        for r in &self.reference_types {
                            for c in &r.columns {
                                if c.0 == col.0 {
                                    cnt += 1;
                                    t = Some(c);
                                }
                            }
                        }
                        if cnt > 1 {
                            let mut issue = Issue::err(
                                "Ambigious reference", col);
                            for r in &self.reference_types {
                                for c in &r.columns {
                                    if c.0 == col.0 {
                                        issue = issue.frag("Defined here", &r.name);
                                    }
                                }
                            }
                            self.issues.push(issue);
                            return FullType::invalid();
                        }
                    }
                    2 => {
                        let tbl = match &i[0] {
                            sql_ast::IdentifierPart::Name(n) => n,
                            sql_ast::IdentifierPart::Star(v) => {
                                self.issues.push(Issue::err("Not supported here", v));
                                return FullType::invalid();
                            }
                        };
                        let col = match &i[1] {
                            sql_ast::IdentifierPart::Name(n) => n,
                            sql_ast::IdentifierPart::Star(v) => {
                                self.issues.push(Issue::err("Not supported here", v));
                                return FullType::invalid();
                            }
                        };
                        for r in &self.reference_types {
                            if r.name.0 == tbl.0 {
                                for c in &r.columns {
                                    if c.0 == col.0 {
                                        t = Some(c);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        self.issues.push(Issue::err("Bad identifier length", expression));
                        return FullType::invalid();
                    }
                }
                match t {
                    None => {
                        self.issues.push(Issue::err("Unknown identifier", expression));
                        FullType::invalid()
                    }
                    Some((_, type_)) => {
                        type_.clone()
                    }
                }
            },
            Expression::Arg((idx, span)) => {
                FullType::new(Type::Arg(*idx, span.clone()), false)
            },
            Expression::Exists(_) => {
                self.issues.push(Issue::todo(expression));
                FullType::invalid()
            },
            Expression::In { lhs, rhs, in_span, not_in } => {
                self.issues.push(Issue::todo(expression));
                FullType::invalid()
            },
            Expression::Is(e, is, _) => {
                let t = self.type_expression(e, false);
                match is {
                    sql_ast::Is::Null => {
                        if t.not_null {
                            self.issues.push(Issue::warn("Is newer null", e));
                        }
                        FullType::new(Type::Bool, true)
                    },
                    sql_ast::Is::NotNull => {
                        if t.not_null {
                            self.issues.push(Issue::warn("Is newer null", e));
                        }
                        if outer_where {
                            // If were are in the outer part of a where expression possibly behind ands,
                            // and the expression is an identifier, we can mark the columns not_null
                            // the reference_types
                            if let Expression::Identifier(parts) = e.as_ref() {
                                if let Some(sql_ast::IdentifierPart::Name((n0, _))) = parts.get(0) {
                                    if parts.len() == 1 {
                                        for r in &mut self.reference_types {
                                            for c in &mut r.columns {
                                                if &c.0 == n0 {
                                                    c.1.not_null = true;
                                                }
                                            }
                                        }
                                    } else  if let Some(sql_ast::IdentifierPart::Name((n1, _))) = parts.get(1) {
                                        for r in &mut self.reference_types {
                                            if &r.name.0 == n0 {
                                                for c in &mut r.columns {
                                                    if &c.0 == n1 {
                                                        c.1.not_null = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        FullType::new(Type::Bool, true)
                    },
                    sql_ast::Is::True |
                    sql_ast::Is::NotTrue |
                    sql_ast::Is::False |
                    sql_ast::Is::NotFalse |
                    sql_ast::Is::Unknown |
                    sql_ast::Is::NotUnknown => {
                        self.issues.push(Issue::todo(expression));
                        FullType::invalid()
                    }
                }
            },
            Expression::Invalid => {
                FullType::invalid()
            },
            Expression::Case { case_span, value, whens, else_, end_span } => {
                self.issues.push(Issue::todo(expression));
                FullType::invalid()
            },
        }
    }

    fn type_select(&mut self, select: &Select<'a>) -> SelectType<'a> {
        let old_reference_type = std::mem::take(&mut self.reference_types);

        for flag in &select.flags {
            match &flag {
                sql_ast::SelectFlag::All(_) => self.issues.push(Issue::todo(flag)),
                sql_ast::SelectFlag::Distinct(_) => self.issues.push(Issue::todo(flag)),
                sql_ast::SelectFlag::DistinctRow(_) => self.issues.push(Issue::todo(flag)),
                sql_ast::SelectFlag::StraightJoin(_) => self.issues.push(Issue::todo(flag)),
                sql_ast::SelectFlag::HighPriority(_) |
                sql_ast::SelectFlag::SqlSmallResult(_) |
                sql_ast::SelectFlag::SqlBigResult(_) |
                sql_ast::SelectFlag::SqlBufferResult(_) |
                sql_ast::SelectFlag::SqlNoCache(_) |
                sql_ast::SelectFlag::SqlCalcFoundRows(_) => ()
            }
        }

        if let Some(references) = &select.table_references {
            for reference in references {
                self.type_reference(reference, false);
            }
        }

        if let Some((where_, _)) = &select.where_ {
            let t = self.type_expression(where_, true);
            self.ensure_bool(where_, &t.t);
        }

        let mut result: Vec<(Option<&'a str>, FullType<'a>, Span)> = Vec::new();
        let mut select_refence = ReferenceType {
            name: ("", 0..0),
            columns: Vec::new(),
        };

        let mut add_result_issues = Vec::new();
        let mut add_result = |name: Option<&'a str>, type_: FullType<'a>, span: Span, as_: bool| {
            if let Some(name) = name {
                if as_ {
                    select_refence.columns.push(
                        (name, type_.clone())
                    );
                }
                for (on, _, os) in &result {
                    if Some(name) == *on {
                        add_result_issues.push(Issue::warn(format!("Multiple columns with the name '{}'", name), &span).frag("Also defined here", os));
                    }
                }
            }
            result.push((name, type_, span));
        };

        for e in &select.select_exprs {
            if let Expression::Identifier(parts) = &e.expr {
                match parts.len() {
                    1 => {
                        match &parts[0] {
                            sql_ast::IdentifierPart::Name(col) => {
                                let mut cnt = 0;
                                let mut t = None;
                                for r in &self.reference_types {
                                    for c in &r.columns {
                                        if c.0 == col.0 {
                                            cnt += 1;
                                            t = Some(c);
                                        }
                                    }
                                }
                                let name = e.as_.as_ref().unwrap_or(col);
                                if cnt > 1 {
                                    let mut issue = Issue::err(
                                        "Ambigious reference", col);
                                    for r in &self.reference_types {
                                        for c in &r.columns {
                                            if c.0 == col.0 {
                                                issue = issue.frag("Defined here", &r.name);
                                            }
                                        }
                                    }
                                    self.issues.push(issue);
                                    add_result(Some(name.0), FullType::invalid(), name.1.clone(), e.as_.is_some());
                                } else if let Some(t) = t {
                                    add_result(Some(name.0), t.1.clone(), name.1.clone(), e.as_.is_some());
                                } else {
                                    self.issues.push(Issue::err("Unknown identifier", col));
                                    add_result(Some(name.0), FullType::invalid(), name.1.clone(), e.as_.is_some());
                                }
                            }
                            sql_ast::IdentifierPart::Star(v) => {
                                if let Some(as_) = &e.as_ {
                                    self.issues.push(Issue::err("As not supported for *", as_));
                                }
                                for r in &self.reference_types {
                                    for c in &r.columns {
                                        add_result(Some(c.0), c.1.clone(), v.clone(), false);
                                    }
                                }
                            }
                        };
                    }
                    2 => {
                        let tbl = match &parts[0] {
                            sql_ast::IdentifierPart::Name(n) => n,
                            sql_ast::IdentifierPart::Star(v) => {
                                self.issues.push(Issue::err("Not supported here", v));
                                continue;
                            }
                        };
                        match &parts[1] {
                            sql_ast::IdentifierPart::Name(col) => {
                                let mut t = None;
                                for r in &self.reference_types {
                                    if r.name.0 == tbl.0 {
                                        for c in &r.columns {
                                            if c.0 == col.0 {
                                                t = Some(c);
                                            }
                                        }
                                    }
                                }
                                let name = e.as_.as_ref().unwrap_or(col);
                                if let Some(t) = t {
                                    add_result(Some(name.0), t.1.clone(), name.1.clone(), e.as_.is_some());
                                } else {
                                    self.issues.push(Issue::err("Unknown identifier", col));
                                    add_result(Some(name.0), FullType::invalid(), name.1.clone(), e.as_.is_some());
                                }
                            }
                            sql_ast::IdentifierPart::Star(v) => {
                                if let Some(as_) = &e.as_ {
                                    self.issues.push(Issue::err("As not supported for *", as_));
                                }
                                let mut t = None;
                                for r in &self.reference_types {
                                    if r.name.0 == tbl.0 {
                                        t=Some(r);
                                    }
                                }
                                if let Some(t) = t {
                                    for c in &t.columns {
                                        add_result(Some(c.0), c.1.clone(), v.clone(), false);
                                    }
                                } else {
                                    self.issues.push(Issue::err("Unknown table", tbl));
                                }
                            }
                        }
                    }
                    _ => {
                        self.issues.push(Issue::err("Invalid identifier", &e.expr))
                    }
                }
            } else {
                let type_ = self.type_expression(&e.expr, false);
                if let Some((as_, as_span)) = &e.as_ {
                    add_result(Some(*as_), type_, as_span.clone(), true);
                } else {
                    self.issues.push(Issue::warn("Unnamed column in select", e));
                    add_result(None, type_, 0..0, false);
                };
            }
        }
        self.issues.extend(add_result_issues.into_iter());
        self.reference_types.push(select_refence);

        if let Some((_, group_by)) = &select.group_by {
            for e in group_by {
                self.type_expression(e, false);
            }
        }

        if let Some((_, order_by)) = &select.order_by {
            for (e,_) in order_by {
                self.type_expression(e, false);
            }
        }

        if let Some((limit_spam, _, _)) = &select.limit {
            self.issues.push(Issue::todo(limit_spam));
        }

        self.reference_types = old_reference_type;

        SelectType {
            columns: result.into_iter().map(|(name, type_, _)| SelectTypeColumn{ name, type_}).collect()
        }
    }

    fn type_delete(&mut self, delete: &Delete<'a>) {
        let old_reference_type = std::mem::take(&mut self.reference_types);

        for flag in &delete.flags {
            match flag {
                sql_ast::DeleteFlag::LowPriority(_) |
                sql_ast::DeleteFlag::Quick(_) |
                sql_ast::DeleteFlag::Ignore(_) => ()
            }
        };

        if delete.table.len() != 1 {
            self.issues.push(Issue::todo(&delete.table.opt_span().unwrap()));
            return;
        }

        let identifier = &delete.table[0];
        if let Some(s) = self.schemas.schemas.get(&identifier.0) {
            let mut columns = Vec::new();
            for (n, t) in &s.columns {
                columns.push((
                    *n,
                    t.type_.ref_clone()
                ));
            }
            self.reference_types.push(ReferenceType{
                name: (identifier.0, identifier.1.clone()),
                columns
            });
        } else {
            self.issues.push(Issue::err("Unknown table or view", identifier))
        }

        if let Some((where_,_)) = &delete.where_ {
            let t = self.type_expression(where_, false);
            self.ensure_bool(where_, &t.t);
        }

        self.reference_types = old_reference_type;
    }

    fn type_insert(&mut self,
        insert: &Insert<'a>) {

        for flag in &insert.flags {
            match &flag {
                sql_ast::InsertFlag::LowPriority(_) |
                sql_ast::InsertFlag::HighPriority(_) |
                sql_ast::InsertFlag::Delayed(_) |
                sql_ast::InsertFlag::Ignore(_) => (),
            }
        }

        if let Some(v) = insert.table.get(1..) {
            for t in v {
                self.issues.push(Issue::todo(t));
            }
        }

        let t = &insert.table[0];
        let s = if let Some(schema) = self.schemas.schemas.get(t.0) {
            if schema.view {
                self.issues.push(Issue::err("Inserts into views not yet implemented", t));
            }
            let mut col_types = Vec::new();
            for col in &insert.columns {
                if let Some(schema_col) = schema.columns.get(col.0) {
                    col_types.push(schema_col.type_.ref_clone());
                } else {
                    self.issues.push(Issue::err("No such column in schema", col));
                }
            }
            Some(col_types)
        } else {
            self.issues.push(Issue::err("Unknown table", t));
            None
        };


        if let Some(values) = &insert.values {
            for row in &values.1 {
                for e in row {
                    self.type_expression(e, false);
                    //TODO compare types
                }
            }
        }

        if let Some(select) = &insert.select {
            self.issues.push(Issue::todo(select));
        }
    }

    fn type_statement(&mut self,
        statement: &Statement<'a>) {
        match statement {
            Statement::Select(s) => {let st = self.type_select(s);
                println!("HI {:#?}", st);
            }
            Statement::Delete(d) => {
                self.type_delete(d);
            },
            Statement::Insert(i) => {
                self.type_insert(i);
            }
            //Statement::Update(_) => todo!(),
            //Statement::Union(_) => todo!(),
            //Statement::Replace(_) => todo!(),
            s => self.issues.push(Issue::err("Cannot type statement of this type", s)),
        }
    }
}



#[cfg(test)]
mod tests {
    use codespan_reporting::{
        diagnostic::{Diagnostic, Label},
        files::SimpleFiles,
        term::{
            self,
            termcolor::{ColorChoice, StandardStream},
        },
    };
    use sql_ast::{Level, parse_statements};


    use crate::{Typer, schema::parse_schemas};

    #[test]
    fn it_works() {
        let src = std::fs::read_to_string("schema.sql").expect("Failed to read file");
        let (schemas, issues) = parse_schemas(&src);

        if !issues.is_empty() {
            let mut files = SimpleFiles::new();
            let file_id = files.add("schema.sql", &src);
            let writer = StandardStream::stderr(ColorChoice::Always);
            let err = issues.iter().any(|i| i.level == Level::Error);
            let config = codespan_reporting::term::Config::default();
            for issue in issues {
                let mut labels = vec![Label::primary(file_id, issue.span)];
                for (message, span) in issue.fragments {
                    labels.push(Label::secondary(file_id, span).with_message(message));
                }
                let d = match issue.level {
                    Level::Error => Diagnostic::error(),
                    Level::Warning => Diagnostic::warning(),
                };
                let d = d.with_message(issue.message).with_labels(labels);
                term::emit(&mut writer.lock(), &config, &files, &d).unwrap();
            }
            if err {
                panic!("Errors parsing schema");
            }
        }

        let src = std::fs::read_to_string("qs.sql").expect("Failed to read file");

        let (statements, mut issues) = parse_statements(&src);

        for statement in statements {
            let mut typer = Typer{schemas: &schemas, issues: &mut issues, reference_types: Vec::new()};
            typer.type_statement(&statement);
        }


        if !issues.is_empty() {
            let mut files = SimpleFiles::new();
            let file_id = files.add("qs.sql", &src);
            let writer = StandardStream::stderr(ColorChoice::Always);
            let err = issues.iter().any(|i| i.level == Level::Error);
            let config = codespan_reporting::term::Config::default();
            for issue in issues {
                let mut labels = vec![Label::primary(file_id, issue.span)];
                for (message, span) in issue.fragments {
                    labels.push(Label::secondary(file_id, span).with_message(message));
                }
                let d = match issue.level {
                    Level::Error => Diagnostic::error(),
                    Level::Warning => Diagnostic::warning(),
                };
                let d = d.with_message(issue.message).with_labels(labels);
                term::emit(&mut writer.lock(), &config, &files, &d).unwrap();
            }
            if err {
                panic!("Error in statements");
            }
        }

        //println!("HI {:#?}", schema);
        panic!("HERE");
    }
}
