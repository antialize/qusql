use std::{collections::{HashMap, HashSet}, borrow::Cow};

use sql_ast::{parse_statements, Spanned, OptSpanned, Span, CreateDefinition, DataType, Statement, Select, TableReference, Expression, BinaryOperator, Delete, Function, Insert};
pub use sql_ast::{Issue, Level};

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
    Enum(HashSet<Cow<'a, str>>),
    Set(HashSet<Cow<'a, str>>),
}

#[derive(Debug)]
pub struct Column<'a> {
    pub identifier_span: Span,
    pub type_: Type<'a>,
    pub not_null: bool,
    pub auto_increment: bool,
}

#[derive(Debug)]
pub struct Schema<'a> {
    pub identifier_span: Span,
    pub columns: HashMap<&'a str, Column<'a>>,
    pub view: bool,
}

#[derive(Debug)]
pub struct Procedure {
}

#[derive(Debug)]
pub struct Functions {
}



#[derive(Debug)]

pub struct Schemas<'a> {
    schemas: HashMap<&'a str, Schema<'a>>,
    procedures: HashMap<&'a str, Procedure>,
    functions: HashMap<&'a str, Functions>,

}

fn parse_column<'a>(data_type: DataType<'a>, identifier_span: Span,_issues: &mut Vec<Issue>) -> Column<'a> {
    let mut not_null = false;
    let mut unsigned = false;
    let mut auto_increment = false;
    for p in data_type.properties {
        match p {
            sql_ast::DataTypeProperty::Signed(_) => unsigned = false,
            sql_ast::DataTypeProperty::Unsigned(_) => unsigned = true,
            sql_ast::DataTypeProperty::Null(_) => not_null = false,
            sql_ast::DataTypeProperty::NotNull(_) => not_null = true,
            sql_ast::DataTypeProperty::AutoIncrement(_) => auto_increment = true,
            _ => {}
            // TODO default,
        }
    }
    let type_ = match data_type.type_ {
        sql_ast::Type::TinyInt(v) => {
            if !unsigned && matches!(v, Some((1, _))) {
                Type::Bool
            } else if unsigned {Type::U8} else {Type::I8}
        }
        sql_ast::Type::SmallInt(_) => if unsigned {Type::U16} else {Type::I16},
        sql_ast::Type::Int(_) => if unsigned {Type::U32} else {Type::I32},
        sql_ast::Type::BigInt(_) => if unsigned {Type::U64} else {Type::I64},
        sql_ast::Type::VarChar(_) => Type::Text,
        sql_ast::Type::TinyText(_) => Type::Text,
        sql_ast::Type::MediumText(_) => Type::Text,
        sql_ast::Type::Text(_) => Type::Text,
        sql_ast::Type::LongText(_) => Type::Text,
        sql_ast::Type::Enum(e) => Type::Enum(e.into_iter().map(|(s,_)| s).collect()),
        sql_ast::Type::Set(s) => Type::Set(s.into_iter().map(|(s,_)| s).collect()),
        sql_ast::Type::Float(_) => Type::F32,
        sql_ast::Type::Double(_) => Type::F64,
        sql_ast::Type::DateTime(_) => Type::DateTime,
        sql_ast::Type::Timestamp(_) => Type::Timestamp,
        sql_ast::Type::Time(_) => Type::Time,
        sql_ast::Type::TinyBlob(_) => Type::Bytes,
        sql_ast::Type::MediumBlob(_) => Type::Bytes,
        sql_ast::Type::Date => Type::Date,
        sql_ast::Type::Blob(_) => Type::Bytes,
        sql_ast::Type::LongBlob(_) => Type::Bytes,
        sql_ast::Type::VarBinary(_) => Type::Bytes,
    };
    Column{
        identifier_span,
        type_,
        not_null,
        auto_increment,
    }
}



pub fn parse_schemas(src: &str) -> (Schemas<'_>, Vec<Issue>) {
    let (statements, mut issues) = parse_statements(src);

    let mut schemas = Schemas { schemas: Default::default(), procedures: Default::default(), functions: Default::default() };


    for statement in statements {
        match statement {
            sql_ast::Statement::CreateTable(t) => {
                let mut replace = false;

                let mut schema = Schema{view: false, identifier_span: t.identifier.1.clone(), columns: HashMap::new() };

                for o in t.create_options {
                    match o {
                        sql_ast::CreateOption::OrReplace(_) => {replace = true;},
                        sql_ast::CreateOption::Temporary(s) =>
                            issues.push(Issue::err("Not supported", &s)),
                        sql_ast::CreateOption::Algorithm(_, _) => {},
                        sql_ast::CreateOption::Definer { .. } => {},
                        sql_ast::CreateOption::SqlSecurityDefiner(_, _) => {},
                        sql_ast::CreateOption::SqlSecurityUser(_, _) => {},
                    }
                }
                // TODO: do we care about table options
                for d in t.create_definitions {
                    match d {
                        sql_ast::CreateDefinition::ColumnDefinition { identifier, data_type } => {
                            let column = parse_column(data_type, identifier.1.clone(), &mut issues);
                            match schema.columns.entry(identifier.0) {
                                std::collections::hash_map::Entry::Occupied(e) => {
                                    issues.push(Issue::err("Column allready defined", &identifier.1).frag(
                                        "Defined here", &e.get().identifier_span
                                    ));
                                },
                                std::collections::hash_map::Entry::Vacant(e) => {e.insert(column);}
                            }
                        }
                    }
                }
                match schemas.schemas.entry(t.identifier.0) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if t.if_not_exists.is_none() {
                            issues.push(Issue::err("Table already defined", &t.identifier).frag(
                                "Defined here", &e.get().identifier_span
                            ));
                        }
                    },
                    std::collections::hash_map::Entry::Vacant(e) => {e.insert(schema);}
                }
            },
            sql_ast::Statement::CreateView(v) => {
                let mut replace = false;
                let schema = Schema{view: true, identifier_span: v.name.1.clone(), columns: HashMap::new() };
                for o in v.create_options {
                    match o {
                        sql_ast::CreateOption::OrReplace(_) => {replace = true;},
                        sql_ast::CreateOption::Temporary(s) =>
                            issues.push(Issue::err("Not supported", &s)),
                        sql_ast::CreateOption::Algorithm(_, _) => {},
                        sql_ast::CreateOption::Definer { .. } => {},
                        sql_ast::CreateOption::SqlSecurityDefiner(_, _) => {},
                        sql_ast::CreateOption::SqlSecurityUser(_, _) => {},
                    }
                }
                // TODO typecheck view query to find schema
                match schemas.schemas.entry(v.name.0) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if replace {
                            e.insert(schema);
                        } else if v.if_not_exists.is_none() {
                            issues.push(Issue::err("View already defined", &v.name).frag(
                                "Defined here", &e.get().identifier_span
                            ));
                        }
                    },
                    std::collections::hash_map::Entry::Vacant(e) => {e.insert(schema);}
                }
            },
            sql_ast::Statement::CreateTrigger(_) => {},
            // sql_ast::Statement::CreateFunction(_) => todo!(),
            // sql_ast::Statement::Select(_) => todo!(),
            // sql_ast::Statement::Delete(_) => todo!(),
            // sql_ast::Statement::Insert(_) => todo!(),
            // sql_ast::Statement::Update(_) => todo!(),
            sql_ast::Statement::DropTable(t) => {
                for (n, s) in t.tables {
                    match schemas.schemas.entry(n) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            if e.get().view {
                                issues.push(Issue::err("Name defines a view not a table",&s).frag("View defined here", &e.get().identifier_span))
                            } else {
                                e.remove();
                            }
                        },
                        std::collections::hash_map::Entry::Vacant(_) => {
                            if t.if_exists.is_none() {
                                issues.push(Issue::err("A table with this name does not exist to drop",&s));
                            }
                        }
                    }
                }
            },
            sql_ast::Statement::DropFunction(f) => {
                match schemas.functions.entry(f.function.0) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        e.remove();
                    },
                    std::collections::hash_map::Entry::Vacant(_) => {
                        if f.if_exists.is_none() {
                            issues.push(Issue::err("A function with this name does not exist to drop",&f.function.1));
                        }
                    }
                }
            }
            sql_ast::Statement::DropProcedure(p) => {
                match schemas.procedures.entry(p.procedure.0) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        e.remove();
                    },
                    std::collections::hash_map::Entry::Vacant(_) => {
                        if p.if_exists.is_none() {
                            issues.push(Issue::err("A procedure with this name does not exist to drop",&p.procedure.1));
                        }
                    }
                }
            },
            //sql_ast::Statement::DropEvent(_) => todo!(),
            sql_ast::Statement::DropDatabase(_) => {},
            sql_ast::Statement::DropServer(_) => {},
            sql_ast::Statement::DropTrigger(_) => {},
            sql_ast::Statement::DropView(v) => {
                for (n, s) in v.views {
                    match schemas.schemas.entry(n) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            if !e.get().view {
                                issues.push(Issue::err("Name defines a table not a view",&s).frag("Table defined here", &e.get().identifier_span));
                            } else {
                                e.remove();
                            }
                        },
                        std::collections::hash_map::Entry::Vacant(_) => {
                            if v.if_exists.is_none() {
                                issues.push(Issue::err("A view with this name does not exist to drop",&s));
                            }
                        }
                    }
                }
            }
            sql_ast::Statement::Set(_) => {},
            sql_ast::Statement::AlterTable(a) => {
                let e = match schemas.schemas.entry(a.table.0) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        let e = e.into_mut();
                        if e.view {
                            issues.push(Issue::err("Cannot alter view",&a.table.1));
                            continue;
                        }
                        e
                    },
                    std::collections::hash_map::Entry::Vacant(_) => {
                        if a.if_exists.is_none() {
                            issues.push(Issue::err("Table not found",&a.table.1));
                        }
                        continue;
                    }
                };
                for s in a.alter_specifications {
                    match s {
                        sql_ast::AlterSpecification::AddIndex {..} => {},
                        sql_ast::AlterSpecification::AddForeginKey { .. } => {},
                        sql_ast::AlterSpecification::Modify { if_exists, col, definition, .. } => {
                            let c = match e.columns.get_mut(col.0) {
                                Some(v) => v,
                                None => {
                                    if if_exists.is_none() {
                                        issues.push(Issue::err("No such column in table",&col.1).frag("Table defined here", &e.identifier_span));
                                    }
                                    continue
                                }
                            };
                            *c = parse_column(definition, col.1, &mut issues);
                        },
                    }
                }
            },
            // sql_ast::Statement::Block(_) => todo!(),
            // sql_ast::Statement::If(_) => todo!(),
            // sql_ast::Statement::Invalid => todo!(),
            // sql_ast::Statement::Union(_) => todo!(),
            // sql_ast::Statement::Replace(_) => todo!(),
            // sql_ast::Statement::Case(_) => todo!(),
            s => issues.push(Issue::err( "Unsupported statement in schema definition", &s)),
        }
    }
    (schemas, issues)
}

struct ReferenceType<'a> {
    name: (&'a str, Span),
    columns: Vec<(&'a str, Type<'a>, bool)>,
}

struct Typer<'a> {
    issues: &'a mut Vec<Issue>,
    schemas: &'a Schemas<'a>,
    reference_types: Vec<ReferenceType<'a>>,
}

#[derive(Debug, Clone)]
pub struct SelectTypeColumn<'a> {
    pub name: Option<&'a str>,
    pub type_: Type<'a>,
    pub not_null: bool,
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
                        columns.push((
                            *n,
                            t.type_.clone(),
                            t.not_null && !force_null
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
                        self.ensure_bool(e, &t.0);
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

    fn type_binary_expression(&mut self, op: &BinaryOperator, op_span: &Span, lhs: &Expression<'a>, rhs: &Expression<'a>, outer_where: bool) -> (Type<'a>, bool) {
        let outer_where = matches!(op, BinaryOperator::And) && outer_where;
        let lhs_type = self.type_expression(lhs, outer_where);
        let rhs_type = self.type_expression(rhs, outer_where);
        match op {
            BinaryOperator::Or | BinaryOperator::Xor | BinaryOperator::And => {
                self.ensure_bool(lhs, &lhs_type.0);
                self.ensure_bool(rhs, &rhs_type.0);
                (Type::Bool, lhs_type.1 && rhs_type.1)
            },
            BinaryOperator::Eq | BinaryOperator::Neq | BinaryOperator::GtEq | BinaryOperator::Gt | BinaryOperator::LtEq | BinaryOperator::Lt => {
                if lhs_type.0 == Type::Null {
                    self.issues.push(Issue::warn("Comparison with null", lhs));
                }
                if rhs_type.0 == Type::Null{
                    self.issues.push(Issue::warn("Comparison with null", rhs));
                }

                let ok = match &lhs_type.0 {
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
                        match &rhs_type.0 {
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
                                self.constrain_arg(*idx, &lhs_type.0, lhs_type.1);
                                true
                            },
                            _ => false,
                        }
                    }
                    Type::Text => {
                        match &rhs_type.0 {
                            Type::Text | Type::Null | Type::Invalid | Type::Enum(_) => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.0, lhs_type.1);
                                true
                            }
                            _ => false,
                        }
                    }
                    Type::Bytes => {
                        match &rhs_type.0 {
                            Type::Bytes | Type::Null | Type::Invalid => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.0, lhs_type.1);
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
                        match &rhs_type.0 {
                            Type::Bool | Type::Null | Type::Invalid => true,
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &lhs_type.0, lhs_type.1);
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
                        match &rhs_type.0 {
                            Type::Text | Type::Null | Type::Invalid => true,
                            Type::Enum(e2) => !e.is_disjoint(e2),
                            Type::Arg(idx, _) => {
                                self.constrain_arg(*idx, &Type::Text, lhs_type.1);
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
                            .frag(format!("Of type {:?}", lhs_type.0), lhs)
                            .frag(format!("Of type {:?}", rhs_type.0), rhs)
                    );
                }
                (Type::Bool, lhs_type.1 && rhs_type.1)
            },
            BinaryOperator::NullSafeEq => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::ShiftLeft | BinaryOperator::ShiftRight => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Add => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Subtract => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Divide => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Div => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Mod => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Mult => {
                self.issues.push(Issue::todo(op_span));
                (Type::Invalid, false)
            },
            BinaryOperator::Like | BinaryOperator::NotLike => {
                self.ensure_text(lhs, &lhs_type.0);
                self.ensure_text(rhs, &rhs_type.0);
                (Type::Bool, lhs_type.1 && rhs_type.1)
            },
        }
    }

    fn type_function(&mut self, func: &Function<'a>, args: &Vec<Expression<'a>>, span: &Span) -> (Type<'a>, bool) {
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
                (Type::U64 , true)
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
                (Type::F64 , true)
            }
            _ => {
                self.issues.push(Issue::todo(span));
                (Type::Invalid, false)
            }
        }
    }

    fn type_expression(&mut self, expression: &Expression<'a>, outer_where: bool) -> (Type<'a>, bool) {
        match expression {
            Expression::Binary { op, op_span, lhs, rhs } => {
                self.type_binary_expression(op, op_span, lhs, rhs, outer_where)
            },
            Expression::Unary { op, op_span, operand } => {
                self.issues.push(Issue::todo(expression));
                (Type::Invalid, false)
            },
            Expression::Subquery(_) => {
                self.issues.push(Issue::todo(expression));
                (Type::Invalid, false)
            },
            Expression::Null(_) => (Type::Null, false),
            Expression::Bool(_, _) => (Type::Bool, true),
            Expression::String(_) => (Type::Text, true),
            Expression::Integer(_) => {
                (Type::Integer, true)
            },
            Expression::Float(_) => {
                self.issues.push(Issue::todo(expression));
                (Type::Float, true)
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
                                return (Type::Invalid, false);
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
                            return (Type::Invalid, false);
                        }
                    }
                    2 => {
                        let tbl = match &i[0] {
                            sql_ast::IdentifierPart::Name(n) => n,
                            sql_ast::IdentifierPart::Star(v) => {
                                self.issues.push(Issue::err("Not supported here", v));
                                return (Type::Invalid, false);
                            }
                        };
                        let col = match &i[1] {
                            sql_ast::IdentifierPart::Name(n) => n,
                            sql_ast::IdentifierPart::Star(v) => {
                                self.issues.push(Issue::err("Not supported here", v));
                                return (Type::Invalid, false);
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
                        return (Type::Invalid, false);
                    }
                }
                match t {
                    None => {
                        self.issues.push(Issue::err("Unknown identifier", expression));
                        (Type::Invalid, false)
                    }
                    Some((_, t, not_null)) => {
                        (t.clone(), *not_null)
                    }
                }
            },
            Expression::Arg((idx, span)) => {
                (Type::Arg(*idx, span.clone()), false)
            },
            Expression::Exists(_) => {
                self.issues.push(Issue::todo(expression));
                (Type::Invalid, false)
            },
            Expression::In { lhs, rhs, in_span, not_in } => {
                self.issues.push(Issue::todo(expression));
                (Type::Invalid, false)
            },
            Expression::Is(e, is, _) => {
                let t = self.type_expression(e, false);
                match is {
                    sql_ast::Is::Null => {
                        if t.1 {
                            self.issues.push(Issue::warn("Is newer null", e));
                        }
                        (Type::Bool, true)
                    },
                    sql_ast::Is::NotNull => {
                        if t.1 {
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
                                                    c.2 = true;
                                                }
                                            }
                                        }
                                    } else  if let Some(sql_ast::IdentifierPart::Name((n1, _))) = parts.get(1) {
                                        for r in &mut self.reference_types {
                                            if &r.name.0 == n0 {
                                                for c in &mut r.columns {
                                                    if &c.0 == n1 {
                                                        c.2 = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        (Type::Bool, true)
                    },
                    sql_ast::Is::True |
                    sql_ast::Is::NotTrue |
                    sql_ast::Is::False |
                    sql_ast::Is::NotFalse |
                    sql_ast::Is::Unknown |
                    sql_ast::Is::NotUnknown => {
                        self.issues.push(Issue::todo(expression));
                        (Type::Invalid, false)
                    }
                }
            },
            Expression::Invalid => {
                (Type::Invalid, false)
            },
            Expression::Case { case_span, value, whens, else_, end_span } => {
                self.issues.push(Issue::todo(expression));
                (Type::Invalid, false)
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
            self.ensure_bool(where_, &t.0);
        }

        let mut result: Vec<(Option<&'a str>, Type<'a>, bool, Span)> = Vec::new();
        let mut select_refence = ReferenceType {
            name: ("", 0..0),
            columns: Vec::new(),
        };

        let mut add_result_issues = Vec::new();
        let mut add_result = |name: Option<&'a str>, type_: Type<'a>, not_null:bool, span: Span, as_: bool| {
            if let Some(name) = name {
                if as_ {
                    select_refence.columns.push(
                        (name, type_.clone(), not_null)
                    );
                }
                for (on, _, _, os) in &result {
                    if Some(name) == *on {
                        add_result_issues.push(Issue::warn(format!("Multiple columns with the name '{}'", name), &span).frag("Also defined here", os));
                    }
                }
            }
            result.push((name, type_, not_null, span));
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
                                    add_result(Some(name.0), Type::Invalid, false, name.1.clone(), e.as_.is_some());
                                } else if let Some(t) = t {
                                    add_result(Some(name.0), t.1.clone(), t.2, name.1.clone(), e.as_.is_some());
                                } else {
                                    self.issues.push(Issue::err("Unknown identifier", col));
                                    add_result(Some(name.0), Type::Invalid, false, name.1.clone(), e.as_.is_some());
                                }
                            }
                            sql_ast::IdentifierPart::Star(v) => {
                                if let Some(as_) = &e.as_ {
                                    self.issues.push(Issue::err("As not supported for *", as_));
                                }
                                for r in &self.reference_types {
                                    for c in &r.columns {
                                        add_result(Some(c.0), c.1.clone(), c.2, v.clone(), false);
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
                                    add_result(Some(name.0), t.1.clone(), t.2, name.1.clone(), e.as_.is_some());
                                } else {
                                    self.issues.push(Issue::err("Unknown identifier", col));
                                    add_result(Some(name.0), Type::Invalid, false, name.1.clone(), e.as_.is_some());
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
                                        add_result(Some(c.0), c.1.clone(), c.2, v.clone(), false);
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
                    add_result(Some(*as_), type_.0, type_.1, as_span.clone(), true);
                } else {
                    self.issues.push(Issue::warn("Unnamed column in select", e));
                    add_result(None, type_.0, type_.1, 0..0, false);
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
            columns: result.into_iter().map(|(name, type_, not_null, _)| SelectTypeColumn{ name, type_, not_null }).collect()
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
                    t.type_.clone(),
                    t.not_null
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
            self.ensure_bool(where_, &t.0);
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
                    col_types.push((schema_col.type_.clone(), schema_col.not_null));
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


    use crate::{parse_schemas, Typer};

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
