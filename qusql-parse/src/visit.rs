use crate::{
    DoBody, Expression, Is, JoinSpecification, JsonTableColumn, JsonTableOnErrorEmpty,
    OnConflictAction, Select, Statement, TableReference, WindowFrameBound, WindowSpec,
};

/// Visitor pattern for traversing the AST.
///
/// Each `visit_*` method recursively walks the node's children by default via
/// the corresponding public `walk_*` free function. Override individual methods
/// to observe or transform specific node kinds, then call the `walk_*` function
/// to continue the descent.
///
/// # Return type
///
/// Every method returns `Result<Self::T, Self::E>`. `T: Default` allows walk
/// functions to return a value even when visiting many children and discarding
/// each intermediate result - they simply return `Ok(T::default())` after
/// processing all children. For pure side-effects set `T = ()`. For
/// early-termination, encode the "found" value in `E` and short-circuit with
/// `Err(...)`.
pub trait Visitor<'a>: Sized {
    /// The success value produced when visiting a node.
    type T: Default;
    /// The error / early-exit value.
    type E;

    /// Visit a [`Statement`] node. Recurses into all child statements and expressions by default.
    fn visit_statement(&mut self, stmt: &Statement<'a>) -> Result<Self::T, Self::E> {
        walk_statement(self, stmt)
    }

    /// Visit an [`Expression`] node. Recurses into all child expressions by default.
    fn visit_expression(&mut self, expr: &Expression<'a>) -> Result<Self::T, Self::E> {
        walk_expression(self, expr)
    }

    /// Visit a [`Select`] node. Recurses into all select expressions, table references,
    /// and clause expressions (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, etc.) by default.
    fn visit_select(&mut self, select: &Select<'a>) -> Result<Self::T, Self::E> {
        walk_select(self, select)
    }

    /// Visit a [`TableReference`] node. Recurses into subqueries, JOIN children,
    /// and any expressions (JSON path, function arguments, ON clause) by default.
    fn visit_table_reference(&mut self, tr: &TableReference<'a>) -> Result<Self::T, Self::E> {
        walk_table_reference(self, tr)
    }
}

// -- private helpers ----------------------------------------------------------

/// Recurse into the partition-by, order-by, and frame bound expressions of a window specification.
fn walk_window_spec<'a, V: Visitor<'a>>(v: &mut V, spec: &WindowSpec<'a>) -> Result<V::T, V::E> {
    if let Some((_, exprs)) = &spec.partition_by {
        for expr in exprs {
            v.visit_expression(expr)?;
        }
    }
    if let Some((_, items)) = &spec.order_by {
        for (expr, _) in items {
            v.visit_expression(expr)?;
        }
    }
    if let Some(frame) = &spec.frame {
        walk_window_frame_bound(v, &frame.start)?;
        if let Some((_, end)) = &frame.between {
            walk_window_frame_bound(v, end)?;
        }
    }
    Ok(V::T::default())
}

/// Recurse into the expression of a window frame bound, if any.
/// `UNBOUNDED PRECEDING/FOLLOWING` and `CURRENT ROW` have no child expressions.
fn walk_window_frame_bound<'a, V: Visitor<'a>>(
    v: &mut V,
    bound: &WindowFrameBound<'a>,
) -> Result<V::T, V::E> {
    match bound {
        WindowFrameBound::Preceding(expr, _) | WindowFrameBound::Following(expr, _) => {
            v.visit_expression(expr)
        }
        WindowFrameBound::UnboundedPreceding(_)
        | WindowFrameBound::CurrentRow(_)
        | WindowFrameBound::UnboundedFollowing(_) => Ok(V::T::default()),
    }
}

/// Recurse into the path and ON EMPTY/ERROR expressions within a `JSON_TABLE` column definition.
/// For `Nested` columns this recurses into the nested column list as well.
fn walk_json_table_column<'a, V: Visitor<'a>>(
    v: &mut V,
    col: &JsonTableColumn<'a>,
) -> Result<V::T, V::E> {
    match col {
        JsonTableColumn::Column {
            path,
            on_empty,
            on_error,
            ..
        } => {
            v.visit_expression(path)?;
            if let Some((JsonTableOnErrorEmpty::Default(expr), _)) = on_empty {
                v.visit_expression(expr)?;
            }
            if let Some((JsonTableOnErrorEmpty::Default(expr), _)) = on_error {
                v.visit_expression(expr)?;
            }
            Ok(V::T::default())
        }
        JsonTableColumn::Ordinality { .. } => Ok(V::T::default()),
        JsonTableColumn::Nested { path, columns, .. } => {
            v.visit_expression(path)?;
            for col in columns {
                walk_json_table_column(v, col)?;
            }
            Ok(V::T::default())
        }
    }
}

// -- public walk functions ----------------------------------------------------

/// Default recursion for [`Statement`] nodes.
pub fn walk_statement<'a, V: Visitor<'a>>(v: &mut V, stmt: &Statement<'a>) -> Result<V::T, V::E> {
    match stmt {
        // -- DML ------------------------------------------------------------------
        Statement::Select(s) => v.visit_select(s),

        Statement::CompoundQuery(q) => {
            v.visit_statement(&q.left)?;
            for branch in &q.with {
                v.visit_statement(&branch.statement)?;
            }
            if let Some((_, items)) = &q.order_by {
                for (expr, _) in items {
                    v.visit_expression(expr)?;
                }
            }
            if let Some((_, offset, count)) = &q.limit {
                if let Some(o) = offset {
                    v.visit_expression(o)?;
                }
                v.visit_expression(count)?;
            }
            Ok(V::T::default())
        }

        Statement::WithQuery(q) => {
            for block in &q.with_blocks {
                v.visit_statement(&block.statement)?;
            }
            v.visit_statement(&q.statement)
        }

        Statement::InsertReplace(i) => {
            if let Some((_, rows)) = &i.values {
                for row in rows {
                    for expr in row {
                        v.visit_expression(expr)?;
                    }
                }
            }
            if let Some(sel) = &i.select {
                v.visit_statement(sel)?;
            }
            if let Some(set) = &i.set {
                for p in &set.pairs {
                    v.visit_expression(&p.value)?;
                }
            }
            if let Some(odk) = &i.on_duplicate_key_update {
                for p in &odk.pairs {
                    v.visit_expression(&p.value)?;
                }
            }
            if let Some(oc) = &i.on_conflict
                && let OnConflictAction::DoUpdateSet { sets, where_, .. } = &oc.action
            {
                for (_, expr) in sets {
                    v.visit_expression(expr)?;
                }
                if let Some((_, expr)) = where_ {
                    v.visit_expression(expr)?;
                }
            }
            if let Some((_, exprs)) = &i.returning {
                for se in exprs {
                    v.visit_expression(&se.expr)?;
                }
            }
            Ok(V::T::default())
        }

        Statement::Update(u) => {
            for tr in &u.tables {
                v.visit_table_reference(tr)?;
            }
            for (_, expr) in &u.set {
                v.visit_expression(expr)?;
            }
            if let Some((expr, _)) = &u.where_ {
                v.visit_expression(expr)?;
            }
            if let Some((_, refs)) = &u.from {
                for tr in refs {
                    v.visit_table_reference(tr)?;
                }
            }
            if let Some((_, exprs)) = &u.returning {
                for se in exprs {
                    v.visit_expression(&se.expr)?;
                }
            }
            Ok(V::T::default())
        }

        Statement::Delete(d) => {
            for tr in &d.using {
                v.visit_table_reference(tr)?;
            }
            if let Some((expr, _)) = &d.where_ {
                v.visit_expression(expr)?;
            }
            if let Some((_, items)) = &d.order_by {
                for (expr, _) in items {
                    v.visit_expression(expr)?;
                }
            }
            if let Some((_, offset, count)) = &d.limit {
                if let Some(o) = offset {
                    v.visit_expression(o)?;
                }
                v.visit_expression(count)?;
            }
            if let Some((_, exprs)) = &d.returning {
                for se in exprs {
                    v.visit_expression(&se.expr)?;
                }
            }
            Ok(V::T::default())
        }

        Statement::Values(vals) => {
            for row in &vals.rows {
                for expr in row {
                    v.visit_expression(expr)?;
                }
            }
            if let Some((_, items)) = &vals.order_by {
                for (expr, _) in items {
                    v.visit_expression(expr)?;
                }
            }
            if let Some((_, expr)) = &vals.limit {
                v.visit_expression(expr)?;
            }
            if let Some((_, expr)) = &vals.offset {
                v.visit_expression(expr)?;
            }
            if let Some(fetch) = &vals.fetch
                && let Some(expr) = &fetch.count
            {
                v.visit_expression(expr)?;
            }
            Ok(V::T::default())
        }

        Statement::Set(s) => {
            for (_, exprs) in &s.values {
                for expr in exprs {
                    v.visit_expression(expr)?;
                }
            }
            Ok(V::T::default())
        }

        Statement::Call(c) => {
            for expr in &c.args {
                v.visit_expression(expr)?;
            }
            Ok(V::T::default())
        }

        // -- Expression-bearing statements ------------------------------------------
        Statement::Return(r) => v.visit_expression(&r.expr),
        Statement::Perform(p) => v.visit_expression(&p.expr),

        Statement::Raise(r) => {
            for expr in &r.args {
                v.visit_expression(expr)?;
            }
            for (_, _, expr) in &r.using {
                v.visit_expression(expr)?;
            }
            Ok(V::T::default())
        }

        Statement::Assign(a) => {
            v.visit_expression(&a.target)?;
            v.visit_select(&a.value)
        }

        Statement::PlpgsqlExecute(e) => {
            v.visit_expression(&e.command)?;
            for expr in &e.using {
                v.visit_expression(expr)?;
            }
            Ok(V::T::default())
        }

        // -- Control-flow / compound statements ---------------------------------------
        Statement::Block(b) => {
            for stmt in &b.statements {
                v.visit_statement(stmt)?;
            }
            for handler in &b.exception_handlers {
                for stmt in &handler.statements {
                    v.visit_statement(stmt)?;
                }
            }
            Ok(V::T::default())
        }

        Statement::If(i) => {
            for cond in &i.conditions {
                v.visit_select(&cond.search_condition)?;
                for stmt in &cond.then {
                    v.visit_statement(stmt)?;
                }
            }
            if let Some((_, stmts)) = &i.else_ {
                for stmt in stmts {
                    v.visit_statement(stmt)?;
                }
            }
            Ok(V::T::default())
        }

        Statement::While(w) => {
            v.visit_expression(&w.condition)?;
            for stmt in &w.body {
                v.visit_statement(stmt)?;
            }
            Ok(V::T::default())
        }

        Statement::Loop(l) => {
            for stmt in &l.body {
                v.visit_statement(stmt)?;
            }
            Ok(V::T::default())
        }

        Statement::Repeat(r) => {
            for stmt in &r.body {
                v.visit_statement(stmt)?;
            }
            v.visit_expression(&r.condition)
        }

        Statement::Case(c) => {
            if let Some(expr) = &c.value {
                v.visit_expression(expr)?;
            }
            for when in &c.whens {
                v.visit_expression(&when.when)?;
                for stmt in &when.then {
                    v.visit_statement(stmt)?;
                }
            }
            if let Some((_, stmts)) = &c.else_ {
                for stmt in stmts {
                    v.visit_statement(stmt)?;
                }
            }
            Ok(V::T::default())
        }

        // -- Statement wrappers -------------------------------------------------------
        Statement::Explain(e) => v.visit_statement(&e.statement),
        Statement::DeclareCursor(d) => v.visit_statement(&d.query),
        Statement::DeclareCursorMariaDb(d) => v.visit_select(&d.query),
        Statement::DeclareHandler(d) => v.visit_statement(&d.statement),
        Statement::Prepare(p) => v.visit_statement(&p.statement),

        Statement::DeclareVariable(d) => {
            if let Some((_, sel)) = &d.default {
                v.visit_select(sel)?;
            }
            Ok(V::T::default())
        }

        Statement::Do(d) => {
            if let DoBody::Statements(stmts) = &d.body {
                for stmt in stmts {
                    v.visit_statement(stmt)?;
                }
            }
            Ok(V::T::default())
        }

        // -- Leaf statements (DDL, TCL, SHOW, ...) -------------------------------------
        // These have no child expressions or statements to recurse into.
        Statement::AlterSchema(_)
        | Statement::AlterTable(_)
        | Statement::AlterRole(_)
        | Statement::AlterType(_)
        | Statement::AlterOperator(_)
        | Statement::AlterOperatorClass(_)
        | Statement::AlterOperatorFamily(_)
        | Statement::CreateIndex(_)
        | Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::CreateTrigger(_)
        | Statement::CreateFunction(_)
        | Statement::CreateProcedure(_)
        | Statement::CreateDatabase(_)
        | Statement::CreateSchema(_)
        | Statement::CreateSequence(_)
        | Statement::CreateServer(_)
        | Statement::CreateRole(_)
        | Statement::CreateOperator(_)
        | Statement::CreateTypeEnum(_)
        | Statement::CreateOperatorClass(_)
        | Statement::CreateOperatorFamily(_)
        | Statement::CreateExtension(_)
        | Statement::CreateDomain(_)
        | Statement::CreateConstraintTrigger(_)
        | Statement::CreateTablePartitionOf(_)
        | Statement::DropIndex(_)
        | Statement::DropTable(_)
        | Statement::DropFunction(_)
        | Statement::DropProcedure(_)
        | Statement::DropSequence(_)
        | Statement::DropEvent(_)
        | Statement::DropDatabase(_)
        | Statement::DropSchema(_)
        | Statement::DropServer(_)
        | Statement::DropTrigger(_)
        | Statement::DropView(_)
        | Statement::DropExtension(_)
        | Statement::DropOperator(_)
        | Statement::DropOperatorFamily(_)
        | Statement::DropOperatorClass(_)
        | Statement::DropDomain(_)
        | Statement::DropType(_)
        | Statement::RenameTable(_)
        | Statement::TruncateTable(_)
        | Statement::RefreshMaterializedView(_)
        | Statement::CommentOn(_)
        | Statement::Signal(_)
        | Statement::Kill(_)
        | Statement::ShowTables(_)
        | Statement::ShowDatabases(_)
        | Statement::ShowProcessList(_)
        | Statement::ShowVariables(_)
        | Statement::ShowStatus(_)
        | Statement::ShowColumns(_)
        | Statement::ShowCreateTable(_)
        | Statement::ShowCreateDatabase(_)
        | Statement::ShowCreateView(_)
        | Statement::ShowCharacterSet(_)
        | Statement::ShowCollation(_)
        | Statement::ShowEngines(_)
        | Statement::Flush(_)
        | Statement::Unlock(_)
        | Statement::Lock(_)
        | Statement::Begin(_)
        | Statement::End(_)
        | Statement::Commit(_)
        | Statement::StartTransaction(_)
        | Statement::CopyFrom(_)
        | Statement::CopyTo(_)
        | Statement::Stdin(_)
        | Statement::OpenCursor(_)
        | Statement::CloseCursor(_)
        | Statement::FetchCursor(_)
        | Statement::Leave(_)
        | Statement::Iterate(_)
        | Statement::Grant(_)
        | Statement::ExecuteFunction(_)
        | Statement::Analyze(_)
        | Statement::Invalid(_) => Ok(V::T::default()),
    }
}

/// Default recursion for [`Expression`] nodes.
pub fn walk_expression<'a, V: Visitor<'a>>(v: &mut V, expr: &Expression<'a>) -> Result<V::T, V::E> {
    match expr {
        Expression::Binary(e) => {
            v.visit_expression(&e.lhs)?;
            v.visit_expression(&e.rhs)
        }
        Expression::Unary(e) => v.visit_expression(&e.operand),
        Expression::Subquery(e) => v.visit_statement(&e.expression),
        Expression::Exists(e) => v.visit_statement(&e.subquery),
        Expression::Extract(e) => v.visit_expression(&e.date),
        Expression::Trim(e) => {
            if let Some(what) = &e.what {
                v.visit_expression(what)?;
            }
            v.visit_expression(&e.value)
        }
        Expression::In(e) => {
            v.visit_expression(&e.lhs)?;
            for rhs_expr in &e.rhs {
                v.visit_expression(rhs_expr)?;
            }
            Ok(V::T::default())
        }
        Expression::Between(e) => {
            v.visit_expression(&e.lhs)?;
            v.visit_expression(&e.low)?;
            v.visit_expression(&e.high)
        }
        Expression::MemberOf(e) => {
            v.visit_expression(&e.lhs)?;
            v.visit_expression(&e.rhs)
        }
        Expression::Is(e) => {
            v.visit_expression(&e.lhs)?;
            match &e.is {
                Is::DistinctFrom(rhs) | Is::NotDistinctFrom(rhs) => v.visit_expression(rhs),
                _ => Ok(V::T::default()),
            }
        }
        Expression::Case(e) => {
            if let Some(val) = &e.value {
                v.visit_expression(val)?;
            }
            for when in &e.whens {
                v.visit_expression(&when.when)?;
                v.visit_expression(&when.then)?;
            }
            if let Some((_, else_expr)) = &e.else_ {
                v.visit_expression(else_expr)?;
            }
            Ok(V::T::default())
        }
        Expression::Cast(e) => v.visit_expression(&e.expr),
        Expression::Convert(e) => v.visit_expression(&e.expr),
        Expression::TypeCast(e) => v.visit_expression(&e.expr),
        Expression::Function(e) => {
            for arg in &e.args {
                v.visit_expression(arg)?;
            }
            Ok(V::T::default())
        }
        Expression::WindowFunction(e) => {
            for arg in &e.args {
                v.visit_expression(arg)?;
            }
            walk_window_spec(v, &e.over.window_spec)
        }
        Expression::AggregateFunction(e) => {
            for arg in &e.args {
                v.visit_expression(arg)?;
            }
            if let Some((_, items)) = &e.within_group {
                for (expr, _) in items {
                    v.visit_expression(expr)?;
                }
            }
            if let Some((_, expr)) = &e.filter {
                v.visit_expression(expr)?;
            }
            if let Some(over) = &e.over {
                walk_window_spec(v, &over.window_spec)?;
            }
            Ok(V::T::default())
        }
        Expression::Char(e) => {
            for arg in &e.args {
                v.visit_expression(arg)?;
            }
            Ok(V::T::default())
        }
        Expression::GroupConcat(e) => v.visit_expression(&e.expr),
        Expression::TimestampAdd(e) => {
            v.visit_expression(&e.interval)?;
            v.visit_expression(&e.datetime)
        }
        Expression::TimestampDiff(e) => {
            v.visit_expression(&e.e1)?;
            v.visit_expression(&e.e2)
        }
        Expression::MatchAgainst(e) => {
            for col in &e.columns {
                v.visit_expression(col)?;
            }
            v.visit_expression(&e.expr)
        }
        Expression::Array(e) => {
            for elem in &e.elements {
                v.visit_expression(elem)?;
            }
            Ok(V::T::default())
        }
        Expression::ArraySubscript(e) => {
            v.visit_expression(&e.expr)?;
            v.visit_expression(&e.lower)?;
            if let Some(upper) = &e.upper {
                v.visit_expression(upper)?;
            }
            Ok(V::T::default())
        }
        Expression::Quantifier(e) => v.visit_expression(&e.operand),
        Expression::FieldAccess(e) => v.visit_expression(&e.expr),
        Expression::Row(e) => {
            for elem in &e.elements {
                v.visit_expression(elem)?;
            }
            Ok(V::T::default())
        }
        // Leaf expressions - no children to recurse into.
        Expression::Null(_)
        | Expression::Default(_)
        | Expression::Bool(_)
        | Expression::String(_)
        | Expression::Integer(_)
        | Expression::Float(_)
        | Expression::ListHack(_)
        | Expression::Arg(_)
        | Expression::Identifier(_)
        | Expression::Variable(_)
        | Expression::UserVariable(_)
        | Expression::Interval(_)
        | Expression::Invalid(_) => Ok(V::T::default()),
    }
}

/// Default recursion for [`Select`] nodes.
pub fn walk_select<'a, V: Visitor<'a>>(v: &mut V, select: &Select<'a>) -> Result<V::T, V::E> {
    for se in &select.select_exprs {
        v.visit_expression(&se.expr)?;
    }
    if let Some(refs) = &select.table_references {
        for tr in refs {
            v.visit_table_reference(tr)?;
        }
    }
    if let Some((expr, _)) = &select.where_ {
        v.visit_expression(expr)?;
    }
    if let Some((_, exprs)) = &select.group_by {
        for expr in exprs {
            v.visit_expression(expr)?;
        }
    }
    if let Some((expr, _)) = &select.having {
        v.visit_expression(expr)?;
    }
    if let Some((_, items)) = &select.order_by {
        for (expr, _) in items {
            v.visit_expression(expr)?;
        }
    }
    if let Some((_, offset, count)) = &select.limit {
        if let Some(o) = offset {
            v.visit_expression(o)?;
        }
        v.visit_expression(count)?;
    }
    if let Some((_, exprs)) = &select.distinct_on {
        for expr in exprs {
            v.visit_expression(expr)?;
        }
    }
    if let Some((_, expr)) = &select.offset {
        v.visit_expression(expr)?;
    }
    if let Some((_, expr)) = &select.fetch {
        v.visit_expression(expr)?;
    }
    Ok(V::T::default())
}

/// Default recursion for [`TableReference`] nodes.
pub fn walk_table_reference<'a, V: Visitor<'a>>(
    v: &mut V,
    tr: &TableReference<'a>,
) -> Result<V::T, V::E> {
    match tr {
        TableReference::Table { .. } => Ok(V::T::default()),
        TableReference::Query { query, .. } => v.visit_statement(query),
        TableReference::JsonTable {
            json_expr,
            path,
            columns,
            ..
        } => {
            v.visit_expression(json_expr)?;
            v.visit_expression(path)?;
            for col in columns {
                walk_json_table_column(v, col)?;
            }
            Ok(V::T::default())
        }
        TableReference::Function { args, .. } => {
            for arg in args {
                v.visit_expression(arg)?;
            }
            Ok(V::T::default())
        }
        TableReference::Join {
            left,
            right,
            specification,
            ..
        } => {
            v.visit_table_reference(left)?;
            v.visit_table_reference(right)?;
            if let Some(JoinSpecification::On(expr, _)) = specification {
                v.visit_expression(expr)?;
            }
            Ok(V::T::default())
        }
    }
}
