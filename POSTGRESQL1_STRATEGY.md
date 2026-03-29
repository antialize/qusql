# Implementation Strategy: postgresql1.sql Typer Support

This document lays out the work needed to make the schema typer
(`qusql-type/src/schema.rs`) handle real-world PostgreSQL migration scripts
such as `type_test/postgresql1.sql`.

Every change below is **general-purpose** — no hardcoded function names or
file-specific patterns. Each phase adds support for a SQL language feature
that was previously missing or stubbed out.

---

## Current State

The **parser** already handles postgresql1.sql — `parse-test --multiple`
reports `success: true`.

The **typer** (`Schemas::parse_schemas`) fails with 13 "Unsupported
statement" errors. Currently it only tracks table schemas (columns + types),
views, and indices. Procedure/function maps exist but store empty structs.
Many statement types fall through to the catch-all error arm.

---

## Implementation Plan

### Phase 1: Store functions (`CREATE [OR REPLACE] FUNCTION`)

**Goal:** Register function definitions in `Schemas` so the typer knows
what functions exist, their parameters, and return types.

**Current state:** `Schemas.functions` maps to an empty `Functions {}` struct.
`Statement::CreateFunction` is silently ignored.

**Changes to `schema.rs`:**

1. Replace the empty `Functions` struct:
   ```rust
   pub struct FunctionDef<'a> {
       pub name: Identifier<'a>,
       pub params: Vec<FunctionParam<'a>>,
       pub return_type: Option<DataType<'a>>,
       pub span: Span,
   }
   ```
   Update `Schemas.functions` to `BTreeMap<Identifier<'a>, FunctionDef<'a>>`.

2. Handle `Statement::CreateFunction`:
   - Check `create_options` for `OrReplace`.
   - Store name, parameters, return type, and span.
   - If `OR REPLACE`, overwrite any existing entry; otherwise check for
     duplicates.

3. Handle `Statement::CreateProcedure` the same way for `Schemas.procedures`.

**`issue_todo` for features not yet needed:**
- Function body interpretation (PL/pgSQL execution, `EXECUTE`, `RAISE`, etc.)
- Function characteristics (`STABLE`, `STRICT`, `PARALLEL SAFE`, `SECURITY DEFINER`, etc.)
- Overloaded functions (same name, different argument types)

---

### Phase 2: Handle `SELECT` statements in schema context

**Goal:** Process `SELECT` statements that appear at the top level of a
schema definition. In migration scripts, bare `SELECT` calls are commonly
used to invoke functions that perform DDL.

**Changes to `schema.rs`:**

1. In the `Statement::Select` match arm (currently falls through to the error
   catch-all), check which features are used:
   - If the SELECT has a `FROM` clause, `WHERE`, `GROUP BY`, `HAVING`,
     `ORDER BY`, `LIMIT`, or `OFFSET` → `issue_todo` (these are query
     features not needed for schema processing).
   - If the SELECT has multiple select expressions → `issue_todo`.

2. For a simple SELECT with a single expression:
   - If the expression is a function call (`Expression::Function`), look up
     the function in `Schemas.functions`.
   - If the function is found and any of its arguments is a string literal,
     try to parse that string as a sequence of SQL statements. This handles
     the general pattern of "call a function whose argument is SQL text."
   - Re-parse each string argument that looks like SQL (may contain
     semicolons / multiple statements) using `parse_statements` with the
     same dialect and options.
   - Recursively feed the parsed statements through the schema processing
     loop.
   - If the function is not found, or no string arguments contain parseable
     SQL, silently ignore (the function call is a runtime operation).

3. Non-function single expressions → silently ignore (e.g. `SELECT 1`).

**Notes:**
- This is fully general — it works for any function name, not just a
  specific one.
- Re-parsed statements operate on the same `Schemas` state.
- Error spans from re-parsed SQL will be relative to the inner string.
  Adjust spans by the offset of the string literal within the outer file
  so that error messages point to the correct source location.

---

### Phase 3: Execute `DO` blocks

**Goal:** Process statements inside `DO` blocks instead of silently ignoring
them.

**Current state:** The parser produces `DoBody::Statements(stmts)` (parsed
body) or `DoBody::String(s, span)` (unparsed dollar-quoted string). The
typer ignores all `Do` statements.

**Changes:**

1. `DoBody::Statements(stmts)` → iterate the statement list and process each
   through the schema handler recursively.
2. `DoBody::String(s, span)` → re-parse `s` with `parse_statements` and
   process each resulting statement.

---

### Phase 4: Handle `Block` and `If` statements

**Goal:** Process PL/pgSQL control-flow statements that appear inside `DO`
blocks.

**`Block` (`BEGIN ... END`):**
Process each statement in `block.statements` sequentially through the schema
handler.

**`If` (`IF ... THEN ... [ELSEIF ...] [ELSE ...] END IF`):**
Execute the first condition's `then` branch unconditionally. Ignore other
branches and the condition expressions. This is correct for idempotent
migration scripts where `IF EXISTS` / `IF NOT EXISTS` guards protect against
re-application — the first-time ("happy path") branch always matches the
sequential state we're building.

**`issue_todo` for:**
- Actually evaluating IF conditions against schema state
- `ELSEIF` / `ELSE` branch execution

---

### Phase 5: DML statements (`INSERT`, `UPDATE`, `DELETE`)

**Goal:** Accept DML statements in schema context without errors.

DML does not change schema shape — it modifies data. The typer only tracks
structural metadata (tables, columns, types, indices).

**Changes:**

1. `Statement::InsertReplace` → silently ignore.
2. `Statement::Update` → silently ignore.
3. `Statement::Delete` → silently ignore.

---

### Phase 6: `CREATE TYPE ... AS ENUM`

**Goal:** Track user-defined enum types so that columns referencing them are
understood.

**Changes to `Schemas`:**

1. Add `types: BTreeMap<Identifier<'a>, TypeDef<'a>>`.
2. Define:
   ```rust
   pub enum TypeDef<'a> {
       Enum { values: Vec<SString<'a>>, span: Span },
   }
   ```
3. Handle `Statement::CreateTypeEnum`:
   - Check `create_options` for `OrReplace`.
   - Insert into `types` map.

**Column type resolution:**
The existing `Type::Named(_) => BaseType::String.into()` path (with the
`TODO lookup name??` comment) is where named types are resolved. Extend
this to look up the name in `Schemas.types` and:
- If found as an `Enum`, produce `Type::Enum(values)`.
- If not found, fall back to current behavior.

This requires passing `Schemas` (or at least the types map) into
`parse_column`.

**`issue_todo` for:**
- `CREATE TYPE ... AS (composite)` — composite types
- `ALTER TYPE` — modifying existing types

---

### Phase 7: `DROP TYPE`

**Goal:** Remove types from the registry.

**Investigation:** Determine what AST node the parser produces for
`DROP TYPE name`. Wire it into the typer to remove from `Schemas.types`.

If the parser doesn't have a `DropType` statement variant, this needs parser
work first.

---

### Phase 8: Additional `ALTER TABLE` specifications

**Goal:** Handle the `AlterSpecification` variants that currently emit
"Not supported" errors.

| Specification | Action |
|---------------|--------|
| `AddTableConstraint` | Ignore — constraints don't affect column types |
| `DisableTrigger` | Ignore — trigger state is not schema structure |
| `EnableTrigger` | Ignore — trigger state is not schema structure |
| `DropIndex` | Remove from `Schemas.indices` (same logic as `DropIndex` statement) |

**`issue_todo` for remaining variants not needed yet:**
- `DropForeignKey`, `DropPrimaryKey` → safe to ignore, but unimplemented
- `RenameColumn` → would need to update column name in schema
- `RenameTo` → would need to re-key the table in `Schemas.schemas`
- `Change` → MySQL-specific column rename+modify

---

### Phase 9: `CREATE TABLE ... LIKE`

**Goal:** Support the `LIKE other_table` clause in `CREATE TABLE`.

**Changes:**

1. Determine how the parser represents `LIKE` in the `CreateDefinition` enum.
2. When processing a `LIKE` clause, look up the source table in
   `Schemas.schemas` and copy all its columns into the new table.
3. Process any additional column definitions or constraints that follow.

**`issue_todo` for:**
- `INCLUDING` / `EXCLUDING` options (e.g. `LIKE t INCLUDING DEFAULTS`)

---

### Phase 10: `COMMENT ON` (parser + typer)

**Goal:** Parse `COMMENT ON <object_type> <name> IS <string>` statements.

**Parser changes (qusql-parse):**

1. Ensure `COMMENT` exists as a keyword.
2. Add `Statement::CommentOn(Box<CommentOn<'a>>)`.
3. Define:
   ```rust
   pub struct CommentOn<'a> {
       pub comment_span: Span,
       pub on_span: Span,
       pub object_type: CommentObjectType,
       pub name: QualifiedName<'a>,
       pub is_span: Span,
       pub comment: Option<SString<'a>>,
   }

   pub enum CommentObjectType {
       Column(Span),
       Table(Span),
       Index(Span),
       Function(Span),
       Schema(Span),
       // extend as needed
   }
   ```
4. Parse the statement in the main statement dispatcher.

**Typer changes:**

Handle `Statement::CommentOn` → silently ignore.

---

### Phase 11: `DROP INDEX` inside `ALTER TABLE`

The existing `DropIndex` statement handler already works. The
`AlterSpecification::DropIndex` variant inside `ALTER TABLE` is covered
by Phase 8.

---

## Implementation Order

```
Phase 1  (Store functions)          ← Replaces empty Functions{} struct
Phase 2  (SELECT statements)        ← Depends on Phase 1 for function lookup
Phase 3  (DO blocks)                ← Required: revisions contain DO blocks
Phase 4  (Block / If)               ← Required: DO blocks contain these
Phase 5  (DML: INSERT/UPDATE/DELETE) ← Required: scripts contain data transforms
Phase 6  (CREATE TYPE ENUM)          ← Independent of above
Phase 7  (DROP TYPE)                 ← Depends on Phase 6
Phase 8  (ALTER TABLE specs)         ← Independent
Phase 9  (CREATE TABLE LIKE)         ← Independent, needs schemas populated
Phase 10 (COMMENT ON)               ← Parser change + typer ignore
```

Phases 6–10 are independent of each other and can be done in any order.
The critical path is **1 → 2 → 3 → 4 → 5**, which unblocks the bulk
of the file.

---

## What We Are NOT Doing

- **Simulating a database:** We don't store row data, evaluate expressions at
  runtime, or implement a query executor. DML is silently ignored.
- **Evaluating PL/pgSQL:** We don't interpret loops, RAISE, EXECUTE,
  variable assignments, or exception handlers inside function bodies.
- **Evaluating IF conditions:** IF blocks inside DO blocks have their first
  branch executed unconditionally.
- **Implementing USING clauses:** `ALTER COLUMN SET DATA TYPE ... USING expr`
  — we take the target type and ignore the USING expression (it's runtime).
- **Hardcoding function names:** The SELECT → function → re-parse-string
  path is fully general and works for any function whose argument is SQL text.

---

## Testing Strategy

After each phase, run:

```bash
cargo build --all && cargo clippy --all
cd parse-test && python3 test.py test-postgresql
cd type_test && python3 test.py
```

Initially, postgresql1 remains in `KNOWN_FAILING`. As phases are completed:

1. After Phase 2: re-run type_test on postgresql1.sql to see the errors
   shift from "Unsupported statement Select" to errors inside the revisions.
2. After each subsequent phase: the error count should decrease.
3. When all errors are resolved: remove postgresql1 from `KNOWN_FAILING`,
   generate `postgresql1.json` as the expected output, and commit.
