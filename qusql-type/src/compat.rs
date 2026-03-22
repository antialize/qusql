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

//! Dialect-aware type compatibility functions.
//!
//! The three exported functions (`implicit_coerce`, `binary_coerce`,
//! `resolve_function_return`) replace the old monolithic `matched_type` /
//! `ensure_base` approach and explicitly separate dialect-specific rules from
//! strict-mode rules.
//!
//! # Usage
//!
//! ```ignore
//! let dialect = options.parse_options.get_dialect();
//! let strict  = options.strict;
//!
//! match implicit_coerce(dialect, strict, &lhs_type, &rhs_type) {
//!     Coercion::Exact(t) | Coercion::Implicit(t) => t,
//!     Coercion::Incompatible => { typer.err("Type mismatch", span); Type::Invalid }
//! }
//! ```

use crate::type_::{BaseType, Type};
use qusql_parse::SQLDialect;

/// Result of a type compatibility check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Coercion<'a> {
    /// The types are directly compatible — no conversion required.
    Exact(Type<'a>),
    /// An implicit conversion exists in this dialect's relaxed mode
    /// (i.e. when `strict = false`).  The contained type is the result type
    /// after conversion.
    Implicit(Type<'a>),
    /// The types are incompatible regardless of dialect or strict mode.
    Incompatible,
}

/// Can `from` be used where `to` is expected in the given `dialect`?
///
/// - If `strict = true`, cross-category coercions are always `Incompatible`.
/// - If `strict = false`, the dialect's implicit coercion rules apply.
///
/// Returns the result type on success:
/// - `Exact` when no conversion is needed.
/// - `Implicit` when the dialect would silently convert (but strict mode would reject it).
pub(crate) fn implicit_coerce<'a>(
    dialect: SQLDialect,
    strict: bool,
    from: &Type<'a>,
    to: &Type<'a>,
) -> Coercion<'a> {
    let from_base = from.base();
    let to_base = to.base();

    // Any wildcard — compatible with everything.
    if from_base == BaseType::Any {
        return Coercion::Exact(to.clone());
    }
    if to_base == BaseType::Any {
        return Coercion::Exact(from.clone());
    }

    // Same category — compatible.
    if from_base == to_base {
        return Coercion::Exact(to.clone());
    }

    if strict {
        return Coercion::Incompatible;
    }

    match dialect {
        SQLDialect::MariaDB => mysql_coerce(from_base, to_base, to),
        SQLDialect::PostgreSQL => postgres_coerce(from_base, to_base, to),
        SQLDialect::Sqlite => Coercion::Implicit(to.clone()),
    }
}

/// What is the result type of binary operator `op` given `lhs` and `rhs`?
///
/// Returns `Incompatible` when the combination is not allowed.
/// The caller should use the inner `Type` as the expression result type.
///
/// *Note*: this function only handles the type-compatibility aspect.
/// Checking argument types (e.g. that `AND` requires booleans) is still
/// done at the call site.
pub(crate) fn binary_coerce<'a>(
    dialect: SQLDialect,
    strict: bool,
    lhs: &Type<'a>,
    rhs: &Type<'a>,
) -> Coercion<'a> {
    // Try lhs → rhs first; then rhs → lhs.
    match implicit_coerce(dialect.clone(), strict, lhs, rhs) {
        Coercion::Incompatible => implicit_coerce(dialect, strict, rhs, lhs),
        ok => ok,
    }
}

/// What type does `func` return given the argument types and the target
/// `context` category?
///
/// Currently implemented for cases where the return type is context-dependent:
/// - `NOW()` returns `TimeStamp` in a timestamp context, `DateTime` otherwise.
///
/// For all other functions, callers continue to use the existing `type_function`
/// logic.  This function will be expanded incrementally.
pub(crate) fn resolve_now_return(context: BaseType) -> BaseType {
    match context {
        BaseType::TimeStamp => BaseType::TimeStamp,
        _ => BaseType::DateTime,
    }
}

// ── Dialect-specific coercion rules ─────────────────────────────────────────

fn mysql_coerce<'a>(from: BaseType, to: BaseType, to_type: &Type<'a>) -> Coercion<'a> {
    use BaseType::*;
    match (from, to) {
        // Numeric ↔ string (MySQL coerces freely in both directions)
        (String, Integer | Float | Decimal) | (Integer | Float | Decimal, String) => {
            Coercion::Implicit(to_type.clone())
        }

        // Integer ↔ float
        (Integer, Float | Decimal)
        | (Float | Decimal, Integer)
        | (Float, Decimal)
        | (Decimal, Float) => Coercion::Implicit(to_type.clone()),

        // Any numeric / string used as bool (non-zero / non-empty = true)
        (Integer | Float | Decimal | String, Bool) | (Bool, Integer | Float | Decimal | String) => {
            Coercion::Implicit(to_type.clone())
        }

        // Temporal ↔ string (MySQL parses date/time strings implicitly)
        (String, Date | DateTime | Time | TimeStamp)
        | (Date | DateTime | Time | TimeStamp, String) => Coercion::Implicit(to_type.clone()),

        // Temporal ↔ integer (e.g. 20240101 as a date)
        (Integer, Date | DateTime | TimeStamp) | (Date | DateTime | TimeStamp, Integer) => {
            Coercion::Implicit(to_type.clone())
        }

        _ => Coercion::Incompatible,
    }
}

fn postgres_coerce<'a>(from: BaseType, to: BaseType, to_type: &Type<'a>) -> Coercion<'a> {
    use BaseType::*;
    match (from, to) {
        // PostgreSQL allows implicit integer width promotion
        // (e.g. i32 → i64). This is mostly handled at the concrete-type level,
        // but the base-category match here handles the generic Integer case.
        (Integer, Integer) => Coercion::Exact(to_type.clone()),

        // Float widening
        (Float, Float) | (Integer, Float) => Coercion::Implicit(to_type.clone()),

        // String literals typed as 'unknown' resolve to the target type.
        // This is handled via the context parameter; here we just allow it
        // as an implicit coercion so matched_type doesn't reject it.
        (String, Date | DateTime | Time | TimeStamp | Uuid | Network) => {
            Coercion::Implicit(to_type.clone())
        }

        _ => Coercion::Incompatible,
    }
}
