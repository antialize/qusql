use crate::{Identifier, Span, Spanned};

/// Special algorithm used for table creation
#[derive(Clone, Debug)]
pub enum CreateAlgorithm {
    Undefined(Span),
    Merge(Span),
    TempTable(Span),
}
impl Spanned for CreateAlgorithm {
    fn span(&self) -> Span {
        match &self {
            CreateAlgorithm::Undefined(s) => s.span(),
            CreateAlgorithm::Merge(s) => s.span(),
            CreateAlgorithm::TempTable(s) => s.span(),
        }
    }
}

/// Options for create statement
#[derive(Clone, Debug)]
pub enum CreateOption<'a> {
    OrReplace(Span),
    /// TEMPORARY or LOCAL TEMPORARY (PostgreSQL)
    Temporary {
        local_span: Option<Span>,
        temporary_span: Span,
    },
    /// MATERIALIZED (for VIEWs, PostgreSQL)
    Materialized(Span),
    /// CONCURRENTLY (for INDEX, PostgreSQL)
    Concurrently(Span),
    Unique(Span),
    Algorithm(Span, CreateAlgorithm),
    Definer {
        definer_span: Span,
        user: Identifier<'a>,
        host: Identifier<'a>,
    },
    SqlSecurityDefiner(Span, Span),
    SqlSecurityInvoker(Span, Span),
    SqlSecurityUser(Span, Span),
}
impl<'a> Spanned for CreateOption<'a> {
    fn span(&self) -> Span {
        match &self {
            CreateOption::OrReplace(v) => v.span(),
            CreateOption::Temporary {
                local_span,
                temporary_span,
            } => temporary_span.join_span(local_span),
            CreateOption::Materialized(v) => v.span(),
            CreateOption::Concurrently(v) => v.span(),
            CreateOption::Algorithm(s, a) => s.join_span(a),
            CreateOption::Definer {
                definer_span,
                user,
                host,
            } => definer_span.join_span(user).join_span(host),
            CreateOption::SqlSecurityDefiner(a, b) => a.join_span(b),
            CreateOption::SqlSecurityInvoker(a, b) => a.join_span(b),
            CreateOption::SqlSecurityUser(a, b) => a.join_span(b),
            CreateOption::Unique(v) => v.span(),
        }
    }
}
