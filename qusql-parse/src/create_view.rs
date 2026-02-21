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
use crate::{
    QualifiedName, Span, Spanned, Statement,
    create_option::CreateOption,
    keywords::Keyword,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
    statement::parse_compound_query,
};
use alloc::vec::Vec;

/// Represent a create view statement
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, CreateView, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// #
/// let sql = "CREATE ALGORITHM=UNDEFINED DEFINER=`phpmyadmin`@`localhost` SQL SECURITY DEFINER
///    VIEW `v1`
///    AS SELECT
///         `t1`.`id` AS `id`,
///         `t1`.`c1` AS `c1`,
///         (SELECT `t2`.`c2` FROM `t2` WHERE `t2`.`id` = `t1`.`c3`) AS `c2`
///         FROM `t1` WHERE `t1`.`deleted` IS NULL;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
///
/// # assert!(issues.is_ok());
/// let create: CreateView = match stmts.pop() {
///     Some(Statement::CreateView(c)) => c,
///     _ => panic!("We should get an create view statement")
/// };
///
/// assert!(create.name.identifier.as_str() == "v1");
/// println!("{:#?}", create.select)
/// ```

#[derive(Clone, Debug)]
pub struct CreateView<'a> {
    /// Span of "CREATE"
    pub create_span: Span,
    /// Options after "CREATE"
    pub create_options: Vec<CreateOption<'a>>,
    /// Span of "VIEW"
    pub view_span: Span,
    /// Span of "IF NOT EXISTS" if specified
    pub if_not_exists: Option<Span>,
    /// Name of the created view
    pub name: QualifiedName<'a>,
    /// Span of "AS"
    pub as_span: Span,
    /// The select statement following "AS"
    pub select: Statement<'a>,
}

impl<'a> Spanned for CreateView<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.view_span)
            .join_span(&self.if_not_exists)
            .join_span(&self.name)
            .join_span(&self.as_span)
            .join_span(&self.select)
    }
}

pub(crate) fn parse_create_view<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    create_options: Vec<CreateOption<'a>>,
) -> Result<CreateView<'a>, ParseError> {
    let view_span = parser.consume_keyword(Keyword::VIEW)?;

    let if_not_exists = if let Some(if_) = parser.skip_keyword(Keyword::IF) {
        Some(
            parser
                .consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?
                .join_span(&if_),
        )
    } else {
        None
    };

    let name = parse_qualified_name(parser)?;
    // TODO (column_list)

    let as_span = parser.consume_keyword(Keyword::AS)?;

    let select = parse_compound_query(parser)?;

    // TODO [WITH [CASCADED | LOCAL] CHECK OPTION]

    Ok(CreateView {
        create_span,
        create_options,
        view_span,
        if_not_exists,
        name,
        as_span,
        select,
    })
}
