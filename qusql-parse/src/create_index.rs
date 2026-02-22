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
    Expression, Identifier, QualifiedName, Span, Spanned,
    alter_table::{IndexCol, IndexColExpr, parse_operator_class},
    create_option::CreateOption,
    expression::parse_expression,
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name,
};
use alloc::vec::Vec;

#[derive(Clone, Debug)]
pub enum UsingIndexMethod {
    Gist(Span),
    Bloom(Span),
    Brin(Span),
    Hnsw(Span),
    Gin(Span),
    BTree(Span),
    Hash(Span),
    RTree(Span),
}

impl Spanned for UsingIndexMethod {
    fn span(&self) -> Span {
        match self {
            UsingIndexMethod::Gist(s) => s.clone(),
            UsingIndexMethod::Bloom(s) => s.clone(),
            UsingIndexMethod::Brin(s) => s.clone(),
            UsingIndexMethod::Hnsw(s) => s.clone(),
            UsingIndexMethod::BTree(s) => s.clone(),
            UsingIndexMethod::Hash(s) => s.clone(),
            UsingIndexMethod::RTree(s) => s.clone(),
            UsingIndexMethod::Gin(s) => s.clone(),
        }
    }
}

pub(crate) fn parse_using_index_method<'a>(
    parser: &mut Parser<'a, '_>,
    using_span: Span,
) -> Result<UsingIndexMethod, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::GIST) => {
            let gist_span = parser.consume_keyword(Keyword::GIST)?;
            Ok(UsingIndexMethod::Gist(using_span.join_span(&gist_span)))
        }
        Token::Ident(_, Keyword::BLOOM) => {
            let bloom_span = parser.consume_keyword(Keyword::BLOOM)?;
            Ok(UsingIndexMethod::Bloom(using_span.join_span(&bloom_span)))
        }
        Token::Ident(_, Keyword::BRIN) => {
            let brin_span = parser.consume_keyword(Keyword::BRIN)?;
            Ok(UsingIndexMethod::Brin(using_span.join_span(&brin_span)))
        }
        Token::Ident(_, Keyword::HNSW) => {
            let hnsw_span = parser.consume_keyword(Keyword::HNSW)?;
            Ok(UsingIndexMethod::Hnsw(using_span.join_span(&hnsw_span)))
        }
        Token::Ident(_, Keyword::GIN) => {
            let gin_span = parser.consume_keyword(Keyword::GIN)?;
            Ok(UsingIndexMethod::Gin(using_span.join_span(&gin_span)))
        }
        Token::Ident(_, Keyword::BTREE) => {
            let btree_span = parser.consume_keyword(Keyword::BTREE)?;
            Ok(UsingIndexMethod::BTree(using_span.join_span(&btree_span)))
        }
        Token::Ident(_, Keyword::HASH) => {
            let hash_span = parser.consume_keyword(Keyword::HASH)?;
            Ok(UsingIndexMethod::Hash(using_span.join_span(&hash_span)))
        }
        Token::Ident(_, Keyword::RTREE) => {
            let rtree_span = parser.consume_keyword(Keyword::RTREE)?;
            Ok(UsingIndexMethod::RTree(using_span.join_span(&rtree_span)))
        }
        _ => Err(parser
            .err_here("Expected GIST, BLOOM, BRIN, HNSW, BTREE, HASH, or RTREE after USING")?),
    }
}

#[derive(Clone, Debug)]
pub enum CreateIndexOption<'a> {
    UsingIndex(UsingIndexMethod),
    Algorithm(Span, Identifier<'a>),
    Lock(Span, Identifier<'a>),
}

impl<'a> Spanned for CreateIndexOption<'a> {
    fn span(&self) -> Span {
        match self {
            CreateIndexOption::UsingIndex(method) => method.span(),
            CreateIndexOption::Algorithm(s, i) => s.join_span(i),
            CreateIndexOption::Lock(s, i) => s.join_span(i),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IncludeClause<'a> {
    pub include_span: Span,
    pub l_paren_span: Span,
    pub columns: Vec<Identifier<'a>>,
    pub r_paren_span: Span,
}

impl<'a> Spanned for IncludeClause<'a> {
    fn span(&self) -> Span {
        self.include_span
            .join_span(&self.l_paren_span)
            .join_span(&self.columns)
            .join_span(&self.r_paren_span)
    }
}

#[derive(Clone, Debug)]
pub struct CreateIndex<'a> {
    pub create_span: Span,
    pub create_options: Vec<CreateOption<'a>>,
    pub index_span: Span,
    pub index_name: Option<Identifier<'a>>,
    pub if_not_exists: Option<Span>,
    pub on_span: Span,
    pub table_name: QualifiedName<'a>,
    pub index_options: Vec<CreateIndexOption<'a>>,
    pub l_paren_span: Span,
    pub column_names: Vec<IndexCol<'a>>,
    pub r_paren_span: Span,
    pub include_clause: Option<IncludeClause<'a>>,
    pub where_: Option<(Span, Expression<'a>)>,
    pub nulls_distinct: Option<(Span, Option<Span>)>,
}

impl<'a> Spanned for CreateIndex<'a> {
    fn span(&self) -> Span {
        self.create_span
            .join_span(&self.create_options)
            .join_span(&self.index_span)
            .join_span(&self.index_name)
            .join_span(&self.on_span)
            .join_span(&self.table_name)
            .join_span(&self.index_options)
            .join_span(&self.l_paren_span)
            .join_span(&self.column_names)
            .join_span(&self.r_paren_span)
            .join_span(&self.include_clause)
            .join_span(&self.where_)
            .join_span(&self.nulls_distinct)
    }
}

pub(crate) fn parse_create_index<'a>(
    parser: &mut Parser<'a, '_>,
    create_span: Span,
    mut create_options: Vec<CreateOption<'a>>,
) -> Result<CreateIndex<'a>, ParseError> {
    let index_span = parser.consume_keyword(Keyword::INDEX)?;

    // PostgreSQL: CONCURRENTLY
    if let Some(concurrently_span) = parser.skip_keyword(Keyword::CONCURRENTLY) {
        parser.postgres_only(&concurrently_span);
        create_options.push(CreateOption::Concurrently(concurrently_span));
    }

    let if_not_exists = if let Some(s) = parser.skip_keyword(Keyword::IF) {
        Some(s.join_span(&parser.consume_keywords(&[Keyword::NOT, Keyword::EXISTS])?))
    } else {
        None
    };

    // PostgreSQL: index name is optional, ON can come directly after INDEX
    let index_name = if let Token::Ident(_, Keyword::ON) = &parser.token {
        // Unnamed index
        None
    } else {
        // Named index
        Some(parser.consume_plain_identifier()?)
    };

    // MySQL/MariaDB require index names
    if index_name.is_none() && parser.options.dialect.is_maria() {
        parser.err("Index name required", &index_span);
    }

    let on_span = parser.consume_keyword(Keyword::ON)?;
    let table_name = parse_qualified_name(parser)?;

    // PostgreSQL: USING (GIST|BLOOM|BRIN|HNSW) before column list
    let mut index_options = Vec::new();
    if let Some(using_span) = parser.skip_keyword(Keyword::USING) {
        let using_index_method = parse_using_index_method(parser, using_span)?;
        index_options.push(CreateIndexOption::UsingIndex(using_index_method));
    }

    let l_paren_span = parser.consume_token(Token::LParen)?;
    let mut column_names = Vec::new();
    loop {
        // Check if this is a functional index expression (starts with '(')
        let expr = if parser.token == Token::LParen {
            // Functional index: parse expression
            parser.consume_token(Token::LParen)?;
            let expression = parse_expression(parser, false)?;
            parser.consume_token(Token::RParen)?;
            IndexColExpr::Expression(expression)
        } else {
            // Regular column name
            let name = parser.consume_plain_identifier()?;
            IndexColExpr::Column(name)
        };

        let size = if parser.skip_token(Token::LParen).is_some() {
            let size = parser.recovered("')'", &|t| t == &Token::RParen, |parser| {
                parser.consume_int()
            })?;
            parser.consume_token(Token::RParen)?;
            Some(size)
        } else {
            None
        };

        // Parse optional operator class (PostgreSQL)
        let opclass = parse_operator_class(parser)?;

        // Parse optional ASC | DESC
        let asc = parser.skip_keyword(Keyword::ASC);
        let desc = if asc.is_none() {
            parser.skip_keyword(Keyword::DESC)
        } else {
            None
        };

        column_names.push(IndexCol {
            expr,
            size,
            opclass,
            asc,
            desc,
        });

        if parser.skip_token(Token::Comma).is_none() {
            break;
        }
    }

    let r_paren_span = parser.consume_token(Token::RParen)?;

    // PostgreSQL: INCLUDE clause
    let include_clause = if let Some(include_span) = parser.skip_keyword(Keyword::INCLUDE) {
        let l_paren = parser.consume_token(Token::LParen)?;
        let mut include_cols = Vec::new();
        loop {
            include_cols.push(parser.consume_plain_identifier()?);
            if parser.skip_token(Token::Comma).is_none() {
                break;
            }
        }
        let r_paren = parser.consume_token(Token::RParen)?;
        parser.postgres_only(&include_span);
        Some(IncludeClause {
            include_span,
            l_paren_span: l_paren,
            columns: include_cols,
            r_paren_span: r_paren,
        })
    } else {
        None
    };

    // Parse index options after column list (MySQL/MariaDB)

    // Parse index options after column list (MySQL/MariaDB)
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::USING) => {
                let using_span = parser.consume_keyword(Keyword::USING)?;
                let using_index_method = parse_using_index_method(parser, using_span)?;
                index_options.push(CreateIndexOption::UsingIndex(using_index_method));
            }
            Token::Ident(_, Keyword::ALGORITHM) => {
                let algorithm_span = parser.consume_keyword(Keyword::ALGORITHM)?;
                parser.skip_token(Token::Eq); // Optional =
                let algorithm_value = parser.consume_plain_identifier()?;
                index_options.push(CreateIndexOption::Algorithm(
                    algorithm_span,
                    algorithm_value,
                ));
            }
            Token::Ident(_, Keyword::LOCK) => {
                let lock_span = parser.consume_keyword(Keyword::LOCK)?;
                parser.skip_token(Token::Eq); // Optional =
                let lock_value = parser.consume_plain_identifier()?;
                index_options.push(CreateIndexOption::Lock(lock_span, lock_value));
            }
            _ => break,
        }
    }

    let mut where_ = None;
    if let Some(where_span) = parser.skip_keyword(Keyword::WHERE) {
        let where_expr = parse_expression(parser, false)?;
        if parser.options.dialect.is_maria() {
            parser.err(
                "Partial indexes not supported",
                &where_span.join_span(&where_expr),
            );
        }
        where_ = Some((where_span, where_expr));
    }

    // PostgreSQL: NULLS [NOT] DISTINCT
    let nulls_distinct = if let Some(nulls_span) = parser.skip_keyword(Keyword::NULLS) {
        let not_span = parser.skip_keyword(Keyword::NOT);
        let distinct_span = parser.consume_keyword(Keyword::DISTINCT)?;
        parser.postgres_only(&nulls_span.join_span(&distinct_span));
        Some((nulls_span, not_span))
    } else {
        None
    };

    Ok(CreateIndex {
        create_span,
        create_options,
        index_span,
        index_name,
        if_not_exists,
        on_span,
        table_name,
        index_options,
        l_paren_span,
        column_names,
        r_paren_span,
        include_clause,
        where_,
        nulls_distinct,
    })
}
