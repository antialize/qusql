use alloc::vec::Vec;

use crate::{
    Identifier, Span, Spanned,
    keywords::Restrict,
    lexer::Token,
    parser::{ParseError, Parser},
};

#[derive(Clone, Debug)]
pub struct QualifiedName<'a> {
    pub prefix: Vec<(Identifier<'a>, Span)>,
    pub identifier: Identifier<'a>,
}

impl<'a> Spanned for QualifiedName<'a> {
    fn span(&self) -> Span {
        self.identifier.join_span(&self.prefix)
    }
}

pub(crate) fn parse_qualified_name_restrict<'a>(
    parser: &mut Parser<'a, '_>,
    restricted: Restrict,
) -> Result<QualifiedName<'a>, ParseError> {
    let mut identifier = parser.consume_plain_identifier_restrict(restricted)?;
    let mut prefix = Vec::new();
    while let Some(dot) = parser.skip_token(Token::Period) {
        prefix.push((identifier, dot));
        identifier = parser.consume_plain_identifier_restrict(restricted)?;
    }
    Ok(QualifiedName { prefix, identifier })
}

/// Temporary function will be removed
pub(crate) fn parse_qualified_name<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<QualifiedName<'a>, ParseError> {
    parse_qualified_name_restrict(parser, parser.reserved())
}
