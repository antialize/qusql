use alloc::vec::Vec;

use crate::{
    Identifier, Span, Spanned, lexer::Token, parser::{ParseError, Parser}, restrict::Restrict
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

pub(crate) fn parse_qualified_name<'a>(
    parser: &mut Parser<'a, '_>,
    restricted: &Restrict,
) -> Result<QualifiedName<'a>, ParseError> {
    let mut identifier = parser.consume_plain_identifier(restricted)?;
    let mut prefix = Vec::new();
    while let Some(dot) = parser.skip_token(Token::Period) {
        prefix.push((identifier, dot));
        identifier = parser.consume_plain_identifier(restricted)?;
    }
    Ok(QualifiedName { prefix, identifier })
}

/// Convenience wrapper for parse_qualified_name with no keyword restrictions.
#[inline]
pub(crate) fn parse_qualified_name_unrestricted<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<QualifiedName<'a>, ParseError> {
    parse_qualified_name(parser, &Restrict::empty())
}