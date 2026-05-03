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

use alloc::{borrow::Cow, format, string::String, vec::Vec};

use crate::{
    Identifier, ParseOptions, SString, Span, Spanned,
    issue::{IssueHandle, Issues},
    keywords::{Keyword, Restrict},
    lexer::{Lexer, StringType, Token},
    span::OptSpanned,
};

#[derive(Debug)]
pub(crate) enum ParseError {
    Unrecovered,
}

pub(crate) struct Parser<'a, 'b> {
    pub(crate) token: Token<'a>,
    pub(crate) peeked_token: Option<(Token<'a>, Span)>,
    pub(crate) span: Span,
    pub(crate) lexer: Lexer<'a>,
    pub(crate) issues: &'b mut Issues<'a>,
    pub(crate) arg: usize,
    pub(crate) options: &'b ParseOptions,
    pub(crate) permit_compound_statements: bool,
}

pub(crate) fn decode_single_quoted_string(s: &str) -> Cow<'_, str> {
    if !s.contains('\'') && !s.contains('\\') {
        s.into()
    } else {
        let mut r = String::new();
        let mut chars = s.chars();
        loop {
            match chars.next() {
                None => break,
                Some('\'') => {
                    chars.next();
                    r.push('\'');
                }
                Some(c) => r.push(c),
            }
        }
        r.into()
    }
}

/// Decode a PostgreSQL escape string (`E'...'`).
/// Handles `\\`, `\'`, `\n`, `\t`, `\r`, `\b`, `\f`, `\uXXXX`, `\UXXXXXXXX`,
/// `\xHH` (hex), `\ooo` (octal 1-3 digits), `''` (doubled quote), and any
/// other `\c` → `c` (non-standard passthrough).
pub(crate) fn decode_escape_string(s: &str) -> Cow<'_, str> {
    if !s.contains('\\') && !s.contains('\'') {
        return s.into();
    }
    let mut r = String::new();
    let mut chars = s.chars().peekable();
    loop {
        match chars.next() {
            None => break,
            Some('\'') => {
                // Doubled-quote escape: '' → '
                chars.next();
                r.push('\'');
            }
            Some('\\') => match chars.next() {
                None => break,
                Some('n') => r.push('\n'),
                Some('t') => r.push('\t'),
                Some('r') => r.push('\r'),
                Some('b') => r.push('\x08'),
                Some('f') => r.push('\x0C'),
                Some('\\') => r.push('\\'),
                Some('\'') => r.push('\''),
                Some('"') => r.push('"'),
                Some('0') => r.push('\0'),
                Some('u') => {
                    // \uXXXX — exactly 4 hex digits
                    let mut hex = String::with_capacity(4);
                    for _ in 0..4 {
                        match chars.peek() {
                            Some(&c) if c.is_ascii_hexdigit() => {
                                chars.next();
                                hex.push(c);
                            }
                            _ => break,
                        }
                    }
                    if let Ok(n) = u32::from_str_radix(&hex, 16)
                        && let Some(c) = char::from_u32(n)
                    {
                        r.push(c);
                    }
                }
                Some('U') => {
                    // \UXXXXXXXX — exactly 8 hex digits
                    let mut hex = String::with_capacity(8);
                    for _ in 0..8 {
                        match chars.peek() {
                            Some(&c) if c.is_ascii_hexdigit() => {
                                chars.next();
                                hex.push(c);
                            }
                            _ => break,
                        }
                    }
                    if let Ok(n) = u32::from_str_radix(&hex, 16)
                        && let Some(c) = char::from_u32(n)
                    {
                        r.push(c);
                    }
                }
                Some('x') => {
                    // \xHH — 1 or 2 hex digits
                    let mut hex = String::with_capacity(2);
                    for _ in 0..2 {
                        match chars.peek() {
                            Some(&c) if c.is_ascii_hexdigit() => {
                                chars.next();
                                hex.push(c);
                            }
                            _ => break,
                        }
                    }
                    if !hex.is_empty()
                        && let Ok(n) = u32::from_str_radix(&hex, 16)
                        && let Some(c) = char::from_u32(n)
                    {
                        r.push(c);
                    }
                }
                Some(c @ '1'..='7') => {
                    // \ooo — 1 to 3 octal digits (note: \0 handled above)
                    let mut oct = String::with_capacity(3);
                    oct.push(c);
                    for _ in 0..2 {
                        match chars.peek() {
                            Some(&c) if matches!(c, '0'..='7') => {
                                chars.next();
                                oct.push(c);
                            }
                            _ => break,
                        }
                    }
                    if let Ok(n) = u32::from_str_radix(&oct, 8)
                        && let Some(c) = char::from_u32(n)
                    {
                        r.push(c);
                    }
                }
                Some(c) => r.push(c),
            },
            Some(c) => r.push(c),
        }
    }
    r.into()
}

pub(crate) fn decode_double_quoted_string(s: &str) -> Cow<'_, str> {
    if !s.contains('"') && !s.contains('\\') {
        s.into()
    } else {
        let mut r = String::new();
        let mut chars = s.chars();
        loop {
            match chars.next() {
                None => break,
                Some('\'') => {
                    chars.next();
                    r.push('\'');
                }
                Some(c) => r.push(c),
            }
        }
        r.into()
    }
}

pub(crate) fn decode_hex_string(s: &str) -> Cow<'_, str> {
    let mut bytes = Vec::new();
    let mut chars = s.chars();
    while let Some(c1) = chars.next() {
        if let Some(c2) = chars.next()
            && let (Some(d1), Some(d2)) = (c1.to_digit(16), c2.to_digit(16))
        {
            bytes.push((d1 * 16 + d2) as u8);
        }
    }
    // MySQL hex strings are binary, so we use lossy UTF-8 conversion
    Cow::Owned(String::from_utf8_lossy(&bytes).into_owned())
}

pub(crate) fn decode_binary_string(s: &str) -> Cow<'_, str> {
    let mut bytes = Vec::new();
    let mut bits = 0u8;
    let mut count = 0;

    for c in s.chars() {
        if c == '0' || c == '1' {
            bits = (bits << 1) | (c as u8 - b'0');
            count += 1;
            if count == 8 {
                bytes.push(bits);
                bits = 0;
                count = 0;
            }
        }
    }

    // If there are remaining bits, pad with zeros on the right
    if count > 0 {
        bits <<= 8 - count;
        bytes.push(bits);
    }

    // MySQL binary strings are binary, so we use lossy UTF-8 conversion
    Cow::Owned(String::from_utf8_lossy(&bytes).into_owned())
}

impl<'a, 'b> Parser<'a, 'b> {
    pub(crate) fn new(src: &'a str, issues: &'b mut Issues<'a>, options: &'b ParseOptions) -> Self {
        let mut lexer = Lexer::new(src, &options.dialect, options.get_span_offset());
        let (token, span) = lexer.next_token();
        Self {
            token,
            peeked_token: None,
            span,
            lexer,
            issues,
            arg: 0,
            options,
            permit_compound_statements: options.function_body,
        }
    }

    pub(crate) fn recover(
        &mut self,
        success: impl Fn(&Token<'a>) -> bool,
        fail: impl Fn(&Token<'a>) -> bool,
    ) -> Result<(), ParseError> {
        let mut brackets = Vec::new();
        loop {
            match &self.token {
                t if brackets.is_empty() && success(t) => return Ok(()),
                Token::Eof => return Err(ParseError::Unrecovered),
                Token::Delimiter => return Err(ParseError::Unrecovered),
                t if brackets.is_empty() && fail(t) => return Err(ParseError::Unrecovered),
                Token::LParen => {
                    brackets.push(Token::LParen);
                    self.next();
                }
                Token::LBracket => {
                    brackets.push(Token::LBracket);
                    self.next();
                }
                Token::LBrace => {
                    brackets.push(Token::LBrace);
                    self.next();
                }
                Token::RBrace => {
                    self.next();
                    while let Some(v) = brackets.pop() {
                        if v == Token::LBrace {
                            break;
                        }
                    }
                }
                Token::RBracket => {
                    self.next();
                    while let Some(v) = brackets.pop() {
                        if v == Token::LBracket {
                            break;
                        }
                    }
                }
                Token::RParen => {
                    self.next();
                    while let Some(v) = brackets.pop() {
                        if v == Token::LParen {
                            break;
                        }
                    }
                }
                _ => self.next(),
            }
        }
    }

    pub(crate) fn recovered<T: Default>(
        &mut self,
        expected: &'static str,
        end: &impl Fn(&Token<'a>) -> bool,
        fun: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        let ans = match fun(self) {
            Ok(v) => v,
            Err(_) => {
                self.recover(end, |_| false)?;
                T::default()
            }
        };
        if !end(&self.token) {
            self.expected_error(expected);
            self.recover(end, |_| false)?;
        }
        Ok(ans)
    }

    pub(crate) fn read_from_stdin_and_next(&mut self) -> (&'a str, Span) {
        let stdin = self.lexer.read_from_stdin();
        let (token, span) = self
            .peeked_token
            .take()
            .unwrap_or_else(|| self.lexer.next_token());
        self.token = token;
        self.span = span;
        stdin
    }

    pub(crate) fn next(&mut self) {
        let (token, span) = self
            .peeked_token
            .take()
            .unwrap_or_else(|| self.lexer.next_token());
        self.token = token;
        self.span = span;
    }

    pub(crate) fn peek(&mut self) -> &Token<'a> {
        if self.peeked_token.is_none() {
            self.peeked_token = Some(self.lexer.next_token());
        }
        &self.peeked_token.as_ref().unwrap().0
    }

    pub(crate) fn expected_error(&mut self, name: &'static str) {
        self.err(format!("Expected '{}' here", name), &self.span.span());
    }

    pub(crate) fn err(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        span: &impl Spanned,
    ) -> IssueHandle<'a, '_> {
        self.issues.err(message, span)
    }

    pub(crate) fn warn(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        span: &impl Spanned,
    ) -> IssueHandle<'a, '_> {
        self.issues.warn(message, span)
    }

    pub(crate) fn expected_failure<T>(&mut self, name: &'static str) -> Result<T, ParseError> {
        self.expected_error(name);
        Err(ParseError::Unrecovered)
    }

    /// Check if the given keyword is reserved in the current dialect.
    pub(crate) fn reserved(&self) -> Restrict {
        match self.options.dialect {
            crate::SQLDialect::MariaDB => Restrict::MARIADB,
            crate::SQLDialect::PostgreSQL | crate::SQLDialect::PostGIS => Restrict::POSTGRES,
            crate::SQLDialect::Sqlite => Restrict::SQLITE,
        }
    }

    pub(crate) fn token_to_plain_identifier(
        &mut self,
        token: &Token<'a>,
        span: Span,
    ) -> Result<Identifier<'a>, ParseError> {
        match &token {
            Token::Ident(v, kw) => {
                let v = *v;
                if kw.restricted(self.reserved()) {
                    self.err(
                        format!("'{}' is a reserved identifier use `{}`", v, v),
                        &span,
                    );
                } else if kw != &Keyword::QUOTED_IDENTIFIER
                    && self.options.warn_unquoted_identifiers
                {
                    self.warn(format!("identifiers should be quoted as `{}`", v), &span);
                }
                Ok(Identifier::new(v, span))
            }
            _ => self.expected_failure("identifier"),
        }
    }

    pub(crate) fn consume_plain_identifier_unreserved(
        &mut self,
    ) -> Result<Identifier<'a>, ParseError> {
        self.consume_plain_identifier_restrict(self.reserved())
    }

    pub(crate) fn consume_plain_identifier_restrict(
        &mut self,
        restricted: Restrict,
    ) -> Result<Identifier<'a>, ParseError> {
        match &self.token {
            Token::Ident(v, kw) => {
                let v = *v;
                if kw.restricted(restricted) {
                    self.err(
                        format!("'{}' is a reserved identifier use `{}`", v, v),
                        &self.span.span(),
                    );
                } else if kw != &Keyword::QUOTED_IDENTIFIER
                    && self.options.warn_unquoted_identifiers
                {
                    self.err(
                        format!("identifiers should be quoted as `{}`", v),
                        &self.span.span(),
                    );
                } else if kw == &Keyword::QUOTED_IDENTIFIER && self.options.dialect.is_postgresql()
                {
                    self.err(
                        "quoted identifiers not supported by postgresql",
                        &self.span.span(),
                    );
                }
                Ok(Identifier::new(v, self.consume()))
            }
            Token::String(v, StringType::DoubleQuoted) if self.options.dialect.is_postgresql() => {
                Ok(Identifier::new_case_sensitive(v, self.consume()))
            }
            _ => self.expected_failure("identifier"),
        }
    }

    pub(crate) fn consume_keyword(&mut self, keyword: Keyword) -> Result<Span, ParseError> {
        match &self.token {
            Token::Ident(v, kw) if kw == &keyword => {
                if self.options.warn_none_capital_keywords
                    && v.chars().any(|c| c.is_ascii_lowercase())
                {
                    self.warn(
                        format!(
                            "keyword {} should be in ALL CAPS {}",
                            v,
                            v.to_ascii_uppercase()
                        ),
                        &self.span.span(),
                    );
                }
                Ok(self.consume())
            }
            _ => self.expected_failure(keyword.name()),
        }
    }

    pub(crate) fn consume_keywords(&mut self, keywords: &[Keyword]) -> Result<Span, ParseError> {
        let mut span = self.consume_keyword(keywords[0])?;
        for keyword in &keywords[1..] {
            span = self.consume_keyword(*keyword)?.join_span(&span);
        }
        Ok(span)
    }

    pub(crate) fn skip_keyword(&mut self, keyword: Keyword) -> Option<Span> {
        match &self.token {
            Token::Ident(_, kw) if kw == &keyword => Some(self.consume_keyword(keyword).unwrap()),
            _ => None,
        }
    }

    pub(crate) fn consume_token(&mut self, token: Token) -> Result<Span, ParseError> {
        if self.token != token {
            self.expected_failure(token.name())
        } else {
            Ok(self.consume())
        }
    }

    pub(crate) fn skip_token(&mut self, token: Token) -> Option<Span> {
        if self.token != token {
            None
        } else {
            Some(self.consume())
        }
    }

    pub(crate) fn consume(&mut self) -> Span {
        let span = self.span.clone();
        self.next();
        span
    }

    /// Try to consume a SQL boolean literal: `TRUE`/`ON`/`1` → `Some((true, span))`,
    /// `FALSE`/`OFF`/`0` → `Some((false, span))`, anything else → `None`.
    pub(crate) fn try_parse_bool(&mut self) -> Option<(bool, Span)> {
        match &self.token {
            Token::Ident(_, Keyword::TRUE | Keyword::ON) => Some((true, self.consume())),
            Token::Ident(_, Keyword::FALSE | Keyword::OFF) => Some((false, self.consume())),
            Token::Integer(v) if *v == "1" => Some((true, self.consume())),
            Token::Integer(v) if *v == "0" => Some((false, self.consume())),
            _ => None,
        }
    }

    pub(crate) fn consume_string(&mut self) -> Result<SString<'a>, ParseError> {
        let (mut a, mut b) = match &self.token {
            Token::String(v, kind) => {
                let v = *v;
                let kind = kind.clone();
                let span = self.span.clone();
                self.next();
                let decoded = match kind {
                    StringType::SingleQuoted => decode_single_quoted_string(v),
                    StringType::DoubleQuoted => decode_double_quoted_string(v),
                    StringType::DollarQuoted => Cow::Borrowed(v),
                    StringType::Escape => decode_escape_string(v),
                    StringType::Hex => decode_hex_string(v),
                    StringType::Binary => decode_binary_string(v),
                };
                (decoded, span)
            }
            _ => self.expected_failure("string")?,
        };
        while let Token::String(v, kind) = &self.token {
            let v = *v;
            let kind = kind.clone();
            b = b.join_span(&self.span);
            let decoded = match kind {
                StringType::SingleQuoted => decode_single_quoted_string(v),
                StringType::DoubleQuoted => decode_double_quoted_string(v),
                StringType::DollarQuoted => Cow::Borrowed(v),
                StringType::Escape => decode_escape_string(v),
                StringType::Hex => decode_hex_string(v),
                StringType::Binary => decode_binary_string(v),
            };
            a.to_mut().push_str(decoded.as_ref());
            self.next();
        }
        Ok(SString::new(a, b))
    }

    pub(crate) fn consume_int<T: core::str::FromStr + Default>(
        &mut self,
    ) -> Result<(T, Span), ParseError> {
        match &self.token {
            Token::Integer(v) => {
                let v = match v.parse() {
                    Ok(v) => v,
                    Err(_) => self.err_here("integer outside range").unwrap_or_default(),
                };
                let span = self.span.clone();
                self.next();
                Ok((v, span))
            }
            _ => self.expected_failure("integer"),
        }
    }

    pub(crate) fn consume_signed_int<
        T: core::str::FromStr + Default + core::ops::Neg<Output = T>,
    >(
        &mut self,
    ) -> Result<(T, Span), ParseError> {
        let minus = self.skip_token(Token::Minus);
        if minus.is_none() {
            self.skip_token(Token::Plus);
        }
        match &self.token {
            Token::Integer(v) => {
                let v: T = match v.parse() {
                    Ok(v) => v,
                    Err(_) => self.err_here("integer outside range").unwrap_or_default(),
                };
                if let Some(minus) = minus {
                    let v = -v;
                    let span = minus.join_span(&self.span);
                    self.next();
                    Ok((v, span))
                } else {
                    let span = self.span.clone();
                    self.next();
                    Ok((v, span))
                }
            }
            _ => self.expected_failure("integer"),
        }
    }

    pub(crate) fn consume_float<T: core::str::FromStr + Default>(
        &mut self,
    ) -> Result<(T, Span), ParseError> {
        match &self.token {
            Token::Float(v) => {
                let v = match v.parse() {
                    Ok(v) => v,
                    Err(_) => self.err_here("float outside range").unwrap_or_default(),
                };
                let span = self.span.clone();
                self.next();
                Ok((v, span))
            }
            _ => self.expected_failure("float"),
        }
    }

    pub(crate) fn err_here<T>(
        &mut self,
        message: impl Into<Cow<'static, str>>,
    ) -> Result<T, ParseError> {
        self.err(message, &self.span.span());
        Err(ParseError::Unrecovered)
    }

    pub(crate) fn ice<T>(&mut self, file: &'static str, line: u32) -> Result<T, ParseError> {
        self.err_here(format!("Internal compiler error at {}:{}", file, line))
    }

    pub(crate) fn todo<T>(&mut self, file: &'static str, line: u32) -> Result<T, ParseError> {
        self.err_here(format!("Not yet implemented at {}:{}", file, line))
    }

    /// Verify that the current dialect is PostgreSQL, emitting an error if not.
    /// Only emits an error if the span is present (Some).
    pub(crate) fn postgres_only(&mut self, span: &impl OptSpanned) {
        if !self.options.dialect.is_postgresql()
            && let Some(s) = span.opt_span()
        {
            self.err("Only supported by PostgreSQL", &s);
        }
    }

    /// Verify that the current dialect is MariaDB/MySQL, emitting an error if not.
    /// Only emits an error if the span is present (Some).
    pub(crate) fn maria_only(&mut self, span: &impl OptSpanned) {
        if !self.options.dialect.is_maria()
            && let Some(s) = span.opt_span()
        {
            self.err("Only supported by MariaDB", &s);
        }
    }
}
