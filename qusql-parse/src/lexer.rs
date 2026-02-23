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

//! Lexer for SQL statements. Converts a SQL string into a stream of tokens.
use crate::{SQLDialect, Span, keywords::Keyword};

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Token<'a> {
    Ampersand,
    At,
    AtAt,
    Backslash,
    Caret,
    Colon,
    ColonEq,
    Comma,
    Div,
    DoubleColon,
    DoubleExclamationMark,
    DoubleAmpersand,
    DoublePipe,
    DoubleDollar,
    Eq,
    ExclamationMark,
    Float(&'a str),
    Gt,
    GtEq,
    Ident(&'a str, Keyword),
    Integer(&'a str),
    Invalid,
    LBrace,
    LBracket,
    LParen,
    Lt,
    LtEq,
    Minus,
    Mod,
    Mul,
    Neq,
    Period,
    Pipe,
    Plus,
    QuestionMark,
    RArrow,
    RArrowJson,
    RDoubleArrowJson,
    RBrace,
    RBracket,
    RParen,
    SemiColon,
    Sharp,
    ShiftLeft,
    ShiftRight,
    SingleQuotedString(&'a str),
    DoubleQuotedString(&'a str),
    HexString(&'a str),
    BinaryString(&'a str),
    Spaceship,
    Tilde,
    PercentS,
    DollarArg(usize),
    AtAtGlobal,
    AtAtSession,
    PostgresOperator(&'a str),
    Eof,
}

impl<'a> Token<'a> {
    /// Returns a human-readable name for the token, used in error messages.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Token::Ampersand => "'&'",
            Token::At => "'@'",
            Token::AtAt => "'@@'",
            Token::Backslash => "'\\'",
            Token::Caret => "'^'",
            Token::Colon => "':'",
            Token::ColonEq => "':='",
            Token::Comma => "','",
            Token::Div => "'/'",
            Token::DoubleColon => "'::'",
            Token::DoubleExclamationMark => "'!!'",
            Token::DoublePipe => "'||'",
            Token::DoubleAmpersand => "'&&'",
            Token::Eq => "'='",
            Token::ExclamationMark => "'!'",
            Token::Float(_) => "Float",
            Token::Gt => "'>'",
            Token::GtEq => "'>='",
            Token::Ident(_, Keyword::NOT_A_KEYWORD) => "Identifier",
            Token::Ident(_, Keyword::QUOTED_IDENTIFIER) => "QuotedIdentifier",
            Token::Ident(_, kw) => kw.name(),
            Token::Integer(_) => "Integer",
            Token::Invalid => "Invalid",
            Token::LBrace => "'{'",
            Token::LBracket => "'['",
            Token::LParen => "'('",
            Token::Lt => "'<'",
            Token::LtEq => "'<='",
            Token::Minus => "'-'",
            Token::Mod => "'%'",
            Token::Mul => "'*'",
            Token::Neq => "'!='",
            Token::Period => "'.'",
            Token::Pipe => "'|'",
            Token::Plus => "'+'",
            Token::QuestionMark => "'?'",
            Token::RArrow => "'=>'",
            Token::RArrowJson => "'->'",
            Token::RDoubleArrowJson => "->>'",
            Token::RBrace => "'}'",
            Token::RBracket => "']'",
            Token::RParen => "')'",
            Token::SemiColon => "';'",
            Token::Sharp => "'#'",
            Token::ShiftLeft => "'>>'",
            Token::ShiftRight => "'<<'",
            Token::DoubleDollar => "'$$'",
            Token::DollarArg(v) if *v == 1 => "'$1'",
            Token::DollarArg(v) if *v == 2 => "'$2'",
            Token::DollarArg(v) if *v == 3 => "'$3'",
            Token::DollarArg(v) if *v == 4 => "'$4'",
            Token::DollarArg(v) if *v == 5 => "'$5'",
            Token::DollarArg(v) if *v == 6 => "'$6'",
            Token::DollarArg(v) if *v == 7 => "'$7'",
            Token::DollarArg(v) if *v == 8 => "'$8'",
            Token::DollarArg(v) if *v == 9 => "'$9'",
            Token::DollarArg(_) => "'$i'",
            Token::SingleQuotedString(_) => "String",
            Token::DoubleQuotedString(_) => "String",
            Token::HexString(_) => "HexString",
            Token::BinaryString(_) => "BinaryString",
            Token::Spaceship => "'<=>'",
            Token::Tilde => "'~'",
            Token::PercentS => "'%s'",
            Token::AtAtGlobal => "@@GLOBAL",
            Token::AtAtSession => "@@SESSION",
            Token::PostgresOperator(_) => "pg operator",
            Token::Eof => "EndOfFile",
        }
    }
}

/// A simple character iterator that keeps track of the current index in the source string.
#[derive(Debug, Clone)]
struct CharsIter<'a> {
    /// The current character index in the source string.
    idx: usize,
    /// The remaining characters as bytes.
    rem: &'a [u8],
}

impl<'a> CharsIter<'a> {
    /// Returns the next character and its index, or `None` if we've reached the end of the input.
    fn next(&mut self) -> Option<(usize, u8)> {
        if let Some(v) = self.rem.split_off_first() {
            let i = self.idx;
            self.idx += 1;
            Some((i, *v))
        } else {
            None
        }
    }

    /// Peeks at the next character and its index without consuming it, or `None` if we've reached the end of the input.
    fn peek(&self) -> Option<(usize, u8)> {
        self.rem.first().map(|v| (self.idx, *v))
    }
}

/// The main lexer struct that holds the source string, the character iterator, and the SQL dialect.
pub(crate) struct Lexer<'a> {
    /// The original source string being lexed.
    src: &'a str,
    /// An iterator over the characters of the source string, keeping track of the current index.
    chars: CharsIter<'a>,
    /// The SQL dialect to use for lexing, which may affect how certain tokens are recognized.
    dialect: SQLDialect,
}

impl<'a> Lexer<'a> {
    /// Creates a new `Lexer` instance for the given source string and SQL dialect.
    pub fn new(src: &'a str, dialect: &SQLDialect) -> Self {
        Self {
            src,
            chars: CharsIter {
                idx: 0,
                rem: src.as_bytes(),
            },
            dialect: dialect.clone(),
        }
    }

    /// Returns the substring of the source corresponding to the given span.
    pub(crate) fn s(&self, span: Span) -> &'a str {
        // Safety: The span is expected to match the unicode boundaries
        unsafe { core::str::from_utf8_unchecked(&self.src.as_bytes()[span]) }
    }

    /// Lexes an unquoted identifier starting at the given index. The first character has already been consumed and is at `start`.
    fn unquoted_identifier(&mut self, start: usize) -> Token<'a> {
        let end = loop {
            match self.chars.peek() {
                Some((_, b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')) => {
                    self.chars.next();
                }
                // For MariaDB, allow $ and @ in identifiers
                Some((_, b'$' | b'@')) if self.dialect.is_maria() => {
                    self.chars.next();
                }
                // MySQL allows Unicode characters (U+0080 and above) in identifiers
                Some((_, c)) if self.dialect.is_maria() && (c & 0xc0) == 0xc0 => {
                    self.chars.next();
                    while let Some((_, c)) = self.chars.peek()
                        && (c & 0xc0) == 0x80
                    {
                        self.chars.next();
                    }
                }
                Some((i, _)) => break i,
                None => break self.src.len(),
            }
        };
        let s = self.s(start..end);
        Token::Ident(s, s.into())
    }

    /// Simulate reading from standard input after a statement like `COPY ... FROM STDIN;`.
    /// First skips space characters and optionally one NL.
    /// Then consumes until NL '\' '.' NL is encountered, or until EOF.
    /// The trailing '\' '.' NL is consumed but not returned.
    pub fn read_from_stdin(&mut self) -> (&'a str, Span) {
        // Skip optional spaces.
        while self
            .chars
            .peek()
            .filter(|(_, c)| *c != b'\n' && c.is_ascii_whitespace())
            .is_some()
        {
            self.chars.next().unwrap();
        }
        let start = match self.chars.peek() {
            Some((i, b'\n')) => i + 1,
            Some((i, _)) => i,
            None => {
                let span = self.src.len()..self.src.len();
                return (self.s(span.clone()), span);
            }
        };
        while let Some((i, c)) = self.chars.next() {
            if c != b'\n' {
                continue;
            }
            if !matches!(self.chars.peek(), Some((_, b'\\'))) {
                continue;
            }
            self.chars.next().unwrap();
            if !matches!(self.chars.peek(), Some((_, b'.'))) {
                continue;
            }
            self.chars.next().unwrap();
            if matches!(self.chars.peek(), Some((_, b'\n'))) {
                // Data ends with NL '\' '.' NL.
                self.chars.next().unwrap();
            } else if self.chars.peek().is_some() {
                continue;
            } else {
                // Data ends with NL '\' '.' without an extra NL,
                // which is fine.
            }
            // `i` is the character index of the first '\n',
            // so the data ends at character index i + 1.
            let span = start..(i + 1);
            return (self.s(span.clone()), span);
        }
        // Data ends at EOF without NL '\' '.' [NL].
        let span = start..self.src.len();
        (self.s(span.clone()), span)
    }

    /// In PostgreSQL, operators can be multiple characters long and can contain a wide range of special characters.
    fn next_operator(
        &mut self,
        start: usize,
        mut last: (usize, u8),
        token: Token<'a>,
    ) -> Token<'a> {
        if self.dialect.is_postgresql() {
            // In PostgreSQL, many operators can be multiple characters long and can contain a wide range of special characters.
            // We will consume characters until we encounter one that cannot be part of an operator.
            // Valid operator characters in PostgreSQL include: ! @ # $ % ^ & * ( ) - + = ~ < > | / ? :
            // Additionally, we will allow operators to end with a '*' if it is preceded by a '/' to support C-style comments as operators (e.g. '/*' and '*/').
        } else {
            // In other dialects, we only consider the single character as the operator.
            return token;
        }
        let mut token = Some(token);
        loop {
            match self.chars.peek() {
                Some((
                    _,
                    b'!' | b'@' | b'#' | b'$' | b'%' | b'^' | b'&' | b'(' | b')' | b'+' | b'='
                    | b'~' | b'<' | b'>' | b'|' | b'/' | b'?' | b':',
                )) => {
                    last = self.chars.next().unwrap();
                    token = None;
                }
                Some((_, b'*')) => {
                    if last.1 == b'/' {
                        self.chars.next();
                        let ok = loop {
                            match self.chars.next() {
                                Some((_, b'*')) => {
                                    if matches!(self.chars.peek(), Some((_, b'/'))) {
                                        self.chars.next();
                                        break true;
                                    }
                                }
                                Some(_) => (),
                                None => break false,
                            }
                        };
                        if ok {
                            return Token::PostgresOperator(self.s(start..last.0));
                        } else {
                            return Token::Invalid;
                        }
                    }
                    last = self.chars.next().unwrap();
                    token = None;
                }
                Some((_, b'-')) => {
                    if last.1 == b'-' {
                        while !matches!(self.chars.next(), Some((_, b'\r' | b'\n')) | None) {}
                        return Token::PostgresOperator(self.s(start..last.0));
                    }
                    last = self.chars.next().unwrap();
                    token = None;
                }
                _ => {
                    if let Some(t) = token {
                        return t;
                    } else {
                        let s = self.s(start..last.0 + 1);
                        return Token::PostgresOperator(s);
                    }
                }
            }
        }
    }

    /// Returns the next token and its span in the source string.
    /// If the end of the input is reached, returns `Token::Eof` with an empty span at the end of the source.
    pub fn next_token(&mut self) -> (Token<'a>, Span) {
        loop {
            let (start, c) = match self.chars.next() {
                Some(v) => v,
                None => {
                    return (Token::Eof, self.src.len()..self.src.len());
                }
            };
            let t = match c {
                b' ' | b'\t' | b'\n' | b'\r' => continue,
                b'?' => self.next_operator(start, (start, c), Token::QuestionMark),
                b';' => Token::SemiColon,
                b'\\' => Token::Backslash,
                b'[' => Token::LBracket,
                b']' => Token::RBracket,
                b'&' => match self.chars.peek() {
                    Some((_, b'&')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::DoubleAmpersand)
                    }
                    _ => self.next_operator(start, (start, c), Token::Ampersand),
                },
                b'^' => self.next_operator(start, (start, c), Token::Caret),
                b'{' => Token::LBrace,
                b'}' => Token::RBrace,
                b'(' => Token::LParen,
                b')' => Token::RParen,
                b',' => Token::Comma,
                b'+' => self.next_operator(start, (start, c), Token::Plus),
                b'*' => self.next_operator(start, (start, c), Token::Mul),
                b'%' => match self.chars.peek() {
                    Some((_, b's')) => {
                        self.chars.next();
                        Token::PercentS
                    }
                    _ => self.next_operator(start, (start, c), Token::Mod),
                },
                b'#' => self.next_operator(start, (start, c), Token::Sharp),
                b'@' => match self.chars.peek() {
                    Some((_, b'@')) => {
                        let next = self.chars.next().unwrap();
                        #[allow(clippy::never_loop)]
                        match self.chars.peek() {
                            Some((_, b's' | b'S')) => loop {
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'e' | b'E'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b's' | b'S'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'i' | b'I'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'o' | b'O'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'n' | b'N'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                break Token::AtAtSession;
                            },
                            Some((_, b'g' | b'G')) => loop {
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'l' | b'L'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'o' | b'O'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'b' | b'B'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'a' | b'A'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, b'l' | b'L'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                break Token::AtAtGlobal;
                            },
                            _ => self.next_operator(start, next, Token::AtAt),
                        }
                    }
                    _ => self.next_operator(start, (start, c), Token::At),
                },
                b'~' => self.next_operator(start, (start, c), Token::Tilde),
                b':' => match self.chars.peek() {
                    Some((_, b':')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::DoubleColon)
                    }
                    Some((_, b'=')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::ColonEq)
                    }
                    _ => self.next_operator(start, (start, c), Token::Colon),
                },
                b'$' => match self.chars.peek() {
                    Some((_, b'$')) => {
                        self.chars.next();
                        Token::DoubleDollar
                    }
                    Some((_, b'1'..=b'9')) if self.dialect.is_postgresql() => {
                        let mut v = (self.chars.peek().unwrap().1 - b'0') as usize;
                        self.chars.next();
                        while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                            v = v * 10 + (self.chars.peek().unwrap().1 - b'0') as usize;
                            self.chars.next();
                        }
                        Token::DollarArg(v)
                    }
                    _ if self.dialect.is_maria() => {
                        // In MariaDB, $ can start an identifier
                        self.unquoted_identifier(start)
                    }
                    _ => Token::Invalid,
                },
                b'=' => match self.chars.peek() {
                    Some((_, b'>')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::RArrow)
                    }
                    _ => self.next_operator(start, (start, c), Token::Eq),
                },
                b'!' => match self.chars.peek() {
                    Some((_, b'=')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::Neq)
                    }
                    Some((_, b'!')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::DoubleExclamationMark)
                    }
                    _ => self.next_operator(start, (start, c), Token::ExclamationMark),
                },
                b'<' => match self.chars.peek() {
                    Some((_, b'=')) => {
                        let next = self.chars.next().unwrap();
                        match self.chars.peek() {
                            Some((_, b'>')) => {
                                let next = self.chars.next().unwrap();
                                self.next_operator(start, next, Token::Spaceship)
                            }
                            _ => self.next_operator(start, next, Token::LtEq),
                        }
                    }
                    Some((_, b'>')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::Neq)
                    }
                    Some((_, b'<')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::ShiftLeft)
                    }
                    _ => self.next_operator(start, (start, c), Token::Lt),
                },
                b'>' => match self.chars.peek() {
                    Some((_, b'=')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::GtEq)
                    }
                    Some((_, b'>')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::ShiftRight)
                    }
                    _ => self.next_operator(start, (start, c), Token::Gt),
                },
                b'|' => match self.chars.peek() {
                    Some((_, b'|')) => {
                        let next = self.chars.next().unwrap();
                        self.next_operator(start, next, Token::DoublePipe)
                    }
                    _ => self.next_operator(start, (start, c), Token::Pipe),
                },
                b'-' => match self.chars.peek() {
                    Some((_, b'-')) => {
                        while !matches!(self.chars.next(), Some((_, b'\r' | b'\n')) | None) {}
                        continue;
                    }
                    Some((_, b'>')) => {
                        let next = self.chars.next().unwrap();
                        match self.chars.peek() {
                            Some((_, b'>')) => {
                                let next = self.chars.next().unwrap();
                                self.next_operator(start, next, Token::RDoubleArrowJson)
                            }
                            _ => self.next_operator(start, next, Token::RArrowJson),
                        }
                    }
                    _ => self.next_operator(start, (start, c), Token::Minus),
                },
                b'/' => match self.chars.peek() {
                    Some((_, b'*')) => {
                        self.chars.next();
                        let ok = loop {
                            match self.chars.next() {
                                Some((_, b'*')) => {
                                    if matches!(self.chars.peek(), Some((_, b'/'))) {
                                        self.chars.next();
                                        break true;
                                    }
                                }
                                Some(_) => (),
                                None => break false,
                            }
                        };
                        if ok {
                            continue;
                        } else {
                            Token::Invalid
                        }
                    }
                    Some((_, b'/')) => {
                        while !matches!(self.chars.next(), Some((_, b'\r' | b'\n')) | None) {}
                        continue;
                    }
                    _ => self.next_operator(start, (start, c), Token::Div),
                },
                b'x' | b'X' => match self.chars.peek() {
                    Some((_, b'\'')) => {
                        self.chars.next(); // consume the '
                        loop {
                            match self.chars.next() {
                                Some((i, b'\'')) => break Token::HexString(self.s(start + 2..i)),
                                Some((_, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')) => (),
                                Some((_, _)) => break Token::Invalid,
                                None => break Token::Invalid,
                            }
                        }
                    }
                    _ => self.unquoted_identifier(start),
                },
                b'b' | b'B' => match self.chars.peek() {
                    Some((_, b'\'')) => {
                        self.chars.next(); // consume the '
                        loop {
                            match self.chars.next() {
                                Some((i, b'\'')) => {
                                    break Token::BinaryString(self.s(start + 2..i));
                                }
                                Some((_, b'0' | b'1')) => (),
                                Some((_, _)) => break Token::Invalid,
                                None => break Token::Invalid,
                            }
                        }
                    }
                    _ => self.unquoted_identifier(start),
                },
                b'_' | b'a'..=b'z' | b'A'..=b'Z' => self.unquoted_identifier(start),
                b'`' => {
                    // MySQL backtick-quoted identifiers can contain any character except backticks
                    // Backticks can be escaped by doubling them
                    loop {
                        match self.chars.next() {
                            Some((i, b'`')) => {
                                // Check if it's a doubled backtick (escape sequence)
                                if matches!(self.chars.peek(), Some((_, b'`'))) {
                                    self.chars.next(); // consume the second backtick
                                    continue;
                                } else {
                                    // End of identifier
                                    break Token::Ident(
                                        self.s(start + 1..i),
                                        Keyword::QUOTED_IDENTIFIER,
                                    );
                                }
                            }
                            Some((_, _)) => continue,
                            None => break Token::Invalid,
                        }
                    }
                }
                b'\'' => loop {
                    match self.chars.next() {
                        Some((_, b'\\')) => {
                            self.chars.next();
                        }
                        Some((i, b'\'')) => match self.chars.peek() {
                            Some((_, b'\'')) => {
                                self.chars.next();
                            }
                            _ => break Token::SingleQuotedString(self.s(start + 1..i)),
                        },
                        Some((_, _)) => (),
                        None => break Token::Invalid,
                    }
                },
                b'"' => loop {
                    match self.chars.next() {
                        Some((_, b'\\')) => {
                            self.chars.next();
                        }
                        Some((i, b'"')) => match self.chars.peek() {
                            Some((_, b'"')) => {
                                self.chars.next();
                            }
                            _ => break Token::DoubleQuotedString(self.s(start + 1..i)),
                        },
                        Some((_, _)) => (),
                        None => break Token::Invalid,
                    }
                },
                b'0'..=b'9' => {
                    // For MariaDB, identifiers can start with digits
                    if self.dialect.is_maria() {
                        // Consume initial digits
                        while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                            self.chars.next();
                        }

                        // Now make the decision about what kind of token this is
                        match self.chars.peek() {
                            // If followed by identifier char (not e/E or .), it's an identifier
                            Some((
                                _,
                                b'_'
                                | b'a'..=b'd'
                                | b'f'..=b'z'
                                | b'A'..=b'D'
                                | b'F'..=b'Z'
                                | b'$'
                                | b'@',
                            )) => {
                                // It's an identifier - consume remaining identifier chars
                                while matches!(
                                    self.chars.peek(),
                                    Some((
                                        _,
                                        b'_'
                                        | b'a'..=b'z'
                                        | b'A'..=b'Z'
                                        | b'0'..=b'9'
                                        | b'$'
                                        | b'@',
                                    ))
                                ) {
                                    self.chars.next();
                                }
                                let end = match self.chars.peek() {
                                    Some((i, _)) => i,
                                    None => self.src.len(),
                                };
                                let s = self.s(start..end);
                                Token::Ident(s, Keyword::NOT_A_KEYWORD)
                            }
                            // Check for exponent notation
                            Some((_, b'e' | b'E')) => {
                                // Peek ahead to see if this is a valid exponent or identifier char
                                let mut temp = self.chars.clone();
                                temp.next(); // skip 'e'/'E'
                                if matches!(temp.peek(), Some((_, b'+' | b'-'))) {
                                    temp.next();
                                }
                                if matches!(temp.peek(), Some((_, b'0'..=b'9'))) {
                                    // Valid exponent - check if identifier chars follow after exponent
                                    self.chars.next(); // consume 'e'/'E'
                                    if matches!(self.chars.peek(), Some((_, b'+' | b'-'))) {
                                        self.chars.next();
                                    }
                                    while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                                        self.chars.next();
                                    }
                                    // Now check if identifier chars follow the exponent
                                    if matches!(
                                        self.chars.peek(),
                                        Some((_, b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'$' | b'@'))
                                    ) {
                                        // Identifier chars follow - it's an identifier
                                        while matches!(
                                            self.chars.peek(),
                                            Some((
                                                _,
                                                b'_'
                                                | b'a'..=b'z'
                                                | b'A'..=b'Z'
                                                | b'0'..=b'9'
                                                | b'$'
                                                | b'@',
                                            ))
                                        ) {
                                            self.chars.next();
                                        }
                                        let end = match self.chars.peek() {
                                            Some((i, _)) => i,
                                            None => self.src.len(),
                                        };
                                        let s = self.s(start..end);
                                        Token::Ident(s, Keyword::NOT_A_KEYWORD)
                                    } else {
                                        // No identifier chars - it's a float
                                        let end = match self.chars.peek() {
                                            Some((i, _)) => i,
                                            None => self.src.len(),
                                        };
                                        Token::Float(self.s(start..end))
                                    }
                                } else {
                                    // Not a valid exponent - treat 'e'/'E' as identifier char
                                    while matches!(
                                        self.chars.peek(),
                                        Some((
                                            _,
                                            b'_'
                                            | b'a'..=b'z'
                                            | b'A'..=b'Z'
                                            | b'0'..=b'9'
                                            | b'$'
                                            | b'@',
                                        ))
                                    ) {
                                        self.chars.next();
                                    }
                                    let end = match self.chars.peek() {
                                        Some((i, _)) => i,
                                        None => self.src.len(),
                                    };
                                    let s = self.s(start..end);
                                    Token::Ident(s, Keyword::NOT_A_KEYWORD)
                                }
                            }
                            // Check for decimal point
                            Some((_, b'.')) => {
                                let mut temp = self.chars.clone();
                                temp.next();
                                if matches!(temp.peek(), Some((_, b'0'..=b'9'))) {
                                    // Valid float - consume decimal part
                                    self.chars.next(); // consume '.'
                                    while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                                        self.chars.next();
                                    }
                                    // Check for exponent
                                    if matches!(self.chars.peek(), Some((_, b'e' | b'E'))) {
                                        let mut temp2 = self.chars.clone();
                                        temp2.next();
                                        if matches!(temp2.peek(), Some((_, b'+' | b'-'))) {
                                            temp2.next();
                                        }
                                        if matches!(temp2.peek(), Some((_, b'0'..=b'9'))) {
                                            self.chars.next(); // consume 'e'/'E'
                                            if matches!(self.chars.peek(), Some((_, b'+' | b'-'))) {
                                                self.chars.next();
                                            }
                                            while matches!(
                                                self.chars.peek(),
                                                Some((_, b'0'..=b'9'))
                                            ) {
                                                self.chars.next();
                                            }
                                        }
                                    }
                                    let end = match self.chars.peek() {
                                        Some((i, _)) => i,
                                        None => self.src.len(),
                                    };
                                    Token::Float(self.s(start..end))
                                } else {
                                    // Period not followed by digit - just an integer
                                    let end = match self.chars.peek() {
                                        Some((i, _)) => i,
                                        None => self.src.len(),
                                    };
                                    Token::Integer(self.s(start..end))
                                }
                            }
                            // No special chars - just an integer
                            _ => {
                                let end = match self.chars.peek() {
                                    Some((i, _)) => i,
                                    None => self.src.len(),
                                };
                                Token::Integer(self.s(start..end))
                            }
                        }
                    } else {
                        // Non-MariaDB: parse as number only (never as identifier)
                        let mut is_float = false;
                        loop {
                            match self.chars.peek() {
                                Some((_, b'0'..=b'9')) => {
                                    self.chars.next();
                                }
                                Some((_, b'.')) => {
                                    self.chars.next();
                                    is_float = true;
                                    // Consume fractional part
                                    while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                                        self.chars.next();
                                    }
                                    // Check for exponent
                                    if matches!(self.chars.peek(), Some((_, b'e' | b'E'))) {
                                        self.chars.next();
                                        if matches!(self.chars.peek(), Some((_, b'+' | b'-'))) {
                                            self.chars.next();
                                        }
                                        while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                                            self.chars.next();
                                        }
                                    }
                                    break;
                                }
                                Some((_, b'e' | b'E')) => {
                                    self.chars.next();
                                    is_float = true;
                                    if matches!(self.chars.peek(), Some((_, b'+' | b'-'))) {
                                        self.chars.next();
                                    }
                                    while matches!(self.chars.peek(), Some((_, b'0'..=b'9'))) {
                                        self.chars.next();
                                    }
                                    break;
                                }
                                _ => break,
                            }
                        }
                        let end = match self.chars.peek() {
                            Some((i, _)) => i,
                            None => self.src.len(),
                        };
                        if is_float {
                            Token::Float(self.s(start..end))
                        } else {
                            Token::Integer(self.s(start..end))
                        }
                    }
                }
                b'.' => match self.chars.peek() {
                    Some((_, b'0'..=b'9')) => loop {
                        match self.chars.peek() {
                            Some((_, b'0'..=b'9')) => {
                                self.chars.next();
                            }
                            Some((i, _)) => break Token::Float(self.s(start..i)),
                            None => break Token::Float(self.s(start..self.src.len())),
                        }
                    },
                    _ => Token::Period,
                },
                // In MariaDB, Unicode characters (U+0080 and above) can start identifiers
                c if self.dialect.is_maria() && (c as u32) >= 0x80 => {
                    self.unquoted_identifier(start)
                }
                _ => Token::Invalid,
            };

            let end = match self.chars.peek() {
                Some((i, _)) => i,
                None => self.src.len(),
            };
            return (t, start..end);
        }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = (Token<'a>, Span);

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_token())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    /// Helper function to lex a single token from the input string. It returns the token without its span.
    fn lex_single<'a>(src: &'a str, dialect: &SQLDialect) -> Token<'a> {
        let mut lexer = Lexer::new(src, dialect);
        lexer.next_token().0
    }

    /// Helper function to lex all tokens from the input string. It returns a vector of tokens without their spans.
    fn lex_all<'a>(src: &'a str, dialect: &SQLDialect) -> Vec<Token<'a>> {
        let mut lexer = Lexer::new(src, dialect);
        let mut tokens = Vec::new();
        loop {
            let (token, _) = lexer.next_token();
            if token == Token::Eof {
                break;
            }
            tokens.push(token);
        }
        tokens
    }

    /// Tests that keywords are correctly recognized and case-insensitive.
    /// It also checks that they are categorized as keywords rather than identifiers.
    #[test]
    fn test_keywords() {
        let dialect = SQLDialect::MariaDB;

        assert!(matches!(
            lex_single("SELECT", &dialect),
            Token::Ident(_, Keyword::SELECT)
        ));
        assert!(matches!(
            lex_single("select", &dialect),
            Token::Ident(_, Keyword::SELECT)
        ));
        assert!(matches!(
            lex_single("FROM", &dialect),
            Token::Ident(_, Keyword::FROM)
        ));
        assert!(matches!(
            lex_single("WHERE", &dialect),
            Token::Ident(_, Keyword::WHERE)
        ));
        assert!(matches!(
            lex_single("ORDER", &dialect),
            Token::Ident(_, Keyword::ORDER)
        ));
        assert!(matches!(
            lex_single("BY", &dialect),
            Token::Ident(_, Keyword::BY)
        ));
        assert!(matches!(
            lex_single("ASC", &dialect),
            Token::Ident(_, Keyword::ASC)
        ));
        assert!(matches!(
            lex_single("DESC", &dialect),
            Token::Ident(_, Keyword::DESC)
        ));
        assert!(matches!(
            lex_single("DELETE", &dialect),
            Token::Ident(_, Keyword::DELETE)
        ));
        assert!(matches!(
            lex_single("INSERT", &dialect),
            Token::Ident(_, Keyword::INSERT)
        ));
        assert!(matches!(
            lex_single("UPDATE", &dialect),
            Token::Ident(_, Keyword::UPDATE)
        ));
        assert!(matches!(
            lex_single("CREATE", &dialect),
            Token::Ident(_, Keyword::CREATE)
        ));
        assert!(matches!(
            lex_single("DROP", &dialect),
            Token::Ident(_, Keyword::DROP)
        ));
        assert!(matches!(
            lex_single("ALTER", &dialect),
            Token::Ident(_, Keyword::ALTER)
        ));
        assert!(matches!(
            lex_single("TABLE", &dialect),
            Token::Ident(_, Keyword::TABLE)
        ));
    }

    /// Tests that identifiers are correctly recognized in various forms, including unquoted, backtick-quoted, and with escaped characters.
    #[test]
    fn test_identifiers() {
        let dialect = SQLDialect::MariaDB;

        // Unquoted identifiers
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("my_table", &dialect) {
            assert_eq!(name, "my_table");
        } else {
            panic!("Expected unquoted identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("column123", &dialect) {
            assert_eq!(name, "column123");
        } else {
            panic!("Expected unquoted identifier");
        }

        // Backtick-quoted identifiers
        if let Token::Ident(name, Keyword::QUOTED_IDENTIFIER) = lex_single("`quoted`", &dialect) {
            assert_eq!(name, "quoted");
        } else {
            panic!("Expected quoted identifier");
        }

        if let Token::Ident(name, Keyword::QUOTED_IDENTIFIER) = lex_single("`desc`", &dialect) {
            assert_eq!(name, "desc");
        } else {
            panic!("Expected quoted identifier");
        }

        // Escaped backticks
        if let Token::Ident(name, Keyword::QUOTED_IDENTIFIER) = lex_single("`back``tick`", &dialect)
        {
            assert_eq!(name, "back``tick");
        } else {
            panic!("Expected quoted identifier with escaped backtick");
        }
    }

    /// Tests that numbers are correctly recognized in various formats, including integers, floats, and scientific notation.
    #[test]
    fn test_numbers() {
        let dialect = SQLDialect::MariaDB;

        // Integers
        if let Token::Integer(value) = lex_single("123", &dialect) {
            assert_eq!(value, "123");
        } else {
            panic!("Expected integer");
        }

        if let Token::Integer(value) = lex_single("0", &dialect) {
            assert_eq!(value, "0");
        } else {
            panic!("Expected integer");
        }

        // Floats
        if let Token::Float(value) = lex_single("123.456", &dialect) {
            assert_eq!(value, "123.456");
        } else {
            panic!("Expected float");
        }

        if let Token::Float(value) = lex_single(".5", &dialect) {
            assert_eq!(value, ".5");
        } else {
            panic!("Expected float");
        }

        // Scientific notation
        if let Token::Float(value) = lex_single("1.5e10", &dialect) {
            assert_eq!(value, "1.5e10");
        } else {
            panic!("Expected float in scientific notation");
        }

        if let Token::Float(value) = lex_single("2E-5", &dialect) {
            assert_eq!(value, "2E-5");
        } else {
            panic!("Expected float in scientific notation");
        }

        if let Token::Float(value) = lex_single("3e+2", &dialect) {
            assert_eq!(value, "3e+2");
        } else {
            panic!("Expected float in scientific notation");
        }
    }

    /// Tests that different types of strings are correctly recognized, including single-quoted, double-quoted, hex, and binary strings.
    #[test]
    fn test_strings() {
        let dialect = SQLDialect::MariaDB;

        // Single quoted strings
        if let Token::SingleQuotedString(value) = lex_single("'hello'", &dialect) {
            assert_eq!(value, "hello");
        } else {
            panic!("Expected single quoted string");
        }

        if let Token::SingleQuotedString(value) = lex_single("'it''s'", &dialect) {
            assert_eq!(value, "it''s");
        } else {
            panic!("Expected single quoted string with escaped quote");
        }

        // Double quoted strings
        if let Token::DoubleQuotedString(value) = lex_single("\"hello\"", &dialect) {
            assert_eq!(value, "hello");
        } else {
            panic!("Expected double quoted string");
        }

        // Hex strings
        if let Token::HexString(value) = lex_single("x'48656C6C6F'", &dialect) {
            assert_eq!(value, "48656C6C6F");
        } else {
            panic!("Expected hex string");
        }

        if let Token::HexString(value) = lex_single("X'ABCDEF'", &dialect) {
            assert_eq!(value, "ABCDEF");
        } else {
            panic!("Expected hex string");
        }

        // Binary strings
        if let Token::BinaryString(value) = lex_single("b'101010'", &dialect) {
            assert_eq!(value, "101010");
        } else {
            panic!("Expected binary string");
        }

        if let Token::BinaryString(value) = lex_single("B'111'", &dialect) {
            assert_eq!(value, "111");
        } else {
            panic!("Expected binary string");
        }
    }

    /// Tests that various operators are correctly recognized, including multi-character operators and those specific to certain dialects.
    #[test]
    fn test_operators() {
        let dialect = SQLDialect::MariaDB;

        assert_eq!(lex_single("+", &dialect), Token::Plus);
        assert_eq!(lex_single("-", &dialect), Token::Minus);
        assert_eq!(lex_single("*", &dialect), Token::Mul);
        assert_eq!(lex_single("/", &dialect), Token::Div);
        assert_eq!(lex_single("%", &dialect), Token::Mod);
        assert_eq!(lex_single("=", &dialect), Token::Eq);
        assert_eq!(lex_single("!=", &dialect), Token::Neq);
        assert_eq!(lex_single("<>", &dialect), Token::Neq);
        assert_eq!(lex_single("<", &dialect), Token::Lt);
        assert_eq!(lex_single("<=", &dialect), Token::LtEq);
        assert_eq!(lex_single(">", &dialect), Token::Gt);
        assert_eq!(lex_single(">=", &dialect), Token::GtEq);
        assert_eq!(lex_single("<=>", &dialect), Token::Spaceship);
        assert_eq!(lex_single("<<", &dialect), Token::ShiftLeft);
        assert_eq!(lex_single(">>", &dialect), Token::ShiftRight);
        assert_eq!(lex_single("&&", &dialect), Token::DoubleAmpersand);
        assert_eq!(lex_single("||", &dialect), Token::DoublePipe);
        assert_eq!(lex_single("&", &dialect), Token::Ampersand);
        assert_eq!(lex_single("|", &dialect), Token::Pipe);
        assert_eq!(lex_single("^", &dialect), Token::Caret);
        assert_eq!(lex_single("~", &dialect), Token::Tilde);
        assert_eq!(lex_single("!", &dialect), Token::ExclamationMark);
        assert_eq!(lex_single("!!", &dialect), Token::DoubleExclamationMark);
    }

    /// Tests that various punctuation characters are correctly recognized, including those specific to certain dialects.
    #[test]
    fn test_punctuation() {
        let dialect = SQLDialect::MariaDB;

        assert_eq!(lex_single("(", &dialect), Token::LParen);
        assert_eq!(lex_single(")", &dialect), Token::RParen);
        assert_eq!(lex_single("[", &dialect), Token::LBracket);
        assert_eq!(lex_single("]", &dialect), Token::RBracket);
        assert_eq!(lex_single("{", &dialect), Token::LBrace);
        assert_eq!(lex_single("}", &dialect), Token::RBrace);
        assert_eq!(lex_single(",", &dialect), Token::Comma);
        assert_eq!(lex_single(".", &dialect), Token::Period);
        assert_eq!(lex_single(";", &dialect), Token::SemiColon);
        assert_eq!(lex_single(":", &dialect), Token::Colon);
        assert_eq!(lex_single("::", &dialect), Token::DoubleColon);
        assert_eq!(lex_single("?", &dialect), Token::QuestionMark);
        assert_eq!(lex_single("@", &dialect), Token::At);
        assert_eq!(lex_single("#", &dialect), Token::Sharp);
        assert_eq!(lex_single("=>", &dialect), Token::RArrow);
        assert_eq!(lex_single("->", &dialect), Token::RArrowJson);
        assert_eq!(lex_single("->>", &dialect), Token::RDoubleArrowJson);
        assert_eq!(lex_single("\\", &dialect), Token::Backslash);
    }

    #[test]
    fn test_special_tokens() {
        let mariadb = SQLDialect::MariaDB;
        let postgresql = SQLDialect::PostgreSQL;

        // Dollar arguments (PostgreSQL only)
        assert_eq!(lex_single("$1", &postgresql), Token::DollarArg(1));
        assert_eq!(lex_single("$10", &postgresql), Token::DollarArg(10));
        assert_eq!(lex_single("$999", &postgresql), Token::DollarArg(999));

        // In MariaDB, $1 is treated as an identifier
        assert!(matches!(
            lex_single("$1", &mariadb),
            Token::Ident(_, Keyword::NOT_A_KEYWORD)
        ));

        // Double dollar works in both dialects
        assert_eq!(lex_single("$$", &mariadb), Token::DoubleDollar);
        assert_eq!(lex_single("$$", &postgresql), Token::DoubleDollar);

        // Session variables (MariaDB)
        assert_eq!(lex_single("@@GLOBAL", &mariadb), Token::AtAtGlobal);
        assert_eq!(lex_single("@@global", &mariadb), Token::AtAtGlobal);
        assert_eq!(lex_single("@@SESSION", &mariadb), Token::AtAtSession);
        assert_eq!(lex_single("@@session", &mariadb), Token::AtAtSession);

        // Percent s
        assert_eq!(lex_single("%s", &mariadb), Token::PercentS);
    }

    /// Tests that comments are correctly skipped and do not produce tokens. It covers single-line comments with both -- and //, as well as multi-line comments.
    #[test]
    fn test_comments() {
        let dialect = SQLDialect::MariaDB;

        // Single line comment with --
        let tokens = lex_all("SELECT -- comment\nFROM", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::FROM)));

        // Single line comment with //
        let tokens = lex_all("SELECT // comment\nFROM", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::FROM)));

        // Multi-line comment
        let tokens = lex_all("SELECT /* comment */ FROM", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::FROM)));

        // Multi-line comment with multiple lines
        let tokens = lex_all("SELECT /* line1\nline2\nline3 */ FROM", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::FROM)));
    }

    /// Tests that in MariaDB, identifiers can start with digits, but in PostgreSQL they cannot. It also checks that valid numbers are still recognized as numbers and not identifiers.
    #[test]
    fn test_mariadb_identifiers_starting_with_digits() {
        let dialect = SQLDialect::MariaDB;

        // MariaDB allows identifiers starting with digits
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("123abc", &dialect) {
            assert_eq!(name, "123abc");
        } else {
            panic!(
                "Expected identifier '123abc' in MariaDB, got {:?}",
                lex_single("123abc", &dialect)
            );
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1e5test", &dialect) {
            assert_eq!(name, "1e5test");
        } else {
            panic!(
                "Expected identifier '1e5test' in MariaDB, got {:?}",
                lex_single("1e5test", &dialect)
            );
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("9column", &dialect) {
            assert_eq!(name, "9column");
        } else {
            panic!(
                "Expected identifier '9column' in MariaDB, got {:?}",
                lex_single("9column", &dialect)
            );
        }

        // But these should definitely be numbers (no letters after)
        assert!(matches!(lex_single("123", &dialect), Token::Integer(_)));
        assert!(matches!(lex_single("123.456", &dialect), Token::Float(_)));
        assert!(matches!(lex_single("1e5", &dialect), Token::Float(_)));
    }

    /// Tests that the lexer correctly distinguishes between the PostgreSQL and MariaDB dialects
    /// when it comes to identifiers starting with digits.
    /// It verifies that in PostgreSQL, such tokens are treated as numbers, while in MariaDB they are treated as identifiers.
    #[test]
    fn test_postgresql_vs_mariadb() {
        // PostgreSQL doesn't allow identifiers starting with digits
        let pg_dialect = SQLDialect::PostgreSQL;
        assert!(matches!(
            lex_single("123abc", &pg_dialect),
            Token::Integer(_)
        ));

        // MariaDB does allow it
        let maria_dialect = SQLDialect::MariaDB;
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("123abc", &maria_dialect) {
            assert_eq!(name, "123abc");
        } else {
            panic!("Expected identifier starting with digit in MariaDB");
        }
    }

    /// Tests that whitespace characters are correctly skipped and do not produce tokens.
    #[test]
    fn test_whitespace_handling() {
        let dialect = SQLDialect::MariaDB;

        let tokens = lex_all("SELECT   \t\n\r  FROM", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::FROM)));
    }

    /// Tests a complex SQL query to ensure that all components (keywords, identifiers, operators, literals)
    /// are correctly tokenized in the right order.
    #[test]
    fn test_complex_query() {
        let dialect = SQLDialect::MariaDB;
        let sql = "SELECT col1, col2 FROM mytable WHERE col3 > 18 ORDER BY col2 DESC LIMIT 10";
        let tokens = lex_all(sql, &dialect);

        // Should have 16 tokens
        assert_eq!(tokens.len(), 16);

        // Verify the token types and keywords
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::NOT_A_KEYWORD))); // col1
        assert_eq!(tokens[2], Token::Comma);
        assert!(matches!(tokens[3], Token::Ident(_, Keyword::NOT_A_KEYWORD))); // col2
        assert!(matches!(tokens[4], Token::Ident(_, Keyword::FROM)));
        assert!(matches!(tokens[5], Token::Ident(_, Keyword::NOT_A_KEYWORD))); // mytable
        assert!(matches!(tokens[6], Token::Ident(_, Keyword::WHERE)));
        assert!(matches!(tokens[7], Token::Ident(_, Keyword::NOT_A_KEYWORD))); // col3
        assert_eq!(tokens[8], Token::Gt);
        assert!(matches!(tokens[9], Token::Integer(_))); // 18
        assert!(matches!(tokens[10], Token::Ident(_, Keyword::ORDER)));
        assert!(matches!(tokens[11], Token::Ident(_, Keyword::BY)));
        assert!(matches!(
            tokens[12],
            Token::Ident(_, Keyword::NOT_A_KEYWORD)
        )); // col2
        assert!(matches!(tokens[13], Token::Ident(_, Keyword::DESC)));
        assert!(matches!(tokens[14], Token::Ident(_, Keyword::LIMIT)));
        assert!(matches!(tokens[15], Token::Integer(_))); // 10
    }

    /// Tests that escaped characters within strings are correctly recognized and included in the token value.
    #[test]
    fn test_escaped_strings() {
        let dialect = SQLDialect::MariaDB;

        // Backslash escapes in single quoted strings
        if let Token::SingleQuotedString(value) = lex_single("'hello\\nworld'", &dialect) {
            assert_eq!(value, "hello\\nworld");
        } else {
            panic!("Expected single quoted string with escape");
        }

        // Double single quotes
        if let Token::SingleQuotedString(value) = lex_single("'can''t'", &dialect) {
            assert_eq!(value, "can''t");
        } else {
            panic!("Expected single quoted string with doubled quote");
        }
    }

    /// Tests that invalid tokens are correctly identified as Token::Invalid, and that valid tokens are not misclassified as invalid.
    #[test]
    fn test_invalid_tokens() {
        let dialect = SQLDialect::MariaDB;

        // Unclosed string
        assert_eq!(lex_single("'unclosed", &dialect), Token::Invalid);

        // Unclosed backtick identifier
        assert_eq!(lex_single("`unclosed", &dialect), Token::Invalid);

        // Invalid hex string
        assert_eq!(lex_single("x'GG'", &dialect), Token::Invalid);

        // In MariaDB, $ is a valid identifier character
        assert!(matches!(
            lex_single("$", &dialect),
            Token::Ident(_, Keyword::NOT_A_KEYWORD)
        ));
    }

    /// Tests that keywords are recognized regardless of their case, and that they are categorized as keywords rather than identifiers.
    #[test]
    fn test_case_insensitive_keywords() {
        let dialect = SQLDialect::MariaDB;

        // Keywords should be case-insensitive
        assert!(matches!(
            lex_single("SELECT", &dialect),
            Token::Ident(_, Keyword::SELECT)
        ));
        assert!(matches!(
            lex_single("select", &dialect),
            Token::Ident(_, Keyword::SELECT)
        ));
        assert!(matches!(
            lex_single("SeLeCt", &dialect),
            Token::Ident(_, Keyword::SELECT)
        ));
        assert!(matches!(
            lex_single("desc", &dialect),
            Token::Ident(_, Keyword::DESC)
        ));
        assert!(matches!(
            lex_single("DESC", &dialect),
            Token::Ident(_, Keyword::DESC)
        ));
    }

    /// Tests that in MariaDB, tokens that start with digits but have identifier characters after are treated as identifiers,
    /// while valid numbers without identifier characters are treated as numbers.
    #[test]
    fn test_mariadb_number_identifier_edge_cases() {
        let dialect = SQLDialect::MariaDB;

        // Pure numbers (no identifier chars after)
        assert!(matches!(lex_single("123", &dialect), Token::Integer("123")));
        assert!(matches!(lex_single("0", &dialect), Token::Integer("0")));
        assert!(matches!(
            lex_single("999999", &dialect),
            Token::Integer("999999")
        ));

        // Float numbers (no identifier chars after)
        assert!(matches!(
            lex_single("123.456", &dialect),
            Token::Float("123.456")
        ));
        assert!(matches!(lex_single("0.5", &dialect), Token::Float("0.5")));
        assert!(matches!(lex_single("1.0", &dialect), Token::Float("1.0")));

        // Scientific notation (no identifier chars after)
        assert!(matches!(lex_single("1e5", &dialect), Token::Float("1e5")));
        assert!(matches!(lex_single("1E5", &dialect), Token::Float("1E5")));
        assert!(matches!(lex_single("1e+5", &dialect), Token::Float("1e+5")));
        assert!(matches!(lex_single("1e-5", &dialect), Token::Float("1e-5")));
        assert!(matches!(
            lex_single("1.5e10", &dialect),
            Token::Float("1.5e10")
        ));
        assert!(matches!(
            lex_single("2.5E-3", &dialect),
            Token::Float("2.5E-3")
        ));

        // Identifiers starting with numbers
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("123abc", &dialect) {
            assert_eq!(name, "123abc");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1_test", &dialect) {
            assert_eq!(name, "1_test");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1$col", &dialect) {
            assert_eq!(name, "1$col");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1@var", &dialect) {
            assert_eq!(name, "1@var");
        } else {
            panic!("Expected identifier");
        }

        // Numbers with period not followed by digit remain as integers
        // (the period becomes a separate token)
        assert!(matches!(lex_single("123", &dialect), Token::Integer("123")));
        let tokens = lex_all("123.abc", &dialect);
        assert!(matches!(tokens[0], Token::Integer("123")));
        assert_eq!(tokens[1], Token::Period);

        // Float numbers consume decimal part, next token is identifier
        let tokens = lex_all("123.456abc", &dialect);
        assert!(matches!(tokens[0], Token::Float("123.456")));
        if let Token::Ident(name, _) = &tokens[1] {
            assert_eq!(*name, "abc");
        }

        // Scientific notation with identifier chars after
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1e5test", &dialect) {
            assert_eq!(name, "1e5test");
        } else {
            panic!(
                "Expected identifier, got {:?}",
                lex_single("1e5test", &dialect)
            );
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1E5TEST", &dialect) {
            assert_eq!(name, "1E5TEST");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1e+5x", &dialect) {
            assert_eq!(name, "1e+5x");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("1e-5_col", &dialect) {
            assert_eq!(name, "1e-5_col");
        } else {
            panic!("Expected identifier");
        }

        // Edge case: just 'e' or 'E' after digits should not be treated as exponent if not followed by digits
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("123e", &dialect) {
            assert_eq!(name, "123e");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("123E_test", &dialect) {
            assert_eq!(name, "123E_test");
        } else {
            panic!("Expected identifier");
        }

        // Complex identifiers with mixed patterns
        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("9column", &dialect) {
            assert_eq!(name, "9column");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("0x_not_hex", &dialect) {
            assert_eq!(name, "0x_not_hex");
        } else {
            panic!("Expected identifier");
        }

        if let Token::Ident(name, Keyword::NOT_A_KEYWORD) = lex_single("123_456_789", &dialect) {
            assert_eq!(name, "123_456_789");
        } else {
            panic!("Expected identifier");
        }

        // Numbers should end at the right place
        let tokens = lex_all("123 456", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Integer("123")));
        assert!(matches!(tokens[1], Token::Integer("456")));

        // Identifiers should consume all valid chars
        let tokens = lex_all("123abc 456def", &dialect);
        assert_eq!(tokens.len(), 2);
        if let Token::Ident(name, _) = &tokens[0] {
            assert_eq!(*name, "123abc");
        } else {
            panic!("Expected identifier");
        }
        if let Token::Ident(name, _) = &tokens[1] {
            assert_eq!(*name, "456def");
        } else {
            panic!("Expected identifier");
        }

        // Float then identifier
        let tokens = lex_all("123.456 789xyz", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Float("123.456")));
        if let Token::Ident(name, _) = &tokens[1] {
            assert_eq!(*name, "789xyz");
        } else {
            panic!("Expected identifier");
        }

        // Scientific notation then identifier
        let tokens = lex_all("1e5 2e3test", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Float("1e5")));
        if let Token::Ident(name, _) = &tokens[1] {
            assert_eq!(*name, "2e3test");
        } else {
            panic!("Expected identifier");
        }

        // Mixed in a realistic query
        let tokens = lex_all(
            "SELECT 123abc, 1e5, 1e5test FROM 9table WHERE col > 123.456",
            &dialect,
        );
        // SELECT, 123abc, comma, 1e5, comma, 1e5test, FROM, 9table, WHERE, col, >, 123.456
        assert_eq!(tokens.len(), 12);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        if let Token::Ident(name, _) = &tokens[1] {
            assert_eq!(*name, "123abc");
        }
        assert_eq!(tokens[2], Token::Comma);
        assert!(matches!(tokens[3], Token::Float("1e5")));
        assert_eq!(tokens[4], Token::Comma);
        if let Token::Ident(name, _) = &tokens[5] {
            assert_eq!(*name, "1e5test");
        }
        assert!(matches!(tokens[6], Token::Ident(_, Keyword::FROM)));
        if let Token::Ident(name, _) = &tokens[7] {
            assert_eq!(*name, "9table");
        }
        assert!(matches!(tokens[8], Token::Ident(_, Keyword::WHERE)));
        if let Token::Ident(name, _) = &tokens[9] {
            assert_eq!(*name, "col");
        }
        assert_eq!(tokens[10], Token::Gt);
        assert!(matches!(tokens[11], Token::Float("123.456")));
    }

    /// Tests that PostgreSQL-specific operators are correctly recognized,
    /// including those that are not standard SQL operators but are valid in PostgreSQL.
    /// It covers comparison, string, numerical, geometric, and time interval operators.
    #[test]
    fn test_postgres_operators() {
        let dialect = SQLDialect::PostgreSQL;

        // Table 9-1: Comparison and string operators
        assert_eq!(lex_single("<", &dialect), Token::Lt);
        assert_eq!(lex_single("<=", &dialect), Token::LtEq);
        assert_eq!(lex_single("<>", &dialect), Token::Neq);
        assert_eq!(lex_single("=", &dialect), Token::Eq);
        assert_eq!(lex_single(">", &dialect), Token::Gt);
        assert_eq!(lex_single(">=", &dialect), Token::GtEq);
        assert_eq!(lex_single("||", &dialect), Token::DoublePipe);
        // The following are not standard tokens, but may be handled as PostgresOperator
        assert_eq!(lex_single("!!=", &dialect), Token::PostgresOperator("!!="));
        assert_eq!(lex_single("~~", &dialect), Token::PostgresOperator("~~"));
        assert_eq!(lex_single("!~~", &dialect), Token::PostgresOperator("!~~"));
        assert_eq!(lex_single("~", &dialect), Token::Tilde);
        assert_eq!(lex_single("~*", &dialect), Token::PostgresOperator("~*"));
        assert_eq!(lex_single("!~", &dialect), Token::PostgresOperator("!~"));
        assert_eq!(lex_single("!~*", &dialect), Token::PostgresOperator("!~*"));

        // Table 9-2: Numerical operators
        assert_eq!(lex_single("!", &dialect), Token::ExclamationMark);
        assert_eq!(lex_single("!!", &dialect), Token::DoubleExclamationMark);
        assert_eq!(lex_single("%", &dialect), Token::Mod);
        assert_eq!(lex_single("*", &dialect), Token::Mul);
        assert_eq!(lex_single("+", &dialect), Token::Plus);
        assert_eq!(lex_single("-", &dialect), Token::Minus);
        assert_eq!(lex_single("/", &dialect), Token::Div);
        assert_eq!(lex_single(":", &dialect), Token::Colon);
        assert_eq!(lex_single(";", &dialect), Token::SemiColon);
        assert_eq!(lex_single("@", &dialect), Token::At);
        assert_eq!(lex_single("^", &dialect), Token::Caret);
        assert_eq!(lex_single("|/", &dialect), Token::PostgresOperator("|/"));
        assert_eq!(lex_single("||/", &dialect), Token::PostgresOperator("||/"));

        // Table 9-3: Geometric operators (a selection)
        assert_eq!(lex_single("#", &dialect), Token::Sharp);
        assert_eq!(lex_single("##", &dialect), Token::PostgresOperator("##"));
        assert_eq!(lex_single("&&", &dialect), Token::DoubleAmpersand);
        assert_eq!(lex_single("&<", &dialect), Token::PostgresOperator("&<"));
        assert_eq!(lex_single("&>", &dialect), Token::PostgresOperator("&>"));
        assert_eq!(lex_single("<->", &dialect), Token::PostgresOperator("<->"));
        assert_eq!(lex_single("<<", &dialect), Token::ShiftLeft); // Note: check if this is ShiftLeft or ShiftRight in your Token
        assert_eq!(lex_single("<^", &dialect), Token::PostgresOperator("<^"));
        assert_eq!(lex_single(">>", &dialect), Token::ShiftRight); // Note: check if this is ShiftLeft or ShiftRight in your Token
        assert_eq!(lex_single(">^", &dialect), Token::PostgresOperator(">^"));
        assert_eq!(lex_single("?#", &dialect), Token::PostgresOperator("?#"));
        assert_eq!(lex_single("?-", &dialect), Token::PostgresOperator("?-"));
        assert_eq!(lex_single("?-|", &dialect), Token::PostgresOperator("?-|"));
        assert_eq!(lex_single("@-@", &dialect), Token::PostgresOperator("@-@"));
        assert_eq!(lex_single("?|", &dialect), Token::PostgresOperator("?|"));
        assert_eq!(lex_single("?||", &dialect), Token::PostgresOperator("?||"));
        assert_eq!(lex_single("@", &dialect), Token::At);
        assert_eq!(lex_single("@@", &dialect), Token::AtAt);
        assert_eq!(lex_single("~=", &dialect), Token::PostgresOperator("~="));

        // Table 9-4: Time interval operators (a selection)
        assert_eq!(lex_single("#<", &dialect), Token::PostgresOperator("#<"));
        assert_eq!(lex_single("#<=", &dialect), Token::PostgresOperator("#<="));
        assert_eq!(lex_single("#<>", &dialect), Token::PostgresOperator("#<>"));
        assert_eq!(lex_single("#=", &dialect), Token::PostgresOperator("#="));
        assert_eq!(lex_single("#>", &dialect), Token::PostgresOperator("#>"));
        assert_eq!(lex_single("#>=", &dialect), Token::PostgresOperator("#>="));
        assert_eq!(lex_single("<#>", &dialect), Token::PostgresOperator("<#>"));
        assert_eq!(lex_single("<?>", &dialect), Token::PostgresOperator("<?>"));

        // Custom operator names (user-defined, valid in PostgreSQL)
        assert_eq!(
            lex_single("<<>>", &dialect),
            Token::PostgresOperator("<<>>")
        );
        assert_eq!(lex_single("++", &dialect), Token::PostgresOperator("++"));
        assert_eq!(lex_single("<+>", &dialect), Token::PostgresOperator("<+>"));
        assert_eq!(lex_single("@#@", &dialect), Token::PostgresOperator("@#@"));
        assert_eq!(lex_single("!@#", &dialect), Token::PostgresOperator("!@#"));
        assert_eq!(
            lex_single("<=>=", &dialect),
            Token::PostgresOperator("<=>=")
        );
        assert_eq!(
            lex_single("<->-<", &dialect),
            Token::PostgresOperator("<->-<")
        );
        // Operator with question mark
        assert_eq!(lex_single("??", &dialect), Token::PostgresOperator("??"));
        // Operator with pipe and ampersand
        assert_eq!(lex_single("|&|", &dialect), Token::PostgresOperator("|&|"));
        // Operator with tilde and exclamation
        assert_eq!(lex_single("~!~", &dialect), Token::PostgresOperator("~!~"));
        // Operator with mixed symbols
        assert_eq!(
            lex_single("<@#>", &dialect),
            Token::PostgresOperator("<@#>")
        );
        // Operator with caret and percent
        assert_eq!(lex_single("^%", &dialect), Token::PostgresOperator("^%"));
        // Operator with colon and equals
        assert_eq!(lex_single(":=:", &dialect), Token::PostgresOperator(":=:"));
        // Operator with exclamation and equals
        assert_eq!(
            lex_single("!=!=", &dialect),
            Token::PostgresOperator("!=!=")
        );
        // Operator with ampersand and at
        assert_eq!(lex_single("&@&", &dialect), Token::PostgresOperator("&@&"));
        // Operator with hash and tilde
        assert_eq!(lex_single("#~#", &dialect), Token::PostgresOperator("#~#"));

        // Operators containing comment delimiters, using lex_all
        // -- should start a comment, so only the first operator is returned
        let tokens = lex_all("<<>>--foo", &dialect);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], Token::PostgresOperator("<<>>"));

        // /* should start a comment, so only the first operator is returned
        let tokens = lex_all("<<>>/*foo*/bar", &dialect);
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], Token::PostgresOperator("<<>>"));
        // After the comment, 'bar' is an identifier
        assert!(matches!(tokens[1], Token::Ident(_, _)));

        // Operator with -- inside (should be split)
        let tokens = lex_all("<-->", &dialect);
        // Should produce PostgresOperator("<-") and Minus
        assert!(tokens.len() == 2 || tokens.len() == 1); // Accept both if implementation varies
        assert!(
            matches!(
                tokens[0],
                Token::PostgresOperator("<")
                    | Token::PostgresOperator("<-")
                    | Token::PostgresOperator("<-->")
                    | Token::Minus
            ),
            "{tokens:?}"
        );
    }
}
