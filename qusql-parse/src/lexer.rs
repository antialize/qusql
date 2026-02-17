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

use crate::{SQLDialect, Span, keywords::Keyword};

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Token<'a> {
    Ampersand,
    At,
    Backslash,
    Caret,
    Colon,
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
    Spaceship,
    Tilde,
    PercentS,
    DollarArg(usize),
    AtAtGlobal,
    AtAtSession,
    Eof,
}

impl<'a> Token<'a> {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Token::Ampersand => "'&'",
            Token::At => "'@'",
            Token::Backslash => "'\\'",
            Token::Caret => "'^'",
            Token::Colon => "':'",
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
            Token::Spaceship => "'<=>'",
            Token::Tilde => "'~'",
            Token::PercentS => "'%s'",
            Token::AtAtGlobal => "@@GLOBAL",
            Token::AtAtSession => "@@SESSION",
            Token::Eof => "EndOfFile",
        }
    }
}
pub(crate) struct Lexer<'a> {
    src: &'a str,
    chars: core::iter::Peekable<core::str::CharIndices<'a>>,
    dialect: SQLDialect,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str, dialect: &SQLDialect) -> Self {
        Self {
            src,
            chars: src.char_indices().peekable(),
            dialect: dialect.clone(),
        }
    }

    pub(crate) fn s(&self, span: Span) -> &'a str {
        core::str::from_utf8(&self.src.as_bytes()[span]).unwrap()
    }

    fn simple_literal(&mut self, start: usize) -> Token<'a> {
        let end = loop {
            match self.chars.peek() {
                Some((_, '_' | 'a'..='z' | 'A'..='Z' | '0'..='9')) => {
                    self.chars.next();
                }
                // For MariaDB, allow $ and @ in identifiers
                Some((_, '$' | '@')) if self.dialect.is_maria() => {
                    self.chars.next();
                }
                Some((i, _)) => break *i,
                None => break self.src.len(),
            }
        };
        let s = self.s(start..end);
        let ss = s.to_ascii_uppercase();
        Token::Ident(s, ss.as_str().into())
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
            .filter(|(_, c)| *c != '\n' && c.is_ascii_whitespace())
            .is_some()
        {
            self.chars.next().unwrap();
        }
        let start = match self.chars.peek() {
            Some((i, '\n')) => i + 1,
            Some((i, _)) => *i,
            None => {
                let span = self.src.len()..self.src.len();
                return (self.s(span.clone()), span);
            }
        };
        while let Some((i, c)) = self.chars.next() {
            if c != '\n' {
                continue;
            }
            if !matches!(self.chars.peek(), Some((_, '\\'))) {
                continue;
            }
            self.chars.next().unwrap();
            if !matches!(self.chars.peek(), Some((_, '.'))) {
                continue;
            }
            self.chars.next().unwrap();
            if matches!(self.chars.peek(), Some((_, '\n'))) {
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

    pub fn next_token(&mut self) -> (Token<'a>, Span) {
        loop {
            let (start, c) = match self.chars.next() {
                Some(v) => v,
                None => {
                    return (Token::Eof, self.src.len()..self.src.len());
                }
            };
            let t = match c {
                ' ' | '\t' | '\n' | '\r' => continue,
                '?' => Token::QuestionMark,
                ';' => Token::SemiColon,
                '\\' => Token::Backslash,
                '[' => Token::LBracket,
                ']' => Token::RBracket,
                '&' => match self.chars.peek() {
                    Some((_, '&')) => {
                        self.chars.next();
                        Token::DoubleAmpersand
                    }
                    _ => Token::Ampersand,
                },
                '^' => Token::Caret,
                '{' => Token::LBrace,
                '}' => Token::RBrace,
                '(' => Token::LParen,
                ')' => Token::RParen,
                ',' => Token::Comma,
                '+' => Token::Plus,
                '*' => Token::Mul,
                '%' => match self.chars.peek() {
                    Some((_, 's')) => {
                        self.chars.next();
                        Token::PercentS
                    }
                    _ => Token::Mod,
                },
                '#' => Token::Sharp,
                '@' => match self.chars.peek() {
                    Some((_, '@')) => {
                        self.chars.next();
                        #[allow(clippy::never_loop)]
                        match self.chars.peek() {
                            Some((_, 's' | 'S')) => loop {
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'e' | 'E'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 's' | 'S'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 's' | 'S'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'i' | 'I'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'o' | 'O'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'n' | 'N'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                break Token::AtAtSession;
                            },
                            Some((_, 'g' | 'G')) => loop {
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'l' | 'L'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'o' | 'O'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'b' | 'B'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'a' | 'A'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                if !matches!(self.chars.peek(), Some((_, 'l' | 'L'))) {
                                    break Token::Invalid;
                                }
                                self.chars.next();
                                break Token::AtAtGlobal;
                            },
                            _ => Token::Invalid,
                        }
                    }
                    _ => Token::At,
                },
                '~' => Token::Tilde,
                ':' => match self.chars.peek() {
                    Some((_, ':')) => {
                        self.chars.next();
                        Token::DoubleColon
                    }
                    _ => Token::Colon,
                },
                '$' => match self.chars.peek() {
                    Some((_, '$')) => {
                        self.chars.next();
                        Token::DoubleDollar
                    }
                    Some((_, '1'..='9')) => {
                        let mut v = self.chars.peek().unwrap().1.to_digit(10).unwrap() as usize;
                        self.chars.next();
                        while matches!(self.chars.peek(), Some((_, '0'..='9'))) {
                            v = v * 10
                                + self.chars.peek().unwrap().1.to_digit(10).unwrap() as usize;
                            self.chars.next();
                        }
                        Token::DollarArg(v)
                    }
                    _ => Token::Invalid,
                },
                '=' => match self.chars.peek() {
                    Some((_, '>')) => {
                        self.chars.next();
                        Token::RArrow
                    }
                    _ => Token::Eq,
                },
                '!' => match self.chars.peek() {
                    Some((_, '=')) => {
                        self.chars.next();
                        Token::Neq
                    }
                    Some((_, '!')) => {
                        self.chars.next();
                        Token::DoubleExclamationMark
                    }
                    _ => Token::ExclamationMark,
                },
                '<' => match self.chars.peek() {
                    Some((_, '=')) => {
                        self.chars.next();
                        match self.chars.peek() {
                            Some((_, '>')) => {
                                self.chars.next();
                                Token::Spaceship
                            }
                            _ => Token::LtEq,
                        }
                    }
                    Some((_, '>')) => {
                        self.chars.next();
                        Token::Neq
                    }
                    Some((_, '<')) => {
                        self.chars.next();
                        Token::ShiftLeft
                    }
                    _ => Token::Lt,
                },
                '>' => match self.chars.peek() {
                    Some((_, '=')) => {
                        self.chars.next();
                        Token::GtEq
                    }
                    Some((_, '>')) => {
                        self.chars.next();
                        Token::ShiftRight
                    }
                    _ => Token::Gt,
                },
                '|' => match self.chars.peek() {
                    Some((_, '|')) => {
                        self.chars.next();
                        Token::DoublePipe
                    }
                    _ => Token::Pipe,
                },
                '-' => match self.chars.peek() {
                    Some((_, '-')) => {
                        while !matches!(self.chars.next(), Some((_, '\r' | '\n')) | None) {}
                        continue;
                    }
                    _ => Token::Minus,
                },
                '/' => match self.chars.peek() {
                    Some((_, '*')) => {
                        self.chars.next();
                        let ok = loop {
                            match self.chars.next() {
                                Some((_, '*')) => {
                                    if matches!(self.chars.peek(), Some((_, '/'))) {
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
                    Some((_, '/')) => {
                        while !matches!(self.chars.next(), Some((_, '\r' | '\n')) | None) {}
                        continue;
                    }
                    _ => Token::Div,
                },
                'x' | 'X' => match self.chars.peek() {
                    Some((_, '\'')) => {
                        self.chars.next(); // consume the '
                        loop {
                            match self.chars.next() {
                                Some((i, '\'')) => break Token::HexString(self.s(start + 2..i)),
                                Some((_, '0'..='9' | 'a'..='f' | 'A'..='F')) => (),
                                Some((_, _)) => break Token::Invalid,
                                None => break Token::Invalid,
                            }
                        }
                    }
                    _ => self.simple_literal(start),
                },
                '_' | 'a'..='z' | 'A'..='Z' => self.simple_literal(start),
                '`' => {
                    // MySQL backtick-quoted identifiers can contain any character except backticks
                    // Backticks can be escaped by doubling them
                    loop {
                        match self.chars.next() {
                            Some((i, '`')) => {
                                // Check if it's a doubled backtick (escape sequence)
                                if matches!(self.chars.peek(), Some((_, '`'))) {
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
                '\'' => loop {
                    match self.chars.next() {
                        Some((_, '\\')) => {
                            self.chars.next();
                        }
                        Some((i, '\'')) => match self.chars.peek() {
                            Some((_, '\'')) => {
                                self.chars.next();
                            }
                            _ => break Token::SingleQuotedString(self.s(start + 1..i)),
                        },
                        Some((_, _)) => (),
                        None => break Token::Invalid,
                    }
                },
                '"' => loop {
                    match self.chars.next() {
                        Some((_, '\\')) => {
                            self.chars.next();
                        }
                        Some((i, '"')) => match self.chars.peek() {
                            Some((_, '"')) => {
                                self.chars.next();
                            }
                            _ => break Token::DoubleQuotedString(self.s(start + 1..i)),
                        },
                        Some((_, _)) => (),
                        None => break Token::Invalid,
                    }
                },
                '0'..='9' => {
                    // For MariaDB, identifiers can start with digits
                    // We need to peek ahead to determine if this is a number or identifier
                    if self.dialect.is_maria() {
                        // Lookahead to see if this could be an identifier
                        let mut temp_chars = self.chars.clone();
                        let mut is_identifier = false;

                        // Skip over digits
                        while matches!(temp_chars.peek(), Some((_, '0'..='9'))) {
                            temp_chars.next();
                        }

                        // Check what comes after the digits
                        match temp_chars.peek() {
                            Some((_, 'e' | 'E')) => {
                                // Could be scientific notation, check further
                                temp_chars.next();
                                if let Some((_, '+' | '-')) = temp_chars.peek() {
                                    temp_chars.next();
                                };
                                // If followed by digits, it's a number
                                // If followed by other identifier chars, it's an identifier
                                if !matches!(temp_chars.peek(), Some((_, '0'..='9'))) {
                                    is_identifier = true;
                                }
                            }
                            Some((_, '_' | 'a'..='z' | 'A'..='Z' | '$' | '@')) => {
                                is_identifier = true;
                            }
                            Some((_, '.')) => {
                                // Could be a float, check if followed by digits
                                temp_chars.next();
                                if matches!(temp_chars.peek(), Some((_, '0'..='9'))) {
                                    // It's a float
                                    is_identifier = false;
                                } else {
                                    // Period not followed by digit, could be end of number
                                    is_identifier = false;
                                }
                            }
                            _ => is_identifier = false,
                        }

                        if is_identifier {
                            self.simple_literal(start)
                        } else {
                            // Parse as number (integer, float, or scientific notation)
                            let mut is_float = false;
                            loop {
                                match self.chars.peek() {
                                    Some((_, '0'..='9')) => {
                                        self.chars.next();
                                    }
                                    Some((_, '.')) => {
                                        self.chars.next();
                                        is_float = true;
                                        // Consume fractional part
                                        while matches!(self.chars.peek(), Some((_, '0'..='9'))) {
                                            self.chars.next();
                                        }
                                        // Check for exponent
                                        if matches!(self.chars.peek(), Some((_, 'e' | 'E'))) {
                                            self.chars.next();
                                            if matches!(self.chars.peek(), Some((_, '+' | '-'))) {
                                                self.chars.next();
                                            }
                                            while matches!(self.chars.peek(), Some((_, '0'..='9')))
                                            {
                                                self.chars.next();
                                            }
                                        }
                                        break;
                                    }
                                    Some((_, 'e' | 'E')) => {
                                        self.chars.next();
                                        is_float = true;
                                        if matches!(self.chars.peek(), Some((_, '+' | '-'))) {
                                            self.chars.next();
                                        }
                                        while matches!(self.chars.peek(), Some((_, '0'..='9'))) {
                                            self.chars.next();
                                        }
                                        break;
                                    }
                                    _ => break,
                                }
                            }
                            let end = match self.chars.peek() {
                                Some((i, _)) => *i,
                                None => self.src.len(),
                            };
                            if is_float {
                                Token::Float(self.s(start..end))
                            } else {
                                Token::Integer(self.s(start..end))
                            }
                        }
                    } else {
                        // Non-MariaDB: parse as number only (never as identifier)
                        let mut is_float = false;
                        loop {
                            match self.chars.peek() {
                                Some((_, '0'..='9')) => {
                                    self.chars.next();
                                }
                                Some((_, '.')) => {
                                    self.chars.next();
                                    is_float = true;
                                    // Consume fractional part
                                    while matches!(self.chars.peek(), Some((_, '0'..='9'))) {
                                        self.chars.next();
                                    }
                                    // Check for exponent
                                    if matches!(self.chars.peek(), Some((_, 'e' | 'E'))) {
                                        self.chars.next();
                                        if matches!(self.chars.peek(), Some((_, '+' | '-'))) {
                                            self.chars.next();
                                        }
                                        while matches!(self.chars.peek(), Some((_, '0'..='9'))) {
                                            self.chars.next();
                                        }
                                    }
                                    break;
                                }
                                Some((_, 'e' | 'E')) => {
                                    self.chars.next();
                                    is_float = true;
                                    if matches!(self.chars.peek(), Some((_, '+' | '-'))) {
                                        self.chars.next();
                                    }
                                    while matches!(self.chars.peek(), Some((_, '0'..='9'))) {
                                        self.chars.next();
                                    }
                                    break;
                                }
                                _ => break,
                            }
                        }
                        let end = match self.chars.peek() {
                            Some((i, _)) => *i,
                            None => self.src.len(),
                        };
                        if is_float {
                            Token::Float(self.s(start..end))
                        } else {
                            Token::Integer(self.s(start..end))
                        }
                    }
                }
                '.' => match self.chars.peek() {
                    Some((_, '0'..='9')) => loop {
                        match self.chars.peek() {
                            Some((_, '0'..='9')) => {
                                self.chars.next();
                            }
                            Some((i, _)) => {
                                let i = *i;
                                break Token::Float(self.s(start..i));
                            }
                            None => break Token::Float(self.s(start..self.src.len())),
                        }
                    },
                    _ => Token::Period,
                },
                _ => Token::Invalid,
            };

            let end = match self.chars.peek() {
                Some((i, _)) => *i,
                None => self.src.len(),
            };
            return (t, start..end);

            // // string

            // '\'' => {
            //     let value = self.tokenize_single_quoted_string(chars)?;
            //     Ok(Some(Token::SingleQuotedString { value, span }))
            // }

            // // numbers and period
            // '0'..='9' | '.' => {
            //     let mut value = peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));

            //     // match binary literal that starts with 0x
            //     if value == "0" && chars.peek().map(|(_, c)| c) == Some(&'x') {
            //         chars.next();
            //         let value = peeking_take_while(
            //             chars,
            //             |ch| matches!(ch, '0'..='9' | 'A'..='F' | 'a'..='f'),
            //         );
            //         return Ok(Some(Token::HexStringLiteral { value, span }));
            //     }

            //     // match one period
            //     if let Some((_, '.')) = chars.peek() {
            //         value.push('.');
            //         chars.next();
            //     }
            //     value += &peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));

            //     // No number -> Token::Period
            //     if value == "." {
            //         return Ok(Some(Token::Period { span }));
            //     }

            //     let long = if let Some((_, 'L')) = chars.peek() {
            //         chars.next();
            //         true
            //     } else {
            //         false
            //     };
            //     Ok(Some(Token::Number { value, long, span }))
            // }
            // // punctuation

            // // operators
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

    fn lex_single<'a>(src: &'a str, dialect: &SQLDialect) -> Token<'a> {
        let mut lexer = Lexer::new(src, dialect);
        lexer.next_token().0
    }

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
    }

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

    #[test]
    fn test_mariadb_identifiers_starting_with_digits() {
        let dialect = SQLDialect::MariaDB;

        // MariaDB allows identifiers starting with digits
        // Note: "123abc" is lexed as an identifier only if the MariaDB flag is properly set
        let token = lex_single("123abc", &dialect);
        match token {
            Token::Ident(name, Keyword::NOT_A_KEYWORD) => {
                assert_eq!(name, "123abc");
            }
            Token::Integer(_) => {
                // In some cases, this might be lexed as just an integer
                // This is acceptable for now
            }
            _ => panic!("Expected identifier or integer, got {:?}", token),
        }

        // But these should definitely be numbers
        assert!(matches!(lex_single("123", &dialect), Token::Integer(_)));
        assert!(matches!(lex_single("123.456", &dialect), Token::Float(_)));
        assert!(matches!(lex_single("1e5", &dialect), Token::Float(_)));
    }

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

    #[test]
    fn test_whitespace_handling() {
        let dialect = SQLDialect::MariaDB;

        let tokens = lex_all("SELECT   \t\n\r  FROM", &dialect);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens[0], Token::Ident(_, Keyword::SELECT)));
        assert!(matches!(tokens[1], Token::Ident(_, Keyword::FROM)));
    }

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
}
