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

use crate::{Span, Spanned};

/// Compare `a` byte-for-byte against `b` with `b`'s bytes lowercased.
fn ord_verbatim_vs_lowercased(a: &str, b: &str) -> core::cmp::Ordering {
    for (a_byte, b_byte) in a.bytes().zip(b.bytes()) {
        let b_lower = b_byte.to_ascii_lowercase();
        if a_byte != b_lower {
            return a_byte.cmp(&b_lower);
        }
    }
    a.len().cmp(&b.len())
}

/// Compare two strings in ASCII case-insensitive manner, returning their ordering.
fn ord_ignore_ascii_case(a: &str, b: &str) -> core::cmp::Ordering {
    for (a_byte, b_byte) in a.bytes().zip(b.bytes()) {
        let a_lower = a_byte.to_ascii_lowercase();
        let b_lower = b_byte.to_ascii_lowercase();
        if a_lower != b_lower {
            return a_lower.cmp(&b_lower);
        }
    }
    a.len().cmp(&b.len())
}

/// Simple identifier in code
/// it derefs to its string value
#[derive(Clone, Debug)]
pub struct Identifier<'a> {
    /// Identifier string
    pub value: &'a str,
    /// Span of the value
    pub span: Span,
    /// Whether the identifier is case-sensitive (e.g. double-quoted in PostgreSQL)
    pub case_sensitive: bool,
}

impl<'a> PartialEq for Identifier<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == core::cmp::Ordering::Equal
    }
}
impl<'a> Eq for Identifier<'a> {}

impl<'a> PartialOrd for Identifier<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Identifier<'a> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        match (self.case_sensitive, other.case_sensitive) {
            (false, false) => ord_ignore_ascii_case(self.value, other.value),
            (true, true) => self.value.cmp(other.value),
            (true, false) => ord_verbatim_vs_lowercased(self.value, other.value),
            (false, true) => ord_verbatim_vs_lowercased(other.value, self.value).reverse(),
        }
    }
}

impl<'a> alloc::fmt::Display for Identifier<'a> {
    fn fmt(&self, f: &mut alloc::fmt::Formatter<'_>) -> alloc::fmt::Result {
        self.value.fmt(f)
    }
}

impl<'a> Identifier<'a> {
    /// Produce new identifier given value and span (unquoted, case-insensitive)
    pub fn new(value: &'a str, span: Span) -> Self {
        Identifier {
            value,
            span,
            case_sensitive: false,
        }
    }

    /// Produce a case-sensitive identifier (e.g. PostgreSQL `"Foo"`)
    pub fn new_case_sensitive(value: &'a str, span: Span) -> Self {
        Identifier {
            value,
            span,
            case_sensitive: true,
        }
    }

    /// Get the string representation of the identifier
    pub fn as_str(&self) -> &'a str {
        self.value
    }
}

impl<'a> core::ops::Deref for Identifier<'a> {
    type Target = str;

    fn deref(&self) -> &'a Self::Target {
        self.value
    }
}

impl<'a> Spanned for Identifier<'a> {
    fn span(&self) -> Span {
        self.span.span()
    }
}
