import re
from typing import List

# Minimal Rust lexer using Python regex.
# This module only extracts string literal tokens from Rust source.

TOKEN_SPEC = [
    ("LINE_COMMENT", r"//[^\n]*"),
    ("BLOCK_COMMENT", r"/\*[\s\S]*?\*/"),
    ("WHITESPACE", r"[ \t\r\n]+"),
    # Raw strings with hashes, e.g. r#"..."# or br###"..."###
    ("RAW_HASHED", r'(?:br|r)(?P<HASHES>#+)"(?s:.*?)"(?P=HASHES)'),
    # Raw strings without hashes, e.g. r"..." or br"..."
    ("RAW", r'(?:br|r)"(?s:.*?)"'),
    # Normal double-quoted strings (not raw) - do not include newlines
    ("STRING", r'"(?:\\.|[^"\\\n])*"'),
    ("UNKNOWN", r"."),
]

MASTER_REGEX = re.compile(
    "|".join(f"(?P<{name}>{pattern})" for name, pattern in TOKEN_SPEC), re.MULTILINE
)


def lex(text: str) -> List[str]:
    """Return a list of string literal token values found in `text`.

    Only tokens of kind 'STRING' are returned; other token kinds are skipped.
    The returned strings include their surrounding quotes as in the source.
    """

    def _unescape_normal(s: str) -> str:
        # remove surrounding quotes
        inner = s[1:-1]

        # convert Rust \u{...} escapes to actual chars
        def _repl_u(m):
            code = int(m.group(1), 16)
            try:
                return chr(code)
            except Exception:
                return ""

        inner = re.sub(r"\\u\{([0-9A-Fa-f]+)\}", _repl_u, inner)
        # Use python's unicode_escape for common escapes like \n, \t, \xNN
        try:
            return bytes(inner, "utf-8").decode("unicode_escape")
        except Exception:
            # fallback: unescape only simple sequences
            return inner.replace('\\"', '"').replace("\\\\", "\\")

    def _strip_raw(s: str) -> str:
        # raw forms may start with r or br, possibly followed by hashes before the opening quote
        # find first double-quote and last double-quote
        first = s.find('"')
        last = s.rfind('"')
        if first == -1 or last == -1 or last <= first:
            return ""
        return s[first + 1 : last]

    tokens: List[str] = []
    for m in MASTER_REGEX.finditer(text):
        kind = m.lastgroup
        val = m.group()
        if kind == "STRING":
            tokens.append(_unescape_normal(val))
        elif kind in ("RAW", "RAW_HASHED"):
            tokens.append(_strip_raw(val))
    return tokens
