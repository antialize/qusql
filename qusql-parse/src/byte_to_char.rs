//! ByteToChar is a utility for mapping byte positions in a UTF-8 string to character positions,
//! which is useful for error reporting in parsers.
use alloc::vec::Vec;

/// Struct to map byte positions in a UTF-8 string to character positions, for error reporting.
///
/// Memory usage is optimized by storing cumulative character counts at 128 byte intervals,
/// which allows for efficient mapping of byte positions to character positions without
/// needing to store a mapping for every single byte.
///
/// ```
/// # let sql = "SELECT 1";
/// # let byte_span = 0..1usize;
/// use qusql_parse::ByteToChar;
///
/// let b2c = ByteToChar::new(sql.as_bytes());
/// let char_span = b2c.map_span(byte_span.start..byte_span.end);
/// ```
pub struct ByteToChar<'a> {
    bytes: &'a [u8],
    cnt: Vec<u32>, // Cumulative count of characters up for each 128 byte block
}

impl<'a> ByteToChar<'a> {
    /// Create a new ByteToChar mapping for the given byte slice.
    pub fn new(bytes: &'a [u8]) -> Self {
        let mut cnt = Vec::new();
        let mut char_count = 0;
        for chunk in bytes.chunks(128) {
            cnt.push(char_count);
            char_count += chunk.iter().filter(|&&b| (b & 0xC0) != 0x80).count() as u32;
        }
        Self { bytes, cnt }
    }

    /// Map a byte position to a character position.
    pub fn map(&self, byte_pos: usize) -> usize {
        let block_index = byte_pos / 128;
        let block_start_byte = block_index * 128;
        let char_count_before_block = self.cnt.get(block_index).cloned().unwrap_or(0) as usize;
        let char_count_in_block = self.bytes[block_start_byte..byte_pos]
            .iter()
            .filter(|&&b| (b & 0xC0) != 0x80)
            .count();
        char_count_before_block + char_count_in_block
    }

    /// Map a byte-offset span to a char-offset span.
    pub fn map_span(&self, span: core::ops::Range<usize>) -> core::ops::Range<usize> {
        self.map(span.start)..self.map(span.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_to_char() {
        let s = "Hello, 世界!"; // "Hello, " is 7 bytes, "世界" is 6 bytes, "!" is 1 byte
        let b2c = ByteToChar::new(s.as_bytes());

        assert_eq!(b2c.map(0), 0); // 'H'
        assert_eq!(b2c.map(7), 7); // ','
        assert_eq!(b2c.map(13), 9); // '界'
        assert_eq!(b2c.map(14), 10); // '!'
        assert_eq!(b2c.map_span(0..14), 0..10); // Full string

        // Test with more than one block
        let long_str = "a".repeat(200) + "世界"; // 200 'a' (1 byte each) + 6 bytes for '世界'
        let b2c_long = ByteToChar::new(long_str.as_bytes());
        assert_eq!(b2c_long.map(0), 0); // 'a'
        assert_eq!(b2c_long.map(199), 199); // Last 'a'
        assert_eq!(b2c_long.map(200), 200); // '世'
        assert_eq!(b2c_long.map(206), 202); // '界'
    }
}
