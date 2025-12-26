//! Facilities for decoding values from query result
use crate::{
    constants::{column_flag, type_},
    package_parser::{DecodeError, DecodeResult, PackageParser},
};

/// Information about the column we are decoding a value from
pub struct Column {
    /// The type of the column, see [crate::constants::type_]
    pub r#type: u8,
    /// Column flags, see [crate::constants::column_flag]
    pub flags: u16,
    /// Character set of the column
    pub character_set: u16,
}

/// Decode a value for a specific column
///
/// See <https://mariadb.com/docs/server/reference/clientserver-protocol/4-server-response-packets/resultset-row#date-binary-encoding>
/// for how the values are encoded if you want to implement [Decode] for your own types.
pub trait Decode<'a>: Sized {
    /// Decode the value from the parser given get column information in c,
    /// assuming the value is none null
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self>;

    /// Decode the value from the parser given get column information in c
    fn decode(parser: &mut PackageParser<'a>, c: &Column, null: bool) -> DecodeResult<Self> {
        if null {
            Err(DecodeError::Null)
        } else {
            Self::decode_none_null(parser, c)
        }
    }
}

/// For optional values decode null as [None], and other values as [Some] (v)
impl<'a, T: Decode<'a>> Decode<'a> for Option<T> {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        Ok(Some(T::decode_none_null(parser, c)?))
    }

    fn decode(parser: &mut PackageParser<'a>, c: &Column, null: bool) -> DecodeResult<Self> {
        if null {
            Ok(None)
        } else {
            Ok(Some(T::decode_none_null(parser, c)?))
        }
    }
}

/// Decode a [type_::TINY] as a [bool]
impl<'a> Decode<'a> for bool {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        match c.r#type {
            type_::LONG_LONG => Ok(parser.get_u64()? != 0),
            type_::LONG => Ok(parser.get_u32()? != 0),
            type_::SHORT => Ok(parser.get_u16()? != 0),
            type_::TINY => Ok(parser.get_u8()? != 0),
            _ => Err(DecodeError::TypeError {
                expected: type_::TINY,
                got: c.r#type,
            }),
        }
    }
}

/// Decode a unsigned [type_::TINY] as a [u8]
impl<'a> Decode<'a> for u8 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?.try_into()?),
                type_::SHORT => Ok(parser.get_i16()?.try_into()?),
                type_::TINY => Ok(parser.get_i8()?.try_into()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::TINY,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?.try_into()?),
                type_::SHORT => Ok(parser.get_u16()?.try_into()?),
                type_::TINY => Ok(parser.get_u8()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::TINY,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a signed [type_::TINY] as a [i8]
impl<'a> Decode<'a> for i8 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?.try_into()?),
                type_::SHORT => Ok(parser.get_i16()?.try_into()?),
                type_::TINY => Ok(parser.get_i8()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::TINY,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?.try_into()?),
                type_::SHORT => Ok(parser.get_u16()?.try_into()?),
                type_::TINY => Ok(parser.get_u8()?.try_into()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::TINY,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a unsigned [type_::SHORT] as a [u16]
impl<'a> Decode<'a> for u16 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?.try_into()?),
                type_::SHORT => Ok(parser.get_i16()?.try_into()?),
                type_::TINY => Ok(parser.get_i8()?.try_into()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::SHORT,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?.try_into()?),
                type_::SHORT => Ok(parser.get_u16()?),
                type_::TINY => Ok(parser.get_u8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::SHORT,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a signed [type_::SHORT] as a [i16]
impl<'a> Decode<'a> for i16 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?.try_into()?),
                type_::SHORT => Ok(parser.get_i16()?),
                type_::TINY => Ok(parser.get_i8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::SHORT,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?.try_into()?),
                type_::SHORT => Ok(parser.get_u16()?.try_into()?),
                type_::TINY => Ok(parser.get_u8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::SHORT,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a unsigned [type_::LONG] as a [u32]
impl<'a> Decode<'a> for u32 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?.try_into()?),
                type_::SHORT => Ok(parser.get_i16()?.try_into()?),
                type_::TINY => Ok(parser.get_i8()?.try_into()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?),
                type_::SHORT => Ok(parser.get_u16()?.into()),
                type_::TINY => Ok(parser.get_u8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a signed [type_::LONG] as a [i32]
impl<'a> Decode<'a> for i32 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?),
                type_::SHORT => Ok(parser.get_i16()?.into()),
                type_::TINY => Ok(parser.get_i8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?.try_into()?),
                type_::SHORT => Ok(parser.get_u16()?.into()),
                type_::TINY => Ok(parser.get_u8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a unsigned [type_::LONG_LONG] as a [u64]
impl<'a> Decode<'a> for u64 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?.try_into()?),
                type_::LONG => Ok(parser.get_i32()?.try_into()?),
                type_::SHORT => Ok(parser.get_i16()?.try_into()?),
                type_::TINY => Ok(parser.get_i8()?.try_into()?),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG_LONG,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?),
                type_::LONG => Ok(parser.get_u32()?.into()),
                type_::SHORT => Ok(parser.get_u16()?.into()),
                type_::TINY => Ok(parser.get_u8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG_LONG,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a signed [type_::LONG_LONG] as a [i64]
impl<'a> Decode<'a> for i64 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if (c.flags & column_flag::UNSIGNED) == 0 {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_i64()?),
                type_::LONG => Ok(parser.get_i32()?.into()),
                type_::SHORT => Ok(parser.get_i16()?.into()),
                type_::TINY => Ok(parser.get_i8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG_LONG,
                    got: c.r#type,
                }),
            }
        } else {
            match c.r#type {
                type_::LONG_LONG => Ok(parser.get_u64()?.try_into()?),
                type_::LONG => Ok(parser.get_u32()?.into()),
                type_::SHORT => Ok(parser.get_u16()?.into()),
                type_::TINY => Ok(parser.get_u8()?.into()),
                _ => Err(DecodeError::TypeError {
                    expected: type_::LONG_LONG,
                    got: c.r#type,
                }),
            }
        }
    }
}

/// Decode a unsigned [type_::FLOAT] as a [f32]
impl<'a> Decode<'a> for f32 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        match c.r#type {
            type_::FLOAT => Ok(parser.get_f32()?),
            type_::DOUBLE => Ok(parser.get_f64()? as f32),
            _ => Err(DecodeError::TypeError {
                expected: type_::FLOAT,
                got: c.r#type,
            }),
        }
    }
}

/// Decode a unsigned [type_::DOUBLE] as a [f64]
impl<'a> Decode<'a> for f64 {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        match c.r#type {
            type_::FLOAT => Ok(parser.get_f32()?.into()),
            type_::DOUBLE => Ok(parser.get_f64()?),
            _ => Err(DecodeError::TypeError {
                expected: type_::FLOAT,
                got: c.r#type,
            }),
        }
    }
}

/// Decode blob, string and json types as as a [str] reference
impl<'a> Decode<'a> for &'a str {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        match c.r#type {
            type_::BLOB
            | type_::VAR_STRING
            | type_::STRING
            | type_::JSON
            | type_::BIT
            | type_::ENUM
            | type_::SET
            | type_::TINY_BLOB
            | type_::MEDIUM_BLOB
            | type_::LONG_BLOB => parser.get_lenenc_str(),
            _ => Err(DecodeError::TypeError {
                expected: type_::STRING,
                got: c.r#type,
            }),
        }
    }
}

/// Decode blob, string and json types as as a [String]
impl<'a> Decode<'a> for String {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        let unsigned = (c.flags & column_flag::UNSIGNED) != 0;
        match c.r#type {
            type_::BLOB
            | type_::VAR_STRING
            | type_::STRING
            | type_::JSON
            | type_::BIT
            | type_::ENUM
            | type_::SET
            | type_::TINY_BLOB
            | type_::MEDIUM_BLOB
            | type_::LONG_BLOB => Ok(parser.get_lenenc_str()?.to_string()),
            type_::LONG_LONG if unsigned => Ok(parser.get_u64()?.to_string()),
            type_::LONG_LONG => Ok(parser.get_i64()?.to_string()),
            type_::LONG if unsigned => Ok(parser.get_u32()?.to_string()),
            type_::LONG => Ok(parser.get_i32()?.to_string()),
            type_::SHORT if unsigned => Ok(parser.get_u16()?.to_string()),
            type_::SHORT => Ok(parser.get_i16()?.to_string()),
            type_::TINY if unsigned => Ok(parser.get_u8()?.to_string()),
            type_::TINY => Ok(parser.get_i8()?.to_string()),
            type_::FLOAT => Ok(parser.get_f32()?.to_string()),
            type_::DOUBLE => Ok(parser.get_f64()?.to_string()),
            _ => Err(DecodeError::TypeError {
                expected: type_::LONG,
                got: c.r#type,
            }),
        }
    }
}

/// Decode blob and string types as [[u8]] slice
impl<'a> Decode<'a> for &'a [u8] {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        match c.r#type {
            type_::BLOB
            | type_::VAR_STRING
            | type_::STRING
            | type_::JSON
            | type_::BIT
            | type_::ENUM
            | type_::SET
            | type_::TINY_BLOB
            | type_::MEDIUM_BLOB
            | type_::LONG_BLOB => parser.get_lenenc_blob(),
            _ => Err(DecodeError::TypeError {
                expected: type_::STRING,
                got: c.r#type,
            }),
        }
    }
}

/// Decode blob and string types as [`Vec<u8>`]
impl<'a> Decode<'a> for Vec<u8> {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        let v: &'a [u8] = Decode::decode_none_null(parser, c)?;
        Ok(v.to_vec())
    }
}
