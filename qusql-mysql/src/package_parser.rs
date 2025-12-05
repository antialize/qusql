//! Contains parser used to parse packages
use bytes::Buf;
use thiserror::Error;

/// Error returned by the [PackageParser]
#[derive(Error, Debug)]
pub enum DecodeError {
    /// The package was shorter than expected
    #[error("End of package")]
    EndOfPackage,
    /// You are decoding more columns than there is in the response
    #[error("End of columns")]
    EndOfColumns,
    /// A string in a package was not utf-8 as expected
    #[error("Utf-8 error at {valid_up_to}")]
    Utf8Error {
        /// The string is valid utf-8 until this many bytes
        valid_up_to: u32,
        /// The length of the error
        error_len: Option<u8>,
    },
    /// We expected a non-null value, but found null
    #[error("Unexpected null value")]
    Null,
    /// We expected an unsigned integer but we got aa signed integer
    #[error("Expected unsigned got signed")]
    ExpectedUnsigned,
    /// We expected an signed integer but we got aa unsigned integer
    #[error("Expected signed got unsigned")]
    ExpectedSigned,
    /// The field we are decoding has a different type than expected
    #[error("Type error")]
    TypeError {
        /// The field has this type as defined in [crate::constants::type_]
        got: u8,
        /// We expected this type
        expected: u8,
    },
    /// A variable length encoded field has an unexpected size
    #[error("Invalid size {0}")]
    InvalidSize(u8),
}

const _: () = {
    assert!(size_of::<DecodeError>() <= 8);
};

impl From<bytes::TryGetError> for DecodeError {
    fn from(_value: bytes::TryGetError) -> Self {
        DecodeError::EndOfPackage
    }
}

impl From<std::str::Utf8Error> for DecodeError {
    fn from(value: std::str::Utf8Error) -> Self {
        DecodeError::Utf8Error {
            valid_up_to: value.valid_up_to().try_into().unwrap_or(u32::MAX),
            error_len: value.error_len().map(|v| v.try_into().unwrap_or(0xFF)),
        }
    }
}

/// Result returned by [PackageParser]
pub type DecodeResult<T> = std::result::Result<T, DecodeError>;

/// Parse a Mysql/Mariadb package
#[derive(Clone, Copy)]
pub struct PackageParser<'a>(&'a [u8]);

impl<'a> PackageParser<'a> {
    /// Construct a new [PackageParser] for the given package
    #[allow(unused)]
    pub(crate) fn new(package: &'a [u8]) -> Self {
        Self(package)
    }

    /// Read a u8 from the package
    #[inline]
    pub fn get_u8(&mut self) -> DecodeResult<u8> {
        Ok(self.0.try_get_u8()?)
    }

    /// Read a i8 from the package
    #[inline]
    pub fn get_i8(&mut self) -> DecodeResult<i8> {
        Ok(self.0.try_get_i8()?)
    }

    /// Read a u16 from the package
    #[inline]
    pub fn get_u16(&mut self) -> DecodeResult<u16> {
        Ok(self.0.try_get_u16_le()?)
    }

    /// Read a i16 from the package
    #[inline]
    pub fn get_i16(&mut self) -> DecodeResult<i16> {
        Ok(self.0.try_get_i16_le()?)
    }

    /// Read a u32 from the package
    #[inline]
    pub fn get_u32(&mut self) -> DecodeResult<u32> {
        Ok(self.0.try_get_u32_le()?)
    }

    /// Read a i32 from the package
    #[inline]
    pub fn get_i32(&mut self) -> DecodeResult<i32> {
        Ok(self.0.try_get_i32_le()?)
    }

    /// Read a u64 from the package
    #[inline]
    pub fn get_u64(&mut self) -> DecodeResult<u64> {
        Ok(self.0.try_get_u64_le()?)
    }

    /// Read a i64 from the package
    #[inline]
    pub fn get_i64(&mut self) -> DecodeResult<i64> {
        Ok(self.0.try_get_i64_le()?)
    }

    /// Read a f32 from the package
    #[inline]
    pub fn get_f32(&mut self) -> DecodeResult<f32> {
        Ok(self.0.try_get_f32_le()?)
    }

    /// Read a f64 from the package
    #[inline]
    pub fn get_f64(&mut self) -> DecodeResult<f64> {
        Ok(self.0.try_get_f64_le()?)
    }

    /// Read a u64 from the package
    #[inline]
    pub fn get_u24(&mut self) -> DecodeResult<u32> {
        let a: u32 = self.get_u8()?.into();
        let b: u32 = self.get_u8()?.into();
        let c: u32 = self.get_u8()?.into();
        Ok(a | (b << 8) | (c << 16))
    }

    /// Read a variable encoded length
    ///
    /// See <https://mariadb.com/docs/server/reference/clientserver-protocol/protocol-data-types#length-encoded-integers>
    #[inline]
    pub fn get_lenenc(&mut self) -> DecodeResult<u64> {
        let v = self.get_u8()?;
        Ok(match v {
            0xFC => self.get_u16()?.into(),
            0xFD => self.get_u24()?.into(),
            0xFE => self.get_u64()?,
            v => v.into(),
        })
    }

    /// Read a variable encoded blob
    #[inline]
    pub fn get_lenenc_blob(&mut self) -> DecodeResult<&'a [u8]> {
        let len = self.get_lenenc()?;
        self.get_bytes(len as usize)
    }

    /// Read a variable encoded utf8-string
    #[inline]
    pub fn get_lenenc_str(&mut self) -> DecodeResult<&'a str> {
        let len = self.get_lenenc()?;
        let v = self.get_bytes(len as usize)?;
        Ok(str::from_utf8(v)?)
    }

    /// Skip past a variable encoded string or blob
    #[inline]
    pub fn skip_lenenc_str(&mut self) -> DecodeResult<()> {
        let l = self.get_lenenc()?;
        self.0.advance(l as usize);
        Ok(())
    }

    /// Read a null-terminated string
    #[inline]
    pub fn get_null_str(&mut self) -> DecodeResult<&'a str> {
        match std::ffi::CStr::from_bytes_until_nul(self.0) {
            Ok(v) => {
                let v = v.to_str()?;
                self.0.advance(v.len() + 1);
                Ok(v)
            }
            Err(_) => Err(DecodeError::EndOfPackage),
        }
    }

    /// Skip past a null-terminated string
    #[inline]
    pub fn skip_null_str(&mut self) -> DecodeResult<()> {
        match std::ffi::CStr::from_bytes_until_nul(self.0) {
            Ok(v) => {
                self.0.advance(v.count_bytes() + 1);
                Ok(())
            }
            Err(_) => Err(DecodeError::EndOfPackage),
        }
    }

    /// Read the rest of the package as a utf-8 string
    #[inline]
    pub fn get_eof_str(&mut self) -> DecodeResult<&'a str> {
        let v = str::from_utf8(self.0)?;
        self.0.advance(v.len());
        Ok(v)
    }

    /// Read some bytes from the package
    #[inline]
    pub fn get_bytes(&mut self, len: usize) -> DecodeResult<&'a [u8]> {
        match self.0.get(..len) {
            Some(v) => {
                self.0.advance(len);
                Ok(v)
            }
            None => Err(DecodeError::EndOfPackage),
        }
    }
}
