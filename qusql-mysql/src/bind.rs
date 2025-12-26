//! Provide support for binding arguments to queries
use crate::constants::type_;
use bytes::{BufMut, BytesMut};
use thiserror::Error;

/// Error type returned by [Bind::bind]
#[derive(Error, Debug)]
pub enum BindError {
    /// To many argument has been bound to the query
    #[error("to many arguments given")]
    TooManyArgumentsBound,
    /// Not enough arguments has been bound to the query
    #[error("missing argument")]
    TooFewArgumentsBound,
    /// Error converting between
    #[error("try from int")]
    TryFromInt(#[from] std::num::TryFromIntError),
}

const _: () = {
    assert!(size_of::<BindError>() <= 8);
};

/// Result type returned by [Bind::bind]
pub type BindResult<T> = Result<T, BindError>;

/// Writer used to to compose packages to
pub struct Writer<'a>(&'a mut BytesMut);

impl<'a> Writer<'a> {
    /// Construct a new writer writing into w
    #[allow(unused)]
    pub(crate) fn new(w: &'a mut BytesMut) -> Self {
        Writer(w)
    }

    /// Append a u8 to the package
    #[inline]
    pub fn put_u8(&mut self, v: u8) {
        self.0.put_u8(v);
    }

    /// Append a u16 to the package
    #[inline]
    pub fn put_u16(&mut self, v: u16) {
        self.0.put_u16_le(v);
    }

    /// Append a u24 to the package
    #[inline]
    pub fn put_u24(&mut self, v: u32) {
        self.0.put_u8((v & 0xFF) as u8);
        self.0.put_u8(((v >> 8) & 0xFF) as u8);
        self.0.put_u8(((v >> 16) & 0xFF) as u8);
    }

    /// Append a u32 to the package
    #[inline]
    pub fn put_u32(&mut self, v: u32) {
        self.0.put_u32_le(v);
    }

    /// Append a u64 to the package
    #[inline]
    pub fn put_u64(&mut self, v: u64) {
        self.0.put_u64_le(v);
    }

    /// Append a i8 to the package
    #[inline]
    pub fn put_i8(&mut self, v: i8) {
        self.0.put_i8(v);
    }

    /// Append a i16 to the package
    #[inline]
    pub fn put_i16(&mut self, v: i16) {
        self.0.put_i16_le(v);
    }

    /// Append a i32 to the package
    #[inline]
    pub fn put_i32(&mut self, v: i32) {
        self.0.put_i32_le(v);
    }

    /// Append a i64 to the package
    #[inline]
    pub fn put_i64(&mut self, v: i64) {
        self.0.put_i64_le(v);
    }

    /// Append a f32 to the package
    #[inline]
    pub fn put_f32(&mut self, v: f32) {
        self.0.put_f32_le(v);
    }

    /// Append a f64 to the package
    #[inline]
    pub fn put_f64(&mut self, v: f64) {
        self.0.put_f64_le(v);
    }

    /// Append a variable encode length to the package
    ///
    /// See <https://mariadb.com/docs/server/reference/clientserver-protocol/protocol-data-types#length-encoded-integers>
    #[inline]
    pub fn put_lenenc(&mut self, v: u64) {
        if v < 0xFB {
            self.put_u8(v as u8);
        } else if v <= 0xFFFF {
            self.put_u8(0xFC);
            self.put_u16(v as u16);
        } else if v <= 0xFFFFFF {
            self.put_u8(0xFD);
            self.put_u24(v as u32);
        } else {
            self.put_u8(0xFE);
            self.put_u64(v);
        }
    }

    /// Append the bytes in the slice to the package
    pub fn put_slice(&mut self, src: &[u8]) {
        self.0.put_slice(src);
    }
}

/// Bind a parameter to a query.
///
/// See <https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/server-response-packets-binary-protocol/packet_bindata>
/// to see how each type should be encoded
pub trait Bind {
    /// Should the unsigned flag be set for the value
    const UNSIGNED: bool = false;
    /// The type of the value encode as defined in [crate::constants::type_]
    const TYPE: u8;
    /// Bind this value as the next value to the query.
    ///
    /// Return true if the value is set, and false it it is null.
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool>;
}

/// Bind a [u8] as a unsigned [type_::TINY]
impl Bind for u8 {
    const UNSIGNED: bool = true;
    const TYPE: u8 = type_::TINY;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u8(*self);
        Ok(true)
    }
}

/// Bind a [i8] as a signed [type_::TINY]
impl Bind for i8 {
    const TYPE: u8 = type_::TINY;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_i8(*self);
        Ok(true)
    }
}

/// Bind a [u16] as a unsigned [type_::SHORT]
impl Bind for u16 {
    const UNSIGNED: bool = true;
    const TYPE: u8 = type_::SHORT;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u16(*self);
        Ok(true)
    }
}

/// Bind a [i16] as a signed [type_::SHORT]
impl Bind for i16 {
    const TYPE: u8 = type_::SHORT;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_i16(*self);
        Ok(true)
    }
}

/// Bind a [u32] as a unsigned [type_::LONG]
impl Bind for u32 {
    const UNSIGNED: bool = true;
    const TYPE: u8 = type_::LONG;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u32(*self);
        Ok(true)
    }
}

/// Bind a [i32] as a signed [type_::LONG]
impl Bind for i32 {
    const TYPE: u8 = type_::LONG;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_i32(*self);
        Ok(true)
    }
}

/// Bind a [u64] as a unsigned [type_::LONG_LONG]
impl Bind for u64 {
    const UNSIGNED: bool = true;
    const TYPE: u8 = type_::LONG_LONG;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u64(*self);
        Ok(true)
    }
}

/// Bind a [i64] as a signed [type_::LONG_LONG]
impl Bind for i64 {
    const TYPE: u8 = type_::LONG_LONG;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_i64(*self);
        Ok(true)
    }
}

/// Bind a [f32] as a [type_::FLOAT]
impl Bind for f32 {
    const TYPE: u8 = type_::FLOAT;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_f32(*self);
        Ok(true)
    }
}

/// Bind a [f64] as a [type_::DOUBLE]
impl Bind for f64 {
    const TYPE: u8 = type_::DOUBLE;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_f64(*self);
        Ok(true)
    }
}

/// Bind a [bool] as a [type_::TINY]
impl Bind for bool {
    const UNSIGNED: bool = true;
    const TYPE: u8 = type_::TINY;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u8(*self as u8);
        Ok(true)
    }
}

/// Bind a [String] as a [type_::STRING]
impl Bind for String {
    const TYPE: u8 = type_::STRING;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.len() as u64);
        writer.put_slice(self.as_bytes());
        Ok(true)
    }
}

/// Bind a &[str] as a [type_::STRING]
impl Bind for str {
    const TYPE: u8 = type_::STRING;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.len() as u64);
        writer.put_slice(self.as_bytes());
        Ok(true)
    }
}

/// Bind a [`Vec<u8>`] as a [type_::BLOB]
impl Bind for Vec<u8> {
    const TYPE: u8 = type_::BLOB;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.len() as u64);
        writer.put_slice(self);
        Ok(true)
    }
}

/// Bind a &[[u8]] as a [type_::BLOB]
impl Bind for [u8] {
    const TYPE: u8 = type_::BLOB;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.len() as u64);
        writer.put_slice(self);
        Ok(true)
    }
}

/// Bind an [`Option<T>`] as T if it is [Some], otherwise as Null
impl<T: Bind> Bind for Option<T> {
    const TYPE: u8 = T::TYPE;
    const UNSIGNED: bool = T::UNSIGNED;

    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        match self {
            Some(v) => v.bind(writer),
            None => Ok(false),
        }
    }
}

/// Bind arbitrary references
impl<T: Bind + ?Sized> Bind for &T {
    const TYPE: u8 = T::TYPE;
    const UNSIGNED: bool = T::UNSIGNED;

    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        (*self).bind(writer)
    }
}
