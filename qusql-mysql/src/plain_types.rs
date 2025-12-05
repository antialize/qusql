//! Contains simple types that can be used to represent Mysql/Mariadb types that does not have a corresponding rust type.
use crate::{
    bind::{Bind, BindResult, Writer},
    constants::type_,
    decode::{Column, Decode},
    package_parser::{DecodeError, DecodeResult, PackageParser},
};

/// Construct a new_type wrapper around a [str]. That can be used for string like Mysql/Mariadb types
macro_rules! new_type_str {
    ( $b:ident, $o:ident, $name: ident) => {
        #[doc = concat!("Type that represents a Mysql/Mariadb [type_::", stringify!($name), "]")]
        ///
        /// This is a new-type wrapper around &[str] than can be converted to and from &[str]
        #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
        #[repr(transparent)]
        pub struct $b(str);

        impl $b {
            /// Return a reference to the contained string
            pub fn as_str(&self) -> &str {
                self.into()
            }

            /// Construct a new instance
            pub const fn new(v: &str) -> &$b {
                // Safety: $b is a repr(transparent) str
                unsafe { std::mem::transmute(v) }
            }
        }

        impl std::borrow::ToOwned for $b {
            type Owned = $o;

            fn to_owned(&self) -> Self::Owned {
                $o(self.0.to_owned())
            }
        }

        impl<'a> From<&'a str> for &'a $b {
            fn from(v: &'a str) -> Self {
                // Safety: $b is a repr(transparent) str
                unsafe { std::mem::transmute(v) }
            }
        }

        impl<'a> From<&'a $b> for &'a str {
            fn from(v: &'a $b) -> Self {
                // Safety: $b is a repr(transparent) str
                unsafe { std::mem::transmute(v) }
            }
        }

        #[doc = concat!("Type that represents an owned Mysql/Mariadb [type_::", stringify!($name), "]")]
        ///
        /// This is a new-type wrapper around [String] than can be converted to and from [String]
        #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
        #[repr(transparent)]
        pub struct $o(String);

        impl $o {
            /// Return a reference to the contained string
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl std::borrow::Borrow<$b> for $o {
            fn borrow(&self) -> &$b {
                let v: &str = self.0.borrow();
                v.into()
            }
        }

        impl From<String> for $o {
            fn from(v: String) -> Self {
                $o(v)
            }
        }

        impl From<$o> for String {
            fn from(v: $o) -> Self {
                v.0
            }
        }

        impl Bind for $o {
            const TYPE: u8 = <&$b as Bind>::TYPE;
            #[inline]
            fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
                std::borrow::Borrow::<$b>::borrow(self).bind(writer)
            }
        }

        impl<'a> Decode<'a> for $o {
            fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
                let v: &'a $b = Decode::decode_none_null(parser, c)?;
                Ok(v.to_owned())
            }
        }
    };
}

/// Construct a new_type wrapper around a [[u8]]. That can be used for blob like Mysql/Mariadb types
macro_rules! new_type_bytes {
    ( $b:ident, $o:ident, $name: ident) => {
        #[doc = concat!("Type that represents a Mysql/Mariadb [type_::", stringify!($name), "]")]
        ///
        /// This is a new-type wrapper around &[[u8]] than can be converted to and from &[[u8]]
        #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
        #[repr(transparent)]
        pub struct $b([u8]);

        impl $b {
            /// Return the a reference to the contained bytes
            pub fn as_bytes(&self) -> &[u8] {
                self.into()
            }

            /// Construct a new instance
            pub const fn new(v: &[u8]) -> &$b {
                // Safety: $b is a repr(transparent) [u8]
                unsafe { std::mem::transmute(v) }
            }
        }

        impl std::borrow::ToOwned for $b {
            type Owned = $o;

            fn to_owned(&self) -> Self::Owned {
                $o(self.0.to_owned())
            }
        }

        impl<'a> From<&'a [u8]> for &'a $b {
            fn from(v: &'a [u8]) -> Self {
                // Safety: $b is a repr(transparent) [u8]
                unsafe { std::mem::transmute(v) }
            }
        }

        impl<'a> From<&'a $b> for &'a [u8] {
            fn from(v: &'a $b) -> Self {
                // Safety: $b is a repr(transparent) [u8]
                unsafe { std::mem::transmute(v) }
            }
        }

        #[doc = concat!("Type that represents an owned Mysql/Mariadb [type_::", stringify!($name), "]")]
        ///
        /// This is a new-type wrapper around [`Vec<u8>`] than can be converted to and from &[`Vec<u8>`]
        #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
        #[repr(transparent)]
        pub struct $o(Vec<u8>);

        impl $o {
            /// Return the a reference to the contained bytes
            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }
        }

        impl std::borrow::Borrow<$b> for $o {
            fn borrow(&self) -> &$b {
                let v: &[u8] = self.0.borrow();
                v.into()
            }
        }

        impl From<Vec<u8>> for $o {
            fn from(v: Vec<u8>) -> Self {
                $o(v)
            }
        }

        impl From<$o> for Vec<u8> {
            fn from(v: $o) -> Self {
                v.0
            }
        }

        impl Bind for $o {
            const TYPE: u8 = <&$b as Bind>::TYPE;
            #[inline]
            fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
                std::borrow::Borrow::<$b>::borrow(self).bind(writer)
            }
        }

        impl<'a> Decode<'a> for $o {
            fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
                let v: &'a $b = Decode::decode_none_null(parser, c)?;
                Ok(v.to_owned())
            }
        }
    };
}
new_type_str!(Decimal, OwnedDecimal, DECIMAL);

impl Bind for &Decimal {
    const TYPE: u8 = type_::DECIMAL;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.as_str().len() as u64);
        writer.put_slice(self.as_str().as_bytes());
        Ok(true)
    }
}

impl<'a> Decode<'a> for &'a Decimal {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::NEW_DECIMAL && c.r#type != type_::DECIMAL {
            return Err(DecodeError::TypeError {
                expected: type_::NEW_DECIMAL,
                got: c.r#type,
            });
        }
        Ok(parser.get_lenenc_str()?.into())
    }
}

new_type_str!(Json, OwnedJson, JSON);

impl Bind for &Json {
    // For some Some reason mariadb will not accept type_::JSON but instead wants type_::STRING
    const TYPE: u8 = type_::STRING;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.as_str().len() as u64);
        writer.put_slice(self.as_str().as_bytes());
        Ok(true)
    }
}
impl<'a> Decode<'a> for &'a Json {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::JSON
            && c.r#type != type_::STRING
            && !(c.r#type == type_::BLOB && c.character_set != 63)
        {
            return Err(DecodeError::TypeError {
                expected: type_::JSON,
                got: c.r#type,
            });
        }
        Ok(parser.get_lenenc_str()?.into())
    }
}

new_type_bytes!(Bit, OwnedBits, BIT);

impl Bind for &Bit {
    // For some Some reason mariadb will not accept type_::BIT but instead wants type_::BLOB
    const TYPE: u8 = type_::BLOB;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_lenenc(self.as_bytes().len() as u64);
        writer.put_slice(self.as_bytes());
        Ok(true)
    }
}
impl<'a> Decode<'a> for &'a Bit {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::BIT {
            return Err(DecodeError::TypeError {
                expected: type_::BIT,
                got: c.r#type,
            });
        }
        Ok(parser.get_lenenc_blob()?.into())
    }
}

/// Type that represents a Mysql/Mariadb [type_::YEAR]
///
/// This is a new-type wrapper around [i16] than can be converted to and from [i16]
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
pub struct Year(i16);

impl Year {
    /// Construct a new [Year] from a [i16]
    pub const fn new(v: i16) -> Self {
        Year(v)
    }
}

impl From<i16> for Year {
    fn from(value: i16) -> Self {
        Year(value)
    }
}

impl From<Year> for i16 {
    fn from(value: Year) -> Self {
        value.0
    }
}

/// Encode the [Year] as a [type_::SHORT]
impl Bind for Year {
    // For some reson mariadb wants a short here not a type_::YEAR
    const TYPE: u8 = type_::SHORT;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_i16(self.0);
        Ok(true)
    }
}

/// Decode a [type_::YEAR] as a [Year]
impl<'a> Decode<'a> for Year {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::YEAR {
            return Err(DecodeError::TypeError {
                expected: type_::YEAR,
                got: c.r#type,
            });
        }
        Ok(parser.get_i16()?.into())
    }
}

/// Type that represents a Mysql/Mariadb [type_::TIME]
#[derive(PartialEq, Eq, Debug, Default)]
pub struct Time {
    /// True if the time is positive
    pub positive: bool,
    /// The number of days in the time
    pub days: u32,
    /// The number of hours in the time
    pub hours: u8,
    /// The number of minutes in the time
    pub minutes: u8,
    /// The number of seconds in the time
    pub seconds: u8,
    /// The number of micro seconds in the time
    pub microseconds: u32,
}

/// Encode the [Time] as a [type_::TIME]
impl Bind for Time {
    const TYPE: u8 = type_::TIME;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u8(if self.microseconds == 0 { 8 } else { 12 });
        writer.put_u8(if self.positive { 0 } else { 1 });
        writer.put_u32(self.days);
        writer.put_u8(self.hours);
        writer.put_u8(self.minutes);
        writer.put_u8(self.seconds);
        if self.microseconds != 0 {
            writer.put_u32(self.microseconds);
        }
        Ok(true)
    }
}

/// Decode a [type_::TIME] as a [Time]
impl<'a> Decode<'a> for Time {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::TIME {
            return Err(DecodeError::TypeError {
                expected: type_::TIME,
                got: c.r#type,
            });
        }
        let long = match parser.get_u8()? {
            0 => {
                return Ok(Time {
                    positive: true,
                    ..Default::default()
                });
            }
            8 => false,
            12 => true,
            len => return Err(DecodeError::InvalidSize(len)),
        };
        let positive = parser.get_u8()? == 0;
        let days = parser.get_u32()?;
        let hours = parser.get_u8()?;
        let minutes = parser.get_u8()?;
        let seconds = parser.get_u8()?;
        let microseconds = if long { parser.get_u32()? } else { 0 };
        Ok(Time {
            positive,
            days,
            hours,
            minutes,
            seconds,
            microseconds,
        })
    }
}

/// Type representing a Mysql/Mariadb [type_::TIMESTAMP]
#[derive(PartialEq, Eq, Debug)]
pub struct Timestamp {
    /// The year of the timestamp
    pub year: i16,
    /// The month of the timestamp, 1 for january
    pub month: u8,
    /// The day of the timestamp, starting from 1
    pub day: u8,
    /// The hour of the timestamp 0-23
    pub hour: u8,
    /// The minute of the timestamp 0-59
    pub minute: u8,
    /// The second of the timestamp 0-59
    pub second: u8,
    /// The microseconds of the timestamp 0-999999
    pub msec: u32,
}

/// Encode a [Timestamp] as a [type_::TIMESTAMP]
impl Bind for Timestamp {
    const TYPE: u8 = type_::TIMESTAMP;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u8(if self.msec == 0 { 7 } else { 11 });
        writer.put_i16(self.year);
        writer.put_u8(self.month);
        writer.put_u8(self.day);
        writer.put_u8(self.hour);
        writer.put_u8(self.minute);
        writer.put_u8(self.second);
        if self.msec != 0 {
            writer.put_u32(self.msec);
        }
        Ok(true)
    }
}

/// Decode a [type_::TIMESTAMP] as a [Timestamp]
impl<'a> Decode<'a> for Timestamp {
    fn decode_none_null(p: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::TIMESTAMP {
            return Err(DecodeError::TypeError {
                expected: type_::TIMESTAMP,
                got: c.r#type,
            });
        }
        let len = p.get_u8().unwrap();
        match len {
            0 => Ok(Timestamp {
                year: 0,
                month: 0,
                day: 0,
                hour: 0,
                minute: 0,
                second: 0,
                msec: 0,
            }),
            4 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                Ok(Timestamp {
                    year,
                    month,
                    day,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    msec: 0,
                })
            }
            7 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                let hour = p.get_u8().unwrap();
                let minute = p.get_u8().unwrap();
                let second = p.get_u8().unwrap();
                Ok(Timestamp {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    msec: 0,
                })
            }
            11 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                let hour = p.get_u8().unwrap();
                let minute = p.get_u8().unwrap();
                let second = p.get_u8().unwrap();
                let msec = p.get_u32().unwrap();
                Ok(Timestamp {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    msec,
                })
            }
            _ => Err(DecodeError::InvalidSize(len)),
        }
    }
}

/// Type representing a Mysql/Mariadb [type_::DATETIME]
#[derive(PartialEq, Eq, Debug)]
pub struct DateTime {
    /// The year of the datetime
    pub year: i16,
    /// The month of the datetime, 1 for january
    pub month: u8,
    /// The day of the datetime, starting from 1
    pub day: u8,
    /// The hour of the datetime 0-23
    pub hour: u8,
    /// The minute of the datetime 0-59
    pub minute: u8,
    /// The second of the datetime 0-59
    pub second: u8,
    /// The microseconds of the datetime 0-999999
    pub msec: u32,
}

/// Encode a [DateTime] as a [type_::DATETIME]
impl Bind for DateTime {
    const TYPE: u8 = type_::DATETIME;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        let ts = Timestamp {
            year: self.year,
            month: self.month,
            day: self.day,
            hour: self.hour,
            minute: self.minute,
            second: self.second,
            msec: self.msec,
        };
        let ts = &ts;
        ts.bind(writer)
    }
}

/// Decode a [type_::DATETIME] as a [DateTime]
impl<'a> Decode<'a> for DateTime {
    fn decode_none_null(p: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::DATE && c.r#type != type_::DATETIME {
            return Err(DecodeError::TypeError {
                expected: type_::DATETIME,
                got: c.r#type,
            });
        }
        let len = p.get_u8().unwrap();
        match len {
            0 => Ok(DateTime {
                year: 0,
                month: 0,
                day: 0,
                hour: 0,
                minute: 0,
                second: 0,
                msec: 0,
            }),
            4 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                Ok(DateTime {
                    year,
                    month,
                    day,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    msec: 0,
                })
            }
            7 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                let hour = p.get_u8().unwrap();
                let minute = p.get_u8().unwrap();
                let second = p.get_u8().unwrap();
                Ok(DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    msec: 0,
                })
            }
            11 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                let hour = p.get_u8().unwrap();
                let minute = p.get_u8().unwrap();
                let second = p.get_u8().unwrap();
                let msec = p.get_u32().unwrap();
                Ok(DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    msec,
                })
            }
            _ => Err(DecodeError::InvalidSize(len)),
        }
    }
}

/// Type representing a Mysql/Mariadb [type_::DATE]
#[derive(PartialEq, Eq, Debug)]
pub struct Date {
    /// The year of the date
    pub year: i16,
    /// The month of the date, 1 for january
    pub month: u8,
    /// The day of the date, starting from 1
    pub day: u8,
}

/// Encode a [Date] as a [type_::DATE]
impl Bind for Date {
    const TYPE: u8 = type_::DATE;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        writer.put_u8(4);
        writer.put_i16(self.year);
        writer.put_u8(self.month);
        writer.put_u8(self.day);
        Ok(true)
    }
}

/// Decode a [type_::DATE] as a [Date]
impl<'a> Decode<'a> for Date {
    fn decode_none_null(p: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::DATE {
            return Err(DecodeError::TypeError {
                expected: type_::DATE,
                got: c.r#type,
            });
        }
        let len = p.get_u8().unwrap();
        match len {
            0 => Ok(Date {
                year: 0,
                month: 0,
                day: 0,
            }),
            4 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                Ok(Date { year, month, day })
            }
            _ => Err(DecodeError::InvalidSize(len)),
        }
    }
}
