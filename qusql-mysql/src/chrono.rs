//! Contains bindings for various chrono types
use crate::{
    bind::{Bind, BindResult, Writer},
    constants::type_,
    decode::{Column, Decode},
    package_parser::{DecodeError, DecodeResult, PackageParser},
};

impl Bind for chrono::NaiveTime {
    const TYPE: u8 = type_::TIME;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        use chrono::Timelike;
        let ms = self.nanosecond() / 1000;
        writer.put_u8(if ms == 0 { 8 } else { 12 });
        writer.put_u8(self.hour().try_into()?);
        writer.put_u8(self.minute().try_into()?);
        writer.put_u8(self.second().try_into()?);
        if ms != 0 {
            writer.put_u32(ms);
        }
        Ok(true)
    }
}

impl<'a> Decode<'a> for chrono::NaiveTime {
    fn decode_none_null(parser: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::TIME {
            return Err(DecodeError::TypeError {
                expected: type_::TIME,
                got: c.r#type,
            });
        }
        let long = match parser.get_u8()? {
            0 => return Ok(chrono::NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()),
            8 => false,
            12 => true,
            len => return Err(DecodeError::InvalidSize(len)),
        };
        let positive = parser.get_u8()? == 0;
        let days = parser.get_u32()?;
        let hour = parser.get_u8()?;
        let min = parser.get_u8()?;
        let sec = parser.get_u8()?;
        let micro = if long { parser.get_u32()? } else { 0 };
        if days != 0 || !positive {
            return Err(DecodeError::InvalidValue);
        }
        chrono::NaiveTime::from_hms_micro_opt(hour.into(), min.into(), sec.into(), micro)
            .ok_or(DecodeError::InvalidValue)
    }
}

impl Bind for chrono::NaiveDateTime {
    const TYPE: u8 = type_::DATETIME;

    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        use chrono::{Datelike, Timelike};
        let msec = self.time().nanosecond() / 1000;
        writer.put_u8(if msec == 0 { 7 } else { 11 });
        writer.put_i16(self.date().year().try_into()?);
        writer.put_u8(self.date().month().try_into()?);
        writer.put_u8(self.date().day().try_into()?);
        writer.put_u8(self.time().hour().try_into()?);
        writer.put_u8(self.time().minute().try_into()?);
        writer.put_u8(self.time().second().try_into()?);
        if msec != 0 {
            writer.put_u32(msec);
        }
        Ok(true)
    }
}

impl<'a> Decode<'a> for chrono::NaiveDateTime {
    fn decode_none_null(p: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::TIMESTAMP && c.r#type != type_::DATE && c.r#type != type_::DATETIME {
            return Err(DecodeError::TypeError {
                expected: type_::TIMESTAMP,
                got: c.r#type,
            });
        }

        let len = p.get_u8().unwrap();
        match len {
            0 => Ok(chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(0, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            )),
            4 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                Ok(chrono::NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year.into(), month.into(), day.into())
                        .ok_or(DecodeError::InvalidValue)?,
                    chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ))
            }
            7 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                let hour = p.get_u8().unwrap();
                let min = p.get_u8().unwrap();
                let sec = p.get_u8().unwrap();
                Ok(chrono::NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year.into(), month.into(), day.into())
                        .ok_or(DecodeError::InvalidValue)?,
                    chrono::NaiveTime::from_hms_opt(hour.into(), min.into(), sec.into())
                        .ok_or(DecodeError::InvalidValue)?,
                ))
            }
            11 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                let hour = p.get_u8().unwrap();
                let min = p.get_u8().unwrap();
                let sec = p.get_u8().unwrap();
                let micro = p.get_u32().unwrap();
                Ok(chrono::NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year.into(), month.into(), day.into())
                        .ok_or(DecodeError::InvalidValue)?,
                    chrono::NaiveTime::from_hms_micro_opt(
                        hour.into(),
                        min.into(),
                        sec.into(),
                        micro,
                    )
                    .ok_or(DecodeError::InvalidValue)?,
                ))
            }
            _ => Err(DecodeError::InvalidSize(len)),
        }
    }
}

impl Bind for chrono::NaiveDate {
    const TYPE: u8 = type_::DATE;
    #[inline]
    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        use chrono::Datelike;
        writer.put_u8(4);
        writer.put_i16(self.year().try_into()?);
        writer.put_u8(self.month().try_into()?);
        writer.put_u8(self.day().try_into()?);
        Ok(true)
    }
}

impl<'a> Decode<'a> for chrono::NaiveDate {
    fn decode_none_null(p: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        if c.r#type != type_::DATE {
            return Err(DecodeError::TypeError {
                expected: type_::DATE,
                got: c.r#type,
            });
        }
        let len = p.get_u8().unwrap();
        match len {
            0 => Ok(chrono::NaiveDate::from_ymd_opt(0, 1, 1).unwrap()),
            4 => {
                let year = p.get_i16().unwrap();
                let month = p.get_u8().unwrap();
                let day = p.get_u8().unwrap();
                chrono::NaiveDate::from_ymd_opt(year.into(), month.into(), day.into())
                    .ok_or(DecodeError::InvalidValue)
            }
            _ => Err(DecodeError::InvalidSize(len)),
        }
    }
}

impl Bind for chrono::DateTime<chrono::Utc> {
    const TYPE: u8 = type_::DATETIME;

    fn bind(&self, writer: &mut Writer<'_>) -> BindResult<bool> {
        self.naive_utc().bind(writer)
    }
}

impl<'a> Decode<'a> for chrono::DateTime<chrono::Utc> {
    fn decode_none_null(p: &mut PackageParser<'a>, c: &Column) -> DecodeResult<Self> {
        Ok(chrono::NaiveDateTime::decode_none_null(p, c)?.and_utc())
    }
}
