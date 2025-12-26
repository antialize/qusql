//! Contains Structs to contains and parse rows
use crate::{
    constants::type_,
    decode::{Column, Decode},
    package_parser::{DecodeError, DecodeResult, PackageParser},
};

/// A row returned by a Mysql/Mariadb query
pub struct Row<'a> {
    /// Columns in the row
    columns: &'a [Column],
    /// The package containing the row
    package: &'a [u8],
}

impl<'a> Row<'a> {
    /// Construct a new row instance
    pub(crate) fn new(columns: &'a [Column], package: &'a [u8]) -> Self {
        Self { columns, package }
    }

    /// Construct a parser for the row. That can read fields one at a time
    pub fn parse(&self) -> RowParser<'a> {
        let mut parser = PackageParser::new(self.package);
        parser.get_u8().unwrap();
        let nulls = parser.get_bytes((self.columns.len() + 7 + 2) / 8).unwrap();
        RowParser {
            columns: self.columns,
            nulls,
            parser,
            idx: 0,
        }
    }

    /// Decode the row as a tuple using the [FromRow] trait
    ///
    /// ```no_run
    /// use qusql_mysql::{row::Row, package_parser::DecodeResult};
    ///
    /// fn test(row: &Row) -> DecodeResult<()> {
    ///     let (v1, v2): (u8, &str) = row.read()?;
    ///     Ok(())
    /// }
    /// ```
    pub fn read<T: FromRow<'a>>(&self) -> DecodeResult<T> {
        T::from_row(self)
    }
}

/// Parse fields of a row One at a time
pub struct RowParser<'a> {
    /// Index of the next column to parse
    idx: usize,
    /// List of all the column types
    columns: &'a [Column],
    /// Null map
    nulls: &'a [u8],
    /// Parser used to parse the fields
    parser: PackageParser<'a>,
}

impl<'a> RowParser<'a> {
    /// Return type information about the next column to read, or None if we there are no more columns
    pub fn get_next_column_info(&self) -> Option<&Column> {
        self.columns.get(self.idx)
    }

    /// Decode the next field as T
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next<T: Decode<'a>>(&mut self) -> DecodeResult<T> {
        let idx = self.idx;
        let c = self.columns.get(idx).ok_or(DecodeError::EndOfColumns)?;
        let null = self.nulls[(idx + 2) / 8] & (1 << ((idx + 2) % 8)) != 0;
        let v = T::decode(&mut self.parser, c, null)?;
        self.idx += 1;
        Ok(v)
    }

    /// Skip the next field
    #[inline]
    pub fn skip(&mut self) -> DecodeResult<()> {
        let idx = self.idx;
        let c = self.columns.get(idx).ok_or(DecodeError::EndOfColumns)?;
        let null = self.nulls[(idx + 2) / 8] & (1 << ((idx + 2) % 8)) != 0;
        if !null {
            match c.r#type {
                type_::DECIMAL
                | type_::NEW_DECIMAL
                | type_::STRING
                | type_::JSON
                | type_::VAR_CHAR
                | type_::ENUM
                | type_::SET
                | type_::TINY_BLOB
                | type_::MEDIUM_BLOB
                | type_::LONG_BLOB
                | type_::BLOB
                | type_::VAR_STRING
                | type_::BIT
                | type_::GEOMETRY => {
                    self.parser.skip_lenenc_str()?;
                }
                type_::TINY => self.parser.skip_bytes(1),
                type_::SHORT | type_::YEAR => self.parser.skip_bytes(2),
                type_::LONG | type_::FLOAT => self.parser.skip_bytes(4),
                type_::LONG_LONG | type_::DOUBLE => self.parser.skip_bytes(8),
                type_::TIMESTAMP | type_::DATE | type_::DATETIME | type_::TIME => {
                    let len = self.parser.get_u8()?;
                    self.parser.skip_bytes(len.into());
                }
                type_::INT24 => self.parser.skip_bytes(3),
                _ => return Err(DecodeError::InvalidValue),
            }
        }
        self.idx += 1;
        Ok(())
    }
}

/// Decode a row as a tuple
pub trait FromRow<'r>: Sized {
    /// Decode the row as Self
    fn from_row(row: &Row<'r>) -> DecodeResult<Self>;
}

/// Implement [FromRow] for a tuple
macro_rules! impl_from_row_for_tuple {
    ($( $T:ident ,)+) => {
        impl<'r, $($T,)+> FromRow<'r> for ($($T,)+)
        where
            $($T: crate::decode::Decode<'r>,)+
        {
            #[inline]
            fn from_row(row: &Row<'r> ) -> DecodeResult<Self> {
                let mut parser= row.parse();
                Ok((
                    ($(parser.next::<$T>()?,)+)
                ))
            }
        }
    };
}

impl<'r> FromRow<'r> for () {
    fn from_row(_: &Row<'r>) -> DecodeResult<Self> {
        Ok(())
    }
}

impl_from_row_for_tuple!(T1,);
impl_from_row_for_tuple!(T1, T2,);
impl_from_row_for_tuple!(T1, T2, T3,);
impl_from_row_for_tuple!(T1, T2, T3, T4,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13,);
impl_from_row_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14,);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21,
    T22,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21,
    T22, T23,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21,
    T22, T23, T24,
);
impl_from_row_for_tuple!(
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21,
    T22, T23, T24, T25,
);
