//! Proc macros to perform type sql queries similarly to sqlx::query, but without the need
//! to run `cargo sqlx prepare`
//!
//! A schema definition must be placed in "sqlx-type-schema.sql" in the root of a using crate:
//!
//! ```sql
//! DROP TABLE IF EXISTS `t1`;
//! CREATE TABLE `t1` (
//!     `id` int(11) NOT NULL,
//!     `cbool` tinyint(1) NOT NULL,
//!     `cu8` tinyint UNSIGNED NOT NULL,
//!     `cu16` smallint UNSIGNED NOT NULL,
//!     `cu32` int UNSIGNED NOT NULL,
//!     `cu64` bigint UNSIGNED NOT NULL,
//!     `ci8` tinyint,
//!     `ci16` smallint,
//!     `ci32` int,
//!     `ci64` bigint,
//!     `ctext` varchar(100) NOT NULL,
//!     `cbytes` blob,
//!     `cf32` float,
//!     `cf64` double
//! ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
//!
//! ALTER TABLE `t1`
//!     MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
//! ```
//! See [sql_type::schema] for a detailed description.
//!
//! [sql_type::schema]: https://docs.rs/sql-type/latest/sql_type/schema/index.html
//!
//! This schema can then be used to type queries:
//!
//! ``` no_run
//! use {std::env, sqlx::MySqlPool, sqlx_type::query};
//!
//! async fn test() -> Result<(), sqlx::Error> {
//!     let pool = MySqlPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//!
//!     let id = query!("INSERT INTO `t1` (`cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ctext`)
//!         VALUES (?, ?, ?, ?, ?, ?)", true, 8, 1243, 42, 42, "Hello world")
//!         .execute(&pool).await?.last_insert_id();
//!
//!     let row = query!("SELECT `cu16`, `ctext`, `ci32` FROM `t1` WHERE `id`=?", id)
//!         .fetch_one(&pool).await?;
//!
//!     assert_eq!(row.cu16, 1234);
//!     assert_eq!(row.ctext, "Hello would");
//!     assert!(row.ci32.is_none());
//!     Ok(())
//! }
//! ```
#![forbid(unsafe_code)]
#[allow(clippy::single_component_path_imports)]
use sqlx_type_macro;

pub use crate::sqlx_type_macro::{query, query_as};

/// Tag type for integer input
#[doc(hidden)]
pub struct Integer;

/// Tag type for float input
#[doc(hidden)]
pub struct Float;

/// Tag type for timestamp input
#[doc(hidden)]
pub struct Timestamp;

/// Tag type for datetime input
#[doc(hidden)]
pub struct DateTime;

/// Tag type for date input
#[doc(hidden)]
pub struct Date;

/// Tag type for time input
#[doc(hidden)]
pub struct Time;

/// Tag type for time input
#[doc(hidden)]
pub struct Any;


/// If ArgIn<T> is implemented for J, it means that J can be used as for arguments of type T
#[doc(hidden)]
pub trait ArgIn<T> {}
pub trait ArgOut<T, const IDX: usize> {}

macro_rules! arg_io {
    ( $dst: ty, $t: ty ) => {
        impl ArgIn<$dst> for $t {}
        impl ArgIn<$dst> for &$t {}
        impl ArgIn<Option<$dst>> for $t {}
        impl ArgIn<Option<$dst>> for &$t {}
        impl ArgIn<Option<$dst>> for Option<$t> {}
        impl ArgIn<Option<$dst>> for Option<&$t> {}
        impl ArgIn<Option<$dst>> for &Option<$t> {}
        impl ArgIn<Option<$dst>> for &Option<&$t> {}

        impl<const IDX: usize> ArgOut<$dst, IDX> for $t {}
        impl<const IDX: usize> ArgOut<Option<$dst>, IDX> for Option<$t> {}
        impl<const IDX: usize> ArgOut<$dst, IDX> for Option<$t> {}
    };
}


arg_io!(Any, u64);
arg_io!(Any, i64);
arg_io!(Any, u32);
arg_io!(Any, i32);
arg_io!(Any, u16);
arg_io!(Any, i16);
arg_io!(Any, u8);
arg_io!(Any, i8);
arg_io!(Any, String);
arg_io!(Any, f64);
arg_io!(Any, f32);
arg_io!(Any, &str);

arg_io!(Integer, u64);
arg_io!(Integer, i64);
arg_io!(Integer, u32);
arg_io!(Integer, i32);
arg_io!(Integer, u16);
arg_io!(Integer, i16);
arg_io!(Integer, u8);
arg_io!(Integer, i8);

arg_io!(String, String);

arg_io!(Float, f64);
arg_io!(Float, f32);

arg_io!(u64, u64);
arg_io!(i64, i64);
arg_io!(u32, u32);
arg_io!(i32, i32);
arg_io!(u16, u16);
arg_io!(i16, i16);
arg_io!(u8, u8);
arg_io!(i8, i8);
arg_io!(bool, bool);
arg_io!(f32, f32);
arg_io!(f64, f64);

arg_io!(&str, &str);
arg_io!(&str, String);
arg_io!(&str, std::borrow::Cow<'_, str>);

arg_io!(&[u8], &[u8]);
arg_io!(&[u8], Vec<u8>);
arg_io!(Vec<u8>, Vec<u8>);

arg_io!(Timestamp, chrono::NaiveDateTime);
arg_io!(DateTime, chrono::NaiveDateTime);
arg_io!(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>);
arg_io!(Timestamp, chrono::DateTime<chrono::Utc>);

#[doc(hidden)]
pub fn check_arg<T, T2: ArgIn<T>>(_: &T2) {}

#[doc(hidden)]
pub fn check_arg_list_hack<T, T2: ArgIn<T>>(_: &[T2]) {}

#[doc(hidden)]
pub fn arg_out<T, T2: ArgOut<T, IDX>, const IDX: usize>(v: T2) -> T2 {
    v
}

#[doc(hidden)]
pub fn convert_list_query(query: &str, list_sizes: &[usize]) -> String {
    let mut query_iter = query.split("_LIST_");
    let mut query = query_iter.next().expect("None empty query").to_string();
    for size in list_sizes {
        if *size == 0 {
            query.push_str("NULL");
        } else {
            for i in 0..*size {
                if i == 0 {
                    query.push('?');
                } else {
                    query.push_str(", ?");
                }
            }
        }
        query.push_str(query_iter.next().expect("More _LIST_ in query"));
    }
    if query_iter.next().is_some() {
        panic!("Too many _LIST_ in query");
    }
    query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_list_query() {
        // This assert would fire and test will fail.
        // Please note, that private functions can be tested too!
        assert_eq!(
            &convert_list_query("FOO (_LIST_) X _LIST_ O _LIST_ BAR (_LIST_)", &[0, 1, 2, 3]),
            "FOO (NULL) X ? O ?, ? BAR (?, ?, ?)"
        );
    }
}
