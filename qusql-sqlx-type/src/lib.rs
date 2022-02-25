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
//! use std::env, sqlx::MySqlPool, sqlx_type::query;
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

/// If ArgIn<T> is implemented for J, it means that J can be used as for arguments of type T
#[doc(hidden)]
pub trait ArgIn<T> {}

macro_rules! arg_in {
    ( $dst: ty, $t: ty ) => {
        impl ArgIn<$dst> for $t {}
        impl ArgIn<$dst> for &$t {}
        impl ArgIn<Option<$dst>> for $t {}
        impl ArgIn<Option<$dst>> for &$t {}
        impl ArgIn<Option<$dst>> for Option<$t> {}
        impl ArgIn<Option<$dst>> for Option<&$t> {}
        impl ArgIn<Option<$dst>> for &Option<$t> {}
        impl ArgIn<Option<$dst>> for &Option<&$t> {}
    };
}

arg_in!(Integer, u64);
arg_in!(Integer, i64);
arg_in!(Integer, u32);
arg_in!(Integer, i32);
arg_in!(Integer, u16);
arg_in!(Integer, i16);
arg_in!(Integer, u8);
arg_in!(Integer, i8);

arg_in!(Float, f64);
arg_in!(Float, f32);

arg_in!(u64, u64);
arg_in!(i64, i64);
arg_in!(u32, u32);
arg_in!(i32, i32);
arg_in!(u16, u16);
arg_in!(i16, i16);
arg_in!(u8, u8);
arg_in!(i8, i8);
arg_in!(bool, bool);
arg_in!(f32, f32);
arg_in!(f64, f64);

arg_in!(&str, &str);
arg_in!(&str, String);
arg_in!(&str, std::borrow::Cow<'_, str>);

arg_in!(&[u8], &[u8]);
arg_in!(&[u8], Vec<u8>);

arg_in!(Timestamp, chrono::NaiveDateTime);
arg_in!(DateTime, chrono::NaiveDateTime);

#[doc(hidden)]
pub fn check_arg<T, T2: ArgIn<T>>(_: &T2) {}
