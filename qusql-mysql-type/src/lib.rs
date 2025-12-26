//! Proc macros to perform type typed mysql queries on top of qusql-mysql.
//!
//! The queries are typed based on a schema definition, that must be placed in "qusql-mysql-type-schema.sql"
//! in the root of a using crate:
//!
//! ```sql
//! DROP TABLE IF EXISTS `t1`;
//! CREATE TABLE `t1` (
//!     `id` int(11) NOT NULL,
//!     `cbool` tinyint(1) NOT NULL DEFAULT false,
//!     `cu8` tinyint UNSIGNED NOT NULL DEFAULT 0,
//!     `cu16` smallint UNSIGNED NOT NULL DEFAULT 1,
//!     `cu32` int UNSIGNED NOT NULL DEFAULT 2,
//!     `cu64` bigint UNSIGNED NOT NULL DEFAULT 3,
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
//! See [qusql_type::schema] for a detailed description.
//!
//! [qusql_type::schema]: https://docs.rs/qusql-type/latest/qusql_type/schema/index.html
//!
//! This schema can then be used to type queries:
//!
//! ```no_run
//! use qusql_mysql::connection::{ConnectionOptions, ConnectionError, ExecutorExt};
//! use qusql_mysql::pool::{Pool, PoolOptions};
//! use qusql_mysql_type::{execute, fetch_one};
//!
//! async fn test() -> Result<(), ConnectionError> {
//!     let pool = Pool::connect(
//!         ConnectionOptions::from_url("mysql://user:pw@127.0.0.1:3307/db").unwrap(),
//!         PoolOptions::new().max_connections(10)
//!     ).await?;
//!
//!     let mut conn = pool.acquire().await?;
//!
//!     let id = execute!(&mut conn, "INSERT INTO `t1` (
//!        `cbool`, `cu8`, `cu16`, `cu32`, `cu64`, `ctext`)
//!         VALUES (?, ?, ?, ?, ?, ?)",
//!         true, 8, 1243, 42, 42, "Hello world").await?.last_insert_id();
//!
//!     let row = fetch_one!(&mut conn,
//!         "SELECT `cu16`, `ctext`, `ci32` FROM `t1` WHERE `id`=?", id).await?;
//!
//!     assert_eq!(row.cu16, 1234);
//!     assert_eq!(row.ctext, "Hello would");
//!     assert!(row.ci32.is_none());
//!     Ok(())
//! }
//! ```
#![forbid(unsafe_code)]
#[allow(clippy::single_component_path_imports)]
use qusql_mysql_type_macro;

#[doc(hidden)]
pub use qusql_mysql_type_macro::execute_impl;

/// Statically checked execute
///
/// This expands into a [qusql_mysql::ExecutorExt::execute].
///
/// The type supplied arguments are checked against the query.
///
/// ```no_run
/// use qusql_mysql_type::execute;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let res = execute!(
///         conn,
///         "INSERT INTO `t1` (`cu32`, `ctext`) VALUES (?,?)",
///         42, "hello").await.unwrap();
///     println!("{}", res.last_insert_id());
/// }
/// ```
#[macro_export]
macro_rules! execute {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::execute_impl!(($executor), $stmt, $(($args),)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_one_impl;

/// Statically checked fetch_one with borrowed values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_one_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields are reference into the parsed mysql package.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_one;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let row = fetch_one!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a &str
/// }
/// ```
#[macro_export]
macro_rules! fetch_one {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)?) => {
        qusql_mysql_type::fetch_one_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_one_owned_impl;

/// Statically checked fetch_one with owned values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_one_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields as represented as [String] and [`Vec<u8>`].
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_one_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let row = fetch_one_owned!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a String
/// }
/// ```
#[macro_export]
macro_rules! fetch_one_owned {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_one_owned_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_one_as_impl;

/// Statically checked fetch_one with borrowed values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_one_map] into a give row type, with borrowed text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_one_as;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row<'a> {
///         cu32: u32,
///         ctext: &'a str
///     }
///     let row = fetch_one_as!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     println!("{} {}", row.cu32, row.ctext);
/// }
/// ```
#[macro_export]
macro_rules! fetch_one_as {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_one_as_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_one_as_owned_impl;

/// Statically checked fetch_one with owned values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_one_map] into a give row type, with owned text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_one_as_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row {
///         cu32: u32,
///         ctext: String
///     }
///     let row = fetch_one_as_owned!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     println!("{} {}", row.cu32, row.ctext);
/// }
/// ```
#[macro_export]
macro_rules! fetch_one_as_owned {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_one_as_owned_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_optional_impl;

/// Statically checked fetch_optional with borrowed values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_optional_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields are reference into the parsed mysql package.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_optional;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let row = fetch_optional!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     if let Some(row) = row {
///         println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a &str
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_optional {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)?) => {
        qusql_mysql_type::fetch_optional_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_optional_owned_impl;

/// Statically checked fetch_optional with owned values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_optional_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields as represented as [String] and [`Vec<u8>`].
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_optional_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let row = fetch_optional_owned!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     if let Some(row) = row {
///         println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a String
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_optional_owned {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_optional_owned_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_optional_as_impl;

/// Statically checked fetch_optional with borrowed values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_optional_map] into a give row type, with borrowed text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_optional_as;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row<'a> {
///         cu32: u32,
///         ctext: &'a str
///     }
///     let row = fetch_optional_as!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     if let Some(row) = row {
///         println!("{} {}", row.cu32, row.ctext);
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_optional_as {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_optional_as_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_optional_as_owned_impl;

/// Statically checked fetch_optional with owned values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_optional_map] into a give row type, with owned text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_optional_as_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row {
///         cu32: u32,
///         ctext: String
///     }
///     let row = fetch_optional_as_owned!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     if let Some(row) = row {
///         println!("{} {}", row.cu32, row.ctext);
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_optional_as_owned {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_optional_as_owned_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_all_impl;

/// Statically checked fetch_all with borrowed values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_all_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields are reference into the parsed mysql package.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_all;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let rows = fetch_all!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     for row in rows {
///         println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a &str
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_all {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_all_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_all_owned_impl;

/// Statically checked fetch_all with owned values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_all_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields as represented as [String] and [`Vec<u8>`].
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_all_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let rows = fetch_all_owned!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     for row in rows {
///         println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a String
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_all_owned {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_all_owned_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_all_as_impl;

/// Statically checked fetch_all with borrowed values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_all_map] into a give row type, with borrowed text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_all_as;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row<'a> {
///         cu32: u32,
///         ctext: &'a str
///     }
///     let rows = fetch_all_as!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     for row in rows {
///         println!("{} {}", row.r#cu32, row.r#ctext);
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_all_as {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_all_as_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_all_as_owned_impl;

/// Statically checked fetch_all with owned values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_all_map] into a give row type, with owned text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_all_as_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row {
///         cu32: u32,
///         ctext: String
///     }
///     let rows = fetch_all_as_owned!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     for row in rows {
///         println!("{} {}", row.cu32, row.ctext);
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_all_as_owned {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)?) => {
        qusql_mysql_type::fetch_all_as_owned_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_impl;

/// Statically checked streaming fetch with borrowed values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields are reference into the parsed mysql package.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let mut row_iter = fetch!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     while let Some(row) = row_iter.next().await.unwrap() {
///         println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a &str
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_owned_impl;

/// Statically checked streaming fetch with owned values
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_map], where a Row type is generated based the columns
/// returned by the query. Text and blob fields as represented as [String] and [`Vec<u8>`].
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     let mut row_iter  = fetch_owned!(
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     while let Some(row) = row_iter.next().await.unwrap() {
///         println!("{} {}", row.cu32, row.ctext); // Here row.ctext is a String
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_owned {
    ( $executor: expr, $stmt: literal $(, $args:expr )* $(,)?) => {
        qusql_mysql_type::fetch_owned_impl!(($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_as_impl;

/// Statically checked streaming fetch with borrowed values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_map] into a give row type, with borrowed text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_as;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row<'a> {
///         cu32: u32,
///         ctext: &'a str
///     }
///     let mut row_iter  = fetch_as!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     while let Some(row) = row_iter.next().await.unwrap() {
///         println!("{} {}", row.r#cu32, row.r#ctext);
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_as {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_as_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

#[doc(hidden)]
pub use qusql_mysql_type_macro::fetch_as_owned_impl;

/// Statically checked streaming fetch with owned values into custom type
///
/// This expands into a [qusql_mysql::ExecutorExt::fetch_map] into a give row type, with owned text and blob fields.
/// The type supplied arguments are checked against the query
///
/// ```no_run
/// use qusql_mysql_type::fetch_as_owned;
///
/// async fn run(conn: &mut qusql_mysql::Connection) {
///     struct Row {
///         cu32: u32,
///         ctext: String
///     }
///     let mut row_iter = fetch_as_owned!(
///         Row,
///         conn,
///         "SELECT `cu32`, `ctext` FROM `t1` WHERE `id` = ? AND `ctext` != ?",
///         42, "hello").await.unwrap();
///     while let Some(row) = row_iter.next().await.unwrap() {
///         println!("{} {}", row.cu32, row.ctext);
///     }
/// }
/// ```
#[macro_export]
macro_rules! fetch_as_owned {
    ( $rt: ty, $executor: expr, $stmt: literal $(, $args:expr )* $(,)? ) => {
        qusql_mysql_type::fetch_as_owned_impl!($rt, ($executor), $stmt, $($args,)*)
    };
}

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

/// If `ArgIn<T>` is implemented for `J`, it means that `J` can be used as for arguments of type `T`
pub trait ArgIn<T> {}

/// If `ArgOut<N, T>` is implemented for `J`, it means that `J` can be construct from column of type `T`
///
/// `N` is used used with a struct named after the column to make figuring out what column has the
/// wrong type feasible
pub trait ArgOut<N, T> {}

/// Implement [ArgIn] and [ArgOut] for the given types
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

        impl<N> ArgOut<N, $dst> for $t {}
        impl<N> ArgOut<N, Option<$dst>> for Option<$t> {}
        impl<N> ArgOut<N, $dst> for Option<$t> {}
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

#[cfg(feature = "chrono")]
arg_io!(Timestamp, chrono::NaiveDateTime);
#[cfg(feature = "chrono")]
arg_io!(DateTime, chrono::NaiveDateTime);
#[cfg(feature = "chrono")]
arg_io!(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>);
#[cfg(feature = "chrono")]
arg_io!(Timestamp, chrono::DateTime<chrono::Utc>);

#[doc(hidden)]
pub fn check_arg<T, T2: ArgIn<T>>(_: &T2) {}

#[doc(hidden)]
pub fn check_arg_list_hack<T, T2: ArgIn<T>>(_: &qusql_mysql::List<'_, T2>) {}

#[doc(hidden)]
pub fn check_arg_out<N, T, T2: ArgOut<N, T>>(_: &T2) {}

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
