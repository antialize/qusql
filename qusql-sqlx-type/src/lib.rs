use sqlx_type_macro;
//use sqlx::{mysql::MySqlConnectOptions, MySqlPool};

pub use crate::sqlx_type_macro::{query, query_as};
pub struct Integer;
pub struct Float;

pub struct Timestamp;
pub struct DateTime;
pub struct Date;
pub struct Time;

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

pub fn check_arg<T, T2: ArgIn<T>>(_: &T2) {}
