//! Contains Structs to contains and parse rows

use crate::bind::ListBind;
use crate::connection::{ConnectionResult, Query};

/// Decode a row as a tuple
pub trait Args: Sized {
    /// Decode the row as Self
    fn bind_args<'a>(&self, query: Query<'a>) -> ConnectionResult<Query<'a>>;

    /// For every list argument in order, return the number of items in the list
    fn list_lengths(&self, out: &mut Vec<usize>);
}

/// Implement [Args] for a tuple
macro_rules! impl_args_for_tuple {
    ($($idx:tt $T:ident),+) => {
        impl<$($T,)+> Args for ($($T,)+)
        where
            $($T: ListBind,)+
        {
            #[inline]
            fn bind_args<'a>(&self, query: Query<'a>) -> ConnectionResult<Query<'a>> {
                $(
                    let query = if let Some(cnt) = self.$idx.list_length() {
                        let mut query = query;
                        for idx in 0..cnt {
                            query = query.bind::<$T::T>(&self.$idx.get(idx))?;
                        }
                        query
                    } else {
                        query.bind::<$T::T>(&self.$idx.single())?
                    }
                ;)+
                Ok(query)
            }

            #[inline]
            fn list_lengths(&self, out: &mut Vec<usize>) {
                $(if let Some(v) = self.$idx.list_length() {
                    out.push(v);
                })+
            }
        }
    };
}

impl Args for () {
    fn bind_args<'a>(&self, query: Query<'a>) -> ConnectionResult<Query<'a>> {
        Ok(query)
    }

    fn list_lengths(&self, _: &mut Vec<usize>) {}
}

impl_args_for_tuple!(0 T1);
impl_args_for_tuple!(0 T1, 1 T2);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13);
impl_args_for_tuple!(0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19, 19 T20
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19, 19 T20, 20 T21
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19, 19 T20, 20 T21,
    21 T22
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19, 19 T20, 20 T21,
    21 T22, 22 T23
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19, 19 T20, 20 T21,
    21 T22, 22 T23, 23 T24
);
impl_args_for_tuple!(
    0 T1, 1 T2, 2 T3, 3 T4, 4 T5, 5 T6, 6 T7, 7 T8, 8 T9, 9 T10, 10 T11, 11 T12, 12 T13, 13 T14, 14 T15, 15 T16, 16 T17, 17 T18, 18 T19, 19 T20, 20 T21,
    21 T22, 22 T23, 23 T24, 24 T25
);
