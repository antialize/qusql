//! Restrict Set Abstraction for Context-Sensitive Keyword Restriction
//!
//! This module provides the `Restrict` type, a lightweight abstraction for efficiently
//! representing a set of restricted SQL keywords in context-sensitive parsing situations.
//!
//! The restrict set is used to prevent certain keywords from being parsed as identifiers
//! in specific parser contexts (e.g., after certain clause boundaries or as option delimiters).
//!

use alloc::vec::Vec;
use crate::keywords::Keyword;


/// Internal representation for the restrict set.
/// Optimized for 0, 1, or 2 slices, but can grow to more.
#[derive(Default, Debug, Clone)]
enum RestrictInner {
    /// No restrictions (empty set)
    #[default]
    Empty,
    /// One slice of restricted keywords
    One(&'static [Keyword]),
    /// Two slices of restricted keywords
    Two(&'static [Keyword], &'static [Keyword]),
    /// More than two slices (rare)
    More(Vec<&'static [Keyword]>),
}


/// A context-sensitive set of restricted keywords for parsing.
///
/// Use this to efficiently check if a keyword is restricted in the current context,
/// and to join additional restrictions as you descend into sub-parsers.
#[derive(Debug, Clone, Default)]
pub (crate) struct Restrict(RestrictInner);


impl Restrict {
    /// Create a new, empty restrict set (no restrictions).
    pub (crate) fn empty() -> Self {
        Restrict(RestrictInner::Empty)
    }

    /// Create a new set of restricted keywords from a single slice.
    pub (crate) fn new(keywords: &'static [Keyword]) -> Self {
        Restrict(RestrictInner::One(keywords))
    }

    /// Check if the given keyword is restricted in this set.
    pub (crate) fn contains(&self, kw: Keyword) -> bool {
        // Only restrictable keywords can ever be in the restrict set
        if kw >= Keyword::NOT_A_KEYWORD {
            return false;
        }
        match &self.0 {
            RestrictInner::Empty => false,
            RestrictInner::One(slice) => slice.contains(&kw),
            RestrictInner::Two(slice1, slice2) => slice1.contains(&kw) || slice2.contains(&kw),
            RestrictInner::More(vec) => vec.iter().any(|slice| slice.contains(&kw)),
        }
    }

    /// Join (union) this restrict set with another slice of restricted keywords.
    /// Returns a new Restrict set containing all restrictions from both.
    pub (crate) fn join(&self, other: &'static [Keyword]) -> Self {
        match &self.0 {
            RestrictInner::Empty => Restrict(RestrictInner::One(other)),
            RestrictInner::One(slice) => Restrict(RestrictInner::Two(slice, other)),
            RestrictInner::Two(slice1, slice2) => {
                let mut vec = Vec::new();
                vec.push(*slice1);
                vec.push(*slice2);
                vec.push(other);
                Restrict(RestrictInner::More(vec))
            }
            RestrictInner::More(vec) => {
                let mut new_vec = vec.clone();
                new_vec.push(other);
                Restrict(RestrictInner::More(new_vec))
            }
        }
    }
}
