//! Error used within the tests, loosely based on anyhow
use std::{borrow::Cow, num::TryFromIntError, str::Utf8Error};

#[derive(Debug)]
enum InnerError {
    Bail(Cow<'static, str>),
    Io(std::io::Error),
    IsNone,
    TryFromInt(TryFromIntError),
    Utf8(Utf8Error),
}

impl From<std::io::Error> for InnerError {
    fn from(value: std::io::Error) -> Self {
        InnerError::Io(value)
    }
}

impl From<TryFromIntError> for InnerError {
    fn from(value: TryFromIntError) -> Self {
        InnerError::TryFromInt(value)
    }
}

impl From<Utf8Error> for InnerError {
    fn from(value: Utf8Error) -> Self {
        InnerError::Utf8(value)
    }
}

/// Errors encountered during testing
#[derive(Debug)]
pub struct Error {
    inner: InnerError,
    context: Vec<Cow<'static, str>>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            InnerError::Bail(e) => writeln!(f, "Bail error: {e}")?,
            InnerError::Io(e) => writeln!(f, "Io error: {e}")?,
            InnerError::IsNone => writeln!(f, "Unexpected none")?,
            InnerError::TryFromInt(e) => writeln!(f, "FromInt error: {e}")?,
            InnerError::Utf8(e) => writeln!(f, "Bail error: {e}")?,
        }
        if !self.context.is_empty() {
            writeln!(f, "Context:")?;
            for context in &self.context {
                writeln!(f, "- {context}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

impl Error {
    /// A textual bail error
    pub fn bail(message: impl Into<Cow<'static, str>>) -> Error {
        Error {
            inner: InnerError::Bail(message.into()),
            context: vec![],
        }
    }
}

impl<T: Into<InnerError>> From<T> for Error {
    fn from(value: T) -> Self {
        Error {
            inner: value.into(),
            context: vec![],
        }
    }
}

pub trait Context<T> {
    /// Add context to an error messages
    fn context(self, message: impl Into<Cow<'static, str>>) -> Result<T>;
}

impl<T> Context<T> for Option<T> {
    fn context(self, message: impl Into<Cow<'static, str>>) -> Result<T> {
        match self {
            Some(v) => Ok(v),
            None => Err(Error {
                inner: InnerError::IsNone,
                context: vec![message.into()],
            }),
        }
    }
}

impl<T, E: Into<InnerError>> Context<T> for std::result::Result<T, E> {
    fn context(self, message: impl Into<Cow<'static, str>>) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error {
                inner: e.into(),
                context: vec![message.into()],
            }),
        }
    }
}

impl<T> Context<T> for std::result::Result<T, Error> {
    fn context(self, message: impl Into<Cow<'static, str>>) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(mut e) => {
                e.context.push(message.into());
                Err(e)
            }
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
