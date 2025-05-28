// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use bincode::error::{DecodeError, EncodeError};
use std::array::TryFromSliceError;
use std::fmt::{Display, Formatter};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// Represents errors that may occur during encoding or decoding operations
/// within key-value store or network protocol contexts.
///
/// This enum captures encoding-related failures such as malformed input,
/// unsupported data formats, or internal encoding bugs. It is designed to provide
/// precise error feedback for low-level serialization and deserialization logic.
#[derive(Debug, PartialEq)]
pub struct Error(pub String);

#[macro_export]
macro_rules! invalid_data {
    ($($args:tt)*) => { Err(Error(format!($($args)*)).into()) };
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {}

impl From<EncodeError> for Error {
    fn from(value: EncodeError) -> Self {
        Self(value.to_string())
    }
}

impl From<DecodeError> for Error {
    fn from(value: DecodeError) -> Self {
        Self(value.to_string())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Self {
        Self(err.to_string())
    }
}

impl From<TryFromSliceError> for Error {
    fn from(err: TryFromSliceError) -> Self {
        Self(err.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Self(err.to_string())
    }
}
