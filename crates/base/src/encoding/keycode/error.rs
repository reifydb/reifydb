// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::array::TryFromSliceError;
use std::fmt::{Display, Formatter};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

#[derive(Debug)]
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

impl std::error::Error for Error {}
