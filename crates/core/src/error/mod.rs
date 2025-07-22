// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{de, ser};
use std::fmt::{Display, Formatter};

pub mod diagnostic;
mod r#macro;

#[derive(Debug, PartialEq)]
pub struct Error(pub diagnostic::Diagnostic);

use diagnostic::{Diagnostic, render::DefaultRenderer};

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let out = DefaultRenderer::render_string(&self.0);
        f.write_str(out.as_str())
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        self.0
    }
}

impl std::error::Error for Error {}

// Serde integration - implement serde::de::Error trait
impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        crate::error!(diagnostic::serialization::serde_deserialize_error(msg.to_string()))
    }
}

// Serde integration - implement serde::ser::Error trait
impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        crate::error!(diagnostic::serialization::serde_serialize_error(msg.to_string()))
    }
}

// Conversion from TryFromIntError for keycode serialization
impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        crate::error!(diagnostic::conversion::integer_conversion_error(err))
    }
}

// Conversion from TryFromSliceError for keycode serialization
impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        crate::error!(diagnostic::conversion::array_conversion_error(err))
    }
}

// Conversion from FromUtf8Error for keycode serialization
impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        crate::error!(diagnostic::conversion::utf8_conversion_error(err))
    }
}

// Conversion from bincode errors for bincode serialization
impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        crate::error!(diagnostic::serialization::bincode_encode_error(err))
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        crate::error!(diagnostic::serialization::bincode_decode_error(err))
    }
}
