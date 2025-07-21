// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{Display, Formatter};
use serde::{de, ser};

pub mod diagnostic;
mod r#macro;

#[derive(Debug, PartialEq)]
pub struct Error(pub diagnostic::Diagnostic);

use diagnostic::{DefaultRenderer, Diagnostic};

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

// Helper functions to create generic errors from strings
impl From<String> for Error {
    fn from(message: String) -> Self {
        Error(Diagnostic {
            code: "GENERIC_001".to_string(),
            statement: None,
            message,
            column: None,
            span: None,
            label: None,
            help: None,
            notes: vec![],
            cause: None,
        })
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Error::from(message.to_string())
    }
}

// Serde integration - implement serde::de::Error trait
impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error(Diagnostic {
            code: "SERDE_001".to_string(),
            statement: None,
            message: format!("Serde deserialization error: {}", msg),
            column: None,
            span: None,
            label: None,
            help: Some("Check data format and structure".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}

// Serde integration - implement serde::ser::Error trait
impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error(Diagnostic {
            code: "SERDE_003".to_string(),
            statement: None,
            message: format!("Serde serialization error: {}", msg),
            column: None,
            span: None,
            label: None,
            help: Some("Check data format and structure".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}

// Conversions from common serde-related errors
impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error(Diagnostic {
            code: "SERDE_002".to_string(),
            statement: None,
            message: format!("Bincode encode error: {}", err),
            column: None,
            span: None,
            label: None,
            help: Some("Check binary data format".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error(Diagnostic {
            code: "SERDE_004".to_string(),
            statement: None,
            message: format!("Bincode decode error: {}", err),
            column: None,
            span: None,
            label: None,
            help: Some("Check binary data format".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}

// Additional standard library error conversions for keycode serialization
impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error(Diagnostic {
            code: "CONV_001".to_string(),
            statement: None,
            message: format!("Array conversion error: {}", err),
            column: None,
            span: None,
            label: None,
            help: Some("Check array size requirements".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error(Diagnostic {
            code: "CONV_002".to_string(),
            statement: None,
            message: format!("UTF-8 conversion error: {}", err),
            column: None,
            span: None,
            label: None,
            help: Some("Check string encoding".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error(Diagnostic {
            code: "CONV_003".to_string(),
            statement: None,
            message: format!("Integer conversion error: {}", err),
            column: None,
            span: None,
            label: None,
            help: Some("Check integer range limits".to_string()),
            notes: vec![],
            cause: None,
        })
    }
}