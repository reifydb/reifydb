//! Error types for the operator SDK

use std::fmt;

/// Operator SDK error type
#[derive(Debug)]
pub enum Error {
    /// Configuration error
    Configuration(String),

    /// State operation error
    State(String),

    /// Serialization error
    Serialization(String),

    /// JSON error
    Json(serde_json::Error),

    /// FFI error
    FFI(String),

    /// Invalid input
    InvalidInput(String),

    /// Not implemented
    NotImplemented(String),

    /// Other error
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            Error::State(msg) => write!(f, "State error: {}", msg),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::Json(err) => write!(f, "JSON error: {}", err),
            Error::FFI(msg) => write!(f, "FFI error: {}", msg),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Error::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
            Error::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Json(err) => Some(err),
            _ => None,
        }
    }
}

/// Result type alias
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::Serialization(format!("Encode error: {}", err))
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::Serialization(format!("Decode error: {}", err))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(err)
    }
}

impl From<reifydb_core::Error> for Error {
    fn from(err: reifydb_core::Error) -> Self {
        Error::Other(err.to_string())
    }
}