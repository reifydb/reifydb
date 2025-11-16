//! Error types for the operator SDK

use std::fmt;

/// FFI operator error type
#[derive(Debug)]
pub enum FFIError {
	/// Configuration error
	Configuration(String),

	/// State operation error
	StateError(String),

	/// Serialization error
	Serialization(String),

	/// Invalid input parameters
	InvalidInput(String),

	/// Memory allocation error
	MemoryError(String),

	/// Operation timeout
	Timeout,

	/// Operation not implemented or not supported
	NotImplemented(String),

	/// Generic error
	Other(String),
}

impl fmt::Display for FFIError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			FFIError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
			FFIError::StateError(msg) => write!(f, "State error: {}", msg),
			FFIError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
			FFIError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
			FFIError::MemoryError(msg) => write!(f, "Memory error: {}", msg),
			FFIError::Timeout => write!(f, "Operation timeout"),
			FFIError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
			FFIError::Other(msg) => write!(f, "{}", msg),
		}
	}
}

impl std::error::Error for FFIError {}

/// Convert FFIError to reifydb_core::Error
impl From<FFIError> for reifydb_core::Error {
	fn from(err: FFIError) -> Self {
		use reifydb_type::{Error, internal};
		Error(internal!(format!("{}", err)))
	}
}

/// Convert reifydb_core::Error to FFIError
impl From<reifydb_core::Error> for FFIError {
	fn from(err: reifydb_core::Error) -> Self {
		FFIError::Other(err.to_string())
	}
}

/// Result type alias for FFI operations
pub type Result<T, E = FFIError> = std::result::Result<T, E>;
