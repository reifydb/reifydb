//! Error types for FFI operations

use std::fmt;

/// FFI operation error
#[derive(Debug)]
pub enum FFIError {
	/// Invalid input parameters
	InvalidInput(String),
	/// State operation error
	StateError(String),
	/// Memory allocation error
	MemoryError(String),
	/// Operation timeout
	Timeout,
	/// Operation not supported
	NotSupported,
	/// Generic error
	Other(String),
}

impl fmt::Display for FFIError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			FFIError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
			FFIError::StateError(msg) => write!(f, "State error: {}", msg),
			FFIError::MemoryError(msg) => write!(f, "Memory error: {}", msg),
			FFIError::Timeout => write!(f, "Operation timeout"),
			FFIError::NotSupported => write!(f, "Operation not supported"),
			FFIError::Other(msg) => write!(f, "FFI error: {}", msg),
		}
	}
}

impl std::error::Error for FFIError {}

/// Convert FFIError to reifydb Error
impl From<FFIError> for reifydb_core::Error {
	fn from(err: FFIError) -> Self {
		use reifydb_type::{Error, internal};
		Error(internal!(format!("{}", err)))
	}
}

/// FFI operation result
pub type FFIResult<T> = Result<T, FFIError>;
