// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Error types for the operator SDK

use std::{error, fmt};

use reifydb_core::internal;
use reifydb_type::error::Error;

/// FFI operator error type
#[derive(Debug)]
pub enum FFIError {
	/// Configuration error
	Configuration(String),

	/// Required configuration key is missing
	MissingConfiguration {
		operator: &'static str,
		key: &'static str,
	},

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
			FFIError::MissingConfiguration {
				operator,
				key,
			} => {
				write!(f, "{operator} requires '{key}' configuration")
			}
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

impl error::Error for FFIError {}

/// Convert FFIError to Error
impl From<FFIError> for Error {
	fn from(err: FFIError) -> Self {
		Error(Box::new(internal!(format!("{}", err))))
	}
}

/// Convert Error to FFIError
impl From<Error> for FFIError {
	fn from(err: Error) -> Self {
		FFIError::Other(err.to_string())
	}
}

/// Result type alias for FFI operations
pub type Result<T, E = FFIError> = std::result::Result<T, E>;
