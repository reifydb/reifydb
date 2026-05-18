// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error, fmt};

use reifydb_core::internal;
use reifydb_type::error::Error;

#[derive(Debug)]
pub enum FFIError {
	Configuration(String),

	MissingConfiguration {
		operator: &'static str,
		key: &'static str,
	},

	StateError(String),

	Serialization(String),

	InvalidInput(String),

	MemoryError(String),

	Timeout,

	NotImplemented(String),

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

impl From<FFIError> for Error {
	fn from(err: FFIError) -> Self {
		Error(Box::new(internal!(format!("{}", err))))
	}
}

impl From<Error> for FFIError {
	fn from(err: Error) -> Self {
		FFIError::Other(err.to_string())
	}
}

pub type Result<T, E = FFIError> = std::result::Result<T, E>;
