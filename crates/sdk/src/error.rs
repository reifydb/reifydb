// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use reifydb_core::internal;
use reifydb_value::error::Error;

#[derive(Debug)]
pub enum SdkError {
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

impl fmt::Display for SdkError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SdkError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
			SdkError::MissingConfiguration {
				operator,
				key,
			} => {
				write!(f, "{operator} requires '{key}' configuration")
			}
			SdkError::StateError(msg) => write!(f, "State error: {}", msg),
			SdkError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
			SdkError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
			SdkError::MemoryError(msg) => write!(f, "Memory error: {}", msg),
			SdkError::Timeout => write!(f, "Operation timeout"),
			SdkError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
			SdkError::Other(msg) => write!(f, "{}", msg),
		}
	}
}

impl error::Error for SdkError {}

impl From<SdkError> for Error {
	fn from(err: SdkError) -> Self {
		Error(Box::new(internal!(format!("{}", err))))
	}
}

impl From<Error> for SdkError {
	fn from(err: Error) -> Self {
		SdkError::Other(err.to_string())
	}
}

pub type Result<T, E = SdkError> = std::result::Result<T, E>;
