// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use reifydb_core::internal;
use reifydb_value::{
	error::{ColumnReadReason, Error},
	value::value_type::ValueType,
};

#[derive(Debug)]
pub enum SdkError {
	Configuration(String),

	MissingConfiguration {
		operator: &'static str,
		key: &'static str,
	},

	OperatorError(String),

	Serialization(String),

	InvalidInput(String),

	ColumnRead {
		column: String,
		column_type: ValueType,
		target: &'static str,
		reason: ColumnReadReason,
	},

	MemoryError(String),

	Timeout,

	NotImplemented(String),

	Other(String),

	Engine(Error),
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
			SdkError::OperatorError(msg) => write!(f, "State error: {}", msg),
			SdkError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
			SdkError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
			SdkError::ColumnRead {
				column,
				column_type,
				target,
				reason,
			} => write!(f, "cannot read column {column} of {column_type} as {target}: {reason}"),
			SdkError::MemoryError(msg) => write!(f, "Memory error: {}", msg),
			SdkError::Timeout => write!(f, "Operation timeout"),
			SdkError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
			SdkError::Other(msg) => write!(f, "{}", msg),
			SdkError::Engine(err) => write!(f, "{}", err),
		}
	}
}

impl error::Error for SdkError {}

impl From<SdkError> for Error {
	fn from(err: SdkError) -> Self {
		match err {
			SdkError::Engine(err) => err,
			other => Error(Box::new(internal!(format!("{}", other)))),
		}
	}
}

impl From<Error> for SdkError {
	fn from(err: Error) -> Self {
		SdkError::Engine(err)
	}
}

pub type Result<T, E = SdkError> = std::result::Result<T, E>;
