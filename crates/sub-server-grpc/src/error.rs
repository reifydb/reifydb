// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error, fmt, string::FromUtf8Error, sync::Arc};

use reifydb_sub_server::{auth::AuthError, execute::ExecuteError};
use reifydb_type::{error::Diagnostic, value::r#type::Type};
use serde_json::to_string as to_json;
use tonic::Status;

pub enum GrpcError {
	/// Parameter value has wrong byte length for its declared type.
	InvalidByteLength {
		r#type: Type,
		expected: usize,
		actual: usize,
	},
	/// Parameter value contains invalid UTF-8.
	InvalidUtf8(FromUtf8Error),
	/// Date value is out of range.
	InvalidDate {
		days: i32,
	},
	/// DateTime value is out of range.
	InvalidDateTime(String),
	/// Time value is out of range.
	InvalidTime {
		nanos: u64,
	},
	/// Decimal string could not be parsed.
	InvalidDecimal(String),
	/// The parameter type is not supported over gRPC.
	UnsupportedParamType(Type),
	/// Authentication failed.
	Unauthenticated(AuthError),
	/// Query execution timed out.
	Timeout,
	/// Query was cancelled.
	Cancelled,
	/// Query stream disconnected.
	Disconnected,
	/// Database engine returned an error.
	Engine {
		diagnostic: Arc<Diagnostic>,
		statement: String,
	},
}

impl fmt::Display for GrpcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			GrpcError::InvalidByteLength {
				r#type,
				expected,
				actual,
			} => write!(f, "{:?} requires {} bytes, got {}", r#type, expected, actual),
			GrpcError::InvalidUtf8(e) => write!(f, "Invalid UTF-8: {}", e),
			GrpcError::InvalidDate {
				days,
			} => write!(f, "Invalid date days: {}", days),
			GrpcError::InvalidDateTime(msg) => write!(f, "Invalid datetime: {}", msg),
			GrpcError::InvalidTime {
				nanos,
			} => write!(f, "Invalid time nanos: {}", nanos),
			GrpcError::InvalidDecimal(msg) => write!(f, "Invalid decimal: {}", msg),
			GrpcError::UnsupportedParamType(ty) => write!(f, "Unsupported param type: {:?}", ty),
			GrpcError::Unauthenticated(e) => write!(f, "{}", e),
			GrpcError::Timeout => write!(f, "Query execution timed out"),
			GrpcError::Cancelled => write!(f, "Query was cancelled"),
			GrpcError::Disconnected => write!(f, "Query stream disconnected"),
			GrpcError::Engine {
				diagnostic,
				..
			} => write!(f, "Engine error: {}", diagnostic.message),
		}
	}
}

impl fmt::Debug for GrpcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			GrpcError::InvalidByteLength {
				r#type,
				expected,
				actual,
			} => f.debug_struct("InvalidByteLength")
				.field("type", r#type)
				.field("expected", expected)
				.field("actual", actual)
				.finish(),
			GrpcError::InvalidUtf8(e) => f.debug_tuple("InvalidUtf8").field(e).finish(),
			GrpcError::InvalidDate {
				days,
			} => f.debug_struct("InvalidDate").field("days", days).finish(),
			GrpcError::InvalidDateTime(msg) => f.debug_tuple("InvalidDateTime").field(msg).finish(),
			GrpcError::InvalidTime {
				nanos,
			} => f.debug_struct("InvalidTime").field("nanos", nanos).finish(),
			GrpcError::InvalidDecimal(msg) => f.debug_tuple("InvalidDecimal").field(msg).finish(),
			GrpcError::UnsupportedParamType(ty) => f.debug_tuple("UnsupportedParamType").field(ty).finish(),
			GrpcError::Unauthenticated(e) => f.debug_tuple("Unauthenticated").field(e).finish(),
			GrpcError::Timeout => write!(f, "Timeout"),
			GrpcError::Cancelled => write!(f, "Cancelled"),
			GrpcError::Disconnected => write!(f, "Disconnected"),
			GrpcError::Engine {
				diagnostic,
				statement,
			} => f.debug_struct("Engine")
				.field("diagnostic", diagnostic)
				.field("statement", statement)
				.finish(),
		}
	}
}

impl error::Error for GrpcError {}

impl From<AuthError> for GrpcError {
	fn from(err: AuthError) -> Self {
		GrpcError::Unauthenticated(err)
	}
}

impl From<ExecuteError> for GrpcError {
	fn from(err: ExecuteError) -> Self {
		match err {
			ExecuteError::Timeout => GrpcError::Timeout,
			ExecuteError::Cancelled => GrpcError::Cancelled,
			ExecuteError::Disconnected => GrpcError::Disconnected,
			ExecuteError::Engine {
				diagnostic,
				statement,
			} => GrpcError::Engine {
				diagnostic,
				statement,
			},
		}
	}
}

impl From<GrpcError> for Status {
	fn from(err: GrpcError) -> Self {
		match err {
			GrpcError::InvalidByteLength {
				..
			}
			| GrpcError::InvalidUtf8(_)
			| GrpcError::InvalidDate {
				..
			}
			| GrpcError::InvalidDateTime(_)
			| GrpcError::InvalidTime {
				..
			}
			| GrpcError::InvalidDecimal(_)
			| GrpcError::UnsupportedParamType(_) => Status::invalid_argument(err.to_string()),
			GrpcError::Unauthenticated(_) => Status::unauthenticated(err.to_string()),
			GrpcError::Timeout => Status::deadline_exceeded(err.to_string()),
			GrpcError::Cancelled => Status::cancelled(err.to_string()),
			GrpcError::Disconnected => Status::internal(err.to_string()),
			GrpcError::Engine {
				diagnostic,
				statement,
			} => {
				let mut diag = (*diagnostic).clone();
				if diag.statement.is_none() && !statement.is_empty() {
					diag.with_statement(statement);
				}
				let json = to_json(&diag).unwrap_or_else(|_| diagnostic.message.clone());
				Status::invalid_argument(json)
			}
		}
	}
}
