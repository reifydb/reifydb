// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error, fmt, string::FromUtf8Error};

use reifydb_sub_server::{auth::AuthError, execute::ExecuteError, subscribe::CreateSubscriptionError};
use reifydb_type::value::r#type::Type;
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
	/// Query/command execution error (timeout, cancelled, engine error, rejected, etc.).
	Execute(ExecuteError),
	/// Subscription creation failed.
	SubscriptionFailed(String),
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
			GrpcError::Execute(e) => write!(f, "{}", e),
			GrpcError::SubscriptionFailed(msg) => write!(f, "Subscription failed: {}", msg),
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
			GrpcError::Execute(e) => f.debug_tuple("Execute").field(e).finish(),
			GrpcError::SubscriptionFailed(msg) => f.debug_tuple("SubscriptionFailed").field(msg).finish(),
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
		GrpcError::Execute(err)
	}
}

impl From<CreateSubscriptionError> for GrpcError {
	fn from(err: CreateSubscriptionError) -> Self {
		match err {
			CreateSubscriptionError::Execute(e) => GrpcError::Execute(e),
			CreateSubscriptionError::ExtractionFailed => {
				GrpcError::SubscriptionFailed("Failed to extract subscription ID".to_string())
			}
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
			GrpcError::Execute(ref inner) => match inner {
				ExecuteError::Timeout => Status::deadline_exceeded(err.to_string()),
				ExecuteError::Cancelled => Status::cancelled(err.to_string()),
				ExecuteError::Disconnected => Status::internal(err.to_string()),
				ExecuteError::Engine {
					diagnostic,
					rql,
				} => {
					let mut diag = (**diagnostic).clone();
					if diag.rql.is_none() && !rql.is_empty() {
						diag.with_rql(rql.clone());
					}
					let json = to_json(&diag).unwrap_or_else(|_| diagnostic.message.clone());
					Status::invalid_argument(json)
				}
				ExecuteError::Rejected {
					..
				} => Status::permission_denied(err.to_string()),
			},
			GrpcError::SubscriptionFailed(_) => Status::internal(err.to_string()),
		}
	}
}
