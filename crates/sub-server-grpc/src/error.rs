// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use reifydb_codec::error::DecodeError;
use reifydb_sub_server::{auth::AuthError, execute::ExecuteError, subscription::errors::CreateSubscriptionError};
use reifydb_value::error::Diagnostic;
use serde_json::to_string as to_json;
use tonic::{Code, Status};

pub(crate) fn diagnostic_status(code: Code, diagnostic_code: &str, message: String) -> Status {
	let diagnostic = Diagnostic {
		code: diagnostic_code.to_string(),
		message,
		..Default::default()
	};
	let json = to_json(&diagnostic).unwrap_or_else(|_| diagnostic.message.clone());
	Status::new(code, json)
}

pub enum GrpcError {
	InvalidParamEncoding(DecodeError),

	Unauthenticated(AuthError),

	AuthUnavailable(AuthError),

	Execute(ExecuteError),

	SubscriptionFailed(String),
}

impl fmt::Display for GrpcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			GrpcError::InvalidParamEncoding(e) => write!(f, "Invalid param encoding: {}", e),
			GrpcError::Unauthenticated(e) => write!(f, "{}", e),
			GrpcError::AuthUnavailable(e) => write!(f, "{}", e),
			GrpcError::Execute(e) => write!(f, "{}", e),
			GrpcError::SubscriptionFailed(msg) => write!(f, "Subscription failed: {}", msg),
		}
	}
}

impl fmt::Debug for GrpcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			GrpcError::InvalidParamEncoding(e) => f.debug_tuple("InvalidParamEncoding").field(e).finish(),
			GrpcError::Unauthenticated(e) => f.debug_tuple("Unauthenticated").field(e).finish(),
			GrpcError::AuthUnavailable(e) => f.debug_tuple("AuthUnavailable").field(e).finish(),
			GrpcError::Execute(e) => f.debug_tuple("Execute").field(e).finish(),
			GrpcError::SubscriptionFailed(msg) => f.debug_tuple("SubscriptionFailed").field(msg).finish(),
		}
	}
}

impl error::Error for GrpcError {}

impl From<AuthError> for GrpcError {
	fn from(err: AuthError) -> Self {
		match err {
			AuthError::Internal => GrpcError::AuthUnavailable(err),
			_ => GrpcError::Unauthenticated(err),
		}
	}
}

impl From<ExecuteError> for GrpcError {
	fn from(err: ExecuteError) -> Self {
		GrpcError::Execute(err)
	}
}

impl From<DecodeError> for GrpcError {
	fn from(err: DecodeError) -> Self {
		GrpcError::InvalidParamEncoding(err)
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
			GrpcError::InvalidParamEncoding(_) => Status::invalid_argument(err.to_string()),
			GrpcError::Unauthenticated(_) => Status::unauthenticated(err.to_string()),
			GrpcError::AuthUnavailable(_) => Status::internal(err.to_string()),
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn storage_failure_answers_internal_while_a_bad_token_answers_unauthenticated() {
		// Unauthenticated tells the client to refresh its token, hiding a fault only the server can fix.
		assert_eq!(
			Status::from(GrpcError::from(AuthError::InvalidToken)).code(),
			Code::Unauthenticated,
			"a token that was never issued is the client's fault"
		);
		assert_eq!(
			Status::from(GrpcError::from(AuthError::Expired)).code(),
			Code::Unauthenticated,
			"an expired token is the client's fault"
		);
		assert_eq!(
			Status::from(GrpcError::from(AuthError::Internal)).code(),
			Code::Internal,
			"authentication that could not reach storage is the server's fault"
		);
	}
}
