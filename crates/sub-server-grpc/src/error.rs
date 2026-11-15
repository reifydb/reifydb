// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use reifydb_codec::error::DecodeError;
use reifydb_sub_server::{auth::AuthError, execute::ExecuteError};
use reifydb_value::error::Diagnostic;
use serde_json::to_string as to_json;
use tonic::{Code, Status, metadata::MetadataValue};

pub(crate) const SUBSCRIBE_INDEX_HEADER: &str = "x-subscribe-index";

pub(crate) fn with_subscribe_index(mut status: Status, index: usize) -> Status {
	status.metadata_mut().insert(SUBSCRIBE_INDEX_HEADER, MetadataValue::from(index as u64));
	status
}

pub(crate) fn diagnostic_status(code: Code, diagnostic_code: &str, message: String) -> Status {
	let diagnostic = Diagnostic {
		code: diagnostic_code.to_string(),
		message,
		..Default::default()
	};
	let json = to_json(&diagnostic).unwrap_or_else(|_| diagnostic.message.clone());
	Status::new(code, json)
}

pub enum SubServerGrpcError {
	InvalidParamEncoding(DecodeError),

	InvalidSubscribeOptions {
		index: usize,
		message: String,
	},

	Unauthenticated(AuthError),

	AuthUnavailable(AuthError),

	Execute(ExecuteError),
}

impl fmt::Display for SubServerGrpcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SubServerGrpcError::InvalidParamEncoding(e) => write!(f, "Invalid param encoding: {}", e),
			SubServerGrpcError::InvalidSubscribeOptions {
				message,
				..
			} => write!(f, "Invalid subscribe options: {}", message),
			SubServerGrpcError::Unauthenticated(e) => write!(f, "{}", e),
			SubServerGrpcError::AuthUnavailable(e) => write!(f, "{}", e),
			SubServerGrpcError::Execute(e) => write!(f, "{}", e),
		}
	}
}

impl fmt::Debug for SubServerGrpcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SubServerGrpcError::InvalidParamEncoding(e) => {
				f.debug_tuple("InvalidParamEncoding").field(e).finish()
			}
			SubServerGrpcError::InvalidSubscribeOptions {
				index,
				message,
			} => f.debug_struct("InvalidSubscribeOptions")
				.field("index", index)
				.field("message", message)
				.finish(),
			SubServerGrpcError::Unauthenticated(e) => f.debug_tuple("Unauthenticated").field(e).finish(),
			SubServerGrpcError::AuthUnavailable(e) => f.debug_tuple("AuthUnavailable").field(e).finish(),
			SubServerGrpcError::Execute(e) => f.debug_tuple("Execute").field(e).finish(),
		}
	}
}

impl error::Error for SubServerGrpcError {}

impl From<AuthError> for SubServerGrpcError {
	fn from(err: AuthError) -> Self {
		match err {
			AuthError::Internal => SubServerGrpcError::AuthUnavailable(err),
			_ => SubServerGrpcError::Unauthenticated(err),
		}
	}
}

impl From<ExecuteError> for SubServerGrpcError {
	fn from(err: ExecuteError) -> Self {
		SubServerGrpcError::Execute(err)
	}
}

impl From<DecodeError> for SubServerGrpcError {
	fn from(err: DecodeError) -> Self {
		SubServerGrpcError::InvalidParamEncoding(err)
	}
}

impl From<SubServerGrpcError> for Status {
	fn from(err: SubServerGrpcError) -> Self {
		match err {
			SubServerGrpcError::InvalidParamEncoding(_) => Status::invalid_argument(err.to_string()),
			SubServerGrpcError::InvalidSubscribeOptions {
				index,
				..
			} => with_subscribe_index(Status::invalid_argument(err.to_string()), index),
			SubServerGrpcError::Unauthenticated(_) => Status::unauthenticated(err.to_string()),
			SubServerGrpcError::AuthUnavailable(_) => Status::internal(err.to_string()),
			SubServerGrpcError::Execute(ref inner) => match inner {
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
			Status::from(SubServerGrpcError::from(AuthError::InvalidToken)).code(),
			Code::Unauthenticated,
			"a token that was never issued is the client's fault"
		);
		assert_eq!(
			Status::from(SubServerGrpcError::from(AuthError::Expired)).code(),
			Code::Unauthenticated,
			"an expired token is the client's fault"
		);
		assert_eq!(
			Status::from(SubServerGrpcError::from(AuthError::Internal)).code(),
			Code::Internal,
			"authentication that could not reach storage is the server's fault"
		);
	}

	#[test]
	fn a_bad_option_names_its_item_in_a_header() {
		// The index must survive as a header, not only inside the message text.
		let status = Status::from(SubServerGrpcError::InvalidSubscribeOptions {
			index: 1,
			message: "subscription 1: bad linger".to_string(),
		});

		assert_eq!(status.code(), Code::InvalidArgument);
		assert_eq!(status.metadata().get(SUBSCRIBE_INDEX_HEADER).map(|v| v.to_str().unwrap()), Some("1"));
		assert!(status.message().contains("subscription 1: bad linger"), "{}", status.message());
	}
}
