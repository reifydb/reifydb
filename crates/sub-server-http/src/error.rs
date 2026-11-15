// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error, fmt};

use axum::{
	Json,
	http::StatusCode,
	response::{IntoResponse, Response},
};
use reifydb_sub_server::{auth::AuthError, execute::ExecuteError};
use reifydb_value::error::Diagnostic;
use serde::Serialize;
use tracing::{debug, error};

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
	pub error: String,

	pub code: String,
}

impl ErrorResponse {
	pub fn new(code: impl Into<String>, error: impl Into<String>) -> Self {
		Self {
			code: code.into(),
			error: error.into(),
		}
	}
}

#[derive(Debug, Serialize)]
pub struct DiagnosticResponse {
	pub diagnostic: Diagnostic,
}

#[derive(Debug)]
pub enum SubServerHttpError {
	Auth(AuthError),

	Execute(ExecuteError),

	BadRequest(String),

	InvalidParams(String),

	NotFound(String),

	MethodNotAllowed(String),

	Internal(String),
}

impl From<AuthError> for SubServerHttpError {
	fn from(e: AuthError) -> Self {
		SubServerHttpError::Auth(e)
	}
}

impl From<ExecuteError> for SubServerHttpError {
	fn from(e: ExecuteError) -> Self {
		SubServerHttpError::Execute(e)
	}
}

impl fmt::Display for SubServerHttpError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SubServerHttpError::Auth(e) => write!(f, "Authentication error: {}", e),
			SubServerHttpError::Execute(e) => write!(f, "Execution error: {}", e),
			SubServerHttpError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
			SubServerHttpError::InvalidParams(msg) => write!(f, "Invalid params: {}", msg),
			SubServerHttpError::NotFound(msg) => write!(f, "Not found: {}", msg),
			SubServerHttpError::MethodNotAllowed(msg) => write!(f, "Method not allowed: {}", msg),
			SubServerHttpError::Internal(msg) => write!(f, "Internal error: {}", msg),
		}
	}
}

impl error::Error for SubServerHttpError {}

impl IntoResponse for SubServerHttpError {
	fn into_response(self) -> Response {
		if let SubServerHttpError::Execute(ExecuteError::Engine {
			diagnostic,
			rql,
		}) = self
		{
			debug!("Engine error: {}", diagnostic.message);
			let mut diag = (*diagnostic).clone();
			if diag.rql.is_none() && !rql.is_empty() {
				diag.with_rql(rql);
			}
			let status = if diag.code.starts_with("POLICY_") {
				StatusCode::FORBIDDEN
			} else {
				StatusCode::BAD_REQUEST
			};
			let body = Json(DiagnosticResponse {
				diagnostic: diag,
			});
			return (status, body).into_response();
		}

		let (status, code, message) = match &self {
			SubServerHttpError::Auth(AuthError::MissingCredentials) => {
				(StatusCode::UNAUTHORIZED, "AUTH_REQUIRED", "Authentication required")
			}
			SubServerHttpError::Auth(AuthError::InvalidToken) => {
				(StatusCode::UNAUTHORIZED, "INVALID_TOKEN", "Invalid authentication token")
			}
			SubServerHttpError::Auth(AuthError::Expired) => {
				(StatusCode::UNAUTHORIZED, "TOKEN_EXPIRED", "Authentication token expired")
			}
			SubServerHttpError::Auth(AuthError::InvalidHeader) => {
				(StatusCode::BAD_REQUEST, "INVALID_HEADER", "Malformed authorization header")
			}
			SubServerHttpError::Auth(AuthError::InsufficientPermissions) => {
				(StatusCode::FORBIDDEN, "FORBIDDEN", "Insufficient permissions for this operation")
			}
			SubServerHttpError::Auth(AuthError::Internal) => {
				(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error")
			}
			SubServerHttpError::Execute(ExecuteError::Timeout) => {
				(StatusCode::GATEWAY_TIMEOUT, "QUERY_TIMEOUT", "Query execution timed out")
			}
			SubServerHttpError::Execute(ExecuteError::Cancelled) => {
				(StatusCode::BAD_REQUEST, "QUERY_CANCELLED", "Query was cancelled")
			}
			SubServerHttpError::Execute(ExecuteError::Disconnected) => {
				error!("Query stream disconnected unexpectedly");
				(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error")
			}
			SubServerHttpError::Execute(ExecuteError::Rejected {
				code,
				message,
			}) => {
				let body = Json(ErrorResponse::new(code, message));
				return (StatusCode::FORBIDDEN, body).into_response();
			}
			SubServerHttpError::Execute(ExecuteError::Engine {
				..
			}) => {
				unreachable!()
			}
			SubServerHttpError::BadRequest(msg) => {
				let body = Json(ErrorResponse::new("BAD_REQUEST", msg.clone()));
				return (StatusCode::BAD_REQUEST, body).into_response();
			}
			SubServerHttpError::InvalidParams(msg) => {
				let body = Json(ErrorResponse::new("INVALID_PARAMS", msg.clone()));
				return (StatusCode::BAD_REQUEST, body).into_response();
			}
			SubServerHttpError::NotFound(msg) => {
				let body = Json(ErrorResponse::new("NOT_FOUND", msg.clone()));
				return (StatusCode::NOT_FOUND, body).into_response();
			}
			SubServerHttpError::MethodNotAllowed(msg) => {
				let body = Json(ErrorResponse::new("METHOD_NOT_ALLOWED", msg.clone()));
				return (StatusCode::METHOD_NOT_ALLOWED, body).into_response();
			}
			SubServerHttpError::Internal(msg) => {
				error!("Internal error: {}", msg);
				(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error")
			}
		};

		let body = Json(ErrorResponse::new(code, message));
		(status, body).into_response()
	}
}

#[cfg(test)]
pub mod tests {
	use serde_json::to_string;

	use super::*;

	#[test]
	fn test_error_response_serialization() {
		let resp = ErrorResponse::new("TEST_CODE", "Test error message");
		let json = to_string(&resp).unwrap();
		assert!(json.contains("TEST_CODE"));
		assert!(json.contains("Test error message"));
	}

	#[test]
	fn test_app_error_display() {
		let err = SubServerHttpError::BadRequest("Invalid JSON".to_string());
		assert_eq!(err.to_string(), "Bad request: Invalid JSON");
	}

	#[test]
	fn storage_failure_answers_500_while_a_bad_token_answers_401() {
		// A 401 tells the client its token is bad, so it retries forever and never sees the real fault.
		assert_eq!(
			SubServerHttpError::Auth(AuthError::InvalidToken).into_response().status(),
			StatusCode::UNAUTHORIZED,
			"a token that was never issued is the client's fault"
		);
		assert_eq!(
			SubServerHttpError::Auth(AuthError::Expired).into_response().status(),
			StatusCode::UNAUTHORIZED,
			"an expired token is the client's fault"
		);
		assert_eq!(
			SubServerHttpError::Auth(AuthError::Internal).into_response().status(),
			StatusCode::INTERNAL_SERVER_ERROR,
			"authentication that could not reach storage is the server's fault"
		);
	}
}
