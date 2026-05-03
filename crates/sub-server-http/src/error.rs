// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error, fmt};

use axum::{
	Json,
	http::StatusCode,
	response::{IntoResponse, Response},
};
use reifydb_sub_server::{auth::AuthError, execute::ExecuteError};
use reifydb_type::error::Diagnostic;
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
pub enum AppError {
	Auth(AuthError),

	Execute(ExecuteError),

	BadRequest(String),

	InvalidParams(String),

	NotFound(String),

	MethodNotAllowed(String),

	Internal(String),
}

impl From<AuthError> for AppError {
	fn from(e: AuthError) -> Self {
		AppError::Auth(e)
	}
}

impl From<ExecuteError> for AppError {
	fn from(e: ExecuteError) -> Self {
		AppError::Execute(e)
	}
}

impl fmt::Display for AppError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			AppError::Auth(e) => write!(f, "Authentication error: {}", e),
			AppError::Execute(e) => write!(f, "Execution error: {}", e),
			AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
			AppError::InvalidParams(msg) => write!(f, "Invalid params: {}", msg),
			AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
			AppError::MethodNotAllowed(msg) => write!(f, "Method not allowed: {}", msg),
			AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
		}
	}
}

impl error::Error for AppError {}

impl IntoResponse for AppError {
	fn into_response(self) -> Response {
		if let AppError::Execute(ExecuteError::Engine {
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
			AppError::Auth(AuthError::MissingCredentials) => {
				(StatusCode::UNAUTHORIZED, "AUTH_REQUIRED", "Authentication required")
			}
			AppError::Auth(AuthError::InvalidToken) => {
				(StatusCode::UNAUTHORIZED, "INVALID_TOKEN", "Invalid authentication token")
			}
			AppError::Auth(AuthError::Expired) => {
				(StatusCode::UNAUTHORIZED, "TOKEN_EXPIRED", "Authentication token expired")
			}
			AppError::Auth(AuthError::InvalidHeader) => {
				(StatusCode::BAD_REQUEST, "INVALID_HEADER", "Malformed authorization header")
			}
			AppError::Auth(AuthError::InsufficientPermissions) => {
				(StatusCode::FORBIDDEN, "FORBIDDEN", "Insufficient permissions for this operation")
			}
			AppError::Execute(ExecuteError::Timeout) => {
				(StatusCode::GATEWAY_TIMEOUT, "QUERY_TIMEOUT", "Query execution timed out")
			}
			AppError::Execute(ExecuteError::Cancelled) => {
				(StatusCode::BAD_REQUEST, "QUERY_CANCELLED", "Query was cancelled")
			}
			AppError::Execute(ExecuteError::Disconnected) => {
				error!("Query stream disconnected unexpectedly");
				(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error")
			}
			AppError::Execute(ExecuteError::Rejected {
				code,
				message,
			}) => {
				let body = Json(ErrorResponse::new(code, message));
				return (StatusCode::FORBIDDEN, body).into_response();
			}
			AppError::Execute(ExecuteError::Engine {
				..
			}) => {
				unreachable!()
			}
			AppError::BadRequest(msg) => {
				let body = Json(ErrorResponse::new("BAD_REQUEST", msg.clone()));
				return (StatusCode::BAD_REQUEST, body).into_response();
			}
			AppError::InvalidParams(msg) => {
				let body = Json(ErrorResponse::new("INVALID_PARAMS", msg.clone()));
				return (StatusCode::BAD_REQUEST, body).into_response();
			}
			AppError::NotFound(msg) => {
				let body = Json(ErrorResponse::new("NOT_FOUND", msg.clone()));
				return (StatusCode::NOT_FOUND, body).into_response();
			}
			AppError::MethodNotAllowed(msg) => {
				let body = Json(ErrorResponse::new("METHOD_NOT_ALLOWED", msg.clone()));
				return (StatusCode::METHOD_NOT_ALLOWED, body).into_response();
			}
			AppError::Internal(msg) => {
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
		let err = AppError::BadRequest("Invalid JSON".to_string());
		assert_eq!(err.to_string(), "Bad request: Invalid JSON");
	}
}
