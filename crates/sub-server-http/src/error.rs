// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! HTTP error handling and response formatting.
//!
//! This module provides error types that implement Axum's `IntoResponse` trait
//! for consistent error responses across all HTTP endpoints.

use axum::{
	Json,
	http::StatusCode,
	response::{IntoResponse, Response},
};
use reifydb_sub_server::{AuthError, ExecuteError};
use reifydb_type::diagnostic::Diagnostic;
use serde::Serialize;

/// JSON error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
	/// Human-readable error message.
	pub error: String,
	/// Machine-readable error code.
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

/// JSON diagnostic error response body (matches WS format).
#[derive(Debug, Serialize)]
pub struct DiagnosticResponse {
	/// Full diagnostic information.
	pub diagnostic: Diagnostic,
}

/// Application error type that converts to HTTP responses.
#[derive(Debug)]
pub enum AppError {
	/// Authentication error.
	Auth(AuthError),
	/// Query/command execution error.
	Execute(ExecuteError),
	/// Request parsing error.
	BadRequest(String),
	/// Internal server error.
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

impl std::fmt::Display for AppError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AppError::Auth(e) => write!(f, "Authentication error: {}", e),
			AppError::Execute(e) => write!(f, "Execution error: {}", e),
			AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
			AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
		}
	}
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
	fn into_response(self) -> Response {
		// Handle engine errors specially - they have full diagnostic info
		if let AppError::Execute(ExecuteError::Engine {
			diagnostic,
			statement,
		}) = self
		{
			tracing::debug!("Engine error: {}", diagnostic.message);
			// Clone the diagnostic and attach the statement
			let mut diag = (*diagnostic).clone();
			if diag.statement.is_none() && !statement.is_empty() {
				diag.with_statement(statement);
			}
			let body = Json(DiagnosticResponse {
				diagnostic: diag,
			});
			return (StatusCode::BAD_REQUEST, body).into_response();
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
				tracing::error!("Query stream disconnected unexpectedly");
				(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error")
			}
			AppError::Execute(ExecuteError::Engine {
				..
			}) => {
				// Already handled above
				unreachable!()
			}
			AppError::BadRequest(msg) => {
				let body = Json(ErrorResponse::new("BAD_REQUEST", msg.clone()));
				return (StatusCode::BAD_REQUEST, body).into_response();
			}
			AppError::Internal(msg) => {
				tracing::error!("Internal error: {}", msg);
				(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error")
			}
		};

		let body = Json(ErrorResponse::new(code, message));
		(status, body).into_response()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_error_response_serialization() {
		let resp = ErrorResponse::new("TEST_CODE", "Test error message");
		let json = serde_json::to_string(&resp).unwrap();
		assert!(json.contains("TEST_CODE"));
		assert!(json.contains("Test error message"));
	}

	#[test]
	fn test_app_error_display() {
		let err = AppError::BadRequest("Invalid JSON".to_string());
		assert_eq!(err.to_string(), "Bad request: Invalid JSON");
	}
}
