// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! HTTP endpoint handlers for query and command execution.
//!
//! This module provides the request handlers for:
//! - `/health` - Health check endpoint
//! - `/v1/query` - Execute read-only queries
//! - `/v1/command` - Execute write commands

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use reifydb_sub_server::{
	AppState, ResponseFrame, convert_frames, execute_command, execute_query,
	extract_identity_from_api_key, extract_identity_from_auth_header,
};
use reifydb_type::Params;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// Request body for query and command endpoints.
#[derive(Debug, Deserialize)]
pub struct StatementRequest {
	/// One or more RQL statements to execute.
	pub statements: Vec<String>,
	/// Optional query parameters.
	#[serde(default)]
	pub params: Option<Params>,
}

/// Response body for query and command endpoints.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
	/// Result frames from query execution.
	pub frames: Vec<ResponseFrame>,
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
	pub status: &'static str,
}

/// Health check endpoint.
///
/// Returns 200 OK if the server is running.
/// This endpoint does not require authentication.
///
/// # Response
///
/// ```json
/// {"status": "ok"}
/// ```
pub async fn health() -> impl IntoResponse {
	(StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

/// Execute a read-only query.
///
/// # Authentication
///
/// Requires one of:
/// - `Authorization: Bearer <token>` header
/// - `X-Api-Key: <key>` header
///
/// # Request Body
///
/// ```json
/// {
///   "statements": ["FROM users FILTER id = $1"],
///   "params": {"$1": 42}
/// }
/// ```
///
/// # Response
///
/// ```json
/// {
///   "frames": [...]
/// }
/// ```
pub async fn handle_query(
	State(state): State<AppState>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Json<QueryResponse>, AppError> {
	// Extract identity from headers
	let identity = extract_identity(&headers)?;

	// Combine statements
	let query = request.statements.join("; ");

	// Get params or default
	let params = request.params.unwrap_or(Params::None);

	// Execute with timeout
	let frames = execute_query(
		state.engine_clone(),
		query,
		identity,
		params,
		state.query_timeout(),
	)
	.await?;

	Ok(Json(QueryResponse {
		frames: convert_frames(frames),
	}))
}

/// Execute a write command.
///
/// Commands include INSERT, UPDATE, DELETE, and DDL statements.
///
/// # Authentication
///
/// Requires one of:
/// - `Authorization: Bearer <token>` header
/// - `X-Api-Key: <key>` header
///
/// # Request Body
///
/// ```json
/// {
///   "statements": ["INSERT INTO users (name) VALUES ($1)"],
///   "params": {"$1": "Alice"}
/// }
/// ```
///
/// # Response
///
/// ```json
/// {
///   "frames": [...]
/// }
/// ```
pub async fn handle_command(
	State(state): State<AppState>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Json<QueryResponse>, AppError> {
	// Extract identity from headers
	let identity = extract_identity(&headers)?;

	// Get params or default
	let params = request.params.unwrap_or(Params::None);

	// Execute with timeout
	let frames = execute_command(
		state.engine_clone(),
		request.statements,
		identity,
		params,
		state.query_timeout(),
	)
	.await?;

	Ok(Json(QueryResponse {
		frames: convert_frames(frames),
	}))
}

/// Extract identity from request headers.
///
/// Tries in order:
/// 1. Authorization header (Bearer token)
/// 2. X-Api-Key header
fn extract_identity(headers: &HeaderMap) -> Result<reifydb_core::interface::Identity, AppError> {
	// Try Authorization header first
	if let Some(auth_header) = headers.get("authorization") {
		let auth_str = auth_header.to_str().map_err(|_| AppError::Auth(reifydb_sub_server::AuthError::InvalidHeader))?;

		return extract_identity_from_auth_header(auth_str).map_err(AppError::Auth);
	}

	// Try X-Api-Key header
	if let Some(api_key) = headers.get("x-api-key") {
		let key = api_key.to_str().map_err(|_| AppError::Auth(reifydb_sub_server::AuthError::InvalidHeader))?;

		return extract_identity_from_api_key(key).map_err(AppError::Auth);
	}

	// No credentials provided
	Err(AppError::Auth(reifydb_sub_server::AuthError::MissingCredentials))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_statement_request_deserialization() {
		let json = r#"{"statements": ["SELECT 1"]}"#;
		let request: StatementRequest = serde_json::from_str(json).unwrap();
		assert_eq!(request.statements, vec!["SELECT 1"]);
		assert!(request.params.is_none());
	}

	#[test]
	fn test_query_response_serialization() {
		let response = QueryResponse { frames: Vec::new() };
		let json = serde_json::to_string(&response).unwrap();
		assert!(json.contains("frames"));
	}

	#[test]
	fn test_health_response_serialization() {
		let response = HealthResponse { status: "ok" };
		let json = serde_json::to_string(&response).unwrap();
		assert_eq!(json, r#"{"status":"ok"}"#);
	}
}
