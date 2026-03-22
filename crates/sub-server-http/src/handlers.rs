// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! HTTP endpoint handler for query and command execution.
//!
//! This module provides the request handler for:
//! - `/health` - Health check endpoint
//! - `/v1/query` - Execute read-only queries
//! - `/v1/command` - Execute write commands

use axum::{
	Json,
	extract::{Query, State},
	http::{HeaderMap, StatusCode, header},
	response::{IntoResponse, Response},
};
use reifydb_core::value::frame::response::{ResponseFrame, convert_frames};
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header, extract_identity_from_auth_token},
	execute::execute,
	interceptor::{Operation, Protocol, RequestContext, RequestMetadata},
	response::resolve_response_json,
	state::AppState,
	wire::WireParams,
};
use reifydb_type::{params::Params, value::identity::IdentityId};
use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// Request body for query and command endpoints.
#[derive(Debug, Deserialize)]
pub struct StatementRequest {
	/// One or more RQL statements to execute.
	pub statements: Vec<String>,
	/// Optional query parameters.
	#[serde(default)]
	pub params: Option<WireParams>,
}

/// Response body for query and command endpoints.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
	/// Result frames from query execution.
	pub frames: Vec<ResponseFrame>,
}

/// Query parameters for response format control.
#[derive(Debug, Deserialize)]
pub struct FormatParams {
	pub format: Option<String>,
	pub unwrap: Option<bool>,
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
	(
		StatusCode::OK,
		Json(HealthResponse {
			status: "ok",
		}),
	)
}

/// Build `RequestMetadata` from HTTP headers.
fn build_metadata(headers: &HeaderMap) -> RequestMetadata {
	let mut metadata = RequestMetadata::new(Protocol::Http);
	for (name, value) in headers.iter() {
		if let Ok(v) = value.to_str() {
			metadata.insert(name.as_str(), v);
		}
	}
	metadata
}

/// Execute a read-only query.
///
/// # Authentication
///
/// Supported via one of:
/// - `Authorization: Bearer <token>` header
/// - `X-Api-Key: <token>` header
/// - No credentials (anonymous access)
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
	Query(format_params): Query<FormatParams>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Response, AppError> {
	execute_and_respond(&state, Operation::Query, &headers, request, &format_params).await
}

/// Execute an admin operation.
///
/// Admin operations include DDL (CREATE TABLE, ALTER, etc.), DML (INSERT, UPDATE, DELETE),
/// and read queries. This is the most privileged execution level.
///
/// # Authentication
///
/// Supported via one of:
/// - `Authorization: Bearer <token>` header
/// - `X-Api-Key: <token>` header
/// - No credentials (anonymous access)
pub async fn handle_admin(
	State(state): State<AppState>,
	Query(format_params): Query<FormatParams>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Response, AppError> {
	execute_and_respond(&state, Operation::Admin, &headers, request, &format_params).await
}

/// Execute a write command.
///
/// Commands include INSERT, UPDATE, and DELETE statements.
///
/// # Authentication
///
/// Supported via one of:
/// - `Authorization: Bearer <token>` header
/// - `X-Api-Key: <token>` header
/// - No credentials (anonymous access)
pub async fn handle_command(
	State(state): State<AppState>,
	Query(format_params): Query<FormatParams>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Response, AppError> {
	execute_and_respond(&state, Operation::Command, &headers, request, &format_params).await
}

/// Shared implementation for query, admin, and command handlers.
async fn execute_and_respond(
	state: &AppState,
	operation: Operation,
	headers: &HeaderMap,
	request: StatementRequest,
	format_params: &FormatParams,
) -> Result<Response, AppError> {
	let identity = extract_identity(headers)?;
	let metadata = build_metadata(headers);
	let params = match request.params {
		None => Params::None,
		Some(wp) => wp.into_params().map_err(|e| AppError::InvalidParams(e))?,
	};

	let ctx = RequestContext {
		identity,
		operation,
		statements: request.statements,
		params,
		metadata,
	};

	let (frames, duration) = execute(
		state.request_interceptors(),
		state.actor_system(),
		state.engine_clone(),
		ctx,
		state.query_timeout(),
		state.clock(),
	)
	.await?;

	let mut response = if format_params.format.as_deref() == Some("json") {
		let resolved = resolve_response_json(frames, format_params.unwrap.unwrap_or(false))
			.map_err(|e| AppError::BadRequest(e))?;
		(StatusCode::OK, [(header::CONTENT_TYPE, resolved.content_type)], resolved.body).into_response()
	} else {
		Json(QueryResponse {
			frames: convert_frames(&frames),
		})
		.into_response()
	};
	response.headers_mut().insert("x-duration-ms", duration.as_millis().to_string().parse().unwrap());
	Ok(response)
}

/// Extract identity from request headers.
///
/// Tries in order:
/// 1. Authorization header (Bearer token)
/// 2. X-Api-Key header (auth token)
/// 3. Falls back to anonymous identity
fn extract_identity(headers: &HeaderMap) -> Result<IdentityId, AppError> {
	// Try Authorization header first
	if let Some(auth_header) = headers.get("authorization") {
		let auth_str = auth_header.to_str().map_err(|_| AppError::Auth(AuthError::InvalidHeader))?;

		return extract_identity_from_auth_header(auth_str).map_err(AppError::Auth);
	}

	// Try X-Api-Key header
	if let Some(auth_token) = headers.get("x-api-key") {
		let token = auth_token.to_str().map_err(|_| AppError::Auth(AuthError::InvalidHeader))?;

		return extract_identity_from_auth_token(token).map_err(AppError::Auth);
	}

	// No credentials provided — anonymous access
	Ok(IdentityId::anonymous())
}

#[cfg(test)]
pub mod tests {
	use serde_json::{from_str, to_string};

	use super::*;

	#[test]
	fn test_statement_request_deserialization() {
		let json = r#"{"statements": ["SELECT 1"]}"#;
		let request: StatementRequest = from_str(json).unwrap();
		assert_eq!(request.statements, vec!["SELECT 1"]);
		assert!(request.params.is_none());
	}

	#[test]
	fn test_query_response_serialization() {
		let response = QueryResponse {
			frames: Vec::new(),
		};
		let json = to_string(&response).unwrap();
		assert!(json.contains("frames"));
	}

	#[test]
	fn test_health_response_serialization() {
		let response = HealthResponse {
			status: "ok",
		};
		let json = to_string(&response).unwrap();
		assert_eq!(json, r#"{"status":"ok"}"#);
	}
}
