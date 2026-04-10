// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! HTTP endpoint handler for query and command execution.
//!
//! This module provides the request handler for:
//! - `/health` - Health check endpoint
//! - `/v1/query` - Execute read-only queries
//! - `/v1/command` - Execute write commands

use std::collections::HashMap;

use axum::{
	Json,
	extract::{Query, State},
	http::{HeaderMap, StatusCode, header},
	response::{IntoResponse, Response},
};
use reifydb_core::{
	actors::server::{Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	value::frame::response::{ResponseFrame, convert_frames},
};
use reifydb_runtime::actor::reply::reply_channel;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	dispatch::dispatch,
	interceptor::{Protocol, RequestContext, RequestMetadata},
	response::resolve_response_json,
	wire::WireParams,
};
use reifydb_type::{params::Params, value::identity::IdentityId};
use serde::{Deserialize, Serialize};

use crate::{error::AppError, state::HttpServerState};

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

/// Response body for logout endpoint.
#[derive(Debug, Serialize)]
pub struct LogoutResponse {
	pub status: String,
}

/// Request body for authentication endpoint.
#[derive(Debug, Deserialize)]
pub struct AuthenticateRequest {
	/// Authentication method: "password", "solana", "token".
	pub method: String,
	/// Credentials (method-specific key-value pairs).
	#[serde(default)]
	pub credentials: HashMap<String, String>,
}

/// Response body for authentication endpoint.
#[derive(Debug, Serialize)]
pub struct AuthenticateResponse {
	/// Authentication status: "authenticated", "challenge", "failed".
	pub status: String,
	/// Session token (present when status is "authenticated").
	#[serde(skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
	/// Identity ID (present when status is "authenticated").
	#[serde(skip_serializing_if = "Option::is_none")]
	pub identity: Option<String>,
	/// Challenge ID (present when status is "challenge").
	#[serde(skip_serializing_if = "Option::is_none")]
	pub challenge_id: Option<String>,
	/// Challenge payload (present when status is "challenge").
	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload: Option<HashMap<String, String>>,
	/// Failure reason (present when status is "failed").
	#[serde(skip_serializing_if = "Option::is_none")]
	pub reason: Option<String>,
}

pub async fn handle_authenticate(
	State(state): State<HttpServerState>,
	Json(request): Json<AuthenticateRequest>,
) -> Result<Response, AppError> {
	let (reply, receiver) = reply_channel();
	let (actor_ref, _handle) = state.spawn_actor();
	actor_ref
		.send(ServerMessage::Authenticate {
			method: request.method,
			credentials: request.credentials,
			reply,
		})
		.ok()
		.ok_or_else(|| AppError::Internal("actor mailbox closed".into()))?;

	let auth_response = receiver.recv().await.map_err(|_| AppError::Internal("actor stopped".into()))?;

	match auth_response {
		ServerAuthResponse::Authenticated {
			identity,
			token,
		} => Ok((
			StatusCode::OK,
			Json(AuthenticateResponse {
				status: "authenticated".to_string(),
				token: Some(token),
				identity: Some(identity.to_string()),
				challenge_id: None,
				payload: None,
				reason: None,
			}),
		)
			.into_response()),
		ServerAuthResponse::Challenge {
			challenge_id,
			payload,
		} => Ok((
			StatusCode::OK,
			Json(AuthenticateResponse {
				status: "challenge".to_string(),
				token: None,
				identity: None,
				challenge_id: Some(challenge_id),
				payload: Some(payload),
				reason: None,
			}),
		)
			.into_response()),
		ServerAuthResponse::Failed {
			reason,
		} => Ok((
			StatusCode::UNAUTHORIZED,
			Json(AuthenticateResponse {
				status: "failed".to_string(),
				token: None,
				identity: None,
				challenge_id: None,
				payload: None,
				reason: Some(reason),
			}),
		)
			.into_response()),
		ServerAuthResponse::Error(reason) => Ok((
			StatusCode::INTERNAL_SERVER_ERROR,
			Json(AuthenticateResponse {
				status: "failed".to_string(),
				token: None,
				identity: None,
				challenge_id: None,
				payload: None,
				reason: Some(reason),
			}),
		)
			.into_response()),
	}
}

pub async fn handle_logout(State(state): State<HttpServerState>, headers: HeaderMap) -> Result<Response, AppError> {
	let auth_header = headers.get("authorization").ok_or(AppError::Auth(AuthError::MissingCredentials))?;
	let auth_str = auth_header.to_str().map_err(|_| AppError::Auth(AuthError::InvalidHeader))?;
	let token = auth_str.strip_prefix("Bearer ").ok_or(AppError::Auth(AuthError::InvalidHeader))?.trim();

	if token.is_empty() {
		return Err(AppError::Auth(AuthError::InvalidToken));
	}

	let (reply, receiver) = reply_channel();
	let (actor_ref, _handle) = state.spawn_actor();
	actor_ref
		.send(ServerMessage::Logout {
			token: token.to_string(),
			reply,
		})
		.ok()
		.ok_or_else(|| AppError::Internal("actor mailbox closed".into()))?;

	let logout_response = receiver.recv().await.map_err(|_| AppError::Internal("actor stopped".into()))?;

	match logout_response {
		ServerLogoutResponse::Ok => Ok((
			StatusCode::OK,
			Json(LogoutResponse {
				status: "ok".to_string(),
			}),
		)
			.into_response()),
		ServerLogoutResponse::InvalidToken => Err(AppError::Auth(AuthError::InvalidToken)),
		ServerLogoutResponse::Error(reason) => Err(AppError::Internal(reason)),
	}
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
pub async fn handle_query(
	State(state): State<HttpServerState>,
	Query(format_params): Query<FormatParams>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Response, AppError> {
	execute_and_respond(&state, Operation::Query, &headers, request, &format_params).await
}

/// Execute an admin operation.
pub async fn handle_admin(
	State(state): State<HttpServerState>,
	Query(format_params): Query<FormatParams>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Response, AppError> {
	execute_and_respond(&state, Operation::Admin, &headers, request, &format_params).await
}

/// Execute a write command.
pub async fn handle_command(
	State(state): State<HttpServerState>,
	Query(format_params): Query<FormatParams>,
	headers: HeaderMap,
	Json(request): Json<StatementRequest>,
) -> Result<Response, AppError> {
	execute_and_respond(&state, Operation::Command, &headers, request, &format_params).await
}

/// Shared implementation for query, admin, and command handlers.
///
/// Dispatches to the ServerActor for engine execution via the shared
/// `dispatch()` function which handles interceptors, timeout, and
/// response conversion.
async fn execute_and_respond(
	state: &HttpServerState,
	operation: Operation,
	headers: &HeaderMap,
	request: StatementRequest,
	format_params: &FormatParams,
) -> Result<Response, AppError> {
	let identity = extract_identity(state, headers)?;
	let metadata = build_metadata(headers);
	let params = match request.params {
		None => Params::None,
		Some(wp) => wp.into_params().map_err(AppError::InvalidParams)?,
	};
	let ctx = RequestContext {
		identity,
		operation,
		statements: request.statements,
		params,
		metadata,
	};

	let (frames, wall_duration) = dispatch(state, ctx).await?;

	// HTTP-specific response formatting
	let mut response = if format_params.format.as_deref() == Some("json") {
		let resolved = resolve_response_json(frames, format_params.unwrap.unwrap_or(false))
			.map_err(AppError::BadRequest)?;
		(StatusCode::OK, [(header::CONTENT_TYPE, resolved.content_type)], resolved.body).into_response()
	} else {
		Json(QueryResponse {
			frames: convert_frames(&frames),
		})
		.into_response()
	};
	response.headers_mut().insert("x-duration-ms", wall_duration.as_millis().to_string().parse().unwrap());
	Ok(response)
}

/// Extract identity from request headers.
///
/// Tries in order:
/// 1. Authorization header (Bearer token)
/// 2. Falls back to anonymous identity
fn extract_identity(state: &HttpServerState, headers: &HeaderMap) -> Result<IdentityId, AppError> {
	// Try Authorization header
	if let Some(auth_header) = headers.get("authorization") {
		let auth_str = auth_header.to_str().map_err(|_| AppError::Auth(AuthError::InvalidHeader))?;

		return extract_identity_from_auth_header(state.auth_service(), auth_str).map_err(AppError::Auth);
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
