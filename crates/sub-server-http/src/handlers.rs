// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! HTTP endpoint handler for query and command execution.
//!
//! This module provides the request handler for:
//! - `/health` - Health check endpoint
//! - `/v1/query` - Execute read-only queries
//! - `/v1/command` - Execute write commands

use std::{collections::HashMap, sync::Arc};

use axum::{
	Json,
	extract::{Path, Query, State},
	http::{HeaderMap, HeaderValue, Method, StatusCode, header},
	response::{IntoResponse, Response},
};
use reifydb_core::{
	actors::server::{Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage},
	interface::catalog::binding::{Binding, BindingFormat, BindingProtocol, HttpMethod},
	metric::ExecutionMetrics,
};
use reifydb_runtime::actor::reply::reply_channel;
use reifydb_sub_server::{
	auth::{AuthError, extract_identity_from_auth_header},
	binding::dispatch_binding,
	dispatch::dispatch,
	format::WireFormat,
	interceptor::{Protocol, RequestContext, RequestMetadata},
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_RBCF, encode_frames_rbcf, resolve_response_json},
	wire::WireParams,
};
use reifydb_type::{
	params::Params,
	value::{Value, frame::frame::Frame, identity::IdentityId, r#type::Type},
};
use reifydb_wire_format::json::{to::convert_frames, types::ResponseFrame};
use serde::{Deserialize, Serialize};
use serde_json::to_string;

use crate::{error::AppError, state::HttpServerState};

/// Request body for query and command endpoints.
#[derive(Debug, Deserialize)]
pub struct StatementRequest {
	/// RQL string to execute.
	pub rql: String,
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
	#[serde(default)]
	pub format: WireFormat,
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
		rql: request.rql,
		params,
		metadata,
	};

	let (frames, metrics) = dispatch(state, ctx).await?;

	let mut response = match format_params.format {
		WireFormat::Rbcf => match encode_frames_rbcf(&frames) {
			Ok(bytes) => (StatusCode::OK, [(header::CONTENT_TYPE, CONTENT_TYPE_RBCF.to_string())], bytes)
				.into_response(),
			Err(e) => return Err(AppError::BadRequest(format!("RBCF encode error: {}", e))),
		},
		WireFormat::Json => {
			let resolved = resolve_response_json(frames, format_params.unwrap.unwrap_or(false))
				.map_err(AppError::BadRequest)?;
			(StatusCode::OK, [(header::CONTENT_TYPE, resolved.content_type)], resolved.body).into_response()
		}
		WireFormat::Frames => {
			let body = to_string(&QueryResponse {
				frames: convert_frames(&frames),
			})
			.map_err(|e| AppError::BadRequest(format!("JSON encode error: {}", e)))?;
			(StatusCode::OK, [(header::CONTENT_TYPE, CONTENT_TYPE_FRAMES.to_string())], body)
				.into_response()
		}
	};
	insert_meta_headers(response.headers_mut(), &metrics);
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

	// No credentials provided - anonymous access
	Ok(IdentityId::anonymous())
}

/// Handler for binding-driven requests mounted at `/api/{*path}`.
///
/// Resolves the HTTP method + remaining path to a `Binding` in the materialized
/// catalog via linear scan, coerces path + query params against the bound
/// procedure's declared parameter types, and dispatches through the shared
/// `dispatch_binding` helper.
pub async fn handle_binding(
	State(state): State<HttpServerState>,
	Path(path): Path<String>,
	method: Method,
	Query(query_params): Query<HashMap<String, String>>,
	headers: HeaderMap,
) -> Result<Response, AppError> {
	let http_method = match method.as_str() {
		"GET" => HttpMethod::Get,
		"POST" => HttpMethod::Post,
		"PUT" => HttpMethod::Put,
		"PATCH" => HttpMethod::Patch,
		"DELETE" => HttpMethod::Delete,
		_ => return Err(AppError::MethodNotAllowed(format!("method `{}` is not supported", method))),
	};
	let request_path = format!("/{}", path);

	// Resolve binding via linear scan over the HTTP-only index (path patterns require scanning).
	let bindings = state.engine().materialized_catalog().list_http_bindings();
	let mut any_path_match = false;
	let mut matched: Option<(Binding, HashMap<String, String>)> = None;
	for b in &bindings {
		let BindingProtocol::Http {
			method: binding_method,
			path: binding_path,
		} = &b.protocol
		else {
			unreachable!("list_http_bindings returns only HTTP bindings")
		};
		if let Some(captures) = match_http_path(binding_path, &request_path) {
			any_path_match = true;
			if binding_method == &http_method {
				matched = Some((b.clone(), captures));
				break;
			}
		}
	}
	let (binding, path_captures) = match matched {
		Some(m) => m,
		None if any_path_match => {
			return Err(AppError::MethodNotAllowed(format!(
				"no binding for method `{}` at `{}`",
				method, request_path
			)));
		}
		None => return Err(AppError::NotFound(format!("no binding for `{}`", request_path))),
	};

	// Resolve procedure + namespace from the binding.
	let procedure =
		state.engine().materialized_catalog().find_procedure(binding.procedure_id).ok_or_else(|| {
			AppError::Internal(format!(
				"binding references missing procedure id {:?}",
				binding.procedure_id
			))
		})?;
	let namespace = state.engine().materialized_catalog().find_namespace(binding.namespace).ok_or_else(|| {
		AppError::Internal(format!("binding references missing namespace id {:?}", binding.namespace))
	})?;

	let param_names: Vec<&str> = procedure.params().iter().map(|p| p.name.as_str()).collect();
	for key in query_params.keys() {
		if !param_names.contains(&key.as_str()) {
			return Err(AppError::BadRequest(format!("unknown parameter `{}`", key)));
		}
		if path_captures.contains_key(key) {
			return Err(AppError::BadRequest(format!("parameter `{}` given in both path and query", key)));
		}
	}

	let mut params: HashMap<String, Value> = HashMap::with_capacity(procedure.params().len());
	for p in procedure.params() {
		let raw = match path_captures.get(&p.name).or_else(|| query_params.get(&p.name)) {
			Some(v) => v,
			None => {
				return Err(AppError::BadRequest(format!("missing required parameter `{}`", p.name)));
			}
		};
		let value = coerce_str_to_value(raw, p.param_type.get_type()).map_err(|e| {
			AppError::BadRequest(format!(
				"parameter `{}`: cannot coerce `{}` to {:?}: {}",
				p.name,
				raw,
				p.param_type.get_type(),
				e
			))
		})?;
		params.insert(p.name.clone(), value);
	}
	let params = if params.is_empty() {
		Params::None
	} else {
		Params::Named(Arc::new(params))
	};

	let identity = extract_identity(&state, &headers)?;
	let metadata = build_metadata(&headers);

	let (frames, metrics) =
		dispatch_binding(&state, namespace.name(), procedure.name(), params, identity, metadata).await?;

	let mut response = encode_binding_response(frames, binding.format)?;
	insert_meta_headers(response.headers_mut(), &metrics);
	Ok(response)
}

fn insert_meta_headers(headers: &mut HeaderMap, metrics: &ExecutionMetrics) {
	headers.insert("x-fingerprint", HeaderValue::from_str(&metrics.fingerprint.to_hex()).unwrap());
	headers.insert("x-duration", HeaderValue::from_str(&metrics.total.to_string()).unwrap());
}

/// Match an HTTP binding path template against a concrete request path.
/// Templates use `{var}` for path captures. Returns the captured map, or `None` if no match.
fn match_http_path(template: &str, request: &str) -> Option<HashMap<String, String>> {
	let t_segments: Vec<&str> = template.split('/').filter(|s| !s.is_empty()).collect();
	let r_segments: Vec<&str> = request.split('/').filter(|s| !s.is_empty()).collect();
	if t_segments.len() != r_segments.len() {
		return None;
	}
	let mut captures = HashMap::new();
	for (t, r) in t_segments.iter().zip(r_segments.iter()) {
		if t.starts_with('{') && t.ends_with('}') {
			let var = &t[1..t.len() - 1];
			captures.insert(var.to_string(), r.to_string());
		} else if t != r {
			return None;
		}
	}
	Some(captures)
}

fn coerce_str_to_value(s: &str, ty: Type) -> Result<Value, String> {
	match ty {
		Type::Boolean => match s {
			"true" | "1" => Ok(Value::Boolean(true)),
			"false" | "0" => Ok(Value::Boolean(false)),
			_ => Err("expected `true`/`false`".into()),
		},
		Type::Utf8 => Ok(Value::Utf8(s.to_string())),
		Type::Int1 => s.parse::<i8>().map(Value::Int1).map_err(|e| e.to_string()),
		Type::Int2 => s.parse::<i16>().map(Value::Int2).map_err(|e| e.to_string()),
		Type::Int4 => s.parse::<i32>().map(Value::Int4).map_err(|e| e.to_string()),
		Type::Int8 => s.parse::<i64>().map(Value::Int8).map_err(|e| e.to_string()),
		Type::Int16 => s.parse::<i128>().map(Value::Int16).map_err(|e| e.to_string()),
		Type::Uint1 => s.parse::<u8>().map(Value::Uint1).map_err(|e| e.to_string()),
		Type::Uint2 => s.parse::<u16>().map(Value::Uint2).map_err(|e| e.to_string()),
		Type::Uint4 => s.parse::<u32>().map(Value::Uint4).map_err(|e| e.to_string()),
		Type::Uint8 => s.parse::<u64>().map(Value::Uint8).map_err(|e| e.to_string()),
		Type::Uint16 => s.parse::<u128>().map(Value::Uint16).map_err(|e| e.to_string()),
		Type::Float4 => s
			.parse::<f32>()
			.map_err(|e| e.to_string())
			.and_then(|v| v.try_into().map(Value::Float4).map_err(|_| "invalid f32".to_string())),
		Type::Float8 => s
			.parse::<f64>()
			.map_err(|e| e.to_string())
			.and_then(|v| v.try_into().map(Value::Float8).map_err(|_| "invalid f64".to_string())),
		other => Err(format!("coercion to {:?} not supported from URL strings", other)),
	}
}

fn encode_binding_response(frames: Vec<Frame>, format: BindingFormat) -> Result<Response, AppError> {
	match format {
		BindingFormat::Rbcf => match encode_frames_rbcf(&frames) {
			Ok(bytes) => {
				Ok((StatusCode::OK, [(header::CONTENT_TYPE, CONTENT_TYPE_RBCF.to_string())], bytes)
					.into_response())
			}
			Err(e) => Err(AppError::BadRequest(format!("RBCF encode error: {}", e))),
		},
		BindingFormat::Json => {
			let resolved = resolve_response_json(frames, false).map_err(AppError::BadRequest)?;
			Ok((StatusCode::OK, [(header::CONTENT_TYPE, resolved.content_type)], resolved.body)
				.into_response())
		}
		BindingFormat::Frames => Ok(Json(QueryResponse {
			frames: convert_frames(&frames),
		})
		.into_response()),
	}
}

#[cfg(test)]
pub mod tests {
	use serde_json::from_str;

	use super::*;

	#[test]
	fn test_match_http_path_static() {
		assert_eq!(match_http_path("/users", "/users"), Some(HashMap::new()));
		assert_eq!(match_http_path("/users", "/other"), None);
	}

	#[test]
	fn test_match_http_path_capture() {
		let caps = match_http_path("/users/{id}", "/users/42").unwrap();
		assert_eq!(caps.get("id"), Some(&"42".to_string()));
	}

	#[test]
	fn test_match_http_path_mismatch_length() {
		assert!(match_http_path("/users/{id}", "/users").is_none());
		assert!(match_http_path("/users/{id}", "/users/42/extra").is_none());
	}

	#[test]
	fn test_coerce_numeric() {
		assert_eq!(coerce_str_to_value("42", Type::Int8).unwrap(), Value::Int8(42));
		assert!(coerce_str_to_value("xx", Type::Int8).is_err());
	}

	#[test]
	fn test_coerce_bool() {
		assert_eq!(coerce_str_to_value("true", Type::Boolean).unwrap(), Value::Boolean(true));
		assert!(coerce_str_to_value("maybe", Type::Boolean).is_err());
	}

	#[test]
	fn test_statement_request_deserialization() {
		let json = r#"{"rql": "SELECT 1"}"#;
		let request: StatementRequest = from_str(json).unwrap();
		assert_eq!(request.rql, "SELECT 1");
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
