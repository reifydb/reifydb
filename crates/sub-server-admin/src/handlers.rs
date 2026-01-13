// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! HTTP endpoint handler for the admin server.
//!
//! This module provides handler for:
//! - `/health` - Health check
//! - `/v1/auth/*` - Authentication endpoints
//! - `/v1/config` - Configuration endpoints
//! - `/v1/execute` - Query execution
//! - `/v1/metrics` - System metrics
//! - Static file serving for the admin UI

use axum::{
	Json,
	body::Body,
	extract::{Path, State},
	http::{Response, StatusCode, header},
	response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::{assets, state::AdminState};

// ============================================================================
// Authentication
// ============================================================================

/// Login request body.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
	pub token: String,
}

/// Login response.
#[derive(Debug, Serialize)]
pub struct LoginResponse {
	pub success: bool,
	pub message: Option<String>,
	pub session_token: Option<String>,
}

/// Auth status response.
#[derive(Debug, Serialize)]
pub struct AuthStatusResponse {
	pub auth_required: bool,
	pub authenticated: bool,
}

/// Handle login request.
pub async fn handle_login(State(state): State<AdminState>, Json(request): Json<LoginRequest>) -> impl IntoResponse {
	if !state.auth_required() {
		return (
			StatusCode::OK,
			Json(LoginResponse {
				success: true,
				message: Some("Auth not required".to_string()),
				session_token: None,
			}),
		);
	}

	if state.auth_token() == Some(&request.token) {
		// TODO: Generate proper session token
		(
			StatusCode::OK,
			Json(LoginResponse {
				success: true,
				message: None,
				session_token: Some("temp_session_token".to_string()),
			}),
		)
	} else {
		(
			StatusCode::BAD_REQUEST,
			Json(LoginResponse {
				success: false,
				message: Some("Invalid token".to_string()),
				session_token: None,
			}),
		)
	}
}

/// Handle logout request.
pub async fn handle_logout() -> impl IntoResponse {
	(
		StatusCode::OK,
		Json(serde_json::json!({
			"success": true,
			"message": "Logged out"
		})),
	)
}

/// Get authentication status.
pub async fn handle_auth_status(State(state): State<AdminState>) -> impl IntoResponse {
	(
		StatusCode::OK,
		Json(AuthStatusResponse {
			auth_required: state.auth_required(),
			// TODO: Check actual auth status from session
			authenticated: !state.auth_required(),
		}),
	)
}

// ============================================================================
// Query Execution
// ============================================================================

/// Execute request body.
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
	pub query: String,
}

/// Execute a query (placeholder).
pub async fn handle_execute(
	State(_state): State<AdminState>,
	Json(request): Json<ExecuteRequest>,
) -> impl IntoResponse {
	// TODO: Execute query using the engine
	(
		StatusCode::OK,
		Json(serde_json::json!({
			"success": true,
			"message": "Query execution not yet implemented",
			"query": request.query
		})),
	)
}

// ============================================================================
// Static Files
// ============================================================================

const FALLBACK_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
    <title>ReifyDB Admin</title>
    <style>
        body { font-family: system-ui; max-width: 800px; margin: 50px auto; padding: 20px; }
        .error { background: #fee; padding: 20px; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>ReifyDB Admin Console</h1>
    <div class="error">
        <p>React app not found. Please build the webapp first.</p>
    </div>
</body>
</html>"#;

/// Serve the index.html file.
pub async fn serve_index() -> impl IntoResponse {
	if let Some(file) = assets::get_embedded_file("index.html") {
		Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_TYPE, file.mime_type)
			.body(Body::from(file.content.to_vec()))
			.unwrap()
	} else {
		Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_TYPE, "text/html")
			.body(Body::from(FALLBACK_HTML))
			.unwrap()
	}
}

/// Serve static assets.
pub async fn serve_static(Path(path): Path<String>) -> impl IntoResponse {
	// The router extracts path without "assets/" prefix (e.g., "index.js")
	// but the manifest stores files with full path (e.g., "assets/index.js")
	let clean_path = path.strip_prefix('/').unwrap_or(&path);
	let full_path = format!("assets/{}", clean_path);

	if let Some(file) = assets::get_embedded_file(&full_path) {
		Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_TYPE, file.mime_type)
			.header(header::CACHE_CONTROL, "public, max-age=31536000")
			.body(Body::from(file.content.to_vec()))
			.unwrap()
	} else {
		Response::builder()
			.status(StatusCode::NOT_FOUND)
			.header(header::CONTENT_TYPE, "text/plain")
			.body(Body::from(format!("Static file not found: {}", full_path)))
			.unwrap()
	}
}
