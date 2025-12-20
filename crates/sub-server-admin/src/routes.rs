// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! HTTP router configuration for the admin server.

use axum::{
	Router,
	routing::{get, post},
};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::{
	handlers::{handle_auth_status, handle_execute, handle_login, handle_logout, serve_index, serve_static},
	state::AdminState,
};

/// Create the admin router with all endpoints and middleware.
///
/// # Endpoints
///
/// - `GET /health` - Health check
/// - `POST /v1/auth/login` - Login
/// - `POST /v1/auth/logout` - Logout
/// - `GET /v1/auth/status` - Auth status
/// - `GET /v1/config` - Get config
/// - `PUT /v1/config` - Update config
/// - `POST /v1/execute` - Execute query
/// - `GET /v1/metrics` - System metrics
/// - `GET /` - Serve admin UI index
/// - `GET /assets/*path` - Serve static assets
/// - `GET /*path` - SPA fallback to index.html
///
/// # Middleware
///
/// Applied in order (outer to inner):
/// 1. Tracing - Logs requests and responses
/// 2. Concurrency limit - Prevents resource exhaustion
pub fn router(state: AdminState) -> Router {
	let max_connections = state.max_connections();

	Router::new()
		// Auth endpoints
		.route("/v1/auth/login", post(handle_login))
		.route("/v1/auth/logout", post(handle_logout))
		.route("/v1/auth/status", get(handle_auth_status))
		// Execute endpoint
		.route("/v1/execute", post(handle_execute))
		// Static file serving
		.route("/", get(serve_index))
		.route("/assets/{*path}", get(serve_static))
		// SPA fallback - serve index.html for unknown routes (for client-side routing)
		.fallback(serve_index)
		// Apply middleware layers
		.layer(TraceLayer::new_for_http())
		.layer(ConcurrencyLimitLayer::new(max_connections))
		.with_state(state)
}
