// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use axum::{
	Router,
	routing::{any, get, post},
};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::{
	handlers::{
		handle_admin, handle_authenticate, handle_binding, handle_command, handle_logout, handle_query, health,
	},
	state::HttpServerState,
};

/// Create the HTTP router with all endpoints and middleware.
///
/// # Endpoints
///
/// - `GET /health` - Health check (no auth required)
/// - `POST /v1/query` - Execute read queries (auth required)
/// - `POST /v1/command` - Execute write commands (auth required)
///
/// # Middleware
///
/// Applied in order (outer to inner):
/// 1. Tracing - Logs requests and responses
/// 2. Concurrency limit - Prevents resource exhaustion
///
/// Note: Request timeouts are handled at the query execution level via
/// `tokio::time::timeout` in the execute module.
///
/// # Arguments
///
/// * `state` - Shared application state containing engine and config
///
/// # Returns
///
/// Configured Axum Router ready to serve requests.
pub fn router(state: HttpServerState) -> Router {
	let max_connections = state.max_connections();
	let admin_enabled = state.admin_enabled();

	let mut app = Router::new()
		// Health check (no auth required)
		.route("/health", get(health))
		// Authentication endpoint (no auth required)
		.route("/v1/authenticate", post(handle_authenticate))
		// Logout endpoint (auth required)
		.route("/v1/logout", post(handle_logout))
		// Query endpoint (auth required)
		.route("/v1/query", post(handle_query))
		// Command endpoint (auth required, DML + Query)
		.route("/v1/command", post(handle_command));

	if admin_enabled {
		// Admin endpoint (auth required, DDL + DML + Query)
		app = app.route("/v1/admin", post(handle_admin));
	}

	// Binding catch-all: dispatched per-request against the materialized catalog.
	app = app.route("/api/{*path}", any(handle_binding));

	app
		// Apply middleware layers - order matters!
		// TraceLayer must be outermost for proper request/response logging
		.layer(TraceLayer::new_for_http())
		// Limit concurrent connections to prevent resource exhaustion
		.layer(ConcurrencyLimitLayer::new(max_connections))
		.with_state(state)
}
