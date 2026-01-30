// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! HTTP router configuration.
//!
//! This module sets up the Axum router with all endpoints and middleware layers.

use axum::{
	Router,
	routing::{get, post},
};
use reifydb_sub_server::state::AppState;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::handlers::{handle_admin, handle_command, handle_query, health};

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
pub fn router(state: AppState) -> Router {
	let max_connections = state.max_connections();

	Router::new()
		// Health check (no auth required)
		.route("/health", get(health))
		// Query endpoint (auth required)
		.route("/v1/query", post(handle_query))
		// Admin endpoint (auth required, DDL + DML + Query)
		.route("/v1/admin", post(handle_admin))
		// Command endpoint (auth required, DML + Query)
		.route("/v1/command", post(handle_command))
		// Apply middleware layers - order matters!
		// TraceLayer must be outermost for proper request/response logging
		.layer(TraceLayer::new_for_http())
		// Limit concurrent connections to prevent resource exhaustion
		.layer(ConcurrencyLimitLayer::new(max_connections))
		.with_state(state)
}
