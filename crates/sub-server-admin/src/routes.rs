// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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

pub fn router(state: AdminState) -> Router {
	let max_connections = state.max_connections();

	Router::new()
		.route("/v1/auth/login", post(handle_login))
		.route("/v1/auth/logout", post(handle_logout))
		.route("/v1/auth/status", get(handle_auth_status))
		.route("/v1/execute", post(handle_execute))
		.route("/", get(serve_index))
		.route("/assets/{*path}", get(serve_static))
		.fallback(serve_index)
		.layer(TraceLayer::new_for_http())
		.layer(ConcurrencyLimitLayer::new(max_connections))
		.with_state(state)
}
