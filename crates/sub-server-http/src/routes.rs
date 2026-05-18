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

pub fn router(state: HttpServerState) -> Router {
	let max_connections = state.max_connections();
	let admin_enabled = state.admin_enabled();

	let mut app = Router::new()
		.route("/health", get(health))
		.route("/v1/authenticate", post(handle_authenticate))
		.route("/v1/logout", post(handle_logout))
		.route("/v1/query", post(handle_query))
		.route("/v1/command", post(handle_command));

	if admin_enabled {
		app = app.route("/v1/admin", post(handle_admin));
	}

	app = app.route("/api/{*path}", any(handle_binding));

	app.layer(TraceLayer::new_for_http()).layer(ConcurrencyLimitLayer::new(max_connections)).with_state(state)
}
