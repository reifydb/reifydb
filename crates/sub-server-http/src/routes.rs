// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	Router,
	routing::{any, get, post},
};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::{
	handlers::{
		handle_admin, handle_authenticate, handle_binding, handle_command, handle_logout, handle_query,
		handle_queue_claim, health,
	},
	state::HttpServerState,
};

pub fn router(state: HttpServerState) -> Router {
	let max_connections = state.max_connections();
	let claim_max_parked = state.claim_max_parked();
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

	let app = app.layer(ConcurrencyLimitLayer::new(max_connections));

	let claim = Router::new()
		.route("/v1/queue/claim", post(handle_queue_claim))
		.layer(ConcurrencyLimitLayer::new(claim_max_parked));

	Router::new().merge(app).merge(claim).layer(TraceLayer::new_for_http()).with_state(state)
}
