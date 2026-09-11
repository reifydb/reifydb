// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod auth_api;
mod db_forward;
mod public;

use axum::{
	Json, Router,
	routing::{get, post},
	serve as axum_serve,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::watch};
use tower_http::trace::TraceLayer;
use tracing::error;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
	Router::new()
		.route("/api/auth/guest", post(auth_api::guest_session))
		.route("/api/auth/register", post(auth_api::register))
		.route("/api/public/status/{slug}", get(public::status))
		.route("/health", get(health))
		.route("/db/v1/authenticate", post(db_forward::forward))
		.route("/db/v1/logout", post(db_forward::forward))
		.layer(TraceLayer::new_for_http())
		.with_state(state)
}

async fn health() -> Json<Value> {
	Json(json!({ "status": "ok" }))
}

pub async fn serve(state: AppState, listener: TcpListener, mut shutdown: watch::Receiver<bool>) {
	let app = router(state);
	let server = axum_serve(listener, app).with_graceful_shutdown(async move {
		let _ = shutdown.changed().await;
	});
	if let Err(e) = server.await {
		error!("uptime http server error: {e}");
	}
}
