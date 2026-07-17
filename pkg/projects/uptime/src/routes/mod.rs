// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod auth_api;
mod db_forward;
mod monitors;
mod public;
mod status_pages;
mod ui;

use axum::{
	Json, Router, middleware,
	routing::{get, post},
	serve as axum_serve,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::watch};
use tower_http::trace::TraceLayer;
use tracing::error;

use crate::{auth, state::AppState};

pub fn router(state: AppState) -> Router {
	let authed = Router::new()
		.route("/me", get(auth_api::me))
		.route("/monitors", get(monitors::list).post(monitors::create))
		.route("/monitors/daily", get(monitors::daily))
		.route("/monitors/{id}", get(monitors::get).put(monitors::update).delete(monitors::delete))
		.route("/monitors/{id}/results", get(monitors::results))
		.route("/status-pages", get(status_pages::list).post(status_pages::create))
		.route(
			"/status-pages/{id}",
			get(status_pages::get).put(status_pages::update).delete(status_pages::delete),
		)
		.route_layer(middleware::from_fn_with_state(state.clone(), auth::require_auth));

	Router::new()
		.route("/api/auth/register", post(auth_api::register))
		.route("/api/auth/login", post(auth_api::login))
		.route("/api/public/status/{slug}", get(public::status))
		.route("/health", get(health))
		.nest("/api", authed)
		.route("/db/v1/authenticate", post(db_forward::forward))
		.route("/db/v1/logout", post(db_forward::forward))
		.route("/assets/{*path}", get(ui::serve_static))
		.fallback(ui::serve_index)
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
