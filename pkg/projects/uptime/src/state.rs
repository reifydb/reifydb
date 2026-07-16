// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb::{
	Clock, Database, auth::service::AuthService, catalog::catalog::Catalog, engine::engine::StandardEngine,
	runtime::context::rng::Rng,
};
use tokio::runtime::Handle;

use crate::cli::RunArgs;

#[derive(Clone)]
pub struct AppState {
	pub engine: StandardEngine,
	pub auth: AuthService,
	pub catalog: Catalog,
	pub clock: Clock,
	pub rng: Rng,
	pub tokio: Handle,
	pub cfg: Arc<RunArgs>,
	pub http: reqwest::Client,
	pub db_auth_base: String,
}

impl AppState {
	pub fn new(db: &Database, cfg: RunArgs) -> Self {
		let engine = db.engine().clone();
		let rng = engine.rng().clone();
		let db_auth_base = format!("http://{}", cfg.reifydb_http_bind);
		Self {
			catalog: db.catalog(),
			auth: db.auth_service().clone(),
			clock: db.clock().clone(),
			tokio: db.runtime().tokio(),
			rng,
			engine,
			cfg: Arc::new(cfg),
			http: reqwest::Client::builder()
				.redirect(reqwest::redirect::Policy::limited(5))
				.build()
				.expect("failed to build http client"),
			db_auth_base,
		}
	}
}
