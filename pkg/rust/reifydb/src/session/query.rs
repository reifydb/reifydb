// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(feature = "async")]
use crate::session::QuerySessionAsync;
use crate::session::QuerySessionSync;
use reifydb_core::{
    interface::{Engine as EngineInterface, Identity, Params, Transaction},
    result::Frame,
};
use reifydb_engine::StandardEngine;
#[cfg(feature = "async")]
use tokio::task::spawn_blocking;

/// Session for executing read-only database queries
pub struct QuerySession<T: Transaction> {
	pub(crate) engine: StandardEngine<T>,
	pub(crate) identity: Identity,
}

impl<T: Transaction> QuerySessionSync<T> for QuerySession<T> {
	fn query(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.query_as(&self.identity, &rql, params).map_err(
			|mut err| {
				err.with_statement(rql);
				err
			},
		)
	}
}

#[cfg(feature = "async")]
impl<T: Transaction> QuerySessionAsync<T> for QuerySession<T> {
	async fn query(
		&self,
		rql: &str,
		params: impl Into<Params> + Send,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();

		let identity = self.identity.clone();
		let engine = self.engine.clone();
		spawn_blocking(move || {
			engine.query_as(&identity, &rql, params).map_err(
				|mut err| {
					err.with_statement(rql.to_string());
					err
				},
			)
		})
		.await
			.unwrap()
	}
}
