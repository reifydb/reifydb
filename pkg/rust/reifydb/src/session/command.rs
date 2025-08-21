// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_core::{
	interface::{Engine as EngineInterface, Identity, Params, Transaction},
	result::Frame,
};
use reifydb_engine::StandardEngine;
#[cfg(feature = "async")]
use tokio::task::spawn_blocking;

#[cfg(feature = "async")]
use crate::session::CommandSessionAsync;
use crate::session::CommandSessionSync;

pub struct CommandSession<T: Transaction> {
	pub(crate) engine: StandardEngine<T>,
	pub(crate) identity: Identity,
}

impl<T: Transaction> CommandSession<T> {
	pub(crate) fn new(
		engine: StandardEngine<T>,
		identity: Identity,
	) -> Self {
		Self {
			engine,
			identity,
		}
	}
}

impl<T: Transaction> CommandSessionSync<T> for CommandSession<T> {
	fn command(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.command_as(&self.identity, &rql, params).map_err(
			|mut err| {
				err.with_statement(rql);
				err
			},
		)
	}
}

#[cfg(feature = "async")]
impl<T: Transaction> CommandSessionAsync<T> for CommandSession<T> {
	async fn command(
		&self,
		rql: &str,
		params: impl Into<Params> + Send,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();

		let identity = self.identity.clone();
		let engine = self.engine.clone();
		spawn_blocking(move || {
			engine.command_as(&identity, &rql, params).map_err(
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
