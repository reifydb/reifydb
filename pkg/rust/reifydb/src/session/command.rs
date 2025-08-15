// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Engine as EngineInterface, Identity, Params, Transaction},
	result::Frame,
};
use reifydb_engine::Engine;
#[cfg(feature = "async")]
use tokio::task::spawn_blocking;

pub struct CommandSession<T: Transaction> {
	pub(crate) engine: Engine<T>,
	pub(crate) identity: Identity,
}

impl<T: Transaction> CommandSession<T> {
	pub(crate) fn new(engine: Engine<T>, identity: Identity) -> Self {
		Self {
			engine,
			identity,
		}
	}

	pub fn query_sync(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.query_as(&self.identity, &rql, params).map_err(
			|mut err| {
				err.set_statement(rql);
				err
			},
		)
	}

	pub fn command_sync(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();
		self.engine.command_as(&self.identity, &rql, params).map_err(
			|mut err| {
				err.set_statement(rql);
				err
			},
		)
	}

	#[cfg(feature = "async")]
	pub async fn command_async(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();

		let identity = self.identity.clone();
		let engine = self.engine.clone();
		spawn_blocking(move || {
			engine.command_as(&identity, &rql, params).map_err(
				|mut err| {
					err.set_statement(rql.to_string());
					err
				},
			)
		})
		.await
		.unwrap()
	}

	#[cfg(feature = "async")]
	pub async fn query_async(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> crate::Result<Vec<Frame>> {
		let rql = rql.to_string();
		let params = params.into();

		let identity = self.identity.clone();
		let engine = self.engine.clone();
		spawn_blocking(move || {
			engine.query_as(&identity, &rql, params).map_err(
				|mut err| {
					err.set_statement(rql.to_string());
					err
				},
			)
		})
		.await
		.unwrap()
	}
}
