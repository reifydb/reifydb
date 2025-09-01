// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Lifecycle hook contexts and implementations

use reifydb_core::{
	Frame,
	event::{EventListener, lifecycle::OnCreateEvent},
	interface::{Engine as _, Identity, Params, Transaction},
	log_error,
};
use reifydb_engine::StandardEngine;

/// Context provided to on_create eventbus
pub struct OnCreateContext<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<'a, T: Transaction> OnCreateContext<T> {
	pub fn new(engine: StandardEngine<T>) -> Self {
		Self {
			engine,
		}
	}

	/// Execute a transactional command as the specified identity
	pub fn command_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::Error> {
		self.engine.command_as(identity, rql, params.into())
	}

	/// Execute a transactional command as root user
	pub fn command_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::Error> {
		let identity = Identity::System {
			id: 0,
			name: "root".to_string(),
		};
		self.engine.command_as(&identity, rql, params.into())
	}

	/// Execute a read-only query as the specified identity
	pub fn query_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::Error> {
		self.engine.query_as(identity, rql, params.into())
	}

	/// Execute a read-only query as root user
	pub fn query_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::Error> {
		let identity = Identity::root();
		self.engine.query_as(&identity, rql, params.into())
	}
}

/// Shared callback implementation for OnCreate hook
pub struct OnCreateEventListener<T: Transaction, F>
where
	F: Fn(&OnCreateContext<T>) -> crate::Result<()> + Send + Sync + 'static,
{
	pub callback: F,
	pub engine: StandardEngine<T>,
}

impl<T: Transaction, F> EventListener<OnCreateEvent>
	for OnCreateEventListener<T, F>
where
	F: Fn(&OnCreateContext<T>) -> crate::Result<()> + Send + Sync + 'static,
{
	fn on(&self, _hook: &OnCreateEvent) {
		let context = OnCreateContext::new(self.engine.clone());
		if let Err(e) = (self.callback)(&context) {
			log_error!("Failed to handle OnCreate event: {}", e);
		}
	}
}
