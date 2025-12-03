// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Lifecycle hook contexts and implementations

use reifydb_core::{
	Frame,
	event::{EventListener, lifecycle::OnCreateEvent},
	interface::{Engine as _, Identity, Params},
};
use reifydb_engine::StandardEngine;
use tracing::error;

/// Context provided to on_create eventbus
pub struct OnCreateContext {
	engine: StandardEngine,
}

impl<'a> OnCreateContext {
	pub fn new(engine: StandardEngine) -> Self {
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
	pub fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::Error> {
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
	pub fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, reifydb_type::Error> {
		let identity = Identity::root();
		self.engine.query_as(&identity, rql, params.into())
	}
}

/// Shared callback implementation for OnCreate hook
pub struct OnCreateEventListener<F>
where
	F: Fn(&OnCreateContext) -> crate::Result<()> + Send + Sync + 'static,
{
	pub callback: F,
	pub engine: StandardEngine,
}

impl<F> EventListener<OnCreateEvent> for OnCreateEventListener<F>
where
	F: Fn(&OnCreateContext) -> crate::Result<()> + Send + Sync + 'static,
{
	fn on(&self, _hook: &OnCreateEvent) {
		let context = OnCreateContext::new(self.engine.clone());
		if let Err(e) = (self.callback)(&context) {
			error!("Failed to handle OnCreate event: {}", e);
		}
	}
}
