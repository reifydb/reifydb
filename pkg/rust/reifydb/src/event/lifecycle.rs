// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Lifecycle hook contexts and implementations

use reifydb_core::{
	event::{EventListener, lifecycle::OnCreateEvent},
	interface::auth::Identity,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_type::{params::Params, value::frame::frame::Frame};

/// Context provided to on_create eventbus
pub struct OnCreateContext {
	engine: StandardEngine,
}

impl OnCreateContext {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}

	/// Execute a transactional command as the specified identity.
	pub fn command_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		self.engine.command_as(identity, rql, params.into())
	}

	/// Execute a transactional command as root user.
	pub fn command_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		let identity = Identity::System {
			id: 0,
			name: "root".to_string(),
		};
		self.command_as(&identity, rql, params)
	}

	/// Execute a read-only query as the specified identity.
	pub fn query_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		self.engine.query_as(identity, rql, params.into())
	}

	/// Execute a read-only query as root user.
	pub fn query_as_root(
		&self,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, reifydb_type::error::Error> {
		let identity = Identity::root();
		self.query_as(&identity, rql, params)
	}
}

/// Shared callback implementation for OnCreate hook
pub struct OnCreateEventListener<F>
where
	F: Fn(OnCreateContext) + Send + Sync + 'static,
{
	pub callback: F,
	pub engine: StandardEngine,
}

impl<F> OnCreateEventListener<F>
where
	F: Fn(OnCreateContext) + Send + Sync + 'static,
{
	pub fn new(engine: StandardEngine, callback: F) -> Self {
		Self {
			callback,
			engine,
		}
	}
}

impl<F> EventListener<OnCreateEvent> for OnCreateEventListener<F>
where
	F: Fn(OnCreateContext) + Send + Sync + 'static,
{
	fn on(&self, _event: &OnCreateEvent) {
		let context = OnCreateContext::new(self.engine.clone());
		(self.callback)(context);
	}
}
