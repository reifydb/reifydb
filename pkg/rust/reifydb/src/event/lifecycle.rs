// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Lifecycle hook contexts and implementations

use async_trait::async_trait;
use futures_util::TryStreamExt;
use reifydb_core::{
	Frame,
	event::{EventListener, lifecycle::OnCreateEvent},
	interface::{Engine as _, Identity, Params},
	stream::StreamError,
};
use reifydb_engine::StandardEngine;
use tracing::error;

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
	pub async fn command_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, StreamError> {
		self.engine.command_as(identity, rql, params.into()).try_collect().await
	}

	/// Execute a transactional command as root user.
	pub async fn command_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, StreamError> {
		let identity = Identity::System {
			id: 0,
			name: "root".to_string(),
		};
		self.command_as(&identity, rql, params).await
	}

	/// Execute a read-only query as the specified identity.
	pub async fn query_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: impl Into<Params>,
	) -> Result<Vec<Frame>, StreamError> {
		self.engine.query_as(identity, rql, params.into()).try_collect().await
	}

	/// Execute a read-only query as root user.
	pub async fn query_as_root(&self, rql: &str, params: impl Into<Params>) -> Result<Vec<Frame>, StreamError> {
		let identity = Identity::root();
		self.query_as(&identity, rql, params).await
	}
}

/// Shared callback implementation for OnCreate hook
pub struct OnCreateEventListener<F, Fut>
where
	F: Fn(OnCreateContext) -> Fut + Send + Sync + 'static,
	Fut: std::future::Future<Output = crate::Result<()>> + Send + 'static,
{
	pub callback: F,
	pub engine: StandardEngine,
	_marker: std::marker::PhantomData<fn() -> Fut>,
}

impl<F, Fut> OnCreateEventListener<F, Fut>
where
	F: Fn(OnCreateContext) -> Fut + Send + Sync + 'static,
	Fut: std::future::Future<Output = crate::Result<()>> + Send + 'static,
{
	pub fn new(engine: StandardEngine, callback: F) -> Self {
		Self {
			callback,
			engine,
			_marker: std::marker::PhantomData,
		}
	}
}

#[async_trait]
impl<F, Fut> EventListener<OnCreateEvent> for OnCreateEventListener<F, Fut>
where
	F: Fn(OnCreateContext) -> Fut + Send + Sync + 'static,
	Fut: std::future::Future<Output = crate::Result<()>> + Send + 'static,
{
	async fn on(&self, _hook: &OnCreateEvent) {
		let context = OnCreateContext::new(self.engine.clone());
		if let Err(e) = (self.callback)(context).await {
			error!("Failed to handle OnCreate event: {}", e);
		}
	}
}
