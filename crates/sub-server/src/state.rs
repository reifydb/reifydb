// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_auth::service::AuthService;
use reifydb_core::actors::server::ServerMessage;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSystem},
	},
	context::{clock::Clock, rng::Rng},
};
use tracing::instrument;

use crate::{actor::ServerActor, interceptor::RequestInterceptorChain};

#[derive(Debug, Clone)]
pub struct StateConfig {
	pub query_timeout: Duration,

	pub request_timeout: Duration,

	pub max_connections: usize,

	pub admin_enabled: bool,
}

impl Default for StateConfig {
	fn default() -> Self {
		Self {
			query_timeout: Duration::from_secs(30),
			request_timeout: Duration::from_secs(60),
			max_connections: 10_000,
			admin_enabled: false,
		}
	}
}

impl StateConfig {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn query_timeout(mut self, timeout: Duration) -> Self {
		self.query_timeout = timeout;
		self
	}

	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	pub fn max_connections(mut self, max: usize) -> Self {
		self.max_connections = max;
		self
	}

	pub fn admin_enabled(mut self, enabled: bool) -> Self {
		self.admin_enabled = enabled;
		self
	}
}

#[derive(Clone)]
pub struct AppState {
	actor_system: ActorSystem,
	engine: StandardEngine,
	auth_service: AuthService,
	config: StateConfig,
	request_interceptors: RequestInterceptorChain,
	clock: Clock,
	rng: Rng,
}

impl AppState {
	pub fn new(
		actor_system: ActorSystem,
		engine: StandardEngine,
		auth_service: AuthService,
		config: StateConfig,
		request_interceptors: RequestInterceptorChain,
		clock: Clock,
		rng: Rng,
	) -> Self {
		Self {
			actor_system,
			engine,
			auth_service,
			config,
			request_interceptors,
			clock,
			rng,
		}
	}

	pub fn clone_with_config(&self, config: StateConfig) -> Self {
		Self {
			actor_system: self.actor_system.clone(),
			engine: self.engine.clone(),
			auth_service: self.auth_service.clone(),
			config,
			request_interceptors: self.request_interceptors.clone(),
			clock: self.clock.clone(),
			rng: self.rng.clone(),
		}
	}

	#[inline]
	pub fn actor_system(&self) -> ActorSystem {
		self.actor_system.clone()
	}

	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	#[inline]
	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}

	#[inline]
	pub fn config(&self) -> &StateConfig {
		&self.config
	}

	#[inline]
	pub fn query_timeout(&self) -> Duration {
		self.config.query_timeout
	}

	#[inline]
	pub fn request_timeout(&self) -> Duration {
		self.config.request_timeout
	}

	#[inline]
	pub fn max_connections(&self) -> usize {
		self.config.max_connections
	}

	#[inline]
	pub fn admin_enabled(&self) -> bool {
		self.config.admin_enabled
	}

	#[inline]
	pub fn request_interceptors(&self) -> &RequestInterceptorChain {
		&self.request_interceptors
	}

	#[inline]
	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	#[inline]
	pub fn rng(&self) -> &Rng {
		&self.rng
	}

	#[inline]
	pub fn auth_service(&self) -> &AuthService {
		&self.auth_service
	}

	#[instrument(name = "actor::spawn_server", level = "debug", skip_all)]
	pub fn spawn_server_actor(&self) -> (ActorRef<ServerMessage>, ActorHandle<ServerMessage>) {
		let actor = ServerActor::new(self.engine.clone(), self.auth_service.clone(), self.clock.clone());
		let handle = self.actor_system.spawn_query("server-req", actor);
		let actor_ref = handle.actor_ref().clone();
		(actor_ref, handle)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_query_defaults() {
		let config = StateConfig::default();
		assert_eq!(config.query_timeout, Duration::from_secs(30));
		assert_eq!(config.request_timeout, Duration::from_secs(60));
		assert_eq!(config.max_connections, 10_000);
	}

	#[test]
	fn test_query_config_builder() {
		let config = StateConfig::new()
			.query_timeout(Duration::from_secs(60))
			.request_timeout(Duration::from_secs(120))
			.max_connections(5_000);

		assert_eq!(config.query_timeout, Duration::from_secs(60));
		assert_eq!(config.request_timeout, Duration::from_secs(120));
		assert_eq!(config.max_connections, 5_000);
	}
}
