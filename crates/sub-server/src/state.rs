// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Application state shared across request handler.
//!
//! This module provides the shared state that is passed to all HTTP and WebSocket
//! handler, including the database engine and query configuration.

use std::time::Duration;

use reifydb_auth::service::AuthService;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{clock::Clock, rng::Rng},
};

use crate::interceptor::RequestInterceptorChain;

/// Configuration for query execution.
#[derive(Debug, Clone)]
pub struct StateConfig {
	/// Timeout for individual query execution.
	/// If a query takes longer than this, it will be cancelled.
	pub query_timeout: Duration,
	/// Timeout for entire HTTP request lifecycle.
	/// This includes reading the request, executing the query, and writing the response.
	pub request_timeout: Duration,
	/// Maximum concurrent connections allowed.
	/// New connections beyond this limit will be rejected.
	pub max_connections: usize,
	/// Whether admin (DDL) operations are enabled on this listener.
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
	/// Create a new QueryConfig with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the query timeout.
	pub fn query_timeout(mut self, timeout: Duration) -> Self {
		self.query_timeout = timeout;
		self
	}

	/// Set the request timeout.
	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	/// Set the maximum connections.
	pub fn max_connections(mut self, max: usize) -> Self {
		self.max_connections = max;
		self
	}

	/// Set whether admin operations are enabled.
	pub fn admin_enabled(mut self, enabled: bool) -> Self {
		self.admin_enabled = enabled;
		self
	}
}

/// Shared application state passed to all request handler.
///
/// This struct is cloneable and cheap to clone since `StandardEngine` uses
/// `Arc` internally. Each handler receives a clone of this state.
///
/// # Example
///
/// ```ignore
/// let state = AppState::new(actor_system, engine, QueryConfig::default(), interceptors);
///
/// // In an axum handler:
/// async fn handle_query(State(state): State<AppState>, ...) {
///     let system = state.actor_system();
///     let engine = state.engine();
///     // ...
/// }
/// ```
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
	/// Create a new AppState with the given actor system, engine, configuration,
	/// and request interceptor chain.
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

	/// Clone this state with a different configuration, preserving the
	/// interceptor chain and other shared resources.
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

	/// Get a clone of the actor system.
	///
	/// This is cheap since `ActorSystem` uses `Arc` internally.
	#[inline]
	pub fn actor_system(&self) -> ActorSystem {
		self.actor_system.clone()
	}

	/// Get a reference to the database engine.
	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	/// Get a clone of the database engine.
	///
	/// This is cheap since `StandardEngine` uses `Arc` internally.
	#[inline]
	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}

	/// Get a reference to the query configuration.
	#[inline]
	pub fn config(&self) -> &StateConfig {
		&self.config
	}

	/// Get the query timeout from configuration.
	#[inline]
	pub fn query_timeout(&self) -> Duration {
		self.config.query_timeout
	}

	/// Get the request timeout from configuration.
	#[inline]
	pub fn request_timeout(&self) -> Duration {
		self.config.request_timeout
	}

	/// Get the maximum connections from configuration.
	#[inline]
	pub fn max_connections(&self) -> usize {
		self.config.max_connections
	}

	/// Get whether admin operations are enabled.
	#[inline]
	pub fn admin_enabled(&self) -> bool {
		self.config.admin_enabled
	}

	/// Get a reference to the request interceptor chain.
	#[inline]
	pub fn request_interceptors(&self) -> &RequestInterceptorChain {
		&self.request_interceptors
	}

	/// Get a reference to the clock.
	#[inline]
	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	/// Get a reference to the RNG.
	#[inline]
	pub fn rng(&self) -> &Rng {
		&self.rng
	}

	/// Get a reference to the authentication service.
	#[inline]
	pub fn auth_service(&self) -> &AuthService {
		&self.auth_service
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_query_configaults() {
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
