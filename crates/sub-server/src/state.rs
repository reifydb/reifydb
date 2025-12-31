// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Application state shared across request handlers.
//!
//! This module provides the shared state that is passed to all HTTP and WebSocket
//! handlers, including the database engine and query configuration.

use std::time::Duration;

use reifydb_engine::StandardEngine;

/// Configuration for query execution.
#[derive(Debug, Clone)]
pub struct QueryConfig {
	/// Timeout for individual query execution.
	/// If a query takes longer than this, it will be cancelled.
	pub query_timeout: Duration,

	/// Timeout for entire HTTP request lifecycle.
	/// This includes reading the request, executing the query, and writing the response.
	pub request_timeout: Duration,

	/// Maximum concurrent connections allowed.
	/// New connections beyond this limit will be rejected.
	pub max_connections: usize,
}

impl Default for QueryConfig {
	fn default() -> Self {
		Self {
			query_timeout: Duration::from_secs(30),
			request_timeout: Duration::from_secs(60),
			max_connections: 10_000,
		}
	}
}

impl QueryConfig {
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
}

/// Shared application state passed to all request handlers.
///
/// This struct is cloneable and cheap to clone since `StandardEngine` uses
/// `Arc` internally. Each handler receives a clone of this state.
///
/// # Example
///
/// ```ignore
/// let state = AppState::new(engine, QueryConfig::default());
///
/// // In an axum handler:
/// async fn handle_query(State(state): State<AppState>, ...) {
///     let engine = state.engine();
///     // ...
/// }
/// ```
#[derive(Clone)]
pub struct AppState {
	engine: StandardEngine,
	config: QueryConfig,
}

impl AppState {
	/// Create a new AppState with the given engine and configuration.
	pub fn new(engine: StandardEngine, config: QueryConfig) -> Self {
		Self {
			engine,
			config,
		}
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
	pub fn config(&self) -> &QueryConfig {
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
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_query_config_defaults() {
		let config = QueryConfig::default();
		assert_eq!(config.query_timeout, Duration::from_secs(30));
		assert_eq!(config.request_timeout, Duration::from_secs(60));
		assert_eq!(config.max_connections, 10_000);
	}

	#[test]
	fn test_query_config_builder() {
		let config = QueryConfig::new()
			.query_timeout(Duration::from_secs(60))
			.request_timeout(Duration::from_secs(120))
			.max_connections(5_000);

		assert_eq!(config.query_timeout, Duration::from_secs(60));
		assert_eq!(config.request_timeout, Duration::from_secs(120));
		assert_eq!(config.max_connections, 5_000);
	}
}
