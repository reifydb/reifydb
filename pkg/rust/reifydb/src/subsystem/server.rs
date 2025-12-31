// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Server subsystem that combines HTTP and WebSocket servers.
//!
//! This module provides `ServerSubsystem` which manages the lifecycle of
//! both HTTP and WebSocket servers using a shared tokio runtime.

use std::any::Any;

use async_trait::async_trait;
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_core::ioc::IocContainer;
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_sub_server::{AppState, QueryConfig};
use reifydb_sub_server_http::HttpSubsystem;
use reifydb_sub_server_ws::WsSubsystem;

/// Configuration for the server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
	/// HTTP server bind address (e.g., "0.0.0.0:8090")
	/// If None, HTTP server is disabled
	pub http_bind_addr: Option<String>,
	/// WebSocket server bind address (e.g., "0.0.0.0:8091")
	/// If None, WebSocket server is disabled
	pub ws_bind_addr: Option<String>,
	/// Query configuration
	pub query_config: QueryConfig,
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			http_bind_addr: Some("0.0.0.0:8090".to_string()),
			ws_bind_addr: Some("0.0.0.0:8091".to_string()),
			query_config: QueryConfig::default(),
		}
	}
}

impl ServerConfig {
	/// Create a new ServerConfig with defaults.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the HTTP bind address. Pass None to disable HTTP server.
	pub fn http_bind_addr<S: Into<String>>(mut self, addr: Option<S>) -> Self {
		self.http_bind_addr = addr.map(|s| s.into());
		self
	}

	/// Set the WebSocket bind address. Pass None to disable WebSocket.
	pub fn ws_bind_addr<S: Into<String>>(mut self, addr: Option<S>) -> Self {
		self.ws_bind_addr = addr.map(|s| s.into());
		self
	}

	/// Set the query configuration.
	pub fn query_config(mut self, config: QueryConfig) -> Self {
		self.query_config = config;
		self
	}
}

/// Server subsystem combining HTTP and WebSocket servers.
///
/// This subsystem manages:
/// - An HTTP server (always enabled)
/// - An optional WebSocket server
///
/// Both servers share the same `AppState` containing the engine and configuration.
pub struct ServerSubsystem {
	config: ServerConfig,
	engine: StandardEngine,
	http_subsystem: Option<HttpSubsystem>,
	ws_subsystem: Option<WsSubsystem>,
}

impl ServerSubsystem {
	/// Create a new server subsystem.
	///
	/// # Arguments
	///
	/// * `config` - Server configuration
	/// * `engine` - Database engine for query execution
	pub fn new(config: ServerConfig, engine: StandardEngine) -> Self {
		Self {
			config,
			engine,
			http_subsystem: None,
			ws_subsystem: None,
		}
	}

	/// Get the HTTP server's bind address, if enabled.
	pub fn http_bind_addr(&self) -> Option<&str> {
		self.config.http_bind_addr.as_deref()
	}

	/// Get the WebSocket server's bind address, if enabled.
	pub fn ws_bind_addr(&self) -> Option<&str> {
		self.config.ws_bind_addr.as_deref()
	}

	/// Get the HTTP server's actual bound port (available after start).
	pub fn http_port(&self) -> Option<u16> {
		self.http_subsystem.as_ref().and_then(|h| h.port())
	}

	/// Get the WebSocket server's actual bound port (available after start).
	pub fn ws_port(&self) -> Option<u16> {
		self.ws_subsystem.as_ref().and_then(|w| w.port())
	}
}

impl HasVersion for ServerSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "server".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "HTTP and WebSocket server subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

#[async_trait]
impl Subsystem for ServerSubsystem {
	fn name(&self) -> &'static str {
		"Server"
	}

	async fn start(&mut self) -> reifydb_core::Result<()> {
		// Idempotent: if already running, return success
		if self.http_subsystem.is_some() || self.ws_subsystem.is_some() {
			return Ok(());
		}

		tracing::info!("Starting server subsystem");

		// Create shared application state
		let state = AppState::new(self.engine.clone(), self.config.query_config.clone());

		// Create and start HTTP subsystem if configured
		let http = if let Some(ref http_addr) = self.config.http_bind_addr {
			let mut http = HttpSubsystem::new(http_addr.clone(), state.clone());
			http.start().await?;
			tracing::info!("HTTP server started on {}", http_addr);
			Some(http)
		} else {
			None
		};

		// Create and start WebSocket subsystem if configured
		let ws = if let Some(ref ws_addr) = self.config.ws_bind_addr {
			let mut ws = WsSubsystem::new(ws_addr.clone(), state);
			ws.start().await?;
			tracing::info!("WebSocket server started on {}", ws_addr);
			Some(ws)
		} else {
			None
		};

		self.http_subsystem = http;
		self.ws_subsystem = ws;

		Ok(())
	}

	async fn shutdown(&mut self) -> reifydb_core::Result<()> {
		tracing::info!("Shutting down server subsystem");

		// Shutdown WebSocket first (if enabled)
		if let Some(ref mut ws) = self.ws_subsystem {
			ws.shutdown().await?;
		}
		self.ws_subsystem = None;

		// Shutdown HTTP
		if let Some(ref mut http) = self.http_subsystem {
			http.shutdown().await?;
		}
		self.http_subsystem = None;

		tracing::info!("Server subsystem shutdown complete");
		Ok(())
	}

	fn is_running(&self) -> bool {
		// Either server running counts as running
		self.http_subsystem.as_ref().map_or(false, |h| h.is_running())
			|| self.ws_subsystem.as_ref().map_or(false, |w| w.is_running())
	}

	fn health_status(&self) -> HealthStatus {
		let http_enabled = self.config.http_bind_addr.is_some();
		let ws_enabled = self.config.ws_bind_addr.is_some();
		let http_running = self.http_subsystem.as_ref().map_or(false, |h| h.is_running());
		let ws_running = self.ws_subsystem.as_ref().map_or(false, |w| w.is_running());

		// Check if enabled servers are running
		let http_ok = !http_enabled || http_running;
		let ws_ok = !ws_enabled || ws_running;

		if http_ok && ws_ok {
			HealthStatus::Healthy
		} else if http_running || ws_running {
			HealthStatus::Degraded {
				description: format!(
					"HTTP: {}, WebSocket: {}",
					if http_enabled {
						if http_running { "running" } else { "stopped" }
					} else {
						"disabled"
					},
					if ws_enabled {
						if ws_running { "running" } else { "stopped" }
					} else {
						"disabled"
					}
				),
			}
		} else {
			HealthStatus::Failed {
				description: "All servers stopped".to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

/// Factory for creating server subsystem instances.
///
/// This factory creates a `ServerSubsystem` which manages both
/// HTTP and WebSocket servers using a shared tokio runtime.
pub struct ServerSubsystemFactory {
	config: ServerConfig,
}

impl ServerSubsystemFactory {
	/// Create a new server subsystem factory with the given configuration.
	pub fn new(config: ServerConfig) -> Self {
		Self { config }
	}
}

#[async_trait]
impl SubsystemFactory<StandardCommandTransaction> for ServerSubsystemFactory {
	async fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let subsystem = ServerSubsystem::new(self.config, engine);
		Ok(Box::new(subsystem))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_config_defaults() {
		let config = ServerConfig::default();
		assert_eq!(config.http_bind_addr, Some("0.0.0.0:8090".to_string()));
		assert_eq!(config.ws_bind_addr, Some("0.0.0.0:8091".to_string()));
	}

	#[test]
	fn test_config_builder() {
		let config = ServerConfig::new()
			.http_bind_addr(Some("127.0.0.1:9000"))
			.ws_bind_addr(Some("127.0.0.1:9001"));

		assert_eq!(config.http_bind_addr, Some("127.0.0.1:9000".to_string()));
		assert_eq!(config.ws_bind_addr, Some("127.0.0.1:9001".to_string()));
	}

	#[test]
	fn test_config_disable_http() {
		let config = ServerConfig::new().http_bind_addr(None::<String>);
		assert!(config.http_bind_addr.is_none());
	}

	#[test]
	fn test_config_disable_ws() {
		let config = ServerConfig::new().ws_bind_addr(None::<String>);
		assert!(config.ws_bind_addr.is_none());
	}

	#[test]
	fn test_config_http_only() {
		let config = ServerConfig::new()
			.http_bind_addr(Some("127.0.0.1:9000"))
			.ws_bind_addr(None::<String>);

		assert_eq!(config.http_bind_addr, Some("127.0.0.1:9000".to_string()));
		assert!(config.ws_bind_addr.is_none());
	}

	#[test]
	fn test_config_ws_only() {
		let config = ServerConfig::new()
			.http_bind_addr(None::<String>)
			.ws_bind_addr(Some("127.0.0.1:9001"));

		assert!(config.http_bind_addr.is_none());
		assert_eq!(config.ws_bind_addr, Some("127.0.0.1:9001".to_string()));
	}
}
