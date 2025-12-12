// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{NetworkConfig, ProtocolConfigs};

/// Main server configuration supporting multiple protocols
#[derive(Debug, Clone)]
pub struct ServerConfig {
	/// HTTP server bind address. None means HTTP is disabled.
	pub http_bind_addr: Option<String>,
	/// WebSocket server bind address. None means WS is disabled.
	pub ws_bind_addr: Option<String>,
	/// Network and performance configuration
	pub network: NetworkConfig,
	/// Protocol-specific configurations
	pub protocols: ProtocolConfigs,
}

impl ServerConfig {
	/// Create a new ServerConfig with defaults
	pub fn new() -> Self {
		Self::default()
	}

	/// Set HTTP bind address. Pass None to disable HTTP server.
	pub fn http_bind_addr<S: Into<String>>(mut self, addr: Option<S>) -> Self {
		self.http_bind_addr = addr.map(|s| s.into());
		self
	}

	/// Set WebSocket bind address. Pass None to disable WS server.
	pub fn ws_bind_addr<S: Into<String>>(mut self, addr: Option<S>) -> Self {
		self.ws_bind_addr = addr.map(|s| s.into());
		self
	}

	/// Configure network settings
	pub fn network(mut self, config: NetworkConfig) -> Self {
		self.network = config;
		self
	}

	/// Configure protocols
	pub fn protocols(mut self, config: ProtocolConfigs) -> Self {
		self.protocols = config;
		self
	}

	/// Enable WebSocket protocol with configuration
	pub fn enable_websocket(mut self, config: Option<super::WebSocketConfig>) -> Self {
		self.protocols.ws = config.or_else(|| Some(Default::default()));
		self
	}

	/// Enable HTTP protocol with configuration
	pub fn enable_http(mut self, config: Option<super::HttpConfig>) -> Self {
		self.protocols.http = config.or_else(|| Some(Default::default()));
		self
	}

	/// Get the effective number of listeners
	pub fn effective_listeners(&self) -> usize {
		self.network.listeners.unwrap_or(1)
	}
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			http_bind_addr: Some("0.0.0.0:8090".to_string()),
			ws_bind_addr: Some("0.0.0.0:8091".to_string()),
			network: NetworkConfig::default(),
			protocols: ProtocolConfigs::default(),
		}
	}
}
