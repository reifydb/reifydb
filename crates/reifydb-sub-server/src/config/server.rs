// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{NetworkConfig, ProtocolConfigs};

/// Main server configuration supporting multiple protocols
#[derive(Debug, Clone)]
pub struct ServerConfig {
	/// Bind address and port
	pub bind_addr: String,

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

	/// Set the bind address
	pub fn bind_addr<S: Into<String>>(mut self, addr: S) -> Self {
		self.bind_addr = addr.into();
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
	pub fn enable_websocket(
		mut self,
		config: Option<super::WebSocketConfig>,
	) -> Self {
		self.protocols.ws = config.or_else(|| Some(Default::default()));
		self
	}

	/// Enable HTTP protocol with configuration
	pub fn enable_http(
		mut self,
		config: Option<super::HttpConfig>,
	) -> Self {
		self.protocols.http =
			config.or_else(|| Some(Default::default()));
		self
	}

	/// Get the effective number of workers
	pub fn effective_workers(&self) -> usize {
		self.network
			.workers
			.unwrap_or_else(|| num_cpus::get_physical().max(1))
	}
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8090".to_string(),
			network: NetworkConfig::default(),
			protocols: ProtocolConfigs::default(),
		}
	}
}
