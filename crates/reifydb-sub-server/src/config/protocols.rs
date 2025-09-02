// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone)]
pub struct ProtocolConfigs {
	pub ws: Option<WebSocketConfig>,
	pub http: Option<HttpConfig>,
}

impl Default for ProtocolConfigs {
	fn default() -> Self {
		Self {
			ws: Some(WebSocketConfig::default()),
			http: None,
		}
	}
}

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
	/// Maximum frame size for WebSocket messages
	pub max_frame_size: usize,

	/// Enable automatic ping/pong for connection keep-alive
	pub enable_ping_pong: bool,

	/// Ping interval in seconds (0 = disabled)
	pub ping_interval: u64,

	/// Maximum time to wait for pong response in seconds
	pub pong_timeout: u64,
}

impl Default for WebSocketConfig {
	fn default() -> Self {
		Self {
			max_frame_size: 16 << 20, // 16MB
			enable_ping_pong: true,
			ping_interval: 30, // 30 seconds
			pong_timeout: 10,  // 10 seconds
		}
	}
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
	/// Maximum request body size
	pub max_request_size: usize,

	/// Request timeout in seconds
	pub request_timeout: u64,

	/// Enable HTTP keep-alive
	pub keep_alive: bool,

	/// Keep-alive timeout in seconds
	pub keep_alive_timeout: u64,
}

impl Default for HttpConfig {
	fn default() -> Self {
		Self {
			max_request_size: 16 << 20, // 16MB
			request_timeout: 30,
			keep_alive: true,
			keep_alive_timeout: 60,
		}
	}
}
