// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone)]
pub struct ProtocolConfigs {
	pub websocket: Option<WebSocketConfig>,
	pub http: Option<HttpConfig>,
	pub binary: Option<BinaryConfig>,
}

impl Default for ProtocolConfigs {
	fn default() -> Self {
		Self {
			websocket: Some(WebSocketConfig::default()),
			http: None,
			binary: None,
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

	/// Enable CORS support
	pub enable_cors: bool,

	/// CORS allowed origins (empty = allow all)
	pub cors_origins: Vec<String>,
}

impl Default for HttpConfig {
	fn default() -> Self {
		Self {
			max_request_size: 16 << 20, // 16MB
			request_timeout: 30,
			keep_alive: true,
			keep_alive_timeout: 60,
			enable_cors: true,
			cors_origins: vec![], // Allow all origins by default
		}
	}
}

#[derive(Debug, Clone)]
pub struct BinaryConfig {
	/// Magic bytes for protocol identification
	pub magic: [u8; 4],

	/// Protocol version
	pub version: u16,

	/// Maximum message size for binary protocol
	pub max_message_size: usize,

	/// Enable compression
	pub enable_compression: bool,

	/// Compression level (if enabled)
	pub compression_level: u8,
}

impl Default for BinaryConfig {
	fn default() -> Self {
		Self {
			magic: [0x52, 0x44, 0x42, 0x01], // "RDB\x01"
			version: 1,
			max_message_size: 64 << 20, // 64MB
			enable_compression: true,
			compression_level: 6, // Balanced compression level
		}
	}
}
