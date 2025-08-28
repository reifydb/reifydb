// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

/// Configuration for the high-performance WebSocket server
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
	/// Address to bind the server to (e.g., "0.0.0.0:8091")
	pub bind_addr: String,

	/// Number of worker threads (defaults to physical CPU count)
	pub workers: Option<usize>,

	/// Enable SO_REUSEPORT for load balancing across workers
	pub reuse_port: bool,

	/// Pin worker threads to specific CPU cores
	pub pin_threads: bool,

	/// Maximum bytes that can be queued per connection for sending
	pub max_outbox_bytes: usize,
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8091".to_string(),
			workers: None, /* Will default to physical CPU count
			                * at runtime */
			reuse_port: true,
			pin_threads: true,
			max_outbox_bytes: 1 << 20, // 1MB
		}
	}
}
