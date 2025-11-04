// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone)]
pub struct NetworkConfig {
	/// Number of worker threads (defaults to number of physical CPU cores)
	pub listeners: Option<usize>,

	/// Enable SO_REUSEPORT for load balancing across workers
	pub reuse_port: bool,

	/// Pin worker threads to specific CPU cores for cache locality
	pub pin_threads: bool,

	/// TCP_NODELAY setting for connections
	pub nodelay: bool,

	/// Maximum buffer size for outgoing data per connection
	pub max_outbox_bytes: usize,

	/// Maximum number of connections per worker
	pub max_connections_per_worker: usize,
}

impl Default for NetworkConfig {
	fn default() -> Self {
		Self {
			listeners: None,
			reuse_port: true,
			pin_threads: true,
			nodelay: true,
			max_outbox_bytes: 1 << 20, // 1MB per connection
			max_connections_per_worker: 1_000,
		}
	}
}
