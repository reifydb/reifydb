// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Configuration for the independent flow processing runtime.

use std::time::Duration;

use tokio::runtime::Builder;

/// Configuration for the flow processing runtime.
#[derive(Debug, Clone)]
pub struct FlowRuntimeConfig {
	/// Number of async worker threads for the tokio runtime.
	pub async_workers: usize,

	/// Maximum number of blocking threads for spawn_blocking.
	pub blocking_workers: usize,

	/// Thread name prefix for worker threads.
	pub thread_name: String,

	/// Timeout for graceful shutdown drain.
	pub drain_timeout: Duration,
}

impl Default for FlowRuntimeConfig {
	fn default() -> Self {
		Self {
			async_workers: num_cpus::get(),
			blocking_workers: 128,
			thread_name: "flow-worker".to_string(),
			drain_timeout: Duration::from_secs(30),
		}
	}
}

impl FlowRuntimeConfig {
	/// Build a tokio runtime with this configuration.
	pub fn build_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
		Builder::new_multi_thread()
			.worker_threads(self.async_workers)
			.max_blocking_threads(self.blocking_workers)
			.thread_name(&self.thread_name)
			.enable_all()
			.build()
	}
}
