// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Configuration for the flow processing runtime.

use std::time::Duration;

/// Configuration for the flow processing runtime.
#[derive(Debug, Clone)]
pub struct FlowRuntimeConfig {
	/// Thread name prefix for worker thread.
	pub thread_name: String,

	/// Timeout for graceful shutdown drain.
	pub drain_timeout: Duration,

	/// Poll interval for CDC changes.
	pub poll_interval: Duration,
}

impl Default for FlowRuntimeConfig {
	fn default() -> Self {
		Self {
			thread_name: "flow-loop".to_string(),
			drain_timeout: Duration::from_secs(30),
			poll_interval: Duration::from_millis(1),
		}
	}
}
