// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

/// Configuration for the TTL GC actor.
#[derive(Debug, Clone)]
pub struct GcConfig {
	/// How often to run the TTL scan. Default: 60 seconds.
	pub scan_interval: Duration,
	/// Max rows to examine per batch during a scan. Default: 1024.
	pub scan_batch_size: usize,
}

impl Default for GcConfig {
	fn default() -> Self {
		Self {
			scan_interval: Duration::from_secs(60),
			scan_batch_size: 1024,
		}
	}
}
