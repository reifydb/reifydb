// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

/// Configuration for the TTL GC actor.
#[derive(Debug, Clone)]
pub struct Config {
	/// How often to run the TTL scan. Default: 60 seconds.
	pub scan_interval: Duration,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			scan_interval: Duration::from_secs(60),
		}
	}
}
