// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

#[derive(Clone, Copy, Debug)]
pub struct FilterConfig {
	pub interval: Duration,
	pub scan_budget: usize,
	pub fill_trigger: f64,
	pub drift_trigger: f64,
	pub size_headroom: f64,
	pub min_size_keys: u64,
}

impl Default for FilterConfig {
	fn default() -> Self {
		Self {
			interval: Duration::from_seconds_const(30),
			scan_budget: 4096,
			fill_trigger: 0.4,
			drift_trigger: 2.0,
			size_headroom: 2.0,
			min_size_keys: 1024,
		}
	}
}
