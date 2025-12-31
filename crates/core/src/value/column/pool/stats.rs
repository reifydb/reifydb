// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

/// Statistics about pool usage
#[derive(Debug, Clone)]
pub struct PoolStats {
	pub available: usize,
	pub total_acquired: usize,
	pub total_released: usize,
}

impl Default for PoolStats {
	fn default() -> Self {
		Self {
			available: 0,
			total_acquired: 0,
			total_released: 0,
		}
	}
}
