// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Statistics about pool usage
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
	pub available: usize,
	pub total_acquired: usize,
	pub total_released: usize,
}
