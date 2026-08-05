// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone)]
pub struct OperatorStoreConfig {
	pub freeze_bytes: u64,
	pub max_frozen: usize,
}

impl Default for OperatorStoreConfig {
	fn default() -> Self {
		Self {
			freeze_bytes: 4 * 1024 * 1024,
			max_frozen: 4,
		}
	}
}
