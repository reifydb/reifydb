// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
	pub close_threshold: ByteSize,
	pub merge_fan_in: usize,
	pub merge_cap: ByteSize,
	pub merge_backlog: usize,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			close_threshold: ByteSize::from_mib(16),
			merge_fan_in: 4,
			merge_cap: ByteSize::from_mib(256),
			merge_backlog: 16,
		}
	}
}
