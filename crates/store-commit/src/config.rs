// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
	pub close_threshold: ByteSize,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			close_threshold: ByteSize::from_mib(16),
		}
	}
}
