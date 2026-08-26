// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{byte_size::ByteSize, count::Count};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageCacheMetrics {
	pub used: ByteSize,
	pub hits: Count,
	pub misses: Count,
	pub connections_sampled: Count,
	pub connections_total: Count,
}
