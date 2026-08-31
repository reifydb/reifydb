// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod scan;
#[cfg(test)]
mod surface;
pub mod tiers;
pub mod typed;

use reifydb_core::default;
use reifydb_store::{
	coverage::plan::DEFAULT_GAP_GUARD,
	tier::range::{RangeConfig, RangeMetrics},
};
use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug)]
pub struct OperatorRangeConfig {
	pub tier_bytes: Option<ByteSize>,
	pub gap_guard: usize,
}

impl OperatorRangeConfig {
	pub fn testing() -> Self {
		Self {
			tier_bytes: Some(default::store::OPERATOR_RANGE_TIER_TESTING),
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}
}

impl From<OperatorRangeConfig> for RangeConfig {
	fn from(config: OperatorRangeConfig) -> Self {
		Self {
			shard_bytes: config.tier_bytes,
			shards: 1,
			gap_guard: config.gap_guard,
		}
	}
}
pub use tiers::OperatorRangeKeyspaceMetrics;

pub type OperatorRangeMetrics = RangeMetrics;
