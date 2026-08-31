// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod tiers;
pub mod typed;

use reifydb_core::default;
use reifydb_store::tier::point::{PointConfig, PointMetrics};
use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug)]
pub struct OperatorPointConfig {
	pub tier_bytes: Option<ByteSize>,
}

impl OperatorPointConfig {
	pub fn testing() -> Self {
		Self {
			tier_bytes: Some(default::store::OPERATOR_POINT_TIER_TESTING),
		}
	}
}

impl From<OperatorPointConfig> for PointConfig {
	fn from(config: OperatorPointConfig) -> Self {
		Self {
			shard_bytes: config.tier_bytes,
			shards: 1,
		}
	}
}
pub use tiers::OperatorPointKeyspaceMetrics;

pub type OperatorPointMetrics = PointMetrics;
