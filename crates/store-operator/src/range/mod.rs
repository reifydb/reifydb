// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod evict;
pub mod partition;
#[cfg(test)]
mod scan;
#[cfg(test)]
mod surface;
pub mod tiers;
pub mod typed;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{default, interface::catalog::flow::OperatorId, key::operator::state::GroupId};
use reifydb_store::{
	coverage::plan::DEFAULT_GAP_GUARD,
	tier::range::{DEFAULT_COVERAGE_INTERVALS, RangeConfig, RangeMetrics},
};
use reifydb_value::byte_size::ByteSize;

use crate::range::tiers::{RangeKeyspaceMetrics, RangeTiers};

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
			coverage_bytes: None,
			coverage_intervals: DEFAULT_COVERAGE_INTERVALS,
		}
	}
}

pub type OperatorRangeMetrics = RangeMetrics;

pub trait RangeSink: Send + Sync + 'static {
	fn lookup(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>>;

	fn overwrite(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow);

	fn insert(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow);

	fn mark_deleted(&self, operator: OperatorId, key: &EncodedKey);

	fn retract(&self, operator: OperatorId, key: &EncodedKey);

	fn invalidate_group(&self, operator: OperatorId, group: GroupId);

	fn invalidate_operator(&self, operator: OperatorId);

	fn keyspace_metrics(&self) -> Vec<RangeKeyspaceMetrics>;

	fn is_absent(&self) -> bool {
		false
	}
}

pub struct NoRange;

impl RangeSink for NoRange {
	fn lookup(&self, _operator: OperatorId, _key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		None
	}

	fn overwrite(&self, _operator: OperatorId, _key: &EncodedKey, _row: EncodedPodRow) {}

	fn insert(&self, _operator: OperatorId, _key: &EncodedKey, _row: EncodedPodRow) {}

	fn mark_deleted(&self, _operator: OperatorId, _key: &EncodedKey) {}

	fn retract(&self, _operator: OperatorId, _key: &EncodedKey) {}

	fn invalidate_group(&self, _operator: OperatorId, _group: GroupId) {}

	fn invalidate_operator(&self, _operator: OperatorId) {}

	fn keyspace_metrics(&self) -> Vec<RangeKeyspaceMetrics> {
		Vec::new()
	}

	fn is_absent(&self) -> bool {
		true
	}
}

#[derive(Clone)]
pub enum OperatorRangeTier {
	Absent,
	Standard(RangeTiers),
}

impl OperatorRangeTier {
	pub fn standard(tiers: Option<RangeTiers>) -> Self {
		match tiers {
			Some(tiers) => Self::Standard(tiers),
			None => Self::Absent,
		}
	}

	pub fn tiers(&self) -> Option<&RangeTiers> {
		match self {
			Self::Absent => None,
			Self::Standard(tiers) => Some(tiers),
		}
	}
}

impl RangeSink for OperatorRangeTier {
	fn lookup(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		match self {
			Self::Absent => None,
			Self::Standard(tiers) => tiers.lookup(operator, key),
		}
	}

	fn overwrite(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
		if let Self::Standard(tiers) = self {
			tiers.overwrite(operator, key, row);
		}
	}

	fn insert(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
		if let Self::Standard(tiers) = self {
			tiers.insert(operator, key, row);
		}
	}

	fn mark_deleted(&self, operator: OperatorId, key: &EncodedKey) {
		if let Self::Standard(tiers) = self {
			tiers.mark_deleted(operator, key);
		}
	}

	fn retract(&self, operator: OperatorId, key: &EncodedKey) {
		if let Self::Standard(tiers) = self {
			tiers.retract(operator, key);
		}
	}

	fn invalidate_group(&self, operator: OperatorId, group: GroupId) {
		if let Self::Standard(tiers) = self {
			tiers.invalidate_group(operator, group);
		}
	}

	fn invalidate_operator(&self, operator: OperatorId) {
		if let Self::Standard(tiers) = self {
			tiers.invalidate_operator(operator);
		}
	}

	fn keyspace_metrics(&self) -> Vec<RangeKeyspaceMetrics> {
		match self {
			Self::Absent => Vec::new(),
			Self::Standard(tiers) => tiers.keyspace_metrics(),
		}
	}

	fn is_absent(&self) -> bool {
		matches!(self, Self::Absent)
	}
}
