// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	default,
	interface::catalog::flow::OperatorId,
	key::operator_state::{KeyspaceId, OperatorStateKey},
};
use reifydb_store::tier::point::{
	PointConfig, PointDomain, PointMetrics, PointShardMetrics, PointSlotMetrics, PointTier,
};
use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug)]
pub struct OperatorPointConfig {
	pub shard_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl OperatorPointConfig {
	pub fn testing() -> Self {
		Self {
			shard_bytes: Some(default::store::OPERATOR_POINT_BUFFER_SHARD_TESTING),
			shards: default::store::OPERATOR_POINT_BUFFER_SHARDS_TESTING as usize,
		}
	}
}

impl From<OperatorPointConfig> for PointConfig {
	fn from(config: OperatorPointConfig) -> Self {
		Self {
			shard_bytes: config.shard_bytes,
			shards: config.shards,
		}
	}
}
pub type OperatorPointTier = PointTier<OperatorDomain>;
pub type OperatorPointMetrics = PointMetrics;
pub type OperatorPointShardMetrics = PointShardMetrics;
pub type OperatorPointKeyspaceMetrics = PointSlotMetrics<OperatorDomain>;

#[derive(Clone, Copy, Debug)]
pub struct OperatorDomain;

impl PointDomain for OperatorDomain {
	type Dimension = OperatorId;
	type Slot = KeyspaceId;
	type Row = EncodedPodRow;

	const SLOTS: usize = 256;

	const SCOPE: &'static str = "operator_point";

	fn slot(key: &EncodedKey) -> Option<usize> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		Some(encode_u8(bytes[offset]) as usize)
	}

	fn caches_points(slot: usize) -> bool {
		KeyspaceId(slot as u8).cache_tiers().caches_points()
	}

	fn slot_at(index: usize) -> Self::Slot {
		KeyspaceId(index as u8)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		slot.name()
	}
}
