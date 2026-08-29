// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{KeyspaceId, OperatorStateKey},
};
use reifydb_store::tier::point::{
	PointConfig, PointDomain, PointMetrics, PointShardMetrics, PointSlotMetrics, PointTier,
};

pub type OperatorPointConfig = PointConfig;
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
