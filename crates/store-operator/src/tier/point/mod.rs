// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Point tier of the operator store: a single-version cache of point reads that also remembers absences, so a
//! key read many times costs one persistent lookup rather than one per read. An entry keys on the whole inner
//! key instead of on a `(operator, group, keyspace)` bucket, so one group's keys spread across every shard and
//! eviction removes one entry rather than every key that happened to share a group.
//!
//! A keyspace the operator store declares uncached is answered from the keyspace byte alone, before any hash
//! or lock, and its miss is charged to a lock free per keyspace counter that folds into the keyspace table.

use std::borrow::Cow;

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{Keyspace, OperatorStateKey},
};
use reifydb_store::tier::point::{
	PointConfig, PointDomain, PointSlotMetrics, PointMetrics, PointShardMetrics, PointTier,
};

pub type OperatorPointConfig = PointConfig;
pub type OperatorPointTier = PointTier<OperatorDomain>;
pub type OperatorPointMetrics = PointMetrics;
pub type OperatorPointShardMetrics = PointShardMetrics;
pub type OperatorPointKeyspaceMetrics = PointSlotMetrics<OperatorDomain>;

/// The operator store's point domain: a dimension is one operator and a counter slot is the keyspace
/// byte the inner key carries.
#[derive(Clone, Copy, Debug)]
pub struct OperatorDomain;

impl PointDomain for OperatorDomain {
	type Dimension = OperatorId;
	type Slot = Keyspace;
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
		Keyspace(slot as u8).cache_policy().caches_points()
	}

	fn slot_at(index: usize) -> Self::Slot {
		Keyspace(index as u8)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		slot.name()
	}
}
