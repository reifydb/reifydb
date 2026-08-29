// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, sync::LazyLock};

use reifydb_codec::{
	key::{decode_u128, encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::state::{GroupId, KeyspaceId, OperatorStateKey},
		typed::ExclusiveUpperEnd,
	},
};

use crate::tier::range::{RangeDomain, prefix_successor};

#[derive(Clone, Copy, Debug)]
pub(super) struct TestDomain;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct TestPartition {
	pub dimension: OperatorId,
	pub group: GroupId,
	pub slot: KeyspaceId,
}

impl TestPartition {
	pub const PREFIX_LEN: usize = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize + 1;

	pub fn of(dimension: OperatorId, key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		let group = GroupId(decode_u128(bytes[..offset].try_into().ok()?));
		Some(Self {
			dimension,
			group,
			slot: KeyspaceId(encode_u8(bytes[offset])),
		})
	}

	pub fn prefix(&self) -> EncodedKey {
		EncodedKey::new(OperatorStateKey::inner_encoded(self.group, self.slot, [0u8; 0]).as_bytes())
	}

	pub fn span(&self) -> (EncodedKey, ExclusiveUpperEnd) {
		let start = self.prefix();
		let end = match prefix_successor(start.as_slice()) {
			Some(successor) => ExclusiveUpperEnd::of(successor),
			None => ExclusiveUpperEnd::Top,
		};
		(start, end)
	}

	pub fn caches_ranges(&self) -> bool {
		self.slot.cache_tiers().caches_ranges()
	}
}

static CACHE_TIERS_RUN_FLOOR: LazyLock<[u8; 256]> = LazyLock::new(|| {
	let mut floor = [0u8; 256];
	let mut lowest = 0u8;
	for keyspace in 0..=u8::MAX {
		if keyspace > 0
			&& KeyspaceId(keyspace).cache_tiers().caches_ranges()
				!= KeyspaceId(keyspace - 1).cache_tiers().caches_ranges()
		{
			lowest = keyspace;
		}
		floor[keyspace as usize] = lowest;
	}
	floor
});

impl RangeDomain for TestDomain {
	type Dimension = OperatorId;
	type Partition = TestPartition;
	type Slot = KeyspaceId;
	type Row = EncodedPodRow;

	const PREFIX_LEN: usize = TestPartition::PREFIX_LEN;
	const SLOTS: usize = 256;

	const SCOPE: &'static str = "operator_range";

	const GAP_SCOPE: &'static str = "operator_range::gaps";

	fn partition(dimension: Self::Dimension, key: &EncodedKey) -> Option<Self::Partition> {
		TestPartition::of(dimension, key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		partition.dimension
	}

	fn span(partition: &Self::Partition) -> (EncodedKey, ExclusiveUpperEnd) {
		partition.span()
	}

	fn head_band(_dimension: Self::Dimension) -> Option<(EncodedKey, EncodedKey)> {
		None
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		partition.caches_ranges()
	}

	fn cache_tiers_run_end(partition: &Self::Partition) -> ExclusiveUpperEnd {
		let floor = CACHE_TIERS_RUN_FLOOR[partition.slot.0 as usize];
		if floor == partition.slot.0 {
			return partition.span().1;
		}
		TestPartition {
			slot: KeyspaceId(floor),
			..*partition
		}
		.span()
		.1
	}

	fn slot(partition: &Self::Partition) -> usize {
		partition.slot.0 as usize
	}

	fn slot_at(index: usize) -> Self::Slot {
		KeyspaceId(index as u8)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		slot.name()
	}
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AdmittingDomain;

impl RangeDomain for AdmittingDomain {
	type Dimension = OperatorId;
	type Partition = TestPartition;
	type Slot = KeyspaceId;
	type Row = EncodedPodRow;

	const PREFIX_LEN: usize = TestPartition::PREFIX_LEN;
	const SLOTS: usize = 256;

	const SCOPE: &'static str = "admitting_range";

	const GAP_SCOPE: &'static str = "admitting_range::gaps";

	fn partition(dimension: Self::Dimension, key: &EncodedKey) -> Option<Self::Partition> {
		TestDomain::partition(dimension, key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		TestDomain::dimension(partition)
	}

	fn span(partition: &Self::Partition) -> (EncodedKey, ExclusiveUpperEnd) {
		TestDomain::span(partition)
	}

	fn head_band(dimension: Self::Dimension) -> Option<(EncodedKey, EncodedKey)> {
		TestDomain::head_band(dimension)
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		TestDomain::caches_ranges(partition)
	}

	fn cache_tiers_run_end(partition: &Self::Partition) -> ExclusiveUpperEnd {
		TestDomain::cache_tiers_run_end(partition)
	}

	fn admits_unproven_writes() -> bool {
		true
	}

	fn slot(partition: &Self::Partition) -> usize {
		TestDomain::slot(partition)
	}

	fn slot_at(index: usize) -> Self::Slot {
		TestDomain::slot_at(index)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		TestDomain::slot_name(slot)
	}
}
