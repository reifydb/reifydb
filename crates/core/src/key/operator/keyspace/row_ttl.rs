// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, partition::Partition, row_number::RowNumber};

use crate::{
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			BoundedKey, DenseKey, KeyLayout,
			direction::{Asc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RowTtlEntryKey {
	pub due: Asc<DateTime>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct PartitionedRowTtlEntryKey {
	pub partition: Asc<Partition>,
	pub due: Asc<DateTime>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RowTtlForwardKey {
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct PartitionedRowTtlForwardKey {
	pub partition: Asc<Partition>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RowTtlArmKey {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct PartitionedRowTtlArmKey {
	pub partition: Asc<Partition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowTtlEntry;

impl Keyspace for RowTtlEntry {
	const ID: KeyspaceId = KeyspaceId::ROW_TTL_ENTRY;
	const NAME: &'static str = "ROW_TTL_ENTRY";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RowTtlEntryKey;
	type Suffix = RowTtlEntryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRowTtlEntry;

impl Keyspace for PartitionedRowTtlEntry {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_ROW_TTL_ENTRY;
	const NAME: &'static str = "PARTITIONED_ROW_TTL_ENTRY";
	const RANGE_CACHED: bool = true;

	type GroupedKey = PartitionedRowTtlEntryKey;
	type Suffix = PartitionedRowTtlEntryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowTtlForward;

impl Keyspace for RowTtlForward {
	const ID: KeyspaceId = KeyspaceId::ROW_TTL_FORWARD;
	const NAME: &'static str = "ROW_TTL_FORWARD";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RowTtlForwardKey;
	type Suffix = RowTtlForwardKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRowTtlForward;

impl Keyspace for PartitionedRowTtlForward {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_ROW_TTL_FORWARD;
	const NAME: &'static str = "PARTITIONED_ROW_TTL_FORWARD";
	const RANGE_CACHED: bool = true;

	type GroupedKey = PartitionedRowTtlForwardKey;
	type Suffix = PartitionedRowTtlForwardKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowTtlArm;

impl Keyspace for RowTtlArm {
	const ID: KeyspaceId = KeyspaceId::ROW_TTL_ARM;
	const NAME: &'static str = "ROW_TTL_ARM";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RowTtlArmKey;
	type Suffix = RowTtlArmKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRowTtlArm;

impl Keyspace for PartitionedRowTtlArm {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_ROW_TTL_ARM;
	const NAME: &'static str = "PARTITIONED_ROW_TTL_ARM";
	const RANGE_CACHED: bool = true;

	type GroupedKey = PartitionedRowTtlArmKey;
	type Suffix = PartitionedRowTtlArmKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
