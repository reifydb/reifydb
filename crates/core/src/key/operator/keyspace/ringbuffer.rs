// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{partition::Partition, row_number::RowNumber};

use crate::{
	interface::store::CacheTiers,
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			TypedKey,
			direction::{Asc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct RingbufferForwardKey {
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct RingbufferEntryKey {
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct RingbufferExpiryKey {
	pub expires_at: Asc<u64>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct RingbufferTtlArmKey {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct RingbufferMetaKey {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct PartitionedRingbufferEntryKey {
	pub partition: Asc<Partition>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct PartitionedRingbufferExpiryKey {
	pub partition: Asc<Partition>,
	pub expires_at: Asc<u64>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct PartitionedRingbufferTtlArmKey {
	pub partition: Asc<Partition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct PartitionedRingbufferMetaKey {
	pub partition: Asc<Partition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingbufferForward;

impl Keyspace for RingbufferForward {
	const ID: KeyspaceId = KeyspaceId::RINGBUFFER_FORWARD;
	const NAME: &'static str = "RINGBUFFER_FORWARD";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = RingbufferForwardKey;
	type Suffix = RingbufferForwardKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingbufferEntry;

impl Keyspace for RingbufferEntry {
	const ID: KeyspaceId = KeyspaceId::RINGBUFFER_ENTRY;
	const NAME: &'static str = "RINGBUFFER_ENTRY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = RingbufferEntryKey;
	type Suffix = RingbufferEntryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingbufferExpiry;

impl Keyspace for RingbufferExpiry {
	const ID: KeyspaceId = KeyspaceId::RINGBUFFER_EXPIRY;
	const NAME: &'static str = "RINGBUFFER_EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = RingbufferExpiryKey;
	type Suffix = RingbufferExpiryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingbufferTtlArm;

impl Keyspace for RingbufferTtlArm {
	const ID: KeyspaceId = KeyspaceId::RINGBUFFER_TTL_ARM;
	const NAME: &'static str = "RINGBUFFER_TTL_ARM";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = RingbufferTtlArmKey;
	type Suffix = RingbufferTtlArmKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingbufferMeta;

impl Keyspace for RingbufferMeta {
	const ID: KeyspaceId = KeyspaceId::RINGBUFFER_META;
	const NAME: &'static str = "RINGBUFFER_META";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = RingbufferMetaKey;
	type Suffix = RingbufferMetaKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRingbufferEntry;

impl Keyspace for PartitionedRingbufferEntry {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_RINGBUFFER_ENTRY;
	const NAME: &'static str = "PARTITIONED_RINGBUFFER_ENTRY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = PartitionedRingbufferEntryKey;
	type Suffix = PartitionedRingbufferEntryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRingbufferExpiry;

impl Keyspace for PartitionedRingbufferExpiry {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_RINGBUFFER_EXPIRY;
	const NAME: &'static str = "PARTITIONED_RINGBUFFER_EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = PartitionedRingbufferExpiryKey;
	type Suffix = PartitionedRingbufferExpiryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRingbufferTtlArm;

impl Keyspace for PartitionedRingbufferTtlArm {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_RINGBUFFER_TTL_ARM;
	const NAME: &'static str = "PARTITIONED_RINGBUFFER_TTL_ARM";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = PartitionedRingbufferTtlArmKey;
	type Suffix = PartitionedRingbufferTtlArmKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionedRingbufferMeta;

impl Keyspace for PartitionedRingbufferMeta {
	const ID: KeyspaceId = KeyspaceId::PARTITIONED_RINGBUFFER_META;
	const NAME: &'static str = "PARTITIONED_RINGBUFFER_META";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = PartitionedRingbufferMetaKey;
	type Suffix = PartitionedRingbufferMetaKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
