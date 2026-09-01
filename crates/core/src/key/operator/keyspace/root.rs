// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::row_number::RowNumber;

use crate::{
	interface::store::CacheTiers,
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			Key,
			direction::{Asc, Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct SourceWatermarkKey {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct SealLedgerKey {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct NodeCounterKey {
	pub kind: Asc<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeCounterKind {
	RowNumber,
	Group,
}

impl NodeCounterKind {
	pub fn tag(&self) -> u8 {
		match self {
			Self::RowNumber => 0,
			Self::Group => 1,
		}
	}

	pub fn from_tag(tag: u8) -> Option<Self> {
		match tag {
			0 => Some(Self::RowNumber),
			1 => Some(Self::Group),
			_ => None,
		}
	}
}

impl NodeCounterKey {
	pub fn of(kind: NodeCounterKind) -> Self {
		Self {
			kind: Asc(kind.tag()),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct GateVisibilityKey {
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct GroupRowMappingKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct GuestRowMappingKey {
	pub id: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct CustomNotCachedKey {
	pub group: Desc<GroupId>,
	pub id: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct CustomNotCachedSuffix {
	pub id: Asc<[u8; 16]>,
}

impl CustomNotCachedSuffix {
	pub const ID_LEN: usize = 16;

	pub fn of(id: &[u8]) -> Option<Self> {
		if id.len() > Self::ID_LEN {
			return None;
		}
		let mut bytes = [0u8; Self::ID_LEN];
		bytes[..id.len()].copy_from_slice(id);
		Some(Self {
			id: Asc(bytes),
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceWatermark;

impl Keyspace for SourceWatermark {
	const ID: KeyspaceId = KeyspaceId::SOURCE_WATERMARK;
	const NAME: &'static str = "SOURCE_WATERMARK";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = SourceWatermarkKey;
	type Suffix = SourceWatermarkKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealLedger;

impl Keyspace for SealLedger {
	const ID: KeyspaceId = KeyspaceId::SEAL_LEDGER;
	const NAME: &'static str = "SEAL_LEDGER";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = SealLedgerKey;
	type Suffix = SealLedgerKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCounter;

impl Keyspace for NodeCounter {
	const ID: KeyspaceId = KeyspaceId::NODE_COUNTER;
	const NAME: &'static str = "NODE_COUNTER";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = NodeCounterKey;
	type Suffix = NodeCounterKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GateVisibility;

impl Keyspace for GateVisibility {
	const ID: KeyspaceId = KeyspaceId::GATE_VISIBILITY;
	const NAME: &'static str = "GATE_VISIBILITY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = GateVisibilityKey;
	type Suffix = GateVisibilityKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupRowMapping;

impl Keyspace for GroupRowMapping {
	const ID: KeyspaceId = KeyspaceId::GROUP_ROW_MAPPING;
	const NAME: &'static str = "GROUP_ROW_MAPPING";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = GroupRowMappingKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
		GroupRowMappingKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestRowMapping;

impl Keyspace for GuestRowMapping {
	const ID: KeyspaceId = KeyspaceId::GUEST_ROW_MAPPING;
	const NAME: &'static str = "GUEST_ROW_MAPPING";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = GuestRowMappingKey;
	type Suffix = GuestRowMappingKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CustomNotCached;

impl Keyspace for CustomNotCached {
	const ID: KeyspaceId = KeyspaceId::CUSTOM_NOT_CACHED;
	const NAME: &'static str = "CUSTOM_NOT_CACHED";
	const CACHE: CacheTiers = CacheTiers::Neither;

	type Key = CustomNotCachedKey;
	type Suffix = CustomNotCachedSuffix;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			CustomNotCachedSuffix {
				id: key.id,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		CustomNotCachedKey {
			group: Desc(group),
			id: suffix.id,
		}
	}
}
