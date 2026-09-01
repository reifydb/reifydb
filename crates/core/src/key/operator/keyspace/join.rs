// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::fingerprint::RowShapeFingerprint;
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
	state::join::ContentVersion,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinLeftKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinRightKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinPublishedKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinPinKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
	pub version: Asc<ContentVersion>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinSchemaKey {
	pub side: Asc<u8>,
	pub fingerprint: Asc<RowShapeFingerprint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinRowExpiryKey {
	pub group: Desc<GroupId>,
	pub side: Asc<u8>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinPinSuffix {
	pub row: Asc<RowNumber>,
	pub version: Asc<ContentVersion>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinRowExpirySuffix {
	pub side: Asc<u8>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct JoinRowMappingKey {
	pub tag: Asc<u8>,
	pub left: Desc<u64>,
	pub right: Desc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinLeft;

impl Keyspace for JoinLeft {
	const ID: KeyspaceId = KeyspaceId::JOIN_LEFT;
	const NAME: &'static str = "JOIN_LEFT";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = JoinLeftKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		JoinLeftKey {
			group: Desc(group),
			row: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinRight;

impl Keyspace for JoinRight {
	const ID: KeyspaceId = KeyspaceId::JOIN_RIGHT;
	const NAME: &'static str = "JOIN_RIGHT";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = JoinRightKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		JoinRightKey {
			group: Desc(group),
			row: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinPublished;

impl Keyspace for JoinPublished {
	const ID: KeyspaceId = KeyspaceId::JOIN_PUBLISHED;
	const NAME: &'static str = "JOIN_PUBLISHED";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = JoinPublishedKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		JoinPublishedKey {
			group: Desc(group),
			row: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinPin;

impl Keyspace for JoinPin {
	const ID: KeyspaceId = KeyspaceId::JOIN_PIN;
	const NAME: &'static str = "JOIN_PIN";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = JoinPinKey;
	type Suffix = JoinPinSuffix;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			JoinPinSuffix {
				row: key.row,
				version: key.version,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		JoinPinKey {
			group: Desc(group),
			row: suffix.row,
			version: suffix.version,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinSchema;

impl Keyspace for JoinSchema {
	const ID: KeyspaceId = KeyspaceId::JOIN_SCHEMA;
	const NAME: &'static str = "JOIN_SCHEMA";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = JoinSchemaKey;
	type Suffix = JoinSchemaKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinRowExpiry;

impl Keyspace for JoinRowExpiry {
	const ID: KeyspaceId = KeyspaceId::JOIN_ROW_EXPIRY;
	const NAME: &'static str = "JOIN_ROW_EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = JoinRowExpiryKey;
	type Suffix = JoinRowExpirySuffix;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			JoinRowExpirySuffix {
				side: key.side,
				row: key.row,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		JoinRowExpiryKey {
			group: Desc(group),
			side: suffix.side,
			row: suffix.row,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinRowMapping;

impl Keyspace for JoinRowMapping {
	const ID: KeyspaceId = KeyspaceId::JOIN_ROW_MAPPING;
	const NAME: &'static str = "JOIN_ROW_MAPPING";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = JoinRowMappingKey;
	type Suffix = JoinRowMappingKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}
