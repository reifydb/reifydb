// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::fingerprint::RowShapeFingerprint;
use reifydb_macro::operator_state;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{
	key::{
		operator::{
			state::{GroupId, GroupStateKey, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			DenseKey, TypedKey,
			direction::{Asc, Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
	state::{join::ContentVersion, typed::typed_key},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinLeftKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinRightKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinPublishedKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinPinKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
	pub version: Asc<ContentVersion>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinSchemaKey {
	pub side: Asc<u8>,
	pub fingerprint: Asc<RowShapeFingerprint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinRowExpiryKey {
	pub group: Desc<GroupId>,
	pub side: Asc<u8>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinPinSuffix {
	pub row: Asc<RowNumber>,
	pub version: Asc<ContentVersion>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinRowExpirySuffix {
	pub side: Asc<u8>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct JoinExpiryDueKey {
	pub at: Asc<DateTime>,
	pub group: Desc<GroupId>,
	pub side: Asc<u8>,
	pub row: Asc<RowNumber>,
}

#[operator_state]
#[derive(Clone)]
pub struct JoinRowExpiryState {
	pub at: DateTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
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
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinLeftKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
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
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinRightKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
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
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinPublishedKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
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
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinPinKey;
	type Suffix = JoinPinSuffix;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			JoinPinSuffix {
				row: key.row,
				version: key.version,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
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
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinSchemaKey;
	type Suffix = JoinSchemaKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinRowExpiry;

impl Keyspace for JoinRowExpiry {
	const ID: KeyspaceId = KeyspaceId::JOIN_ROW_EXPIRY;
	const NAME: &'static str = "JOIN_ROW_EXPIRY";
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinRowExpiryKey;
	type Suffix = JoinRowExpirySuffix;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			JoinRowExpirySuffix {
				side: key.side,
				row: key.row,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		JoinRowExpiryKey {
			group: Desc(group),
			side: suffix.side,
			row: suffix.row,
		}
	}
}

pub fn join_expiry_due_key(at: DateTime, group: GroupId, side: u8, row: RowNumber) -> GroupStateKey {
	typed_key::<JoinExpiryDue>(
		GroupId::ROOT,
		&JoinExpiryDueKey {
			at: Asc(at),
			group: Desc(group),
			side: Asc(side),
			row: Asc(row),
		},
	)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinExpiryDue;

impl Keyspace for JoinExpiryDue {
	const ID: KeyspaceId = KeyspaceId::JOIN_EXPIRY_DUE;
	const NAME: &'static str = "JOIN_EXPIRY_DUE";
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinExpiryDueKey;
	type Suffix = JoinExpiryDueKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinRowMapping;

impl Keyspace for JoinRowMapping {
	const ID: KeyspaceId = KeyspaceId::JOIN_ROW_MAPPING;
	const NAME: &'static str = "JOIN_ROW_MAPPING";
	const RANGE_CACHED: bool = true;

	type GroupedKey = JoinRowMappingKey;
	type Suffix = JoinRowMappingKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
