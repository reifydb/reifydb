// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::row_number::RowNumber;

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
pub struct LookupReadKey {
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LookupRead;

impl Keyspace for LookupRead {
	const ID: KeyspaceId = KeyspaceId::LOOKUP_READ;
	const NAME: &'static str = "LOOKUP_READ";
	const RANGE_CACHED: bool = true;

	type GroupedKey = LookupReadKey;
	type Suffix = LookupReadKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
