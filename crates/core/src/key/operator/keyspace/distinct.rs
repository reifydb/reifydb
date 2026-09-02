// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	interface::store::CacheTiers,
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			TypedKey,
			direction::{Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct DistinctEntryKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct DistinctLayoutKey {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DistinctEntry;

impl Keyspace for DistinctEntry {
	const ID: KeyspaceId = KeyspaceId::DISTINCT_ENTRY;
	const NAME: &'static str = "DISTINCT_ENTRY";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = DistinctEntryKey;
	type Suffix = ();

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::GroupedKey {
		DistinctEntryKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DistinctLayout;

impl Keyspace for DistinctLayout {
	const ID: KeyspaceId = KeyspaceId::DISTINCT_LAYOUT;
	const NAME: &'static str = "DISTINCT_LAYOUT";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = DistinctLayoutKey;
	type Suffix = DistinctLayoutKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
