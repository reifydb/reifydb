// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::util::hash::Hash128;

use crate::{
	interface::store::CacheTiers,
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			Key,
			direction::{Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct ExpiryKey {
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct TumblingExpiryKey {
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
	pub window_start: Desc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct ReapQueueKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expiry;

impl Keyspace for Expiry {
	const ID: KeyspaceId = KeyspaceId::EXPIRY;
	const NAME: &'static str = "EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = ExpiryKey;
	type Suffix = ExpiryKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TumblingExpiry;

impl Keyspace for TumblingExpiry {
	const ID: KeyspaceId = KeyspaceId::TUMBLING_EXPIRY;
	const NAME: &'static str = "TUMBLING_EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = TumblingExpiryKey;
	type Suffix = TumblingExpiryKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReapQueue;

impl Keyspace for ReapQueue {
	const ID: KeyspaceId = KeyspaceId::REAP_QUEUE;
	const NAME: &'static str = "REAP_QUEUE";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = ReapQueueKey;
	type Suffix = ReapQueueKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}
