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
			TypedKey,
			direction::{Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct ExpiryKey {
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct TumblingExpiryKey {
	pub group: Desc<GroupId>,
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
	pub window_start: Desc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct TumblingExpirySuffix {
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
	pub window_start: Desc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TypedKey, HeapSize)]
pub struct ReapQueueKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expiry;

impl Keyspace for Expiry {
	const ID: KeyspaceId = KeyspaceId::ROLLING_EXPIRY;
	const NAME: &'static str = "ROLLING_EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Range;

	type GroupedKey = ExpiryKey;
	type Suffix = ExpiryKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TumblingExpiry;

impl Keyspace for TumblingExpiry {
	const ID: KeyspaceId = KeyspaceId::TUMBLING_EXPIRY;
	const NAME: &'static str = "TUMBLING_EXPIRY";
	const CACHE: CacheTiers = CacheTiers::Range;

	type GroupedKey = TumblingExpiryKey;
	type Suffix = TumblingExpirySuffix;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			TumblingExpirySuffix {
				threshold: key.threshold,
				owner: key.owner,
				window_start: key.window_start,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		TumblingExpiryKey {
			group: Desc(group),
			threshold: suffix.threshold,
			owner: suffix.owner,
			window_start: suffix.window_start,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReapQueue;

impl Keyspace for ReapQueue {
	const ID: KeyspaceId = KeyspaceId::REAP_QUEUE;
	const NAME: &'static str = "REAP_QUEUE";
	const CACHE: CacheTiers = CacheTiers::Both;

	type GroupedKey = ReapQueueKey;
	type Suffix = ReapQueueKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
