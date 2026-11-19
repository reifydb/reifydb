// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::util::hash::Hash128;

use crate::{
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			BoundedKey, DenseKey, KeyLayout,
			direction::{Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RollingAgeOutKey {
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct TumblingSealKey {
	pub group: Desc<GroupId>,
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
	pub window_start: Desc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct TumblingSealSuffix {
	pub threshold: Desc<u64>,
	pub owner: Desc<Hash128>,
	pub window_start: Desc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct ForgetPassQueueKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct CustomManagedDueKey {
	pub threshold: Desc<u64>,
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct CustomManagedLatestKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollingAgeOut;

impl Keyspace for RollingAgeOut {
	const ID: KeyspaceId = KeyspaceId::ROLLING_AGE_OUT;
	const NAME: &'static str = "ROLLING_AGE_OUT";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RollingAgeOutKey;
	type Suffix = RollingAgeOutKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TumblingSeal;

impl Keyspace for TumblingSeal {
	const ID: KeyspaceId = KeyspaceId::TUMBLING_SEAL;
	const NAME: &'static str = "TUMBLING_SEAL";
	const RANGE_CACHED: bool = true;

	type GroupedKey = TumblingSealKey;
	type Suffix = TumblingSealSuffix;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			TumblingSealSuffix {
				threshold: key.threshold,
				owner: key.owner,
				window_start: key.window_start,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		TumblingSealKey {
			group: Desc(group),
			threshold: suffix.threshold,
			owner: suffix.owner,
			window_start: suffix.window_start,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForgetPassQueue;

pub const FORGOTTEN_AT_FLUSH: &[u8] = &[1];

impl Keyspace for ForgetPassQueue {
	const ID: KeyspaceId = KeyspaceId::FORGET_PASS_QUEUE;
	const NAME: &'static str = "FORGET_PASS_QUEUE";
	const RANGE_CACHED: bool = true;

	type GroupedKey = ForgetPassQueueKey;
	type Suffix = ForgetPassQueueKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CustomManagedDue;

impl Keyspace for CustomManagedDue {
	const ID: KeyspaceId = KeyspaceId::CUSTOM_MANAGED_DUE;
	const NAME: &'static str = "CUSTOM_MANAGED_DUE";
	const RANGE_CACHED: bool = true;

	type GroupedKey = CustomManagedDueKey;
	type Suffix = CustomManagedDueKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CustomManagedLatest;

impl Keyspace for CustomManagedLatest {
	const ID: KeyspaceId = KeyspaceId::CUSTOM_MANAGED_LATEST;
	const NAME: &'static str = "CUSTOM_MANAGED_LATEST";
	const RANGE_CACHED: bool = true;

	type GroupedKey = CustomManagedLatestKey;
	type Suffix = CustomManagedLatestKey;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		suffix
	}
}
