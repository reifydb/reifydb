// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

use crate::{
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			BoundedKey, DenseKey, KeyLayout,
			direction::{Asc, Desc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue, KeyValues},
		},
	},
	metrics::heap::HeapSize,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct AccumulatorKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RollingPaneKey {
	pub group: Desc<GroupId>,
	pub pane: Asc<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct SessionKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RollingMetaKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct EngineMetaKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct EmitKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct RowIndexKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct SessionMemberKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct WindowMetaKey {
	pub group: Desc<GroupId>,
	pub window: Desc<Hash128>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct WindowMetaSuffix {
	pub window: Desc<Hash128>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct GuestAccumulatorKey {
	pub group: Desc<GroupId>,
	pub slot: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, KeyLayout, HeapSize)]
pub struct GuestRetainedEntryKey {
	pub group: Desc<GroupId>,
	pub slot: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Accumulator;

impl Keyspace for Accumulator {
	const ID: KeyspaceId = KeyspaceId::ACCUMULATOR;
	const NAME: &'static str = "ACCUMULATOR";
	const RANGE_CACHED: bool = true;

	type GroupedKey = AccumulatorKey;
	type Suffix = ();

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::GroupedKey {
		AccumulatorKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollingPane;

impl Keyspace for RollingPane {
	const ID: KeyspaceId = KeyspaceId::ROLLING_PANE;
	const NAME: &'static str = "ROLLING_PANE";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RollingPaneKey;
	type Suffix = Asc<u64>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.pane)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		RollingPaneKey {
			group: Desc(group),
			pane: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Session;

impl Keyspace for Session {
	const ID: KeyspaceId = KeyspaceId::SESSION;
	const NAME: &'static str = "SESSION";
	const RANGE_CACHED: bool = true;

	type GroupedKey = SessionKey;
	type Suffix = ();

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::GroupedKey {
		SessionKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollingMeta;

impl Keyspace for RollingMeta {
	const ID: KeyspaceId = KeyspaceId::ROLLING_META;
	const NAME: &'static str = "ROLLING_META";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RollingMetaKey;
	type Suffix = ();

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::GroupedKey {
		RollingMetaKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineMeta;

impl Keyspace for EngineMeta {
	const ID: KeyspaceId = KeyspaceId::ENGINE_META;
	const NAME: &'static str = "ENGINE_META";
	const RANGE_CACHED: bool = true;

	type GroupedKey = EngineMetaKey;
	type Suffix = ();

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::GroupedKey {
		EngineMetaKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Emit;

impl Keyspace for Emit {
	const ID: KeyspaceId = KeyspaceId::EMIT;
	const NAME: &'static str = "EMIT";
	const RANGE_CACHED: bool = true;

	type GroupedKey = EmitKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		EmitKey {
			group: Desc(group),
			row: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowIndex;

impl Keyspace for RowIndex {
	const ID: KeyspaceId = KeyspaceId::ROW_INDEX;
	const NAME: &'static str = "ROW_INDEX";
	const RANGE_CACHED: bool = true;

	type GroupedKey = RowIndexKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		RowIndexKey {
			group: Desc(group),
			row: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionMember;

impl Keyspace for SessionMember {
	const ID: KeyspaceId = KeyspaceId::SESSION_MEMBER;
	const NAME: &'static str = "SESSION_MEMBER";
	const RANGE_CACHED: bool = true;

	type GroupedKey = SessionMemberKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		SessionMemberKey {
			group: Desc(group),
			row: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowMeta;

impl Keyspace for WindowMeta {
	const ID: KeyspaceId = KeyspaceId::WINDOW_META;
	const NAME: &'static str = "WINDOW_META";
	const RANGE_CACHED: bool = true;

	type GroupedKey = WindowMetaKey;
	type Suffix = WindowMetaSuffix;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(
			key.group.0,
			WindowMetaSuffix {
				window: key.window,
			},
		)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		WindowMetaKey {
			group: Desc(group),
			window: suffix.window,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestAccumulator;

impl Keyspace for GuestAccumulator {
	const ID: KeyspaceId = KeyspaceId::GUEST_ACCUMULATOR;
	const NAME: &'static str = "GUEST_ACCUMULATOR";
	const RANGE_CACHED: bool = true;

	type GroupedKey = GuestAccumulatorKey;
	type Suffix = Asc<[u8; 16]>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.slot)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		GuestAccumulatorKey {
			group: Desc(group),
			slot: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestRetainedEntry;

impl Keyspace for GuestRetainedEntry {
	const ID: KeyspaceId = KeyspaceId::GUEST_RETAINED_ENTRY;
	const NAME: &'static str = "GUEST_RETAINED_ENTRY";
	const RANGE_CACHED: bool = true;

	type GroupedKey = GuestRetainedEntryKey;
	type Suffix = Asc<[u8; 16]>;

	fn split(key: &Self::GroupedKey) -> (GroupId, Self::Suffix) {
		(key.group.0, key.slot)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::GroupedKey {
		GuestRetainedEntryKey {
			group: Desc(group),
			slot: suffix,
		}
	}
}
