// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

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
pub struct AccumulatorKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct BufferKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct RunningKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct CountKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct SessionKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct RollingMetaKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct EngineMetaKey {
	pub group: Desc<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct EmitKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct RowIndexKey {
	pub group: Desc<GroupId>,
	pub row: Asc<RowNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct WindowMetaKey {
	pub window: Desc<Hash128>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct GuestAccumulatorKey {
	pub group: Desc<GroupId>,
	pub slot: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct GuestBufferKey {
	pub group: Desc<GroupId>,
	pub slot: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct GuestRunningKey {
	pub group: Desc<GroupId>,
	pub slot: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Accumulator;

impl Keyspace for Accumulator {
	const ID: KeyspaceId = KeyspaceId::ACCUMULATOR;
	const NAME: &'static str = "ACCUMULATOR";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = AccumulatorKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
		AccumulatorKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Buffer;

impl Keyspace for Buffer {
	const ID: KeyspaceId = KeyspaceId::BUFFER;
	const NAME: &'static str = "BUFFER";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = BufferKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
		BufferKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Running;

impl Keyspace for Running {
	const ID: KeyspaceId = KeyspaceId::RUNNING;
	const NAME: &'static str = "RUNNING";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = RunningKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
		RunningKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Count;

impl Keyspace for Count {
	const ID: KeyspaceId = KeyspaceId::COUNT;
	const NAME: &'static str = "COUNT";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = CountKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
		CountKey {
			group: Desc(group),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Session;

impl Keyspace for Session {
	const ID: KeyspaceId = KeyspaceId::SESSION;
	const NAME: &'static str = "SESSION";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = SessionKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
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
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = RollingMetaKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
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
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = EngineMetaKey;
	type Suffix = ();

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, ())
	}

	fn join(group: GroupId, _suffix: Self::Suffix) -> Self::Key {
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
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = EmitKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
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
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = RowIndexKey;
	type Suffix = Asc<RowNumber>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.row)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		RowIndexKey {
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
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = WindowMetaKey;
	type Suffix = WindowMetaKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestAccumulator;

impl Keyspace for GuestAccumulator {
	const ID: KeyspaceId = KeyspaceId::GUEST_ACCUMULATOR;
	const NAME: &'static str = "GUEST_ACCUMULATOR";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = GuestAccumulatorKey;
	type Suffix = Asc<[u8; 16]>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.slot)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		GuestAccumulatorKey {
			group: Desc(group),
			slot: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestBuffer;

impl Keyspace for GuestBuffer {
	const ID: KeyspaceId = KeyspaceId::GUEST_BUFFER;
	const NAME: &'static str = "GUEST_BUFFER";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = GuestBufferKey;
	type Suffix = Asc<[u8; 16]>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.slot)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		GuestBufferKey {
			group: Desc(group),
			slot: suffix,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestRunning;

impl Keyspace for GuestRunning {
	const ID: KeyspaceId = KeyspaceId::GUEST_RUNNING;
	const NAME: &'static str = "GUEST_RUNNING";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = GuestRunningKey;
	type Suffix = Asc<[u8; 16]>;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(key.group.0, key.slot)
	}

	fn join(group: GroupId, suffix: Self::Suffix) -> Self::Key {
		GuestRunningKey {
			group: Desc(group),
			slot: suffix,
		}
	}
}
