// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	operator::EncodedOperatorRow,
};
use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId};
use reifydb_runtime::sync::map::Map;

use crate::{
	arena::{Arena, saturating_sub},
	config::OperatorStoreConfig,
	floor::FloorSpec,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionOutcome {
	pub dropped: u64,
	pub reclaimed_bytes: u64,
}

impl CompactionOutcome {
	pub fn is_noop(&self) -> bool {
		self.dropped == 0 && self.reclaimed_bytes == 0
	}
}

#[derive(Debug, Clone)]
pub struct OperatorBatch {
	pub items: Vec<(EncodedKey, EncodedOperatorRow)>,
	pub has_more: bool,
}

impl OperatorBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}
}

#[derive(Clone)]
pub struct OperatorStore {
	inner: Arc<StoreInner>,
}

struct StoreInner {
	arenas: Map<OperatorId, Arc<Arena>>,
	config: OperatorStoreConfig,
	total_bytes: AtomicU64,
}

impl Default for OperatorStore {
	fn default() -> Self {
		Self::new(OperatorStoreConfig::default())
	}
}

impl OperatorStore {
	pub fn new(config: OperatorStoreConfig) -> Self {
		Self {
			inner: Arc::new(StoreInner {
				arenas: Map::new(),
				config,
				total_bytes: AtomicU64::new(0),
			}),
		}
	}

	fn arena(&self, operator: OperatorId) -> Arc<Arena> {
		self.inner.arenas.get_or_insert_with(operator, || Arc::new(Arena::new()))
	}

	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		let arena = self.arena(operator);
		arena.mutate(&self.inner.total_bytes, |inner| inner.set(key, row, &self.inner.config));
	}

	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(arena) = self.inner.arenas.get(&operator) else {
			return;
		};
		arena.mutate(&self.inner.total_bytes, |inner| inner.remove(key, &self.inner.config));
	}

	pub fn remove_range(&self, operator: OperatorId, range: EncodedKeyRange) {
		let Some(arena) = self.inner.arenas.get(&operator) else {
			return;
		};
		arena.mutate(&self.inner.total_bytes, |inner| inner.remove_range(range, &self.inner.config));
	}

	pub fn clear(&self, operator: OperatorId) {
		let Some(arena) = self.inner.arenas.get(&operator) else {
			return;
		};
		arena.mutate(&self.inner.total_bytes, |inner| inner.clear());
	}

	pub fn freeze(&self, operator: OperatorId) {
		let Some(arena) = self.inner.arenas.get(&operator) else {
			return;
		};
		arena.mutate(&self.inner.total_bytes, |inner| inner.freeze());
	}

	pub fn compact(&self, operator: OperatorId, floor: &FloorSpec) -> CompactionOutcome {
		let Some(arena) = self.inner.arenas.get(&operator) else {
			return CompactionOutcome::default();
		};
		let before = arena.bytes();
		let dropped = arena.mutate(&self.inner.total_bytes, |inner| inner.compact(floor));
		CompactionOutcome {
			dropped,
			reclaimed_bytes: before.saturating_sub(arena.bytes()),
		}
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		self.inner.arenas.get(&operator)?.read(|inner| inner.get(key))
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		self.inner.arenas.get(&operator).is_some_and(|arena| arena.read(|inner| inner.contains(key)))
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let Some(arena) = self.inner.arenas.get(&operator) else {
			return OperatorBatch::empty();
		};
		let (items, has_more) = arena.read(|inner| inner.scan(&range, batch_size as usize));
		OperatorBatch {
			items,
			has_more,
		}
	}

	pub fn prefix_batch(&self, operator: OperatorId, prefix: &[u8], batch_size: u64) -> OperatorBatch {
		self.range_batch(operator, EncodedKeyRange::prefix(prefix), batch_size)
	}

	pub fn set_upper(&self, operator: OperatorId, version: CommitVersion) {
		self.arena(operator).set_upper(version);
	}

	pub fn upper(&self, operator: OperatorId) -> CommitVersion {
		self.inner.arenas.get(&operator).map_or(CommitVersion(0), |arena| arena.upper())
	}

	pub fn bytes(&self, operator: OperatorId) -> u64 {
		self.inner.arenas.get(&operator).map_or(0, |arena| arena.bytes())
	}

	pub fn total_bytes(&self) -> u64 {
		self.inner.total_bytes.load(Ordering::Relaxed)
	}

	pub fn drop_arena(&self, operator: OperatorId) {
		if let Some(arena) = self.inner.arenas.remove(&operator) {
			saturating_sub(&self.inner.total_bytes, arena.bytes());
		}
	}
}
