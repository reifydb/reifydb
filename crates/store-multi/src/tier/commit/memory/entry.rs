// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BTreeMap, HashSet},
	mem::size_of,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_runtime::sync::{
	map::Map,
	rwlock::{RwLock, RwLockWriteGuard},
};
use reifydb_value::util::cowvec::CowVec;
use tracing::instrument;

pub(super) type Value = Option<CowVec<u8>>;

pub(super) type CurrentMap = BTreeMap<EncodedKey, (CommitVersion, Value)>;

pub(super) type HistoricalMap = BTreeMap<EncodedKey, BTreeMap<Reverse<CommitVersion>, Value>>;

pub(super) type OldestIndex = BTreeMap<CommitVersion, HashSet<EncodedKey>>;

pub(super) const ENTRY_OVERHEAD: usize = size_of::<EncodedKey>() + size_of::<CommitVersion>() + size_of::<Value>();

pub(super) fn oldest_version(
	current: &CurrentMap,
	historical: &HistoricalMap,
	key: &EncodedKey,
) -> Option<CommitVersion> {
	let hist = historical.get(key).and_then(|m| m.keys().next_back()).map(|r| r.0);
	let cur = current.get(key).map(|(v, _)| *v);
	match (hist, cur) {
		(Some(h), Some(c)) => Some(h.min(c)),
		(Some(h), None) => Some(h),
		(None, cur) => cur,
	}
}

pub(super) fn reconcile_oldest(
	index: &mut OldestIndex,
	key: &EncodedKey,
	old: Option<CommitVersion>,
	new: Option<CommitVersion>,
) {
	if old == new {
		return;
	}
	if let Some(old_v) = old
		&& let Some(bucket) = index.get_mut(&old_v)
	{
		bucket.remove(key);
		if bucket.is_empty() {
			index.remove(&old_v);
		}
	}
	if let Some(new_v) = new {
		index.entry(new_v).or_default().insert(key.clone());
	}
}

pub(super) fn entry_bytes(key: &EncodedKey, value: &Value) -> u64 {
	entry_bytes_with(key.len(), value)
}

pub(super) fn entry_bytes_with(key_len: usize, value: &Value) -> u64 {
	(ENTRY_OVERHEAD + key_len + value.as_ref().map_or(0, |bytes| bytes.len())) as u64
}

pub(super) struct EntryBytes {
	current: AtomicU64,
	historical: AtomicU64,
}

impl EntryBytes {
	fn new() -> Self {
		Self {
			current: AtomicU64::new(0),
			historical: AtomicU64::new(0),
		}
	}

	pub fn add_current(&self, bytes: u64) {
		self.current.fetch_add(bytes, Ordering::Relaxed);
	}

	pub fn sub_current(&self, bytes: u64) {
		saturating_sub(&self.current, bytes);
	}

	pub fn add_historical(&self, bytes: u64) {
		self.historical.fetch_add(bytes, Ordering::Relaxed);
	}

	pub fn sub_historical(&self, bytes: u64) {
		saturating_sub(&self.historical, bytes);
	}

	pub fn current(&self) -> u64 {
		self.current.load(Ordering::Relaxed)
	}

	pub fn historical(&self) -> u64 {
		self.historical.load(Ordering::Relaxed)
	}

	pub fn reset(&self) {
		self.current.store(0, Ordering::Relaxed);
		self.historical.store(0, Ordering::Relaxed);
	}
}

fn saturating_sub(counter: &AtomicU64, amount: u64) {
	let mut observed = counter.load(Ordering::Relaxed);
	loop {
		let next = observed.saturating_sub(amount);
		match counter.compare_exchange_weak(observed, next, Ordering::Relaxed, Ordering::Relaxed) {
			Ok(_) => return,
			Err(actual) => observed = actual,
		}
	}
}

pub(super) struct Entry {
	pub current: Arc<RwLock<CurrentMap>>,

	pub historical: Arc<RwLock<HistoricalMap>>,

	pub oldest: Arc<RwLock<OldestIndex>>,

	pub bytes: Arc<EntryBytes>,
}

impl Entry {
	pub fn new() -> Self {
		Self {
			current: Arc::new(RwLock::new(BTreeMap::new())),
			historical: Arc::new(RwLock::new(BTreeMap::new())),
			oldest: Arc::new(RwLock::new(BTreeMap::new())),
			bytes: Arc::new(EntryBytes::new()),
		}
	}

	#[instrument(name = "store::multi::memory::write_acquire", level = "debug", skip_all)]
	pub fn write_pair(&self) -> (RwLockWriteGuard<'_, CurrentMap>, RwLockWriteGuard<'_, HistoricalMap>) {
		(self.current.write(), self.historical.write())
	}
}

impl Clone for Entry {
	fn clone(&self) -> Self {
		Self {
			current: Arc::clone(&self.current),
			historical: Arc::clone(&self.historical),
			oldest: Arc::clone(&self.oldest),
			bytes: Arc::clone(&self.bytes),
		}
	}
}

pub(super) struct Entries {
	pub(super) data: Map<EntryKind, Entry>,
}

impl Default for Entries {
	fn default() -> Self {
		Self {
			data: Map::new(),
		}
	}
}
