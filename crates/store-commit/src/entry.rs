// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, VecDeque},
	mem::size_of,
	sync::{Arc, atomic::AtomicU64},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind, metrics::heap::HeapSize};
use reifydb_runtime::sync::{
	map::Map,
	mutex::Mutex,
	rwlock::{RwLock, RwLockWriteGuard},
};
use reifydb_value::util::cowvec::CowVec;
use tracing::instrument;

use crate::rows::{ActiveRows, ClosedRows};

pub(super) type Value = Option<CowVec<u8>>;

pub(super) const ENTRY_OVERHEAD: usize = size_of::<EncodedKey>() + size_of::<CommitVersion>() + size_of::<Value>();

pub(super) fn entry_bytes(key: &EncodedKey, value: &Value) -> u64 {
	entry_bytes_with(key.heap_size(), value)
}

pub(super) fn entry_bytes_with(key_heap: usize, value: &Value) -> u64 {
	(ENTRY_OVERHEAD + key_heap + value.as_ref().map_or(0, |bytes| bytes.len())) as u64
}

pub(super) struct Entry {
	pub active: RwLock<ActiveRows>,

	pub closed: RwLock<VecDeque<Arc<ClosedRows>>>,

	pub pending: Mutex<BTreeSet<EncodedKey>>,

	pub retained: Mutex<BTreeSet<EncodedKey>>,

	pub key_count: AtomicU64,
}

impl Entry {
	pub fn new() -> Self {
		Self {
			active: RwLock::new(ActiveRows::new()),
			closed: RwLock::new(VecDeque::new()),
			pending: Mutex::new(BTreeSet::new()),
			retained: Mutex::new(BTreeSet::new()),
			key_count: AtomicU64::new(0),
		}
	}

	#[instrument(name = "store::multi::memory::write_acquire", level = "debug", skip_all)]
	pub fn active_write(&self) -> RwLockWriteGuard<'_, ActiveRows> {
		self.active.write()
	}

	pub fn closed_snapshot(&self) -> Vec<Arc<ClosedRows>> {
		self.closed.read().iter().cloned().collect()
	}
}

pub(super) struct Entries {
	pub(super) data: Map<EntryKind, Arc<Entry>>,
}

impl Default for Entries {
	fn default() -> Self {
		Self {
			data: Map::new(),
		}
	}
}
