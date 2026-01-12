// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use dashmap::DashMap;
use left_right::{Absorb, ReadHandleFactory, WriteHandle};
use parking_lot::Mutex;
use reifydb_type::CowVec;

use crate::tier::EntryKind;

/// Inner storage type - ordered map for range queries
pub(super) type Inner = BTreeMap<CowVec<u8>, Option<CowVec<u8>>>;

/// Operations for the left-right oplog
pub(super) enum Op {
	/// Insert multiple key-value pairs (batch optimization)
	InsertBatch(Vec<(CowVec<u8>, Option<CowVec<u8>>)>),
	/// Clear all entries
	Clear,
}

/// Newtype wrapper to implement Absorb trait
pub(super) struct OrderedMap(pub Inner);

impl Default for OrderedMap {
	fn default() -> Self {
		Self(BTreeMap::new())
	}
}

impl Absorb<Op> for OrderedMap {
	fn absorb_first(&mut self, op: &mut Op, _other: &Self) {
		match op {
			Op::InsertBatch(entries) => {
				for (k, v) in entries.iter() {
					self.0.insert(k.clone(), v.clone());
				}
			}
			Op::Clear => {
				self.0.clear();
			}
		}
	}

	fn absorb_second(&mut self, op: Op, _other: &Self) {
		match op {
			Op::InsertBatch(entries) => {
				for (k, v) in entries {
					self.0.insert(k, v);
				}
			}
			Op::Clear => {
				self.0.clear();
			}
		}
	}

	fn sync_with(&mut self, first: &Self) {
		self.0.clone_from(&first.0);
	}
}

/// Table entry with separate read/write handles for lock-free reads.
/// Uses ReadHandleFactory (which is Send + Sync) to produce thread-local ReadHandles on demand.
pub(super) struct Entry {
	pub reader_factory: ReadHandleFactory<OrderedMap>,
	pub writer: Arc<Mutex<WriteHandle<OrderedMap, Op>>>,
}

impl Entry {
	pub fn new() -> Self {
		let (mut writer, reader) = left_right::new::<OrderedMap, Op>();
		// Make initial empty state visible to readers
		writer.publish();
		// Create a factory from the reader for thread-safe sharing
		let reader_factory = reader.factory();
		Self {
			reader_factory,
			writer: Arc::new(Mutex::new(writer)),
		}
	}
}

impl Clone for Entry {
	fn clone(&self) -> Self {
		Self {
			reader_factory: self.reader_factory.clone(),
			writer: Arc::clone(&self.writer),
		}
	}
}

/// Convert Store to a unique string key for storage in DashMap
pub(super) fn entry_id_to_key(entry: EntryKind) -> String {
	match entry {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Single => "single".to_string(),
		EntryKind::Cdc => "cdc".to_string(),
		EntryKind::Source(id) => format!("source:{}", id),
		EntryKind::Operator(id) => format!("operator:{}", id),
	}
}

/// Table storage using DashMap for concurrent per-table access
pub(super) struct Entries {
	pub(super) data: DashMap<String, Entry>,
}

impl Default for Entries {
	fn default() -> Self {
		Self {
			data: DashMap::new(),
		}
	}
}
