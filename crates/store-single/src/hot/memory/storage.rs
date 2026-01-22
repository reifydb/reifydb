// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Memory implementation of single-version storage.
//!
//! Uses a single BTreeMap wrapped in Arc<RwLock> for concurrent access.

use std::{collections::BTreeMap, ops::Bound, sync::Arc};

use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_type::{Result, util::cowvec::CowVec};
use tracing::instrument;

use crate::tier::{RangeBatch, RangeCursor, RawEntry, TierBackend, TierStorage};

/// Memory-based single-version storage implementation.
///
/// Uses a single BTreeMap with RwLock for concurrent access.
#[derive(Clone)]
pub struct MemoryPrimitiveStorage {
	inner: Arc<MemoryPrimitiveStorageInner>,
}

struct MemoryPrimitiveStorageInner {
	/// Single storage map for all keys
	data: Arc<RwLock<BTreeMap<CowVec<u8>, Option<CowVec<u8>>>>>,
}

impl MemoryPrimitiveStorage {
	#[instrument(name = "store::single::memory::new", level = "debug")]
	pub fn new() -> Self {
		Self {
			inner: Arc::new(MemoryPrimitiveStorageInner {
				data: Arc::new(RwLock::new(BTreeMap::new())),
			}),
		}
	}

}

impl TierStorage for MemoryPrimitiveStorage {
	#[instrument(name = "store::single::memory::get", level = "trace", skip(self, key), fields(key_len = key.len()))]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		let map = self.inner.data.read();
		Ok(map.get(key).cloned().flatten())
	}

	#[instrument(name = "store::single::memory::contains", level = "trace", skip(self, key), fields(key_len = key.len()), ret)]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		let map = self.inner.data.read();
		Ok(map.contains_key(key))
	}

	#[instrument(name = "store::single::memory::set", level = "debug", skip(self, entries), fields(entry_count = entries.len()))]
	fn set(&self, entries: Vec<(CowVec<u8>, Option<CowVec<u8>>)>) -> Result<()> {
		let mut map = self.inner.data.write();
		for (key, value) in entries {
			match value {
				Some(v) => {
					map.insert(key, Some(v));
				}
				None => {
					map.remove(&key);
				}
			}
		}
		Ok(())
	}

	#[instrument(name = "store::single::memory::range_next", level = "trace", skip(self, cursor))]
	fn range_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let map = self.inner.data.read();

		// Adjust start bound based on cursor
		let actual_start = if let Some(ref last_key) = cursor.last_key {
			Bound::Excluded(last_key.as_slice())
		} else {
			start
		};

		// Collect entries in range
		let entries: Vec<RawEntry> = map
			.range::<[u8], _>((actual_start, end))
			.take(batch_size)
			.map(|(k, v)| RawEntry {
				key: k.clone(),
				value: v.clone(),
			})
			.collect();

		// Update cursor
		if let Some(last_entry) = entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
			cursor.exhausted = entries.len() < batch_size;
		} else {
			cursor.exhausted = true;
		}

		Ok(RangeBatch {
			entries,
			has_more: !cursor.exhausted,
		})
	}

	#[instrument(name = "store::single::memory::range_rev_next", level = "trace", skip(self, cursor))]
	fn range_rev_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let map = self.inner.data.read();

		// Adjust end bound based on cursor (reverse iteration)
		let actual_end = if let Some(ref last_key) = cursor.last_key {
			Bound::Excluded(last_key.as_slice())
		} else {
			end
		};

		// Collect entries in reverse
		let entries: Vec<RawEntry> = map
			.range::<[u8], _>((start, actual_end))
			.rev()
			.take(batch_size)
			.map(|(k, v)| RawEntry {
				key: k.clone(),
				value: v.clone(),
			})
			.collect();

		// Update cursor
		if let Some(last_entry) = entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
			cursor.exhausted = entries.len() < batch_size;
		} else {
			cursor.exhausted = true;
		}

		Ok(RangeBatch {
			entries,
			has_more: !cursor.exhausted,
		})
	}

	#[instrument(name = "store::single::memory::ensure_table", level = "trace", skip(self))]
	fn ensure_table(&self) -> Result<()> {
		// No-op for memory storage
		Ok(())
	}

	#[instrument(name = "store::single::memory::clear_table", level = "debug", skip(self))]
	fn clear_table(&self) -> Result<()> {
		let mut map = self.inner.data.write();
		map.clear();
		Ok(())
	}
}

impl TierBackend for MemoryPrimitiveStorage {}
