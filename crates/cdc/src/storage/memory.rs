// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, Bound},
	sync::Arc,
};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_runtime::sync::rwlock::RwLock;

use super::{CdcStorage, CdcStorageResult, DropBeforeResult, DroppedCdcEntry};

#[derive(Clone)]
pub struct MemoryCdcStorage {
	inner: Arc<RwLock<BTreeMap<CommitVersion, Cdc>>>,
}

impl MemoryCdcStorage {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

	pub fn with_entries(entries: impl IntoIterator<Item = Cdc>) -> Self {
		let map: BTreeMap<CommitVersion, Cdc> = entries.into_iter().map(|cdc| (cdc.version, cdc)).collect();
		Self {
			inner: Arc::new(RwLock::new(map)),
		}
	}

	pub fn len(&self) -> usize {
		self.inner.read().len()
	}

	pub fn is_empty(&self) -> bool {
		self.inner.read().is_empty()
	}

	pub fn clear(&self) {
		self.inner.write().clear();
	}
}

impl Default for MemoryCdcStorage {
	fn default() -> Self {
		Self::new()
	}
}

impl CdcStorage for MemoryCdcStorage {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		self.inner.write().insert(cdc.version, cdc.clone());
		Ok(())
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		Ok(self.inner.read().get(&version).cloned())
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let guard = self.inner.read();
		let batch_size = batch_size as usize;

		let range_iter = guard.range((start, end));
		let mut items: Vec<Cdc> = Vec::with_capacity(batch_size.min(64));
		let mut count = 0;

		for (_, cdc) in range_iter {
			if count >= batch_size {
				// We've hit the batch limit, there are more items
				return Ok(CdcBatch {
					items,
					has_more: true,
				});
			}
			items.push(cdc.clone());
			count += 1;
		}

		Ok(CdcBatch {
			items,
			has_more: false,
		})
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.inner.read().get(&version).map(|cdc| cdc.system_changes.len()).unwrap_or(0))
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		Ok(self.inner.read().keys().next().copied())
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		Ok(self.inner.read().keys().next_back().copied())
	}

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		let mut guard = self.inner.write();
		let keys_to_remove: Vec<_> = guard.range(..version).map(|(k, _)| *k).collect();
		let count = keys_to_remove.len();

		let mut entries = Vec::new();
		for key in &keys_to_remove {
			if let Some(cdc) = guard.get(key) {
				for sys_change in &cdc.system_changes {
					entries.push(DroppedCdcEntry {
						key: sys_change.key().clone(),
						value_bytes: sys_change.value_bytes() as u64,
					});
				}
			}
		}

		for key in keys_to_remove {
			guard.remove(&key);
		}

		Ok(DropBeforeResult {
			count,
			entries,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		encoded::{encoded::EncodedValues, key::EncodedKey},
		interface::cdc::SystemChange,
	};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;

	fn make_cdc(version: u64) -> Cdc {
		Cdc::new(
			CommitVersion(version),
			12345,
			Vec::new(),
			vec![SystemChange::Insert {
				key: EncodedKey::new(vec![1, 2, 3]),
				post: EncodedValues(CowVec::new(vec![])),
			}],
		)
	}

	#[test]
	fn test_clone_shares_storage() {
		let storage1 = MemoryCdcStorage::new();
		let storage2 = storage1.clone();

		storage1.write(&make_cdc(1)).unwrap();

		// Both should see the same data
		assert!(storage1.read(CommitVersion(1)).unwrap().is_some());
		assert!(storage2.read(CommitVersion(1)).unwrap().is_some());
	}

	#[test]
	fn test_concurrent_access() {
		use std::thread;

		let storage = MemoryCdcStorage::new();
		let mut handles = vec![];

		// Spawn multiple writers
		for i in 0..10 {
			let s = storage.clone();
			handles.push(thread::spawn(move || {
				s.write(&make_cdc(i)).unwrap();
			}));
		}

		for h in handles {
			h.join().unwrap();
		}

		// All entries should be present
		assert_eq!(storage.len(), 10);
	}

	#[test]
	fn test_range_exclusive_bounds() {
		let storage = MemoryCdcStorage::new();

		for v in 1..=5 {
			storage.write(&make_cdc(v)).unwrap();
		}

		// Exclusive start
		let batch = storage
			.read_range(Bound::Excluded(CommitVersion(2)), Bound::Included(CommitVersion(4)), 100)
			.unwrap();
		assert_eq!(batch.items.len(), 2); // 3, 4
		assert_eq!(batch.items[0].version, CommitVersion(3));
		assert_eq!(batch.items[1].version, CommitVersion(4));

		// Exclusive end
		let batch = storage
			.read_range(Bound::Included(CommitVersion(2)), Bound::Excluded(CommitVersion(4)), 100)
			.unwrap();
		assert_eq!(batch.items.len(), 2); // 2, 3
		assert_eq!(batch.items[0].version, CommitVersion(2));
		assert_eq!(batch.items[1].version, CommitVersion(3));
	}

	#[test]
	fn test_overwrite_entry() {
		let storage = MemoryCdcStorage::new();

		let cdc1 = Cdc::new(
			CommitVersion(1),
			100,
			Vec::new(),
			vec![SystemChange::Insert {
				key: EncodedKey::new(vec![1]),
				post: EncodedValues(CowVec::new(vec![])),
			}],
		);

		let cdc2 = Cdc::new(
			CommitVersion(1),
			200, // Different timestamp
			Vec::new(),
			vec![
				SystemChange::Insert {
					key: EncodedKey::new(vec![2]),
					post: EncodedValues(CowVec::new(vec![])),
				},
				SystemChange::Insert {
					key: EncodedKey::new(vec![3]),
					post: EncodedValues(CowVec::new(vec![])),
				},
			],
		);

		storage.write(&cdc1).unwrap();
		assert_eq!(storage.count(CommitVersion(1)).unwrap(), 1);

		storage.write(&cdc2).unwrap();
		assert_eq!(storage.count(CommitVersion(1)).unwrap(), 2);

		let read = storage.read(CommitVersion(1)).unwrap().unwrap();
		assert_eq!(read.timestamp, 200);
	}
}
